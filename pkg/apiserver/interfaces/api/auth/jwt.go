package auth

import (
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// JWTAuthenticator validates JWT and builds principals.
type JWTAuthenticator struct {
	now func() time.Time
}

// NewJWTAuthenticator creates a JWTAuthenticator.
func NewJWTAuthenticator(nowFn func() time.Time) *JWTAuthenticator {
	if nowFn == nil {
		nowFn = time.Now
	}
	return &JWTAuthenticator{now: nowFn}
}

type jwtHeader struct {
	Alg string `json:"alg"`
	KID string `json:"kid,omitempty"`
	Typ string `json:"typ,omitempty"`
}

// Authenticate verifies JWT and returns principal information.
func (a *JWTAuthenticator) Authenticate(_ context.Context, token string, setting *spec.APIAuthSettingSpec) (*Principal, error) {
	if setting == nil || !setting.Enabled {
		return nil, ErrUnauthorized
	}

	token = strings.TrimSpace(token)
	if token == "" {
		return nil, ErrUnauthorized
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ErrUnauthorized
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrUnauthorized
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrUnauthorized
	}
	signatureBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, ErrUnauthorized
	}

	var header jwtHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, ErrUnauthorized
	}
	header.Alg = strings.ToUpper(strings.TrimSpace(header.Alg))
	header.KID = strings.TrimSpace(header.KID)
	if header.Alg == "" {
		return nil, ErrUnauthorized
	}

	algorithmSet := make(map[string]struct{}, len(setting.JWT.Algorithms))
	for _, alg := range setting.JWT.Algorithms {
		algorithmSet[strings.ToUpper(strings.TrimSpace(alg))] = struct{}{}
	}
	if _, ok := algorithmSet[header.Alg]; !ok {
		return nil, ErrUnauthorized
	}

	signingInput := []byte(parts[0] + "." + parts[1])
	switch header.Alg {
	case spec.APIAuthAlgorithmHS256:
		if !verifyHS256(signingInput, signatureBytes, setting.JWT.HS256.Secret) {
			return nil, ErrUnauthorized
		}
	case spec.APIAuthAlgorithmRS256:
		if err := verifyRS256(signingInput, signatureBytes, header.KID, setting.JWT.RS256.PublicKeys); err != nil {
			return nil, ErrUnauthorized
		}
	default:
		return nil, ErrUnauthorized
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, ErrUnauthorized
	}

	if err := validateStandardClaims(claims, setting.JWT, a.now()); err != nil {
		return nil, ErrUnauthorized
	}

	roles := extractRoles(claims)
	subject := extractStringClaim(claims, "sub")
	return &Principal{
		Subject: subject,
		Roles:   roles,
		Claims:  claims,
	}, nil
}

func verifyHS256(signingInput, signature []byte, secret string) bool {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(signingInput)
	expected := mac.Sum(nil)
	return hmac.Equal(signature, expected)
}

func verifyRS256(signingInput, signature []byte, kid string, keys []spec.APIAuthRS256PublicKeySpec) error {
	if len(keys) == 0 {
		return fmt.Errorf("no rs256 public keys configured")
	}

	candidates := make([]spec.APIAuthRS256PublicKeySpec, 0, len(keys))
	if kid != "" {
		for _, key := range keys {
			if strings.TrimSpace(key.KID) == kid {
				candidates = append(candidates, key)
			}
		}
		for _, key := range keys {
			if strings.TrimSpace(key.KID) == "" {
				candidates = append(candidates, key)
			}
		}
	} else {
		candidates = append(candidates, keys...)
	}

	if len(candidates) == 0 {
		return fmt.Errorf("no candidate key for kid %q", kid)
	}

	hasher := sha256.New()
	_, _ = hasher.Write(signingInput)
	digest := hasher.Sum(nil)

	var lastErr error
	for _, key := range candidates {
		pub, err := parseRSAPublicKey(key.PEM)
		if err != nil {
			lastErr = err
			continue
		}
		if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest, signature); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("rs256 signature verification failed")
}

func parseRSAPublicKey(pemData string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(strings.TrimSpace(pemData)))
	if block == nil {
		return nil, fmt.Errorf("invalid pem public key")
	}

	if pub, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		if rsaPub, ok := pub.(*rsa.PublicKey); ok {
			return rsaPub, nil
		}
		return nil, fmt.Errorf("public key is not rsa")
	}

	if pub, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return pub, nil
	}

	if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
		if rsaPub, ok := cert.PublicKey.(*rsa.PublicKey); ok {
			return rsaPub, nil
		}
		return nil, fmt.Errorf("certificate public key is not rsa")
	}

	return nil, fmt.Errorf("unsupported rsa public key format")
}

func validateStandardClaims(claims map[string]interface{}, cfg spec.APIAuthJWTSpec, now time.Time) error {
	skew := time.Duration(cfg.ClockSkewSeconds) * time.Second
	if skew < 0 {
		skew = 0
	}

	if expValue, ok := claims["exp"]; ok {
		exp, valid := parseNumericDate(expValue)
		if !valid {
			return fmt.Errorf("invalid exp")
		}
		if now.After(exp.Add(skew)) {
			return fmt.Errorf("token expired")
		}
	}

	if nbfValue, ok := claims["nbf"]; ok {
		nbf, valid := parseNumericDate(nbfValue)
		if !valid {
			return fmt.Errorf("invalid nbf")
		}
		if now.Add(skew).Before(nbf) {
			return fmt.Errorf("token not active")
		}
	}

	if iatValue, ok := claims["iat"]; ok {
		iat, valid := parseNumericDate(iatValue)
		if !valid {
			return fmt.Errorf("invalid iat")
		}
		if now.Add(skew).Before(iat) {
			return fmt.Errorf("token issued in future")
		}
	}

	if len(cfg.Issuers) > 0 {
		issValue, ok := claims["iss"]
		if !ok {
			return fmt.Errorf("missing issuer")
		}
		iss, ok := issValue.(string)
		if !ok || iss == "" {
			return fmt.Errorf("missing issuer")
		}
		if !containsString(cfg.Issuers, iss) {
			return fmt.Errorf("issuer not allowed")
		}
	}

	if len(cfg.Audience) > 0 {
		aud, ok := parseAudienceClaim(claims["aud"])
		if !ok || len(aud) == 0 {
			return fmt.Errorf("missing audience")
		}
		if !hasAnyMatch(cfg.Audience, aud) {
			return fmt.Errorf("audience not allowed")
		}
	}

	return nil
}

func parseNumericDate(value interface{}) (time.Time, bool) {
	switch v := value.(type) {
	case float64:
		sec := int64(v)
		return time.Unix(sec, 0), true
	case float32:
		sec := int64(v)
		return time.Unix(sec, 0), true
	case int64:
		return time.Unix(v, 0), true
	case int32:
		return time.Unix(int64(v), 0), true
	case int:
		return time.Unix(int64(v), 0), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return time.Unix(i, 0), true
		}
		if f, err := v.Float64(); err == nil {
			return time.Unix(int64(f), 0), true
		}
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return time.Unix(i, 0), true
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return time.Unix(int64(f), 0), true
		}
	}
	return time.Time{}, false
}

func parseAudienceClaim(value interface{}) ([]string, bool) {
	switch v := value.(type) {
	case string:
		if v == "" {
			return nil, false
		}
		return []string{v}, true
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			str, ok := item.(string)
			if !ok {
				continue
			}
			if str == "" {
				continue
			}
			out = append(out, str)
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	default:
		return nil, false
	}
}

func extractRoles(claims map[string]interface{}) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, 2)

	if rawRoles, ok := claims["roles"]; ok {
		switch v := rawRoles.(type) {
		case []interface{}:
			for _, item := range v {
				role, ok := item.(string)
				if !ok {
					continue
				}
				role = strings.TrimSpace(role)
				if role == "" {
					continue
				}
				key := strings.ToLower(role)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				out = append(out, role)
			}
		case string:
			role := strings.TrimSpace(v)
			if role != "" {
				key := strings.ToLower(role)
				seen[key] = struct{}{}
				out = append(out, role)
			}
		}
	}

	if len(out) > 0 {
		return out
	}

	if rawRole, ok := claims["role"]; ok {
		if role, ok := rawRole.(string); ok {
			role = strings.TrimSpace(role)
			if role != "" {
				return []string{role}
			}
		}
	}
	return out
}

func extractStringClaim(claims map[string]interface{}, key string) string {
	value, ok := claims[key]
	if !ok {
		return ""
	}
	str, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(str)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func hasAnyMatch(left, right []string) bool {
	lookup := make(map[string]struct{}, len(left))
	for _, item := range left {
		lookup[item] = struct{}{}
	}
	for _, item := range right {
		if _, ok := lookup[item]; ok {
			return true
		}
	}
	return false
}

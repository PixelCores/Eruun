package spec

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
)

var supportedHTTPMethods = map[string]struct{}{
	"GET":     {},
	"POST":    {},
	"PUT":     {},
	"PATCH":   {},
	"DELETE":  {},
	"HEAD":    {},
	"OPTIONS": {},
}

// NormalizeAPIAuthSetting trims and canonicalizes API auth settings.
func NormalizeAPIAuthSetting(setting APIAuthSettingSpec) APIAuthSettingSpec {
	setting.JWT.Issuers = normalizeStringList(setting.JWT.Issuers, false)
	setting.JWT.Audience = normalizeStringList(setting.JWT.Audience, false)
	setting.JWT.Algorithms = normalizeStringList(setting.JWT.Algorithms, true)
	setting.JWT.HS256.Secret = strings.TrimSpace(setting.JWT.HS256.Secret)

	for i := range setting.JWT.RS256.PublicKeys {
		setting.JWT.RS256.PublicKeys[i].KID = strings.TrimSpace(setting.JWT.RS256.PublicKeys[i].KID)
		setting.JWT.RS256.PublicKeys[i].PEM = strings.TrimSpace(setting.JWT.RS256.PublicKeys[i].PEM)
	}

	setting.Authorization.DefaultEffect = strings.ToLower(strings.TrimSpace(setting.Authorization.DefaultEffect))
	if setting.Authorization.DefaultEffect == "" {
		setting.Authorization.DefaultEffect = APIAuthDefaultEffectDeny
	}

	for i := range setting.Authorization.Routes {
		setting.Authorization.Routes[i].Method = strings.ToUpper(strings.TrimSpace(setting.Authorization.Routes[i].Method))
		setting.Authorization.Routes[i].Path = strings.TrimSpace(setting.Authorization.Routes[i].Path)
		setting.Authorization.Routes[i].Roles = normalizeStringList(setting.Authorization.Routes[i].Roles, false)
	}

	return setting
}

// ValidateAPIAuthSetting verifies the API auth setting schema and constraints.
func ValidateAPIAuthSetting(setting APIAuthSettingSpec) error {
	setting = NormalizeAPIAuthSetting(setting)
	if !setting.Enabled {
		return nil
	}

	if setting.JWT.ClockSkewSeconds < 0 {
		return fmt.Errorf("jwt clockSkewSeconds must be >= 0")
	}

	if len(setting.JWT.Algorithms) == 0 {
		return fmt.Errorf("jwt algorithms are required when apiAuth is enabled")
	}

	algorithmSet := make(map[string]struct{}, len(setting.JWT.Algorithms))
	for _, alg := range setting.JWT.Algorithms {
		switch alg {
		case APIAuthAlgorithmHS256, APIAuthAlgorithmRS256:
			algorithmSet[alg] = struct{}{}
		default:
			return fmt.Errorf("unsupported jwt algorithm: %s", alg)
		}
	}

	if _, ok := algorithmSet[APIAuthAlgorithmHS256]; ok {
		if setting.JWT.HS256.Secret == "" {
			return fmt.Errorf("jwt hs256.secret is required when HS256 is enabled")
		}
		if setting.JWT.HS256.Secret == APIAuthSecretMaskedValue {
			return fmt.Errorf("jwt hs256.secret cannot use redacted placeholder")
		}
	}

	if _, ok := algorithmSet[APIAuthAlgorithmRS256]; ok {
		keys := setting.JWT.RS256.PublicKeys
		if len(keys) == 0 {
			return fmt.Errorf("jwt rs256.publicKeys is required when RS256 is enabled")
		}
		kidSet := make(map[string]struct{}, len(keys))
		for i, key := range keys {
			if key.PEM == "" {
				return fmt.Errorf("jwt rs256.publicKeys[%d].pem is required", i)
			}
			if _, err := parseRSAPublicKeyPEM(key.PEM); err != nil {
				return fmt.Errorf("jwt rs256.publicKeys[%d].pem is invalid: %w", i, err)
			}
			if key.KID == "" {
				continue
			}
			if _, exists := kidSet[key.KID]; exists {
				return fmt.Errorf("jwt rs256.publicKeys contains duplicate kid: %s", key.KID)
			}
			kidSet[key.KID] = struct{}{}
		}
	}

	switch setting.Authorization.DefaultEffect {
	case APIAuthDefaultEffectDeny, APIAuthDefaultEffectAllow:
	default:
		return fmt.Errorf("authorization defaultEffect must be %q or %q", APIAuthDefaultEffectDeny, APIAuthDefaultEffectAllow)
	}

	if len(setting.Authorization.Routes) == 0 {
		return fmt.Errorf("authorization routes are required when apiAuth is enabled")
	}

	for i, route := range setting.Authorization.Routes {
		if _, ok := supportedHTTPMethods[route.Method]; !ok {
			return fmt.Errorf("authorization routes[%d].method is invalid: %s", i, route.Method)
		}
		if route.Path == "" || !strings.HasPrefix(route.Path, "/") {
			return fmt.Errorf("authorization routes[%d].path is invalid", i)
		}
		if len(route.Roles) == 0 {
			return fmt.Errorf("authorization routes[%d].roles is required", i)
		}
		for j, role := range route.Roles {
			if role == "" {
				return fmt.Errorf("authorization routes[%d].roles[%d] is empty", i, j)
			}
		}
	}

	return nil
}

func normalizeStringList(values []string, upper bool) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		item := strings.TrimSpace(value)
		if item == "" {
			continue
		}
		if upper {
			item = strings.ToUpper(item)
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func parseRSAPublicKeyPEM(pemData string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(strings.TrimSpace(pemData)))
	if block == nil {
		return nil, fmt.Errorf("invalid pem")
	}

	if pub, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		rsaPub, ok := pub.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("public key is not rsa")
		}
		return rsaPub, nil
	}

	if pub, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return pub, nil
	}

	if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
		rsaPub, ok := cert.PublicKey.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("certificate public key is not rsa")
		}
		return rsaPub, nil
	}

	return nil, fmt.Errorf("unsupported rsa public key format")
}

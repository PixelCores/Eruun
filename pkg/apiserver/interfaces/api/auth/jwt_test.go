package auth

import (
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestJWTAuthenticator_HS256(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	authenticator := NewJWTAuthenticator(func() time.Time { return now })

	setting := &spec.APIAuthSettingSpec{
		Enabled: true,
		JWT: spec.APIAuthJWTSpec{
			Algorithms: []string{spec.APIAuthAlgorithmHS256},
			Issuers:    []string{"eruun"},
			Audience:   []string{"eruun-api"},
			HS256: spec.APIAuthHS256Spec{
				Secret: "test-secret",
			},
		},
	}

	token := signHS256Token(t, "test-secret", map[string]interface{}{
		"sub":   "user-1",
		"iss":   "eruun",
		"aud":   []string{"eruun-api"},
		"exp":   now.Add(time.Hour).Unix(),
		"roles": []string{"admin", "reader"},
	})

	principal, err := authenticator.Authenticate(context.Background(), token, setting)
	require.NoError(t, err)
	require.Equal(t, "user-1", principal.Subject)
	require.ElementsMatch(t, []string{"admin", "reader"}, principal.Roles)
}

func TestJWTAuthenticator_RS256(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	authenticator := NewJWTAuthenticator(func() time.Time { return now })

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	publicPEM := encodeRSAPublicKeyToPEM(t, &privateKey.PublicKey)

	setting := &spec.APIAuthSettingSpec{
		Enabled: true,
		JWT: spec.APIAuthJWTSpec{
			Algorithms: []string{spec.APIAuthAlgorithmRS256},
			RS256: spec.APIAuthRS256Spec{
				PublicKeys: []spec.APIAuthRS256PublicKeySpec{
					{KID: "test-key", PEM: publicPEM},
				},
			},
		},
	}

	token := signRS256Token(t, privateKey, "test-key", map[string]interface{}{
		"sub":  "user-2",
		"exp":  now.Add(time.Hour).Unix(),
		"role": "reader",
	})

	principal, err := authenticator.Authenticate(context.Background(), token, setting)
	require.NoError(t, err)
	require.Equal(t, "user-2", principal.Subject)
	require.ElementsMatch(t, []string{"reader"}, principal.Roles)
}

func TestJWTAuthenticator_InvalidOrExpired(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	authenticator := NewJWTAuthenticator(func() time.Time { return now })

	setting := &spec.APIAuthSettingSpec{
		Enabled: true,
		JWT: spec.APIAuthJWTSpec{
			Algorithms: []string{spec.APIAuthAlgorithmHS256},
			HS256: spec.APIAuthHS256Spec{
				Secret: "test-secret",
			},
		},
	}

	invalidToken := signHS256Token(t, "wrong-secret", map[string]interface{}{
		"sub": "user-1",
		"exp": now.Add(time.Hour).Unix(),
	})
	_, err := authenticator.Authenticate(context.Background(), invalidToken, setting)
	require.ErrorIs(t, err, ErrUnauthorized)

	expiredToken := signHS256Token(t, "test-secret", map[string]interface{}{
		"sub": "user-1",
		"exp": now.Add(-time.Hour).Unix(),
	})
	_, err = authenticator.Authenticate(context.Background(), expiredToken, setting)
	require.ErrorIs(t, err, ErrUnauthorized)
}

func TestJWTAuthenticator_IssuerAudienceExactMatch(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	authenticator := NewJWTAuthenticator(func() time.Time { return now })

	setting := &spec.APIAuthSettingSpec{
		Enabled: true,
		JWT: spec.APIAuthJWTSpec{
			Algorithms: []string{spec.APIAuthAlgorithmHS256},
			Issuers:    []string{"eruun"},
			Audience:   []string{"eruun-api"},
			HS256: spec.APIAuthHS256Spec{
				Secret: "test-secret",
			},
		},
	}

	tests := []struct {
		name string
		iss  string
		aud  interface{}
	}{
		{
			name: "issuer case mismatch should fail",
			iss:  "ERUUN",
			aud:  "eruun-api",
		},
		{
			name: "issuer surrounding whitespace should fail",
			iss:  " eruun ",
			aud:  "eruun-api",
		},
		{
			name: "audience case mismatch should fail",
			iss:  "eruun",
			aud:  "ERUUN-API",
		},
		{
			name: "audience surrounding whitespace should fail",
			iss:  "eruun",
			aud:  " eruun-api ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := signHS256Token(t, "test-secret", map[string]interface{}{
				"sub": "user-1",
				"iss": tt.iss,
				"aud": tt.aud,
				"exp": now.Add(time.Hour).Unix(),
			})
			_, err := authenticator.Authenticate(context.Background(), token, setting)
			require.ErrorIs(t, err, ErrUnauthorized)
		})
	}
}

func signHS256Token(t *testing.T, secret string, claims map[string]interface{}) string {
	t.Helper()
	header := map[string]interface{}{
		"alg": "HS256",
		"typ": "JWT",
	}
	signingInput := encodeJWTPart(t, header) + "." + encodeJWTPart(t, claims)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signingInput))
	signature := mac.Sum(nil)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func signRS256Token(t *testing.T, privateKey *rsa.PrivateKey, kid string, claims map[string]interface{}) string {
	t.Helper()
	header := map[string]interface{}{
		"alg": "RS256",
		"typ": "JWT",
		"kid": kid,
	}
	signingInput := encodeJWTPart(t, header) + "." + encodeJWTPart(t, claims)
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, digest[:])
	require.NoError(t, err)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func encodeJWTPart(t *testing.T, value interface{}) string {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(data)
}

func encodeRSAPublicKeyToPEM(t *testing.T, pub *rsa.PublicKey) string {
	t.Helper()
	data, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: data,
	}))
}

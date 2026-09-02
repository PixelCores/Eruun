package spec

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAPIAuthSetting_RS256PEMValidation(t *testing.T) {
	validKey := mustGenerateRSAPublicKeyPEM(t)

	valid := APIAuthSettingSpec{
		Enabled: true,
		JWT: APIAuthJWTSpec{
			Algorithms: []string{APIAuthAlgorithmRS256},
			RS256: APIAuthRS256Spec{
				PublicKeys: []APIAuthRS256PublicKeySpec{
					{KID: "key-1", PEM: validKey},
				},
			},
		},
		Authorization: APIAuthorizationSpec{
			DefaultEffect: APIAuthDefaultEffectDeny,
			Routes: []APIAuthRouteRuleSpec{
				{Method: "GET", Path: "/api/v1/applications", Roles: []string{"reader"}},
			},
		},
	}
	require.NoError(t, ValidateAPIAuthSetting(valid))

	invalid := valid
	invalid.JWT.RS256.PublicKeys[0].PEM = "not-a-pem"
	err := ValidateAPIAuthSetting(invalid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pem is invalid")
}

func TestValidateAPIAuthSetting_RejectMaskedHS256Secret(t *testing.T) {
	setting := APIAuthSettingSpec{
		Enabled: true,
		JWT: APIAuthJWTSpec{
			Algorithms: []string{APIAuthAlgorithmHS256},
			HS256: APIAuthHS256Spec{
				Secret: APIAuthSecretMaskedValue,
			},
		},
		Authorization: APIAuthorizationSpec{
			DefaultEffect: APIAuthDefaultEffectDeny,
			Routes: []APIAuthRouteRuleSpec{
				{Method: "GET", Path: "/api/v1/applications", Roles: []string{"reader"}},
			},
		},
	}

	err := ValidateAPIAuthSetting(setting)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot use redacted placeholder")
}

func mustGenerateRSAPublicKeyPEM(t *testing.T) string {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	}))
}

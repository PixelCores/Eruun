package spec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateOAuthAuthSetting(t *testing.T) {
	valid := OAuthAuthSettingSpec{
		Enabled: true,
		Providers: OAuthProvidersSpec{
			Google: OAuthGoogleProviderSpec{
				ClientID:     "google-client-id",
				ClientSecret: "google-client-secret",
				RedirectURI:  "https://eruun.example.com/api/v1/auth/oauth2/google/callback",
			},
		},
		JWTIssue: OAuthJWTIssueSpec{
			TTLSeconds: 1800,
		},
		RoleMapping: OAuthRoleMappingSpec{
			DefaultRoles: []string{"reader"},
		},
	}
	require.NoError(t, ValidateOAuthAuthSetting(valid))
}

func TestValidateOAuthAuthSetting_RejectMaskedSecret(t *testing.T) {
	setting := OAuthAuthSettingSpec{
		Enabled: true,
		Providers: OAuthProvidersSpec{
			Google: OAuthGoogleProviderSpec{
				ClientID:     "google-client-id",
				ClientSecret: OAuthClientSecretMaskedValue,
				RedirectURI:  "https://eruun.example.com/api/v1/auth/oauth2/google/callback",
			},
		},
		JWTIssue: OAuthJWTIssueSpec{
			TTLSeconds: 1800,
		},
		RoleMapping: OAuthRoleMappingSpec{
			DefaultRoles: []string{"reader"},
		},
	}

	err := ValidateOAuthAuthSetting(setting)
	require.Error(t, err)
	require.Contains(t, err.Error(), "redacted placeholder")
}

func TestValidateOAuthAuthSetting_RejectInvalidURL(t *testing.T) {
	setting := OAuthAuthSettingSpec{
		Enabled: true,
		Providers: OAuthProvidersSpec{
			Google: OAuthGoogleProviderSpec{
				ClientID:     "google-client-id",
				ClientSecret: "google-client-secret",
				RedirectURI:  "/oauth2/callback",
			},
		},
		JWTIssue: OAuthJWTIssueSpec{
			TTLSeconds: 1800,
		},
		RoleMapping: OAuthRoleMappingSpec{
			DefaultRoles: []string{"reader"},
		},
	}

	err := ValidateOAuthAuthSetting(setting)
	require.Error(t, err)
	require.Contains(t, err.Error(), "redirectURI is invalid")
}

func TestNormalizeOAuthAuthSetting_Defaults(t *testing.T) {
	normalized := NormalizeOAuthAuthSetting(OAuthAuthSettingSpec{
		Enabled: true,
		Providers: OAuthProvidersSpec{
			Google: OAuthGoogleProviderSpec{
				ClientID:     "google-client-id",
				ClientSecret: "google-client-secret",
				RedirectURI:  "https://eruun.example.com/api/v1/auth/oauth2/google/callback",
			},
		},
	})

	require.Equal(t, OAuthDefaultJWTIssuer, normalized.JWTIssue.Issuer)
	require.Equal(t, OAuthDefaultJWTAudience, normalized.JWTIssue.Audience)
	require.Equal(t, OAuthDefaultJWTTTL, normalized.JWTIssue.TTLSeconds)
	require.Equal(t, OAuthGoogleDefaultAuthURL, normalized.Providers.Google.AuthURL)
	require.Equal(t, OAuthGoogleDefaultTokenURL, normalized.Providers.Google.TokenURL)
	require.Equal(t, OAuthGoogleDefaultUserInfoURL, normalized.Providers.Google.UserInfoURL)
	require.Equal(t, []string{"openid", "email", "profile"}, normalized.Providers.Google.Scopes)
	require.Equal(t, []string{"reader"}, normalized.RoleMapping.DefaultRoles)
	require.Equal(t, OAuthDefaultStateTTLSeconds, normalized.Security.StateTTLSeconds)
}

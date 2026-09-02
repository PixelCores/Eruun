package spec

import (
	"fmt"
	"net/mail"
	"net/url"
	"strings"
)

// NormalizeOAuthAuthSetting trims and canonicalizes oauth auth settings.
func NormalizeOAuthAuthSetting(setting OAuthAuthSettingSpec) OAuthAuthSettingSpec {
	google := &setting.Providers.Google
	google.ClientID = strings.TrimSpace(google.ClientID)
	google.ClientSecret = strings.TrimSpace(google.ClientSecret)
	google.RedirectURI = strings.TrimSpace(google.RedirectURI)
	google.AuthURL = strings.TrimSpace(google.AuthURL)
	google.TokenURL = strings.TrimSpace(google.TokenURL)
	google.UserInfoURL = strings.TrimSpace(google.UserInfoURL)
	google.Scopes = normalizeStringList(google.Scopes, false)
	if len(google.Scopes) == 0 {
		google.Scopes = append([]string(nil), oauthDefaultScopes...)
	}
	if google.AuthURL == "" {
		google.AuthURL = OAuthGoogleDefaultAuthURL
	}
	if google.TokenURL == "" {
		google.TokenURL = OAuthGoogleDefaultTokenURL
	}
	if google.UserInfoURL == "" {
		google.UserInfoURL = OAuthGoogleDefaultUserInfoURL
	}

	setting.JWTIssue.Issuer = strings.TrimSpace(setting.JWTIssue.Issuer)
	setting.JWTIssue.Audience = strings.TrimSpace(setting.JWTIssue.Audience)
	if setting.JWTIssue.Issuer == "" {
		setting.JWTIssue.Issuer = OAuthDefaultJWTIssuer
	}
	if setting.JWTIssue.Audience == "" {
		setting.JWTIssue.Audience = OAuthDefaultJWTAudience
	}
	if setting.JWTIssue.TTLSeconds == 0 {
		setting.JWTIssue.TTLSeconds = OAuthDefaultJWTTTL
	}

	setting.RoleMapping.DefaultRoles = normalizeStringList(setting.RoleMapping.DefaultRoles, false)
	if len(setting.RoleMapping.DefaultRoles) == 0 {
		setting.RoleMapping.DefaultRoles = []string{"reader"}
	}
	setting.RoleMapping.GoogleHostedDomainToRoles = normalizeRoleMapping(setting.RoleMapping.GoogleHostedDomainToRoles)
	setting.RoleMapping.GoogleEmailToRoles = normalizeRoleMapping(setting.RoleMapping.GoogleEmailToRoles)

	setting.Security.StateTTLSeconds = normalizeStateTTL(setting.Security.StateTTLSeconds)
	return setting
}

// ValidateOAuthAuthSetting validates oauthAuth setting schema and constraints.
func ValidateOAuthAuthSetting(setting OAuthAuthSettingSpec) error {
	setting = NormalizeOAuthAuthSetting(setting)
	if !setting.Enabled {
		return nil
	}

	google := setting.Providers.Google
	if google.ClientID == "" {
		return fmt.Errorf("providers.google.clientId is required when oauthAuth is enabled")
	}
	if google.ClientSecret == "" {
		return fmt.Errorf("providers.google.clientSecret is required when oauthAuth is enabled")
	}
	if google.ClientSecret == OAuthClientSecretMaskedValue {
		return fmt.Errorf("providers.google.clientSecret cannot use redacted placeholder")
	}
	if google.RedirectURI == "" {
		return fmt.Errorf("providers.google.redirectURI is required when oauthAuth is enabled")
	}
	if err := validateAbsoluteURL(google.RedirectURI); err != nil {
		return fmt.Errorf("providers.google.redirectURI is invalid: %w", err)
	}
	if err := validateAbsoluteURL(google.AuthURL); err != nil {
		return fmt.Errorf("providers.google.authURL is invalid: %w", err)
	}
	if err := validateAbsoluteURL(google.TokenURL); err != nil {
		return fmt.Errorf("providers.google.tokenURL is invalid: %w", err)
	}
	if err := validateAbsoluteURL(google.UserInfoURL); err != nil {
		return fmt.Errorf("providers.google.userInfoURL is invalid: %w", err)
	}
	if len(google.Scopes) == 0 {
		return fmt.Errorf("providers.google.scopes are required")
	}

	if setting.JWTIssue.TTLSeconds <= 0 {
		return fmt.Errorf("jwtIssue.ttlSeconds must be > 0")
	}
	if setting.JWTIssue.TTLSeconds > OAuthMaxJWTTTL {
		return fmt.Errorf("jwtIssue.ttlSeconds must be <= %d", OAuthMaxJWTTTL)
	}
	if setting.JWTIssue.Issuer == "" {
		return fmt.Errorf("jwtIssue.issuer is required")
	}
	if setting.JWTIssue.Audience == "" {
		return fmt.Errorf("jwtIssue.audience is required")
	}

	if len(setting.RoleMapping.DefaultRoles) == 0 {
		return fmt.Errorf("roleMapping.defaultRoles are required")
	}
	if err := validateRoleMapKeys(setting.RoleMapping.GoogleHostedDomainToRoles, false); err != nil {
		return err
	}
	if err := validateRoleMapKeys(setting.RoleMapping.GoogleEmailToRoles, true); err != nil {
		return err
	}

	if setting.Security.StateTTLSeconds <= 0 {
		return fmt.Errorf("security.stateTTLSeconds must be > 0")
	}
	return nil
}

func normalizeRoleMapping(input map[string][]string) map[string][]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string][]string, len(input))
	for key, roles := range input {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		if normalizedKey == "" {
			continue
		}
		normalizedRoles := normalizeStringList(roles, false)
		if len(normalizedRoles) == 0 {
			continue
		}
		out[normalizedKey] = normalizedRoles
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func validateRoleMapKeys(m map[string][]string, emailKeys bool) error {
	for key, roles := range m {
		if len(roles) == 0 {
			return fmt.Errorf("role mapping for %q must include at least one role", key)
		}
		if emailKeys {
			if _, err := mail.ParseAddress(key); err != nil {
				return fmt.Errorf("google email mapping key %q is invalid", key)
			}
			continue
		}
		if strings.Contains(key, "@") {
			return fmt.Errorf("google hosted domain mapping key %q is invalid", key)
		}
	}
	return nil
}

func validateAbsoluteURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("must be absolute url")
	}
	return nil
}

func normalizeStateTTL(ttl int64) int64 {
	if ttl == 0 {
		return OAuthDefaultStateTTLSeconds
	}
	return ttl
}

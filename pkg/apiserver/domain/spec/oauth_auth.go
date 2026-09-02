package spec

const (
	OAuthProviderGoogle = "google"

	OAuthGoogleDefaultAuthURL     = "https://accounts.google.com/o/oauth2/v2/auth"
	OAuthGoogleDefaultTokenURL    = "https://oauth2.googleapis.com/token"
	OAuthGoogleDefaultUserInfoURL = "https://openidconnect.googleapis.com/v1/userinfo"

	OAuthDefaultJWTIssuer   = "eruun"
	OAuthDefaultJWTAudience = "eruun-api"
	OAuthDefaultJWTTTL      = int64(3600)
	OAuthMaxJWTTTL          = int64(86400)

	OAuthDefaultStateTTLSeconds = int64(300)

	OAuthClientSecretMaskedValue = APIAuthSecretMaskedValue
)

var oauthDefaultScopes = []string{"openid", "email", "profile"}

// OAuthAuthSettingSpec describes OAuth2 login configuration.
type OAuthAuthSettingSpec struct {
	Enabled     bool                 `json:"enabled"`
	Providers   OAuthProvidersSpec   `json:"providers"`
	JWTIssue    OAuthJWTIssueSpec    `json:"jwtIssue"`
	RoleMapping OAuthRoleMappingSpec `json:"roleMapping"`
	Security    OAuthSecuritySpec    `json:"security,omitempty"`
}

// OAuthProvidersSpec defines upstream identity providers.
type OAuthProvidersSpec struct {
	Google OAuthGoogleProviderSpec `json:"google,omitempty"`
}

// OAuthGoogleProviderSpec defines Google OAuth2 endpoints and credentials.
type OAuthGoogleProviderSpec struct {
	ClientID     string   `json:"clientId,omitempty"`
	ClientSecret string   `json:"clientSecret,omitempty"`
	RedirectURI  string   `json:"redirectURI,omitempty"`
	Scopes       []string `json:"scopes,omitempty"`
	AuthURL      string   `json:"authURL,omitempty"`
	TokenURL     string   `json:"tokenURL,omitempty"`
	UserInfoURL  string   `json:"userInfoURL,omitempty"`
}

// OAuthJWTIssueSpec defines local JWT issue parameters after OAuth login.
type OAuthJWTIssueSpec struct {
	Issuer     string `json:"issuer,omitempty"`
	Audience   string `json:"audience,omitempty"`
	TTLSeconds int64  `json:"ttlSeconds,omitempty"`
}

// OAuthRoleMappingSpec maps external identities to local roles.
type OAuthRoleMappingSpec struct {
	DefaultRoles              []string            `json:"defaultRoles,omitempty"`
	GoogleHostedDomainToRoles map[string][]string `json:"googleHostedDomainToRoles,omitempty"`
	GoogleEmailToRoles        map[string][]string `json:"googleEmailToRoles,omitempty"`
}

// OAuthSecuritySpec defines OAuth runtime security controls.
type OAuthSecuritySpec struct {
	StateTTLSeconds int64 `json:"stateTTLSeconds,omitempty"`
}

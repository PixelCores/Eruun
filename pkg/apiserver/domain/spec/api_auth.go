package spec

const (
	APIAuthAlgorithmHS256 = "HS256"
	APIAuthAlgorithmRS256 = "RS256"

	APIAuthDefaultEffectDeny  = "deny"
	APIAuthDefaultEffectAllow = "allow"

	// APIAuthSecretMaskedValue is the redacted placeholder returned by read APIs.
	// It must not be accepted as a persisted secret in write paths.
	APIAuthSecretMaskedValue = "******"
)

// APIAuthSettingSpec describes API authentication and authorization policy.
type APIAuthSettingSpec struct {
	Enabled       bool                 `json:"enabled"`
	JWT           APIAuthJWTSpec       `json:"jwt"`
	Authorization APIAuthorizationSpec `json:"authorization"`
}

// APIAuthJWTSpec controls JWT verification.
type APIAuthJWTSpec struct {
	Issuers          []string         `json:"issuers,omitempty"`
	Audience         []string         `json:"audience,omitempty"`
	ClockSkewSeconds int64            `json:"clockSkewSeconds,omitempty"`
	Algorithms       []string         `json:"algorithms,omitempty"`
	HS256            APIAuthHS256Spec `json:"hs256,omitempty"`
	RS256            APIAuthRS256Spec `json:"rs256,omitempty"`
}

// APIAuthHS256Spec configures HS256 verification.
type APIAuthHS256Spec struct {
	Secret string `json:"secret,omitempty"`
}

// APIAuthRS256Spec configures RS256 verification.
type APIAuthRS256Spec struct {
	PublicKeys []APIAuthRS256PublicKeySpec `json:"publicKeys,omitempty"`
}

// APIAuthRS256PublicKeySpec defines one RS256 verification key.
type APIAuthRS256PublicKeySpec struct {
	KID string `json:"kid,omitempty"`
	PEM string `json:"pem,omitempty"`
}

// APIAuthorizationSpec controls route-level role checks.
type APIAuthorizationSpec struct {
	DefaultEffect string                 `json:"defaultEffect,omitempty"`
	Routes        []APIAuthRouteRuleSpec `json:"routes,omitempty"`
}

// APIAuthRouteRuleSpec maps one route to allowed roles.
type APIAuthRouteRuleSpec struct {
	Method string   `json:"method"`
	Path   string   `json:"path"`
	Roles  []string `json:"roles"`
}

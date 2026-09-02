package v1

// OAuthLoginResponse is local token payload after oauth callback success.
type OAuthLoginResponse struct {
	AccessToken string   `json:"accessToken"`
	TokenType   string   `json:"tokenType"`
	ExpiresIn   int64    `json:"expiresIn"`
	Subject     string   `json:"subject"`
	Email       string   `json:"email,omitempty"`
	Roles       []string `json:"roles"`
}

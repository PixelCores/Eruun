package v1

type AccountCodeRequest struct {
	Purpose    string `json:"purpose"`
	Provider   string `json:"provider"`
	Identifier string `json:"identifier"`
}
type AccountRegisterRequest struct {
	Provider   string `json:"provider"`
	Identifier string `json:"identifier"`
	Code       string `json:"code"`
	Password   string `json:"password"`
	Name       string `json:"name"`
}
type AccountLoginRequest struct {
	Provider   string `json:"provider"`
	Identifier string `json:"identifier"`
	Password   string `json:"password"`
	Code       string `json:"code"`
}
type AccountPasswordRequest struct {
	Password string `json:"password"`
}
type AccountResetRequest struct {
	Provider   string `json:"provider"`
	Identifier string `json:"identifier"`
	Code       string `json:"code"`
	Password   string `json:"password"`
}
type AccountIdentityRequest struct {
	Provider   string `json:"provider"`
	Identifier string `json:"identifier"`
	Code       string `json:"code"`
}
type OAuthStartRequest struct {
	Link bool `json:"link"`
}
type OAuthCallbackRequest struct {
	Code  string `json:"code"`
	State string `json:"state"`
	Error string `json:"error"`
}
type WorkspaceRequest struct {
	Name string `json:"name"`
}
type WorkspaceMemberRequest struct {
	Role string `json:"role"`
}
type WorkspaceTransferRequest struct {
	UserID string `json:"userId"`
}
type WorkspaceInviteRequest struct {
	Email string `json:"email"`
	Role  string `json:"role"`
}
type WorkspaceAcceptRequest struct {
	Token string `json:"token"`
}
type AccountStatusRequest struct {
	Disabled *bool `json:"disabled"`
}

// ApplicationSummary is the complete allowlist for viewer application listings.
type ApplicationSummary struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Namespace   string `json:"namespace"`
	WorkspaceID string `json:"workspaceID"`
	Version     string `json:"version"`
}

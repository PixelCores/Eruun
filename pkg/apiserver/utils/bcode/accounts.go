package bcode

var (
	ErrAccountInput          = NewBcode(400, 33000, "invalid account request")
	ErrAccountConflict       = NewBcode(409, 33001, "account identity or workspace conflict")
	ErrAccountLinkRequired   = NewBcode(409, 33002, "sign in to the existing account before linking")
	ErrAccountCode           = NewBcode(401, 33003, "verification code is invalid or expired")
	ErrAccountRateLimit      = NewBcode(429, 33004, "too many authentication attempts")
	ErrAccountRecentAuth     = NewBcode(403, 33005, "recent authentication is required")
	ErrAccountPasswordChange = NewBcode(403, 33006, "password change is required")
	ErrAccountDelivery       = NewBcode(502, 33007, "message delivery failed")
	ErrWorkspaceNotEmpty     = NewBcode(409, 33008, "workspace is not empty")
)

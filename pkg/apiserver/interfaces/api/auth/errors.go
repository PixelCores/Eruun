package auth

import "errors"

var (
	// ErrUnauthorized indicates authentication failure.
	ErrUnauthorized = errors.New("unauthorized")
	// ErrForbidden indicates authorization failure.
	ErrForbidden = errors.New("forbidden")
	// ErrPolicyNotFound indicates apiAuth setting does not exist.
	ErrPolicyNotFound = errors.New("api auth policy not found")
	// ErrOAuthSettingNotFound indicates oauthAuth setting does not exist.
	ErrOAuthSettingNotFound = errors.New("oauth auth setting not found")
	// ErrOAuthStateInvalid indicates OAuth state is invalid or expired.
	ErrOAuthStateInvalid = errors.New("oauth state invalid")
	// ErrOAuthCodeExchangeFailed indicates OAuth code exchange failed.
	ErrOAuthCodeExchangeFailed = errors.New("oauth code exchange failed")
	// ErrOAuthUserInfoFailed indicates OAuth user info query failed.
	ErrOAuthUserInfoFailed = errors.New("oauth user info failed")
	// ErrOAuthRoleMappingFailed indicates identity cannot be mapped to roles.
	ErrOAuthRoleMappingFailed = errors.New("oauth role mapping failed")
	// ErrOAuthTokenIssueFailed indicates local JWT issue failed.
	ErrOAuthTokenIssueFailed = errors.New("oauth token issue failed")
	// ErrOAuthConfigInvalid indicates OAuth config is invalid.
	ErrOAuthConfigInvalid = errors.New("oauth config invalid")
)

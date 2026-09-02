package bcode

// ErrOAuthProviderUnsupported oauth provider is unsupported.
var ErrOAuthProviderUnsupported = NewBcode(400, 31000, "oauth provider is unsupported")

// ErrOAuthConfigInvalid oauth config is invalid.
var ErrOAuthConfigInvalid = NewBcode(400, 31001, "oauth config is invalid")

// ErrOAuthStateInvalid oauth state is invalid or expired.
var ErrOAuthStateInvalid = NewBcode(401, 31002, "oauth state is invalid or expired")

// ErrOAuthCodeExchangeFailed oauth code exchange failed.
var ErrOAuthCodeExchangeFailed = NewBcode(401, 31003, "oauth code exchange failed")

// ErrOAuthUserInfoFetchFailed oauth user info fetch failed.
var ErrOAuthUserInfoFetchFailed = NewBcode(401, 31004, "oauth user info fetch failed")

// ErrOAuthRoleMappingFailed oauth role mapping failed.
var ErrOAuthRoleMappingFailed = NewBcode(403, 31005, "oauth role mapping failed")

// ErrOAuthTokenIssueFailed oauth token issue failed.
var ErrOAuthTokenIssueFailed = NewBcode(500, 31006, "oauth token issue failed")

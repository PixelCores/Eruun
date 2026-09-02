package bcode

// ErrSystemSettingTypeInvalid setting type is invalid
var ErrSystemSettingTypeInvalid = NewBcode(400, 30000, "system setting type is invalid")

// ErrSystemSettingValueInvalid setting value is invalid
var ErrSystemSettingValueInvalid = NewBcode(400, 30001, "system setting value is invalid")

// ErrSystemSettingExists setting already exists
var ErrSystemSettingExists = NewBcode(409, 30002, "system setting already exists")

// ErrSystemSettingNotFound setting not found
var ErrSystemSettingNotFound = NewBcode(404, 30003, "system setting not found")

// ErrSystemSettingConnectivityCheckFailed setting connectivity check failed
var ErrSystemSettingConnectivityCheckFailed = NewBcode(400, 30004, "system setting connectivity check failed")

// ErrURLSecurityPolicyUnavailable url security policy cannot be loaded.
var ErrURLSecurityPolicyUnavailable = NewBcode(503, 30005, "url security policy is unavailable")

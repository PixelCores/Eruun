package spec

import "regexp"

// APINamePattern is the shared Application name format for writes, Try and JSON Schema.
const APINamePattern = `^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`

var apiNamePattern = regexp.MustCompile(APINamePattern)

// ValidAPIName is the shared name format used by public application and
// workflow requests. maxLength is the current datastore-backed API limit.
func ValidAPIName(name string, maxLength int) bool {
	return len(name) >= 2 && len(name) <= maxLength && apiNamePattern.MatchString(name)
}

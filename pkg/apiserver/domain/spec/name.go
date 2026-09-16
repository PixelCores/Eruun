package spec

import "regexp"

var apiNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// ValidAPIName is the shared name format used by public application and
// workflow requests. maxLength is the current datastore-backed API limit.
func ValidAPIName(name string, maxLength int) bool {
	return len(name) >= 2 && len(name) <= maxLength && apiNamePattern.MatchString(name)
}

package spec

import (
	"regexp"
	"strings"
)

// APINamePattern is the shared Application name format for writes, Try and JSON Schema.
const APINamePattern = `^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`

var apiNamePattern = regexp.MustCompile(APINamePattern)

// ValidAPIName is the shared name format used by public application and
// workflow requests. maxLength is the current datastore-backed API limit.
func ValidAPIName(name string, maxLength int) bool {
	return len(name) >= 2 && len(name) <= maxLength && apiNamePattern.MatchString(name)
}

// NormalizeName is the stored form of component and workflow step names. They
// are case-insensitive DNS labels, so the write boundary trims and lower-cases
// them once and later comparisons use them as stored.
func NormalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// NormalizeServiceTraitNames gives component Service trait names their stored
// form at the same write boundary. Service names are Kubernetes resource names,
// so only surrounding spaces are dropped; validation rejects any other form.
func NormalizeServiceTraitNames(traits *Traits) {
	if traits == nil {
		return
	}
	for i := range traits.Service {
		traits.Service[i].Name = strings.TrimSpace(traits.Service[i].Name)
	}
}

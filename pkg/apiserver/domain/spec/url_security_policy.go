package spec

const (
	// URLSecurityPolicyDefaultClusterDomain allows in-cluster service discovery by default.
	URLSecurityPolicyDefaultClusterDomain = "*.svc.cluster.local"
	// URLSecurityPolicyDefaultPaaSDomain is a placeholder domain suffix for platform private ingress.
	URLSecurityPolicyDefaultPaaSDomain = "*.paas.example.com"
)

// URLSecurityPolicySpec controls outbound URL safety checks.
type URLSecurityPolicySpec struct {
	AllowPrivateByDefault bool     `json:"allowPrivateByDefault"`
	AllowedHostPatterns   []string `json:"allowedHostPatterns,omitempty"`
	AllowedCIDRs          []string `json:"allowedCIDRs,omitempty"`
}

// DefaultURLSecurityPolicy returns the default outbound URL safety policy.
func DefaultURLSecurityPolicy() URLSecurityPolicySpec {
	return URLSecurityPolicySpec{
		AllowPrivateByDefault: false,
		AllowedHostPatterns: []string{
			URLSecurityPolicyDefaultClusterDomain,
			URLSecurityPolicyDefaultPaaSDomain,
		},
	}
}

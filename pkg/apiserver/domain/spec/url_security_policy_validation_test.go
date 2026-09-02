package spec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeURLSecurityPolicy(t *testing.T) {
	setting := URLSecurityPolicySpec{
		AllowedHostPatterns: []string{
			"  *.SVC.CLUSTER.LOCAL ",
			"api.paas.example.com",
			"api.paas.example.com",
		},
		AllowedCIDRs: []string{
			"10.0.0.0/8",
			"10.0.0.0/8",
			"invalid",
		},
	}
	normalized := NormalizeURLSecurityPolicy(setting)
	require.Equal(t, []string{"*.svc.cluster.local", "api.paas.example.com"}, normalized.AllowedHostPatterns)
	require.Equal(t, []string{"10.0.0.0/8", "invalid"}, normalized.AllowedCIDRs)
}

func TestValidateURLSecurityPolicy(t *testing.T) {
	valid := URLSecurityPolicySpec{
		AllowedHostPatterns: []string{"*.svc.cluster.local", "api.paas.example.com", "127.0.0.1"},
		AllowedCIDRs:        []string{"10.0.0.0/8", "127.0.0.1/32"},
	}
	require.NoError(t, ValidateURLSecurityPolicy(valid))

	invalidWildcard := URLSecurityPolicySpec{
		AllowedHostPatterns: []string{"*.*.svc.cluster.local"},
	}
	err := ValidateURLSecurityPolicy(invalidWildcard)
	require.Error(t, err)

	invalidCIDR := URLSecurityPolicySpec{
		AllowedCIDRs: []string{"10.0.0.0"},
	}
	err = ValidateURLSecurityPolicy(invalidCIDR)
	require.Error(t, err)

	invalidHost := URLSecurityPolicySpec{
		AllowedHostPatterns: []string{"http://svc.cluster.local"},
	}
	err = ValidateURLSecurityPolicy(invalidHost)
	require.Error(t, err)
}

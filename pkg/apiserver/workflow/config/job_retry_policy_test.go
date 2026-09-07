package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestJobRetryPolicyValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy *JobRetryPolicy
		valid  bool
	}{
		{name: "absent preserves defaults", valid: true},
		{name: "explicit stop", policy: &JobRetryPolicy{OnOOM: "stop"}, valid: true},
		{name: "retry", policy: &JobRetryPolicy{OnOOM: "retry", MaxRetries: 2, BackoffSeconds: 3}, valid: true},
		{name: "unknown action", policy: &JobRetryPolicy{OnOOM: "ignore"}},
		{name: "stop rejects retries", policy: &JobRetryPolicy{OnOOM: "stop", MaxRetries: 1}},
		{name: "retry requires count", policy: &JobRetryPolicy{OnOOM: "retry", BackoffSeconds: 1}},
		{name: "retry bounded count", policy: &JobRetryPolicy{OnOOM: "retry", MaxRetries: 11, BackoffSeconds: 1}},
		{name: "retry requires backoff", policy: &JobRetryPolicy{OnOOM: "retry", MaxRetries: 1}},
		{name: "retry bounded backoff", policy: &JobRetryPolicy{OnOOM: "retry", MaxRetries: 1, BackoffSeconds: 3601}},
		{name: "retry rejects implicit resize", policy: &JobRetryPolicy{OnOOM: "retry", MaxRetries: 1, BackoffSeconds: 1, MemoryGrowthFactor: 2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.valid {
				require.NoError(t, tt.policy.Validate())
			} else {
				require.Error(t, tt.policy.Validate())
			}
		})
	}
	valid := JobRetryPolicy{OnOOM: "resize", MaxRetries: 2, BackoffSeconds: 1, MemoryGrowthFactor: 2,
		MaxResources: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")}}
	require.NoError(t, valid.Validate())
	for _, tt := range []struct {
		name   string
		change func(*JobRetryPolicy)
	}{
		{"missing memory cap", func(p *JobRetryPolicy) { delete(p.MaxResources, corev1.ResourceMemory) }},
		{"negative cap", func(p *JobRetryPolicy) { p.MaxResources[corev1.ResourceMemory] = resource.MustParse("-1Gi") }},
		{"unsupported resource", func(p *JobRetryPolicy) { p.MaxResources[corev1.ResourceStorage] = resource.MustParse("1Gi") }},
		{"missing CPU cap", func(p *JobRetryPolicy) { p.CPUGrowthFactor = 2 }},
		{"excessive growth", func(p *JobRetryPolicy) { p.MemoryGrowthFactor = 5 }},
		{"negative CPU growth", func(p *JobRetryPolicy) { p.CPUGrowthFactor = -1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := valid
			policy.MaxResources = valid.MaxResources.DeepCopy()
			tt.change(&policy)
			require.Error(t, policy.Validate())
		})
	}
}

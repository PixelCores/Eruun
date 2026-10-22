package application

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestJobRetryPolicyValidation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		kind      config.JobType
		startTime int64
		onOOM     string
		valid     bool
	}{
		{"instant", config.InstantJob, 0, "stop", true},
		{"scheduled", config.ScheduledJob, 0, "stop", false},
		{"webservice", config.ServerJob, 0, "stop", false},
		{"delayed", config.InstantJob, 1, "stop", false},
		{"bad action", config.InstantJob, 0, "invalid", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			props := apisv1.Properties{StartTime: tt.startTime, JobRetryPolicy: &workflowconfig.JobRetryPolicy{OnOOM: tt.onOOM}}
			errors := validateJobPolicies(tt.kind, props, "component[0].properties")
			if tt.valid {
				require.Empty(t, errors)
				return
			}
			require.Len(t, errors, 1)
			require.Equal(t, "component[0].properties.jobRetryPolicy", errors[0].Field)
			require.Equal(t, apisv1.ErrCodeInvalidJobFailurePolicy, errors[0].Code)
		})
	}
	_, err := normalizeVersionUpdateJobFailurePolicies([]apisv1.ComponentUpdateSpec{{Name: "job", Action: "add", ComponentType: config.ScheduledJob,
		Properties: &apisv1.Properties{JobRetryPolicy: &workflowconfig.JobRetryPolicy{OnOOM: "stop"}}}}, nil)
	require.ErrorContains(t, err, "jobRetryPolicy")
	errors := ValidateTemplateOverrides([]apisv1.CreateComponentRequest{{Template: &spec.TemplateRef{ID: "tmpl"},
		Traits: apisv1.Traits{Init: []spec.InitTraitSpec{{Properties: apisv1.Properties{JobRetryPolicy: &workflowconfig.JobRetryPolicy{OnOOM: "stop"}}}}}}})
	require.Len(t, errors, 1)
	require.Equal(t, "component[0].traits.init[0].properties.jobRetryPolicy", errors[0].Field)
}

func TestJobRetryPolicyTemplateOverride(t *testing.T) {
	props := apisv1.Properties{JobRetryPolicy: &workflowconfig.JobRetryPolicy{OnOOM: "stop"}}
	applyPropertyOverrides(&props, apisv1.Properties{}, config.InstantJob)
	require.Equal(t, "stop", props.JobRetryPolicy.OnOOM)
	policy := &workflowconfig.JobRetryPolicy{OnOOM: "resize", MaxRetries: 1, BackoffSeconds: 1, MemoryGrowthFactor: 2,
		MaxResources: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}
	applyPropertyOverrides(&props, apisv1.Properties{JobRetryPolicy: policy}, config.InstantJob)
	require.Equal(t, policy, props.JobRetryPolicy)
	policy.MaxResources[corev1.ResourceMemory] = resource.MustParse("2Gi")
	require.Equal(t, "1Gi", props.JobRetryPolicy.MaxResources.Memory().String())
}

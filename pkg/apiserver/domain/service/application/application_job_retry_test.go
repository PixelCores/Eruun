package application

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestJobRetryPolicyWriteValidation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		kind      config.JobType
		startTime int64
		onOOM     string
		valid     bool
	}{
		{"instant", config.InstantJob, 0, "stop", true},
		{"delayed", config.InstantJob, 1, "stop", false},
		{"bad action", config.InstantJob, 0, "invalid", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			props := apisv1.Properties{StartTime: tt.startTime, JobRetryPolicy: &workflowconfig.JobRetryPolicy{OnOOM: tt.onOOM}}
			err := normalizeJobFailurePolicyForWrite(tt.kind, &props, "component[0].properties.failurePolicy")
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "jobRetryPolicy")
			}
		})
	}
	_, err := normalizeVersionUpdateJobFailurePolicies([]apisv1.ComponentUpdateSpec{{Name: "job", Action: "add", ComponentType: config.ServerJob,
		Properties: &apisv1.Properties{JobRetryPolicy: &workflowconfig.JobRetryPolicy{OnOOM: "stop"}}}}, nil)
	require.ErrorContains(t, err, "jobRetryPolicy")
	err = validateNoNestedJobFailurePoliciesForWrite(apisv1.Traits{Init: []spec.InitTraitSpec{{Properties: apisv1.Properties{JobRetryPolicy: &workflowconfig.JobRetryPolicy{OnOOM: "stop"}}}}}, "component[0].traits")
	require.ErrorContains(t, err, "properties.jobRetryPolicy")
}

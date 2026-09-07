package validation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestValidateJobRetryPolicyScope(t *testing.T) {
	validator := &validationServiceImpl{}
	for _, tt := range []struct {
		name      string
		kind      config.JobType
		startTime int64
		onOOM     string
		valid     bool
	}{
		{"instant stop", config.InstantJob, 0, "stop", true},
		{"delayed instant", config.InstantJob, 1, "stop", false},
		{"cron", config.ScheduledJob, 0, "stop", false},
		{"webservice", config.ServerJob, 0, "stop", false},
		{"cloud", config.CloudJob, 0, "stop", false},
		{"bad action", config.InstantJob, 0, "unknown", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			component := apisv1.CreateComponentRequest{ComponentType: tt.kind,
				Properties: apisv1.Properties{StartTime: tt.startTime, JobRetryPolicy: &workflowconfig.JobRetryPolicy{OnOOM: tt.onOOM}}}
			errors := validator.validateJobProperties(component, "component[0]")
			if tt.valid {
				require.Empty(t, errors)
			} else {
				requireValidationError(t, errors, "component[0].properties.jobRetryPolicy", apisv1.ErrCodeInvalidJobFailurePolicy)
			}
		})
	}
}

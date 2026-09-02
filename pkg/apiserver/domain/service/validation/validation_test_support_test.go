package validation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func validCallbackTryApplicationRequest() apisv1.CreateApplicationsRequest {
	return apisv1.CreateApplicationsRequest{
		Name:      "callback-app",
		Namespace: "default",
		Version:   "1.0.0",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Namespace:     "default",
				Replicas:      1,
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "deploy-backend",
				WorkflowType: config.JobDeploy,
				Mode:         "StepByStep",
				Components:   []string{"backend"},
			},
		},
	}
}

func requireValidationError(t *testing.T, errors []apisv1.ValidationError, field, code string) {
	t.Helper()
	for _, err := range errors {
		if err.Field == field && err.Code == code {
			return
		}
	}
	require.Failf(t, "missing validation error", "expected field=%q code=%q in %+v", field, code, errors)
}

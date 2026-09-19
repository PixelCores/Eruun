package validation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestTryApplicationEvaluationMatchesWriteContract(t *testing.T) {
	request := apisv1.CreateApplicationsRequest{Name: "evaluation-app", Namespace: "space",
		Components: []apisv1.CreateComponentRequest{{Name: "evaluate", ComponentType: config.InstantJob,
			Traits: spec.Traits{Evaluation: &spec.EvaluationTraitSpec{Env: "ack", Agent: "oracle", TaskPackageID: "12345678-1234-1234-1234-123456789012"}}}},
		Workflow: []apisv1.CreateWorkflowStepRequest{{Name: "evaluate", WorkflowType: config.JobDeploy, Components: []string{"evaluate"}}},
	}
	service := &validationServiceImpl{}
	response := service.TryApplication(context.Background(), request)
	require.True(t, response.Valid, "%+v", response.Errors)
	require.NotNil(t, response.NormalizedSpec.Components[0].Traits.Resources)
	require.Equal(t, 1, response.NormalizedSpec.Components[0].Traits.Evaluation.Attempts)

	request.Components[0].Properties.Command = []string{"true"}
	response = service.TryApplication(context.Background(), request)
	require.False(t, response.Valid)
	require.Contains(t, response.Errors[0].Message, "evaluation owns")

	request.Components[0].Properties.Command = nil
	request.Components[0].ComponentType = config.ServerJob
	request.Components[0].Image = "nginx:1"
	response = service.TryApplication(context.Background(), request)
	require.False(t, response.Valid)
	var found bool
	for _, err := range response.Errors {
		if err.Message == "evaluation is only supported on a top-level job component" {
			found = true
		}
	}
	require.True(t, found)
}

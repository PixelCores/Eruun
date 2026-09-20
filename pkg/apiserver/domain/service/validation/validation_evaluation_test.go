package validation

import (
	"context"
	"strings"
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

func TestTryApplicationEvaluationErrorsPointToEvalTrait(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*apisv1.CreateComponentRequest)
		field     string
		path      string
	}{
		{
			name: "top-level evaluation",
			configure: func(component *apisv1.CreateComponentRequest) {
				component.Traits.Evaluation.Env = "unknown"
			},
			field: "component[0].traits.eval",
			path:  "/components/0/traits/eval",
		},
		{
			name: "init evaluation",
			configure: func(component *apisv1.CreateComponentRequest) {
				evaluation := component.Traits.Evaluation
				component.ComponentType, component.Image = config.ServerJob, "nginx:1"
				component.Traits = spec.Traits{Init: []spec.InitTraitSpec{{Name: "prepare", Image: "busybox:1", Traits: spec.Traits{Evaluation: evaluation}}}}
			},
			field: "component[0].traits.init[0].traits.eval",
			path:  "/components/0/traits/init/0/traits/eval",
		},
		{
			name: "sidecar evaluation",
			configure: func(component *apisv1.CreateComponentRequest) {
				evaluation := component.Traits.Evaluation
				component.ComponentType, component.Image = config.ServerJob, "nginx:1"
				component.Traits = spec.Traits{Sidecar: []spec.SidecarTraitsSpec{{Name: "helper", Image: "busybox:1", Traits: spec.Traits{Evaluation: evaluation}}}}
			},
			field: "component[0].traits.sidecar[0].traits.eval",
			path:  "/components/0/traits/sidecar/0/traits/eval",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			component := apisv1.CreateComponentRequest{Name: "evaluate", ComponentType: config.InstantJob,
				Traits: spec.Traits{Evaluation: &spec.EvaluationTraitSpec{Env: "ack", Agent: "oracle", TaskPackageID: "12345678-1234-1234-1234-123456789012"}}}
			tc.configure(&component)
			request := apisv1.CreateApplicationsRequest{Name: "evaluation-app", Namespace: "space",
				Components: []apisv1.CreateComponentRequest{component},
				Workflow:   []apisv1.CreateWorkflowStepRequest{{Name: "evaluate", WorkflowType: config.JobDeploy, Components: []string{"evaluate"}}},
			}
			response := (&validationServiceImpl{}).TryApplication(context.Background(), request)
			require.False(t, response.Valid)
			var evalErrors []apisv1.TryValidationError
			for _, validationErr := range response.Errors {
				require.NotContains(t, validationErr.Field, ".evaluation")
				require.NotContains(t, validationErr.Path, "/evaluation")
				if strings.HasSuffix(validationErr.Field, ".eval") {
					evalErrors = append(evalErrors, validationErr)
				}
			}
			require.Len(t, evalErrors, 1, "%+v", response.Errors)
			require.Equal(t, tc.field, evalErrors[0].Field)
			require.Equal(t, tc.path, evalErrors[0].Path)
		})
	}
}

package grpcapi

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestTypedApplicationRoundTripAndPresence(t *testing.T) {
	falseValue := false
	input := apis.CreateApplicationsRequest{
		Name: "demo", Namespace: "team", TemplateEnabled: &falseValue,
		Components: []apis.CreateComponentRequest{{
			Name: "api", Image: "registry.example.com/demo:1.0.0", Replicas: 0,
			Properties: spec.Properties{Env: map[string]string{"MODE": "test"}},
			Traits:     spec.Traits{Resources: &spec.ResourceTraitsSpec{CPU: "1", Memory: "2Gi"}},
		}},
	}
	message, err := encodeTypedResponse(input, &eruunv1.AppDTOCreateApplicationsRequest{})
	require.NoError(t, err)
	require.True(t, message.ProtoReflect().Has(message.ProtoReflect().Descriptor().Fields().ByName("template_enabled")))
	require.False(t, message.GetTemplateEnabled())
	require.Len(t, message.Components, 1)
	require.Equal(t, int32(0), message.Components[0].Replicas)
	decoded, err := decodeTypedRequest[apis.CreateApplicationsRequest](message)
	require.NoError(t, err)
	require.Equal(t, input.Name, decoded.Name)
	require.NotNil(t, decoded.TemplateEnabled)
	require.False(t, *decoded.TemplateEnabled)
	require.Equal(t, input.Components[0].Traits.Resources.CPU, decoded.Components[0].Traits.Resources.CPU)
	require.Equal(t, input.Components[0].Properties.Env, decoded.Components[0].Properties.Env)

	absent := &eruunv1.AppDTOCreateApplicationsRequest{Name: "demo"}
	require.False(t, absent.ProtoReflect().Has(absent.ProtoReflect().Descriptor().Fields().ByName("template_enabled")))
	decodedAbsent, err := decodeTypedRequest[apis.CreateApplicationsRequest](absent)
	require.NoError(t, err)
	require.Nil(t, decodedAbsent.TemplateEnabled)
}

func TestTypedNestedDynamicFieldsAndEmbeddedRequest(t *testing.T) {
	input := apis.CreateAndExecApplicationRequest{
		CreateApplicationsRequest: apis.CreateApplicationsRequest{Name: "demo"},
		WorkflowID:                "release", ExecuteAt: 12,
	}
	message, err := encodeTypedResponse(input, &eruunv1.AppDTOCreateAndExecApplicationRequest{})
	require.NoError(t, err)
	require.Equal(t, "demo", message.Name)
	require.Equal(t, "release", message.WorkflowId)
	decoded, err := decodeTypedRequest[apis.CreateAndExecApplicationRequest](message)
	require.NoError(t, err)
	require.Equal(t, input, decoded)

	result := apis.ExecWorkflowResponse{
		TaskID: "task", Status: "waiting",
		AllowedActions: []apis.AllowedAction{{Name: "continue", Method: "POST", Body: map[string]any{"action": "continue"}}},
	}
	response, err := encodeTypedResponse(result, &eruunv1.AppDTOExecWorkflowResponse{})
	require.NoError(t, err)
	require.Len(t, response.AllowedActions, 1)
	require.Equal(t, "continue", response.AllowedActions[0].Body.Fields["action"].GetStringValue())

	jobValue, err := structpb.NewValue(map[string]any{"image": "example:1", "timeoutSeconds": float64(0)})
	require.NoError(t, err)
	job := &eruunv1.SubmitJobRequest{Name: "run", Type: "command", Spec: jobValue}
	encoded, err := proto.Marshal(job)
	require.NoError(t, err)
	copyJob := &eruunv1.SubmitJobRequest{}
	require.NoError(t, proto.Unmarshal(encoded, copyJob))
	require.Equal(t, float64(0), copyJob.Spec.GetStructValue().Fields["timeoutSeconds"].GetNumberValue())
	jsonBody, err := json.Marshal(copyJob.Spec.AsInterface())
	require.NoError(t, err)
	require.Contains(t, string(jsonBody), "timeoutSeconds")
}

func TestTypedWorkflowPropertiesArrayRoundTrip(t *testing.T) {
	step := apis.CreateWorkflowStepRequest{Name: "archive"}
	step.SetWorkflowPropertiesList([]apis.WorkflowProperties{
		{Policies: []string{"api"}, Path: "/var/log/api"},
		{Policies: []string{"worker"}, Path: "/var/log/worker"},
	})
	subStep := apis.CreateWorkflowSubStepRequest{Name: "archive-sidecar"}
	subStep.SetWorkflowPropertiesList([]apis.WorkflowProperties{{Policies: []string{"sidecar"}, Container: "logs"}})
	step.SubSteps = []apis.CreateWorkflowSubStepRequest{subStep}

	applicationSpec := apis.CreateApplicationsRequest{Name: "demo", Workflow: []apis.CreateWorkflowStepRequest{step}}
	message, err := encodeTypedResponse(applicationSpec, &eruunv1.AppDTOCreateApplicationsRequest{})
	require.NoError(t, err)
	require.Len(t, message.Workflow[0].Properties, 2)
	require.Len(t, message.Workflow[0].SubSteps[0].Properties, 1)
	decoded, err := decodeTypedRequest[apis.CreateApplicationsRequest](message)
	require.NoError(t, err)
	require.True(t, decoded.Workflow[0].WorkflowPropertiesFromArray())
	require.Equal(t, step.WorkflowPropertiesList(), decoded.Workflow[0].WorkflowPropertiesList())
	require.Equal(t, subStep.WorkflowPropertiesList(), decoded.Workflow[0].SubSteps[0].WorkflowPropertiesList())

	// ListApplicationWorkflows includes a resubmittable spec assembled from the
	// stored Workflow. Its canonical JSON also contains properties arrays.
	storedSteps, err := model.NewJSONStructByStruct(model.WorkflowSteps{Steps: []*model.WorkflowStep{{
		Name: "archive", Properties: []model.Policies{
			{Policies: []string{"api"}, Path: "/var/log/api"},
			{Policies: []string{"worker"}, Path: "/var/log/worker"},
		},
		SubSteps: []*model.WorkflowSubStep{{Name: "archive-sidecar", Properties: []model.Policies{{Policies: []string{"sidecar"}, Container: "logs"}}}},
	}}})
	require.NoError(t, err)
	assembled, err := assembler.ConvertWorkflowModelToDTO(&model.Workflow{ID: "wf-1", Name: "demo", Steps: storedSteps})
	require.NoError(t, err)
	listed, err := encodeTypedResponse(apis.ListApplicationWorkflowsResponse{
		Workflows: []*apis.ApplicationWorkflow{assembled},
	}, &eruunv1.AppDTOListApplicationWorkflowsResponse{})
	require.NoError(t, err)
	require.Len(t, listed.Workflows[0].Spec.Workflow[0].Properties, 2)
	require.Len(t, listed.Workflows[0].Spec.Workflow[0].SubSteps[0].Properties, 1)

	withoutProperties := apis.CreateApplicationsRequest{Name: "plain", Workflow: []apis.CreateWorkflowStepRequest{{Name: "deploy"}}}
	plain, err := encodeTypedResponse(withoutProperties, &eruunv1.AppDTOCreateApplicationsRequest{})
	require.NoError(t, err)
	require.Empty(t, plain.Workflow[0].Properties)
}

func TestTypedRequestPreservesExplicitEmptyOptionalStrings(t *testing.T) {
	empty := ""
	workflow := &eruunv1.AppDTOUpdateApplicationWorkflowRequest{FailurePolicy: &empty}
	require.True(t, workflow.ProtoReflect().Has(workflow.ProtoReflect().Descriptor().Fields().ByName("failure_policy")))
	updated, err := decodeTypedRequest[apis.UpdateApplicationWorkflowRequest](workflow)
	require.NoError(t, err)
	require.True(t, updated.FailurePolicySet)
	require.Empty(t, updated.FailurePolicy)

	omittedUpdate, err := decodeTypedRequest[apis.UpdateApplicationWorkflowRequest](&eruunv1.AppDTOUpdateApplicationWorkflowRequest{})
	require.NoError(t, err)
	require.False(t, omittedUpdate.FailurePolicySet)
	selectedPolicy := "cleanup_failed"
	selectedUpdate, err := decodeTypedRequest[apis.UpdateApplicationWorkflowRequest](&eruunv1.AppDTOUpdateApplicationWorkflowRequest{FailurePolicy: &selectedPolicy})
	require.NoError(t, err)
	require.True(t, selectedUpdate.FailurePolicySet)
	require.Equal(t, selectedPolicy, string(selectedUpdate.FailurePolicy))

	reset := &eruunv1.AppDTODatabaseResetRequest{Components: []string{"mysql"}, InitSqlurl: &empty}
	require.True(t, reset.ProtoReflect().Has(reset.ProtoReflect().Descriptor().Fields().ByName("init_sqlurl")))
	resetRequest, err := decodeTypedRequest[apis.DatabaseResetRequest](reset)
	require.NoError(t, err)
	require.True(t, resetRequest.InitSQLURLProvided())
	require.Empty(t, resetRequest.InitSQLURL)

	omittedReset, err := decodeTypedRequest[apis.DatabaseResetRequest](&eruunv1.AppDTODatabaseResetRequest{Components: []string{"mysql"}})
	require.NoError(t, err)
	require.False(t, omittedReset.InitSQLURLProvided())
	url := "https://files.example/game.sql"
	selectedReset, err := decodeTypedRequest[apis.DatabaseResetRequest](&eruunv1.AppDTODatabaseResetRequest{Components: []string{"mysql"}, InitSqlurl: &url})
	require.NoError(t, err)
	require.True(t, selectedReset.InitSQLURLProvided())
	require.Equal(t, url, selectedReset.InitSQLURL)
}

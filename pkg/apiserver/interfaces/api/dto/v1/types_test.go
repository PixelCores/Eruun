package v1

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"

	"github.com/stretchr/testify/require"
)

func TestCreateApplicationsRequestJSONTags(t *testing.T) {
	input := `{"name":"demo","namespace":"default","templateEnabled":true,"component":[{"name":"web","type":"webservice","namespace":"comp-ns","replicas":1,"properties":{},"traits":{}}]}`

	var req CreateApplicationsRequest
	err := json.Unmarshal([]byte(input), &req)
	require.NoError(t, err)
	require.Equal(t, "default", req.Namespace)
	require.NotNil(t, req.TemplateEnabled)
	require.True(t, *req.TemplateEnabled)
	require.Len(t, req.Component, 1)
	require.Equal(t, "comp-ns", req.Component[0].Namespace)
}

func TestDatabaseResetRequestDistinguishesOmittedAndProvidedInitSQLURL(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectedURL      string
		expectedProvided bool
	}{
		{
			name:  "omitted",
			input: `{"components":["mysql"]}`,
		},
		{
			name:             "valid URL",
			input:            `{"components":["mysql"],"initSqlUrl":"https://files.example/game.sql"}`,
			expectedURL:      "https://files.example/game.sql",
			expectedProvided: true,
		},
		{
			name:             "empty string",
			input:            `{"components":["mysql"],"initSqlUrl":""}`,
			expectedProvided: true,
		},
		{
			name:             "whitespace",
			input:            `{"components":["mysql"],"initSqlUrl":"   "}`,
			expectedURL:      "   ",
			expectedProvided: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req DatabaseResetRequest
			require.NoError(t, json.Unmarshal([]byte(tt.input), &req))
			require.Equal(t, tt.expectedURL, req.InitSQLURL)
			require.Equal(t, tt.expectedProvided, req.InitSQLURLProvided())
		})
	}
}

func TestDatabaseResetRequestRejectsInvalidInitSQLURLJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "null",
			input: `{"components":["mysql"],"initSqlUrl":null}`,
		},
		{
			name:  "non-string",
			input: `{"components":["mysql"],"initSqlUrl":42}`,
		},
		{
			name:  "unknown field",
			input: `{"components":["mysql"],"initSqlUrl":"https://files.example/game.sql","unknown":true}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req DatabaseResetRequest
			require.Error(t, json.Unmarshal([]byte(tt.input), &req))
		})
	}
}

func TestCreateApplicationsRequestDecodesExplicitEmptyJobFailurePolicy(t *testing.T) {
	input := `{"name":"demo","components":[{"name":"job","type":"job","image":"busybox:latest","properties":{"failurePolicy":""}}]}`

	var req CreateApplicationsRequest
	require.NoError(t, json.Unmarshal([]byte(input), &req))
	require.Len(t, req.Component, 1)
	require.NotNil(t, req.Component[0].Properties.FailurePolicy)
	require.Empty(t, *req.Component[0].Properties.FailurePolicy)
}

func TestUpdateVersionRequestDecodesJobFailurePolicy(t *testing.T) {
	input := `{"version":"1.1.0","components":[{"action":"add","name":"mysql-update-job","type":"job","properties":{"runPolicy":"recreate","failurePolicy":"cleanup_failed"}}]}`

	var req UpdateVersionRequest
	require.NoError(t, json.Unmarshal([]byte(input), &req))
	require.Len(t, req.Components, 1)
	require.NotNil(t, req.Components[0].Properties)
	require.Equal(t, string(config.JobRunPolicyRecreate), req.Components[0].Properties.RunPolicy)
	require.NotNil(t, req.Components[0].Properties.FailurePolicy)
	require.Equal(t, config.WorkflowFailurePolicyCleanupFailed, *req.Components[0].Properties.FailurePolicy)
}

func TestUpdateVersionRequestDistinguishesOmittedAndExplicitJobFailurePolicy(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *config.WorkflowFailurePolicy
	}{
		{name: "omitted", input: `{"version":"1.1.0","components":[{"name":"job","properties":{}}]}`},
		{name: "explicit empty", input: `{"version":"1.1.0","components":[{"name":"job","properties":{"failurePolicy":""}}]}`, expected: jobFailurePolicyPointer("")},
		{name: "explicit whitespace", input: `{"version":"1.1.0","components":[{"name":"job","properties":{"failurePolicy":"   "}}]}`, expected: jobFailurePolicyPointer("   ")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req UpdateVersionRequest
			require.NoError(t, json.Unmarshal([]byte(tt.input), &req))
			require.Len(t, req.Components, 1)
			require.NotNil(t, req.Components[0].Properties)
			if tt.expected == nil {
				require.Nil(t, req.Components[0].Properties.FailurePolicy)
				return
			}
			require.NotNil(t, req.Components[0].Properties.FailurePolicy)
			require.Equal(t, *tt.expected, *req.Components[0].Properties.FailurePolicy)
		})
	}
}

func TestUpdateVersionRequestKeepsStrictPropertiesValidation(t *testing.T) {
	input := []byte(`{"version":"1.1.0","components":[{"name":"job","properties":{"failurePolicy":"cleanup_failed","unknown":true}}]}`)
	var req UpdateVersionRequest
	require.ErrorContains(t, decodeStrictJSON(input, &req), `unknown field "unknown"`)
}

func jobFailurePolicyPointer(policy config.WorkflowFailurePolicy) *config.WorkflowFailurePolicy {
	return &policy
}

func TestCreateApplicationsRequestAcceptsComponentsAlias(t *testing.T) {
	input := `{"name":"demo","namespace":"default","components":[{"name":"web","type":"webservice","namespace":"comp-ns","replicas":1,"properties":{},"traits":{}}]}`

	var req CreateApplicationsRequest
	err := json.Unmarshal([]byte(input), &req)
	require.NoError(t, err)
	require.Len(t, req.Component, 1)
	require.Equal(t, "web", req.Component[0].Name)
}

func TestCreateApplicationsRequestRejectsComponentAliasConflict(t *testing.T) {
	input := `{"name":"demo","component":[],"components":[]}`

	var req CreateApplicationsRequest
	err := json.Unmarshal([]byte(input), &req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot both be set")
}

func TestCreateApplicationsRequestUsesCamelCaseID(t *testing.T) {
	var req CreateApplicationsRequest
	err := json.Unmarshal([]byte(`{"id":"app-1","name":"demo"}`), &req)
	require.NoError(t, err)
	require.Equal(t, "app-1", req.ID)
}

func TestUpdateVersionResponseUsesCamelCaseWorkflowID(t *testing.T) {
	resp := UpdateVersionResponse{
		AppID:               "app-1",
		Version:             "1.1.0",
		ExecutionScope:      "changed_components",
		WorkflowID:          "wf-1",
		TaskID:              "task-1",
		RestartedComponents: []string{"api"},
	}

	raw, err := json.Marshal(resp)
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(raw, &payload))
	require.Equal(t, "wf-1", payload["workflowId"])
	require.Equal(t, "changed_components", payload["executionScope"])
	require.Equal(t, []any{"api"}, payload["restartedComponents"])
	require.NotContains(t, payload, "workflow_id")
	require.NotContains(t, payload, "execution_scope")
	require.NotContains(t, payload, "restarted_components")
}

func TestCreateApplicationsRequestRejectsLegacyIDCaseVariants(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "uppercase",
			input: `{"ID":"app-1","name":"demo"}`,
		},
		{
			name:  "titlecase",
			input: `{"Id":"app-1","name":"demo"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req CreateApplicationsRequest
			err := json.Unmarshal([]byte(tt.input), &req)
			require.Error(t, err)
			require.Contains(t, err.Error(), "unknown field")
		})
	}
}

func TestCreateApplicationsRequestWorkflowArrayShape(t *testing.T) {
	input := `{"name":"demo","workflow":[{"name":"deploy-web","components":["web"]}]}`

	var req CreateApplicationsRequest
	err := json.Unmarshal([]byte(input), &req)
	require.NoError(t, err)
	require.Len(t, req.WorkflowSteps, 1)
	require.Equal(t, "deploy-web", req.WorkflowSteps[0].Name)
	require.Nil(t, req.WorkflowCallback)
	require.Empty(t, req.WorkflowFailurePolicy)
}

func TestCreateApplicationsRequestWorkflowObjectShape(t *testing.T) {
	input := `{"name":"demo","callback":{"success":"https://example.com/app"},"workflow":{"callback":{"success":"https://example.com/workflow"},"failurePolicy":"cleanup_all","steps":[{"name":"deploy-web","components":["web"]}]}}`

	var req CreateApplicationsRequest
	err := json.Unmarshal([]byte(input), &req)
	require.NoError(t, err)
	require.NotNil(t, req.Callback)
	require.Equal(t, "https://example.com/app", req.Callback.Success)
	require.NotNil(t, req.WorkflowCallback)
	require.Equal(t, "https://example.com/workflow", req.WorkflowCallback.Success)
	require.Equal(t, config.WorkflowFailurePolicyCleanupAll, req.WorkflowFailurePolicy)
	require.Len(t, req.WorkflowSteps, 1)
	require.Equal(t, "deploy-web", req.WorkflowSteps[0].Name)
}

func TestCreateApplicationsRequestWorkflowObjectRejectsUnknownField(t *testing.T) {
	input := `{"name":"demo","workflow":{"steps":[],"extra":true}}`

	var req CreateApplicationsRequest
	err := json.Unmarshal([]byte(input), &req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown field")
}

func TestCreateWorkflowStepRequestAcceptsWorkflowTypeAlias(t *testing.T) {
	input := `{"name":"deploy-web","workflowType":"deploy","components":["web"],"subSteps":[{"name":"cleanup-web","workflowType":"cleanup_resources","components":["web"]}]}`

	var step CreateWorkflowStepRequest
	err := json.Unmarshal([]byte(input), &step)
	require.NoError(t, err)
	require.Equal(t, config.JobDeploy, step.WorkflowType)
	require.Len(t, step.SubSteps, 1)
	require.Equal(t, config.JobCleanupResources, step.SubSteps[0].WorkflowType)
}

func TestCreateWorkflowStepRequestRejectsJobTypeAliasConflict(t *testing.T) {
	input := `{"name":"deploy-web","jobType":"deploy","workflowType":"deploy","components":["web"]}`

	var step CreateWorkflowStepRequest
	err := json.Unmarshal([]byte(input), &step)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot both be set")
}

func TestCreateWorkflowSubStepRequestRejectsJobTypeAliasConflict(t *testing.T) {
	input := `{"name":"cleanup-web","jobType":"cleanup_resources","workflowType":"cleanup_resources","components":["web"]}`

	var step CreateWorkflowSubStepRequest
	err := json.Unmarshal([]byte(input), &step)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot both be set")
}

func TestUpdateApplicationWorkflowRequestAcceptsStepsAlias(t *testing.T) {
	input := `{"workflowId":"wf-1","workflowType":"update","failurePolicy":"cleanup_all","steps":[{"name":"deploy-web","jobType":"deploy","components":["web"]}]}`

	var req UpdateApplicationWorkflowRequest
	err := json.Unmarshal([]byte(input), &req)
	require.NoError(t, err)
	require.Equal(t, "wf-1", req.WorkflowID)
	require.Equal(t, config.WorkflowTaskTypeUpdate, req.WorkflowType)
	require.Equal(t, config.WorkflowFailurePolicyCleanupAll, req.FailurePolicy)
	require.True(t, req.FailurePolicySet)
	require.Len(t, req.Workflow, 1)
	require.Equal(t, config.JobDeploy, req.Workflow[0].WorkflowType)
}

func TestUpdateApplicationWorkflowRequestTracksFailurePolicyPresence(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedPolicy config.WorkflowFailurePolicy
		expectedSet    bool
	}{
		{
			name:           "omitted",
			input:          `{"workflowId":"wf-1","steps":[{"name":"deploy-web","jobType":"deploy","components":["web"]}]}`,
			expectedPolicy: "",
			expectedSet:    false,
		},
		{
			name:           "cleanup all",
			input:          `{"workflowId":"wf-1","failurePolicy":"cleanup_all","steps":[{"name":"deploy-web","jobType":"deploy","components":["web"]}]}`,
			expectedPolicy: config.WorkflowFailurePolicyCleanupAll,
			expectedSet:    true,
		},
		{
			name:           "explicit empty",
			input:          `{"workflowId":"wf-1","failurePolicy":"","steps":[{"name":"deploy-web","jobType":"deploy","components":["web"]}]}`,
			expectedPolicy: "",
			expectedSet:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req UpdateApplicationWorkflowRequest
			err := json.Unmarshal([]byte(tt.input), &req)
			require.NoError(t, err)
			require.Equal(t, tt.expectedPolicy, req.FailurePolicy)
			require.Equal(t, tt.expectedSet, req.FailurePolicySet)
		})
	}
}

func TestUpdateApplicationWorkflowRequestAcceptsReadResponsePropertiesArray(t *testing.T) {
	input := `{"workflowId":"wf-1","workflowType":"log_archive_upload","steps":[{"name":"archive-api","workflowType":"log_archive_upload","components":["api"],"properties":[{"policies":["api"],"path":"/var/log/api","container":"api"}],"subSteps":[{"name":"archive-worker","workflowType":"log_archive_upload","components":["worker"],"properties":[{"policies":["worker"],"path":"/var/log/worker"}]}]}]}`

	var req UpdateApplicationWorkflowRequest
	err := json.Unmarshal([]byte(input), &req)
	require.NoError(t, err)
	require.Len(t, req.Workflow, 1)

	step := req.Workflow[0]
	require.True(t, step.WorkflowPropertiesFromArray())
	require.Equal(t, config.JobLogArchiveUpload, step.WorkflowType)
	require.Equal(t, WorkflowProperties{
		Policies:  []string{"api"},
		Path:      "/var/log/api",
		Container: "api",
	}, step.Properties)
	require.Equal(t, []WorkflowProperties{{
		Policies:  []string{"api"},
		Path:      "/var/log/api",
		Container: "api",
	}}, step.WorkflowPropertiesList())

	require.Len(t, step.SubSteps, 1)
	require.True(t, step.SubSteps[0].WorkflowPropertiesFromArray())
	require.Equal(t, []WorkflowProperties{{
		Policies: []string{"worker"},
		Path:     "/var/log/worker",
	}}, step.SubSteps[0].WorkflowPropertiesList())
}

func TestCreateWorkflowStepRequestRejectsPropertiesArrayUnknownField(t *testing.T) {
	input := `{"name":"archive-api","jobType":"log_archive_upload","properties":[{"policies":["api"],"path":"/var/log/api","extra":true}]}`

	var step CreateWorkflowStepRequest
	err := json.Unmarshal([]byte(input), &step)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown field")
}

func TestUpdateApplicationWorkflowRequestRejectsWorkflowAliasConflict(t *testing.T) {
	input := `{"workflow":[{"name":"deploy-web"}],"steps":[{"name":"deploy-web"}]}`

	var req UpdateApplicationWorkflowRequest
	err := json.Unmarshal([]byte(input), &req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot both be set")
}

func TestApplicationBaseJSONTags(t *testing.T) {
	base := ApplicationBase{
		ID:              "app-1",
		Name:            "demo",
		Namespace:       "default",
		WorkflowID:      "wf-1",
		TemplateEnabled: true,
		Resources: ApplicationResources{
			CPUReq:   "160m",
			MemReq:   "260Mi",
			CPULimit: "300m",
			MemLimit: "600Mi",
			Replicas: 1,
		},
	}

	data, err := json.Marshal(base)
	require.NoError(t, err)

	var payload map[string]any
	err = json.Unmarshal(data, &payload)
	require.NoError(t, err)
	require.Equal(t, true, payload["templateEnabled"])
	require.Equal(t, "wf-1", payload["workflowId"])
	require.NotContains(t, payload, "cpuReq")
	require.NotContains(t, payload, "memReq")
	require.NotContains(t, payload, "cpuLimit")
	require.NotContains(t, payload, "memLimit")
	require.NotContains(t, payload, "replicas")
	resources, ok := payload["resources"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "160m", resources["cpuReq"])
	require.Equal(t, "260Mi", resources["memReq"])
	require.Equal(t, "300m", resources["cpuLimit"])
	require.Equal(t, "600Mi", resources["memLimit"])
	require.EqualValues(t, 1, resources["replicas"])
	_, hasLegacy := payload["tmp_enable"]
	require.False(t, hasLegacy)
	_, hasLegacyWorkflowID := payload["workflow_id"]
	require.False(t, hasLegacyWorkflowID)
}

func TestApplicationBaseJSONIncludesZeroResources(t *testing.T) {
	base := ApplicationBase{ID: "app-1"}

	data, err := json.Marshal(base)
	require.NoError(t, err)

	var payload map[string]any
	err = json.Unmarshal(data, &payload)
	require.NoError(t, err)
	resources, ok := payload["resources"].(map[string]any)
	require.True(t, ok)
	require.Empty(t, resources["cpuReq"])
	require.Empty(t, resources["memReq"])
	require.Empty(t, resources["cpuLimit"])
	require.Empty(t, resources["memLimit"])
	require.Zero(t, resources["replicas"])
}

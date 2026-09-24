package v1

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/stretchr/testify/require"
)

func TestCanonicalJSONSchemaDefinesStrictPublicProfiles(t *testing.T) {
	raw, err := CanonicalJSONSchema()
	require.NoError(t, err)

	var schema map[string]any
	require.NoError(t, json.Unmarshal(raw, &schema))
	require.Equal(t, CanonicalJSONSchemaID, schema["$id"])
	definitions := schema["$defs"].(map[string]any)
	for _, name := range []string{"Application", "Component", "Trait", "Workflow", "WorkflowStep"} {
		definition, ok := definitions[name].(map[string]any)
		require.True(t, ok, "missing definition %s", name)
		require.Equal(t, false, definition["additionalProperties"], "definition %s must reject unknown fields", name)
	}

	applicationProperties := definitions["Application"].(map[string]any)["properties"].(map[string]any)
	require.Contains(t, applicationProperties, "components")
	require.NotContains(t, applicationProperties, "component")
	require.Equal(t, float64(31), applicationProperties["name"].(map[string]any)["maxLength"])
	require.Equal(t, "array", applicationProperties["components"].(map[string]any)["type"])
	workflowProperties := definitions["Workflow"].(map[string]any)["properties"].(map[string]any)
	require.Contains(t, workflowProperties, "workflow")
	require.NotContains(t, workflowProperties, "steps")
	require.Equal(t, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, workflowProperties["name"].(map[string]any)["pattern"])
	require.Equal(t, float64(2), workflowProperties["name"].(map[string]any)["minLength"])
	require.Equal(t, float64(63), workflowProperties["name"].(map[string]any)["maxLength"])
	require.ElementsMatch(t, []any{"workflow", "update", "test", "scan", "delivery", "database_reset", "log_archive_upload"}, workflowProperties["workflowType"].(map[string]any)["enum"])
	stepProperties := definitions["WorkflowStep"].(map[string]any)["properties"].(map[string]any)
	require.Equal(t, "array", stepProperties["properties"].(map[string]any)["type"])
	workflowPropertyFields := definitions["WorkflowProperties"].(map[string]any)["properties"].(map[string]any)
	require.Contains(t, workflowPropertyFields, "initSqlUrl")
	require.ElementsMatch(t, []any{"name", "type"}, definitions["Component"].(map[string]any)["required"])

	propertiesDefinition := definitions["Properties"].(map[string]any)["properties"].(map[string]any)
	portsAlternatives := propertiesDefinition["ports"].(map[string]any)["anyOf"].([]any)
	require.Equal(t, "array", portsAlternatives[0].(map[string]any)["type"])
	require.Equal(t, "null", portsAlternatives[1].(map[string]any)["type"])
	envAlternatives := propertiesDefinition["env"].(map[string]any)["anyOf"].([]any)
	require.Equal(t, "object", envAlternatives[0].(map[string]any)["type"])
	require.Equal(t, "null", envAlternatives[1].(map[string]any)["type"])
}

func TestCanonicalSchemaDiscoversSharedEvaluationAndStandaloneJobs(t *testing.T) {
	raw, err := CanonicalJSONSchema()
	require.NoError(t, err)
	var schema map[string]any
	require.NoError(t, json.Unmarshal(raw, &schema))
	require.Len(t, schema["anyOf"], 5, "Job and Component profiles can share the same evaluation shape")
	definitions := schema["$defs"].(map[string]any)
	job := definitions["Job"].(map[string]any)
	require.Equal(t, false, job["additionalProperties"])
	require.Len(t, job["oneOf"], 2)
	jobFields := job["properties"].(map[string]any)
	require.Equal(t, "#/$defs/CommandJobSpec", jobFields["spec"].(map[string]any)["$ref"])
	require.NotContains(t, jobFields, "resultPolicy")
	for _, name := range []string{"Trait", "JobTraits"} {
		fields := definitions[name].(map[string]any)["properties"].(map[string]any)
		require.Equal(t, "#/$defs/EvaluationTraitSpec", fields["eval"].(map[string]any)["$ref"])
		require.NotContains(t, fields, "evaluation")
	}
	evaluation := definitions["EvaluationTraitSpec"].(map[string]any)
	require.Equal(t, false, evaluation["additionalProperties"])
	require.ElementsMatch(t, []any{"env", "agent", "taskPackageId"}, evaluation["required"])
	fields := evaluation["properties"].(map[string]any)
	require.Equal(t, "ack", fields["env"].(map[string]any)["const"])
	require.Equal(t, "string", fields["agent"].(map[string]any)["type"])
	require.Equal(t, float64(16), fields["concurrency"].(map[string]any)["maximum"])
	require.Contains(t, fields, "sandboxResources")
	require.Contains(t, fields, "resultPolicy")
	require.Equal(t, "#/$defs/EvaluationRecoverySpec", fields["recovery"].(map[string]any)["$ref"])
	recovery := definitions["EvaluationRecoverySpec"].(map[string]any)
	require.Equal(t, false, recovery["additionalProperties"])
	require.ElementsMatch(t, []any{"agentVersion", "replaySafe"}, recovery["required"])
	recoveryFields := recovery["properties"].(map[string]any)
	require.Equal(t, true, recoveryFields["replaySafe"].(map[string]any)["const"])
	require.ElementsMatch(t, []any{spec.CodexRecoveryVersion, spec.ClaudeCodeRecoveryVersion}, recoveryFields["agentVersion"].(map[string]any)["enum"])
	require.Equal(t, float64(spec.DefaultCheckpointIntervalSeconds), recoveryFields["checkpointIntervalSeconds"].(map[string]any)["default"])
	require.Len(t, evaluation["allOf"], 2, "recovery must constrain the selected agent and version together")
	require.NotContains(t, fields, "framework")
	require.NotContains(t, fields, "frameworkVersion")
	require.NotContains(t, fields, "options")
}

func TestApplicationAndJobDecodeRecoveryWithoutLosingReplayConsent(t *testing.T) {
	const evaluation = `{"env":"ack","agent":"codex","model":"openai/model","taskPackageId":"12345678-1234-1234-1234-123456789012","recovery":{"agentVersion":"0.154.0","replaySafe":true}}`
	var request CreateApplicationsRequest
	require.NoError(t, json.Unmarshal([]byte(`{"name":"evaluation-app","components":[{"name":"evaluate","type":"job","traits":{"eval":`+evaluation+`}}]}`), &request))
	var standalone spec.JobSpec
	require.NoError(t, spec.DecodeJobJSON([]byte(`{"name":"evaluate","type":"job","traits":{"eval":`+evaluation+`}}`), &standalone))
	require.NoError(t, request.Components[0].Traits.Evaluation.Normalize())
	require.NoError(t, standalone.Normalize())
	require.Equal(t, standalone.Traits.Evaluation.Recovery, request.Components[0].Traits.Evaluation.Recovery)
	require.True(t, standalone.Traits.Evaluation.Recovery.ReplaySafe)
	require.Equal(t, spec.DefaultCheckpointIntervalSeconds, standalone.Traits.Evaluation.Recovery.CheckpointIntervalSeconds)
}

func TestApplicationEvaluationRejectsUnknownBusinessFields(t *testing.T) {
	const body = `{"name":"evaluation-app","components":[{"name":"evaluate","type":"job","traits":{"eval":{"env":"ack","agent":"oracle","taskPackageId":"12345678-1234-1234-1234-123456789012"}}}]}`
	var request CreateApplicationsRequest
	require.NoError(t, json.Unmarshal([]byte(body), &request))
	require.Equal(t, "oracle", request.Components[0].Traits.Evaluation.Agent)

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &payload))
	component := payload["components"].([]any)[0].(map[string]any)
	evaluation := component["traits"].(map[string]any)["eval"].(map[string]any)
	evaluation["framework"] = "harbor"
	invalid, err := json.Marshal(payload)
	require.NoError(t, err)
	require.ErrorContains(t, json.Unmarshal(invalid, &request), "unknown field")
	var standalone spec.JobSpec
	require.ErrorContains(t, spec.DecodeJobJSON([]byte(`{"name":"evaluate","type":"job","traits":{"eval":{"env":"ack","agent":"oracle","taskPackageId":"12345678-1234-1234-1234-123456789012","framework":"harbor"}}}`), &standalone), "unknown field")
	delete(evaluation, "framework")
	traits := component["traits"].(map[string]any)
	traits["evaluation"] = traits["eval"]
	delete(traits, "eval")
	legacy, err := json.Marshal(payload)
	require.NoError(t, err)
	require.ErrorContains(t, json.Unmarshal(legacy, &request), `unknown field "evaluation"`)
}

func TestCanonicalApplicationMarshalProducesOnlyCanonicalFields(t *testing.T) {
	request := CreateApplicationsRequest{
		Name: "demo",
		Components: []CreateComponentRequest{{
			Name:          "web",
			ComponentType: "webservice",
		}},
		Workflow: []CreateWorkflowStepRequest{{Name: "deploy", Components: []string{"web"}}},
	}
	raw, err := json.Marshal(request)
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(raw, &payload))
	require.Contains(t, payload, "components")
	require.NotContains(t, payload, "component")
	require.IsType(t, []any{}, payload["workflow"])
	components := payload["components"].([]any)
	properties := components[0].(map[string]any)["properties"].(map[string]any)
	require.Contains(t, properties, "ports")
	require.Nil(t, properties["ports"], "zero-value Properties currently marshal nil containers as null")
}

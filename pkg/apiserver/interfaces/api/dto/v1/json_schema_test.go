package v1

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
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
	applicationNamePattern := regexp.MustCompile(applicationProperties["name"].(map[string]any)["pattern"].(string))
	for _, name := range []string{"demo.app", "demo-app", strings.Repeat("a", 31)} {
		require.True(t, applicationNamePattern.MatchString(name), "application name %q must match the write API", name)
	}
	for _, name := range []string{"demo..app", "demo.-app", "Demo.app"} {
		require.False(t, applicationNamePattern.MatchString(name), "invalid application name %q", name)
	}
	require.Equal(t, "array", applicationProperties["components"].(map[string]any)["type"])
	workflowProperties := definitions["Workflow"].(map[string]any)["properties"].(map[string]any)
	require.Contains(t, workflowProperties, "workflow")
	require.NotContains(t, workflowProperties, "steps")
	require.Equal(t, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, workflowProperties["name"].(map[string]any)["pattern"])
	require.Equal(t, float64(2), workflowProperties["name"].(map[string]any)["minLength"])
	require.Equal(t, float64(63), workflowProperties["name"].(map[string]any)["maxLength"])
	require.ElementsMatch(t, []any{"workflow", "update", "database_reset", "log_archive_upload"}, workflowProperties["workflowType"].(map[string]any)["enum"])
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
	require.NotContains(t, job, "oneOf")
	jobFields := job["properties"].(map[string]any)
	require.ElementsMatch(t, []any{"name", "stages"}, job["required"])
	require.Equal(t, "#/$defs/JobStage", jobFields["stages"].(map[string]any)["items"].(map[string]any)["$ref"])
	require.EqualValues(t, 0, jobFields["executeAt"].(map[string]any)["minimum"], "POST /jobs accepts executeAt and the strict schema must allow it")
	require.NotContains(t, jobFields, "type")
	require.NotContains(t, jobFields, "spec")
	require.NotContains(t, jobFields, "traits")
	step := definitions["JobStep"].(map[string]any)
	require.Len(t, step["oneOf"], 2)
	stepFields := step["properties"].(map[string]any)
	require.Equal(t, "#/$defs/CommandJobSpec", stepFields["spec"].(map[string]any)["$ref"])
	require.Equal(t, "#/$defs/Trait", stepFields["traits"].(map[string]any)["$ref"])
	retry := definitions["JobStepRetry"].(map[string]any)["properties"].(map[string]any)["backoffLimit"].(map[string]any)
	require.Equal(t, float64(0), retry["minimum"])
	require.Equal(t, float64(10), retry["maximum"])
	require.NotContains(t, jobFields, "resultPolicy")
	for _, name := range []string{"Trait"} {
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
	require.NoError(t, spec.DecodeJobJSON([]byte(`{"name":"evaluate","stages":[{"name":"main","steps":[{"name":"evaluate","type":"job","traits":{"eval":`+evaluation+`}}]}]}`), &standalone))
	require.NoError(t, request.Components[0].Traits.Evaluation.Normalize())
	require.NoError(t, standalone.Normalize())
	require.Equal(t, standalone.Stages[0].Steps[0].Traits.Evaluation.Recovery, request.Components[0].Traits.Evaluation.Recovery)
	require.True(t, standalone.Stages[0].Steps[0].Traits.Evaluation.Recovery.ReplaySafe)
	require.Equal(t, spec.DefaultCheckpointIntervalSeconds, standalone.Stages[0].Steps[0].Traits.Evaluation.Recovery.CheckpointIntervalSeconds)
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
	require.ErrorContains(t, spec.DecodeJobJSON([]byte(`{"name":"evaluate","stages":[{"name":"main","steps":[{"name":"evaluate","type":"job","traits":{"eval":{"env":"ack","agent":"oracle","taskPackageId":"12345678-1234-1234-1234-123456789012","framework":"harbor"}}}]}]}`), &standalone), "unknown field")
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

// The canonical schema mirrors the gateway contract; requests decode with the
// same field set, so the removed ingress and the unreserved egress fail early.
func TestCanonicalSchemaConstrainsGatewayTrait(t *testing.T) {
	raw, err := CanonicalJSONSchema()
	require.NoError(t, err)
	var schema map[string]any
	require.NoError(t, json.Unmarshal(raw, &schema))
	definitions := schema["$defs"].(map[string]any)
	definition := func(name string) map[string]any { return definitions[name].(map[string]any) }
	properties := func(name string) map[string]any { return definition(name)["properties"].(map[string]any) }
	field := func(name, field string) map[string]any { return properties(name)[field].(map[string]any) }

	require.Equal(t, "#/$defs/GatewayTraitSpec", field("Trait", "gateway")["$ref"])
	require.NotContains(t, properties("Trait"), "ingress")
	for _, name := range []string{"GatewayTraitSpec", "GatewayRouteSpec", "GatewayBackendSpec"} {
		require.Equal(t, false, definition(name)["additionalProperties"], name)
	}
	require.NotContains(t, properties("GatewayTraitSpec"), "egress", "phase 1 reserves no outbound field")
	require.ElementsMatch(t, []any{"routes"}, definition("GatewayTraitSpec")["required"])
	require.EqualValues(t, 8, field("GatewayTraitSpec", "hosts")["maxItems"])
	require.Equal(t, true, field("GatewayTraitSpec", "hosts")["uniqueItems"])
	require.EqualValues(t, 1, field("GatewayTraitSpec", "routes")["minItems"])
	require.EqualValues(t, 16, field("GatewayTraitSpec", "routes")["maxItems"])
	require.ElementsMatch(t, []any{spec.GatewayMatchPrefix, spec.GatewayMatchExact}, field("GatewayRouteSpec", "match")["enum"])
	require.Equal(t, spec.GatewayMatchPrefix, field("GatewayRouteSpec", "match")["default"])
	require.EqualValues(t, 1, field("GatewayBackendSpec", "port")["minimum"])
	require.EqualValues(t, 65535, field("GatewayBackendSpec", "port")["maximum"])
	require.Equal(t, []any{map[string]any{
		"if":   map[string]any{"required": []any{"rewrite"}},
		"then": map[string]any{"properties": map[string]any{"match": map[string]any{"const": spec.GatewayMatchPrefix}}},
	}}, definition("GatewayRouteSpec")["allOf"], "rewrite only replaces a matched prefix")

	path := regexp.MustCompile(field("GatewayRouteSpec", "path")["pattern"].(string))
	require.Equal(t, path.String(), field("GatewayRouteSpec", "rewrite")["pattern"])
	require.True(t, path.MatchString("/v1/models"))
	require.False(t, path.MatchString("v1"))
	timeout := regexp.MustCompile(field("GatewayRouteSpec", "timeout")["pattern"].(string))
	for _, value := range []string{"0s", "600s", "1h30m", "500ms"} {
		require.True(t, timeout.MatchString(value), value)
	}
	for _, value := range []string{"600", "1.5s", "10 s", "1d"} {
		require.False(t, timeout.MatchString(value), value)
	}

	// Placement: webservice/store components only, never init, sidecar or Job steps.
	require.Contains(t, definition("Component")["allOf"], map[string]any{
		"if":   map[string]any{"required": []any{"traits"}, "properties": map[string]any{"traits": map[string]any{"required": []any{"gateway"}}}},
		"then": map[string]any{"properties": map[string]any{"type": map[string]any{"enum": []any{"webservice", "store"}}}},
	})
	for _, name := range []string{"InitTraitSpec", "SidecarTraitsSpec"} {
		forbidden := definition(name)["allOf"].([]any)[0].(map[string]any)["properties"].(map[string]any)["traits"].(map[string]any)["not"].(map[string]any)["anyOf"]
		require.Contains(t, forbidden, map[string]any{"required": []any{"gateway"}}, name)
	}
	require.Equal(t, []any{map[string]any{"properties": map[string]any{
		"traits": map[string]any{"not": map[string]any{"required": []any{"gateway"}}},
	}}}, definition("JobStep")["allOf"])

	const component = `{"name":"chat","components":[{"name":"api","type":"webservice","image":"nginx:1.27","traits":%s}]}`
	var request CreateApplicationsRequest
	require.NoError(t, json.Unmarshal([]byte(fmt.Sprintf(component, `{"gateway":{"hosts":["api-ws-a.apps.example.com"],"routes":[{"path":"/v1","match":"exact","timeout":"600s","backend":{"service":"api","port":8080}}]}}`)), &request))
	require.Equal(t, &spec.GatewayTraitSpec{
		Hosts:  []string{"api-ws-a.apps.example.com"},
		Routes: []spec.GatewayRouteSpec{{Path: "/v1", Match: spec.GatewayMatchExact, Timeout: "600s", Backend: &spec.GatewayBackendSpec{Service: "api", Port: 8080}}},
	}, request.Components[0].Traits.Gateway)
	for _, traits := range []string{
		`{"ingress":[{"routes":[{"path":"/"}]}]}`,
		`{"gateway":{"routes":[{}],"egress":[{}]}}`,
		`{"gateway":{"routes":[{"path":"/","serviceName":"api"}]}}`,
		`{"gateway":{"routes":[{"backend":{"service":"api","weight":10}}]}}`,
	} {
		require.ErrorContains(t, json.Unmarshal([]byte(fmt.Sprintf(component, traits)), &request), "unknown field", traits)
	}
}

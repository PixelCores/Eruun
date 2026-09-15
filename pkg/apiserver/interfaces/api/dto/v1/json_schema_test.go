package v1

import (
	"encoding/json"
	"testing"

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

package application

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func evaluationComponent() apisv1.CreateComponentRequest {
	return apisv1.CreateComponentRequest{Name: "evaluate", ComponentType: config.InstantJob,
		Traits: spec.Traits{Evaluation: &spec.EvaluationTraitSpec{Env: "ack", Agent: "oracle", TaskPackageID: "12345678-1234-1234-1234-123456789012"}}}
}

func TestPrepareEvaluationComponentWithoutUserImage(t *testing.T) {
	components, err := prepareComponents("app", "space", []apisv1.CreateComponentRequest{evaluationComponent()})
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Empty(t, components[0].Image)
	var traits spec.Traits
	require.NoError(t, decodeJSONStruct(components[0].Traits, &traits))
	require.Equal(t, 1, traits.Evaluation.Attempts)
	require.Equal(t, 1, traits.Evaluation.Concurrency)
	require.NotNil(t, traits.Resources)
	require.NotNil(t, traits.Evaluation.SandboxResources)
	_, err = prepareComponents("app", "space", []apisv1.CreateComponentRequest{{Name: "command", ComponentType: config.InstantJob}})
	require.Error(t, err, "ordinary job components still require an image")
}

func TestPrepareEvaluationComponentRejectsConflicts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*apisv1.CreateComponentRequest)
	}{
		{"image", func(c *apisv1.CreateComponentRequest) { c.Image = "busybox:1" }},
		{"command", func(c *apisv1.CreateComponentRequest) { c.Properties.Command = []string{"true"} }},
		{"plaintext env", func(c *apisv1.CreateComponentRequest) { c.Properties.Env = map[string]string{"OPENAI_API_KEY": "key"} }},
		{"delayed", func(c *apisv1.CreateComponentRequest) { c.Properties.StartTime = 100 }},
		{"cron", func(c *apisv1.CreateComponentRequest) { c.Properties.Schedule = "* * * * *" }},
		{"run policy", func(c *apisv1.CreateComponentRequest) { c.Properties.RunPolicy = "skip_if_completed" }},
		{"retry", func(c *apisv1.CreateComponentRequest) { c.Properties.JobRetryPolicy = &workflowconfig.JobRetryPolicy{} }},
		{"webservice", func(c *apisv1.CreateComponentRequest) { c.ComponentType = config.ServerJob }},
		{"sidecar", func(c *apisv1.CreateComponentRequest) {
			c.Traits.Sidecar = []spec.SidecarTraitsSpec{{Name: "side", Image: "busybox:1"}}
		}},
		{"nested init", func(c *apisv1.CreateComponentRequest) {
			evaluation := c.Traits.Evaluation
			c.Traits = spec.Traits{Init: []spec.InitTraitSpec{{Name: "init", Image: "busybox:1", Traits: spec.Traits{Evaluation: evaluation}}}}
			c.Image = "busybox:1"
		}},
		{"nested sidecar", func(c *apisv1.CreateComponentRequest) {
			evaluation := c.Traits.Evaluation
			c.Traits = spec.Traits{Sidecar: []spec.SidecarTraitsSpec{{Name: "side", Image: "busybox:1", Traits: spec.Traits{Evaluation: evaluation}}}}
			c.Image = "busybox:1"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := evaluationComponent()
			tc.change(&component)
			_, err := prepareComponents("app", "space", []apisv1.CreateComponentRequest{component})
			require.Error(t, err)
		})
	}
}

func TestVersionUpdateEvaluationValidatesMergedComponent(t *testing.T) {
	request := evaluationComponent()
	component, err := newVersionUpdateComponent(&model.Applications{ID: "app", Namespace: "space"}, apisv1.ComponentUpdateSpec{
		Name: request.Name, ComponentType: request.ComponentType, Traits: &request.Traits,
	})
	require.NoError(t, err)
	service := &applicationsServiceImpl{}
	_, err = service.applyComponentUpdate(component, apisv1.ComponentUpdateSpec{Name: request.Name, Image: "busybox:1"})
	require.Error(t, err)
	require.Empty(t, component.Image, "invalid update must not mutate the stored model")
	_, err = service.applyComponentUpdate(component, apisv1.ComponentUpdateSpec{Name: request.Name, Properties: &spec.Properties{Command: []string{"true"}}})
	require.Error(t, err)
}

func TestEvaluationTemplateOverridesReplaceBusinessInput(t *testing.T) {
	template := evaluationComponent()
	traits, err := model.NewJSONStructByStruct(template.Traits)
	require.NoError(t, err)
	override := spec.Traits{Evaluation: &spec.EvaluationTraitSpec{Env: "ack", Agent: "codex", Model: "openai/test-model",
		TaskPackageID: "87654321-1234-1234-1234-123456789012", SandboxResources: &spec.ResourceTraitsSpec{CPU: "2", Memory: "3Gi"}}}
	component, err := convertComponentFromTemplate(&model.ApplicationComponent{Name: template.Name, ComponentType: config.InstantJob, Traits: traits},
		"evaluate-copy", "app", "space", spec.Properties{}, override, "", nil)
	require.NoError(t, err)
	require.Equal(t, "codex", component.Traits.Evaluation.Agent)
	require.Equal(t, override.Evaluation.TaskPackageID, component.Traits.Evaluation.TaskPackageID)
	require.NotSame(t, override.Evaluation, component.Traits.Evaluation)
	require.NotSame(t, override.Evaluation.SandboxResources, component.Traits.Evaluation.SandboxResources)
	require.Equal(t, "2", component.Traits.Evaluation.SandboxResources.CPULimit)
	require.Empty(t, override.Evaluation.SandboxResources.CPULimit, "normalization must not mutate the override input")

	_, err = convertComponentFromTemplate(&model.ApplicationComponent{Name: template.Name, ComponentType: config.InstantJob, Traits: traits},
		"evaluate-copy", "app", "space", spec.Properties{Command: []string{"true"}}, spec.Traits{}, "", nil)
	require.Error(t, err, "unsupported command overrides must not disappear during template expansion")
}

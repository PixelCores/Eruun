package traits

import (
	"reflect"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

func TestApplyTraitsDeterministicContainerNames(t *testing.T) {
	component := &model.ApplicationComponent{Name: "api", Traits: toJSONStruct(spec.Traits{
		Init:    []spec.InitTraitSpec{{Image: "busybox:1.37"}, {Name: "prepare", Image: "busybox:1.37"}},
		Sidecar: []spec.SidecarTraitsSpec{{Image: "busybox:1.37"}, {Image: "busybox:1.37"}, {Name: "logs", Image: "busybox:1.37"}},
	})}
	base := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: component.Name, Image: "app:1.0"}},
	}}}}
	want := base.DeepCopy()
	_, err := ApplyTraits(component, want)
	require.NoError(t, err)
	require.Equal(t, "api-init-1", want.Spec.Template.Spec.InitContainers[0].Name)
	require.Equal(t, "prepare", want.Spec.Template.Spec.InitContainers[1].Name)
	require.Equal(t, "api-sidecar-1", want.Spec.Template.Spec.Containers[1].Name)
	require.Equal(t, "api-sidecar-2", want.Spec.Template.Spec.Containers[2].Name)
	require.Equal(t, "logs", want.Spec.Template.Spec.Containers[3].Name)

	for i := 0; i < 10; i++ {
		t.Run("render", func(t *testing.T) {
			t.Parallel()
			got := base.DeepCopy()
			_, err := ApplyTraits(component, got)
			require.NoError(t, err)
			require.Equal(t, want, got, "replaying the same component must not change its Pod template")
		})
	}
}

func TestApplyTraitsBoundsGeneratedContainerNames(t *testing.T) {
	component := &model.ApplicationComponent{Name: strings.Repeat("a", 63), Traits: toJSONStruct(spec.Traits{
		Init:    []spec.InitTraitSpec{{Image: "busybox:1.37"}, {Image: "busybox:1.37"}},
		Sidecar: []spec.SidecarTraitsSpec{{Image: "busybox:1.37"}, {Image: "busybox:1.37"}},
	})}
	workload := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: component.Name}},
	}}}}
	_, err := ApplyTraits(component, workload)
	require.NoError(t, err)
	names := map[string]bool{}
	for _, containers := range [][]corev1.Container{workload.Spec.Template.Spec.Containers, workload.Spec.Template.Spec.InitContainers} {
		for _, container := range containers {
			require.Empty(t, validation.IsDNS1123Label(container.Name))
			require.False(t, names[container.Name])
			names[container.Name] = true
		}
	}
}

func TestApplyTraitsRejectsContainerNameCollisionsBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		traits spec.Traits
	}{
		{name: "main and init", traits: spec.Traits{Init: []spec.InitTraitSpec{{Name: "api", Image: "busybox:1.37"}}}},
		{name: "generated and explicit sidecar", traits: spec.Traits{Sidecar: []spec.SidecarTraitsSpec{{Image: "busybox:1.37"}, {Name: "api-sidecar-1", Image: "busybox:1.37"}}}},
		{name: "init and sidecar", traits: spec.Traits{Init: []spec.InitTraitSpec{{Image: "busybox:1.37"}}, Sidecar: []spec.SidecarTraitsSpec{{Name: "api-init-1", Image: "busybox:1.37"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &model.ApplicationComponent{Name: "api", Traits: toJSONStruct(tc.traits)}
			workload := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api"}}}}}}
			before := workload.DeepCopy()
			_, err := ApplyTraits(component, workload)
			require.ErrorContains(t, err, "duplicate container name")
			require.Equal(t, before, workload)
		})
	}
}

func TestApplyTraitsPreservesNestedExclusions(t *testing.T) {
	component := &model.ApplicationComponent{Name: "api", Traits: toJSONStruct(spec.Traits{
		TargetWorkEnv: map[string]string{"pool": "main"},
		Init: []spec.InitTraitSpec{{Image: "busybox:1.37", Traits: spec.Traits{
			TargetWorkEnv: map[string]string{"pool": "nested"},
			Init:          []spec.InitTraitSpec{{Image: "ignored:1.0"}},
			Sidecar:       []spec.SidecarTraitsSpec{{Image: "ignored:1.0"}},
		}}},
		Sidecar: []spec.SidecarTraitsSpec{{Image: "busybox:1.37", Traits: spec.Traits{
			TargetWorkEnv: map[string]string{"pool": "nested"},
			Init:          []spec.InitTraitSpec{{Image: "ignored:1.0"}},
		}}},
	})}
	workload := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api"}}}}}}
	_, err := ApplyTraits(component, workload)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"pool": "main"}, workload.Spec.Template.Spec.NodeSelector)
	require.Len(t, workload.Spec.Template.Spec.InitContainers, 1)
	require.Len(t, workload.Spec.Template.Spec.Containers, 2)
}

func TestApplyTraitsPreservesProcessingOrder(t *testing.T) {
	component := &model.ApplicationComponent{Name: "api"}
	input := &spec.Traits{
		Storage: []spec.StorageTraitSpec{{Name: "data", Type: "invalid"}},
		EnvFrom: []spec.EnvFromSourceSpec{{Type: "invalid"}},
		Init:    []spec.InitTraitSpec{{}},
	}
	_, err := applyTraitsRecursive(component, nil, input, false)
	require.ErrorContains(t, err, "failed to process trait 'storage'")
	input.Storage = nil
	_, err = applyTraitsRecursive(component, nil, input, false)
	require.ErrorContains(t, err, "failed to process trait 'envFrom'")
	input.EnvFrom = nil
	_, err = applyTraitsRecursive(component, nil, input, false)
	require.ErrorContains(t, err, "failed to process trait 'init'")
}

// The reflection is limited to this schema guard: a new public trait must gain
// an explicit rendering owner instead of silently disappearing from dispatch.
func TestTraitFieldsHaveExplicitRenderingOwners(t *testing.T) {
	owners := map[string]string{
		"Storage": "processor", "EnvFrom": "processor", "Envs": "processor", "TargetWorkEnv": "processor",
		"Resources": "processor", "SecurityPolicy": "processor", "Probes": "processor", "RBAC": "processor",
		"Rollout": "processor", "Init": "processor", "Sidecar": "processor", "Ingress": "processor",
		"Evaluation": "evaluation job builder", "Service": "service builder", "Share": "resource reconciliation",
	}
	typ := reflect.TypeFor[spec.Traits]()
	require.Len(t, owners, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		require.Contains(t, owners, typ.Field(i).Name)
	}
}

package traits

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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

func TestApplyTraitsKeepsContainerConfigurationLocal(t *testing.T) {
	containerTraits := func(scope string) spec.Traits {
		choice, tail := scope+"-trait", scope+"-tail"
		return spec.Traits{
			Envs: []spec.SimplifiedEnvSpec{
				{Name: "CHOICE", ValueFrom: spec.ValueSource{Static: &choice}},
				{Name: "TAIL", ValueFrom: spec.ValueSource{Static: &tail}},
			},
			EnvFrom: []spec.EnvFromSourceSpec{
				{Type: "config", SourceName: scope + "-config"},
				{Type: "secret", SourceName: scope + "-secret"},
			},
			Storage: []spec.StorageTraitSpec{
				{Name: "shared", Type: "persistent", ClaimName: "shared-claim", MountPath: "/shared"},
				{Name: scope + "-data", Type: "ephemeral", MountPath: "/data"},
				{Name: scope + "-duplicate", Type: "ephemeral", MountPath: "/data"},
			},
		}
	}
	input := containerTraits("main")
	input.Init = []spec.InitTraitSpec{{Name: "prepare", Image: "busybox:1.37", Traits: containerTraits("init"),
		Properties: spec.Properties{Env: map[string]string{"CHOICE": "init-base"}},
	}}
	input.Sidecar = []spec.SidecarTraitsSpec{{Name: "helper", Image: "busybox:1.37", Traits: containerTraits("sidecar"),
		Env: map[string]string{"CHOICE": "sidecar-base"},
	}}
	component := &model.ApplicationComponent{Name: "API", Namespace: "apps", Traits: toJSONStruct(input)}
	before, err := json.Marshal(component)
	require.NoError(t, err)
	baseEnvFrom := corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "base-config"}}}
	baseMount := corev1.VolumeMount{Name: "base", MountPath: "/base"}
	existingSidecar := corev1.Container{Name: "observer", Env: []corev1.EnvVar{{Name: "EXISTING", Value: "sidecar"}}}
	existingInit := corev1.Container{Name: "bootstrap", Env: []corev1.EnvVar{{Name: "EXISTING", Value: "init"}}}
	workload := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "api", Env: []corev1.EnvVar{{Name: "CHOICE", Value: "main-base"}}, EnvFrom: []corev1.EnvFromSource{baseEnvFrom}, VolumeMounts: []corev1.VolumeMount{baseMount}},
			existingSidecar,
		},
		InitContainers: []corev1.Container{existingInit},
		Volumes:        []corev1.Volume{{Name: "base", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
	}}}}

	objects, err := ApplyTraits(component, workload)
	require.NoError(t, err)
	pod := workload.Spec.Template.Spec
	require.Len(t, pod.Containers, 3)
	require.Len(t, pod.InitContainers, 2)
	require.Equal(t, existingSidecar, *findContainer(pod.Containers, "observer"))
	require.Equal(t, existingInit, *findContainer(pod.InitContainers, "bootstrap"))
	for _, tc := range []struct {
		scope     string
		container *corev1.Container
	}{
		{scope: "main", container: findContainer(pod.Containers, "api")},
		{scope: "init", container: findContainer(pod.InitContainers, "prepare")},
		{scope: "sidecar", container: findContainer(pod.Containers, "helper")},
	} {
		t.Run(tc.scope, func(t *testing.T) {
			require.NotNil(t, tc.container)
			require.Equal(t, []corev1.EnvVar{
				{Name: "CHOICE", Value: tc.scope + "-base"},
				{Name: "CHOICE", Value: tc.scope + "-trait"},
				{Name: "TAIL", Value: tc.scope + "-tail"},
			}, tc.container.Env, "traits must append in order after existing env entries")
			var wantEnvFrom []corev1.EnvFromSource
			var wantMounts []corev1.VolumeMount
			if tc.scope == "main" {
				wantEnvFrom = append(wantEnvFrom, baseEnvFrom)
				wantMounts = append(wantMounts, baseMount)
			}
			wantEnvFrom = append(wantEnvFrom,
				corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: tc.scope + "-config"}}},
				corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: tc.scope + "-secret"}}},
			)
			wantMounts = append(wantMounts, corev1.VolumeMount{Name: "shared", MountPath: "/shared"}, corev1.VolumeMount{Name: tc.scope + "-data", MountPath: "/data"})
			require.Equal(t, wantEnvFrom, tc.container.EnvFrom)
			require.Equal(t, wantMounts, tc.container.VolumeMounts, "mount paths deduplicate within each container only")
		})
	}
	var volumeNames []string
	for _, volume := range pod.Volumes {
		volumeNames = append(volumeNames, volume.Name)
	}
	require.Equal(t, []string{"base", "shared", "main-data", "main-duplicate", "init-data", "init-duplicate", "sidecar-data", "sidecar-duplicate"}, volumeNames)
	require.Len(t, objects, 1, "the shared PVC must deduplicate across all containers")
	pvc, ok := objects[0].(*corev1.PersistentVolumeClaim)
	require.True(t, ok)
	require.Equal(t, "shared-claim", pvc.Name)
	require.Equal(t, "apps", pvc.Namespace)
	after, err := json.Marshal(component)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after), "rendering must not mutate the stored component")
}

func TestApplyTraitsPreservesProcessingOrder(t *testing.T) {
	component := &model.ApplicationComponent{Name: "api"}
	input := &spec.Traits{
		Storage: []spec.StorageTraitSpec{{Name: "data", Type: "invalid"}},
		EnvFrom: []spec.EnvFromSourceSpec{{Type: "invalid"}},
		Init:    []spec.InitTraitSpec{{}},
	}
	_, err := applyTraitsRecursive(&TraitContext{Component: component, componentTraits: input}, input, false)
	require.ErrorContains(t, err, "failed to process trait 'storage'")
	input.Storage = nil
	_, err = applyTraitsRecursive(&TraitContext{Component: component, componentTraits: input}, input, false)
	require.ErrorContains(t, err, "failed to process trait 'envFrom'")
	input.EnvFrom = nil
	_, err = applyTraitsRecursive(&TraitContext{Component: component, componentTraits: input}, input, false)
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

func TestApplyTraitsIngressDefaultsUseOuterComponentForNestedTraits(t *testing.T) {
	for _, tt := range []struct {
		name        string
		services    []spec.ServiceTraitSpec
		properties  *model.JSONStruct
		wantService string
		wantPort    int32
	}{
		{name: "service trait", services: []spec.ServiceTraitSpec{{Name: "outer-service", Ports: []spec.ServicePortTraitSpec{{Port: 8080}}}}, properties: toJSONStruct(model.Properties{Ports: []model.Ports{{Port: 9090}}}), wantService: "outer-service", wantPort: 8080},
		{name: "properties", properties: toJSONStruct(model.Properties{Ports: []model.Ports{{Port: 9090}}}), wantService: "app-api", wantPort: 9090},
		{name: "invalid properties retain defaults", properties: &model.JSONStruct{"ports": "invalid"}, wantService: "app-api", wantPort: 80},
	} {
		t.Run(tt.name, func(t *testing.T) {
			nested := func(name string) spec.Traits {
				return spec.Traits{
					Service: []spec.ServiceTraitSpec{{Name: "nested-service", Ports: []spec.ServicePortTraitSpec{{Port: 1234}}}},
					Ingress: []spec.IngressTraitsSpec{{Name: name, Routes: []spec.IngressRoutes{{}}}},
				}
			}
			component := &model.ApplicationComponent{Name: "api", Namespace: "apps", Properties: tt.properties, Traits: toJSONStruct(spec.Traits{
				Service: tt.services,
				Ingress: []spec.IngressTraitsSpec{{Routes: []spec.IngressRoutes{{}}}, {Routes: []spec.IngressRoutes{{}}}},
				Init:    []spec.InitTraitSpec{{Name: "prepare", Image: "busybox:1.37", Traits: nested("init-ingress")}},
				Sidecar: []spec.SidecarTraitsSpec{{Name: "helper", Image: "busybox:1.37", Traits: nested("sidecar-ingress")}},
			})}
			before, err := json.Marshal(component)
			require.NoError(t, err)
			workload := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api"}}}}}}
			objects, err := ApplyTraits(component, workload)
			require.NoError(t, err)
			require.Len(t, objects, 4)
			names := []string{"init-ingress", "sidecar-ingress", "api-ingress", "api-ingress-2"}
			for i, object := range objects {
				ingress := object.(*networkingv1.Ingress)
				require.Equal(t, names[i], ingress.Name)
				require.Equal(t, "apps", ingress.Namespace)
				backend := ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service
				require.Equal(t, tt.wantService, backend.Name)
				require.Equal(t, tt.wantPort, backend.Port.Number)
			}
			after, err := json.Marshal(component)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after), "rendering must not mutate the stored component")
		})
	}
}

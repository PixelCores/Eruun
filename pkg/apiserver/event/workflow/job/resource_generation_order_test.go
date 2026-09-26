package job

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestGeneratedWorkloadEnvironmentIsStable(t *testing.T) {
	properties := &model.Properties{Env: map[string]string{"Z_LAST": "last", "A_FIRST": "first", "M_MIDDLE": "middle"}}
	propertiesJSON, err := model.NewJSONStructByStruct(properties)
	require.NoError(t, err)
	component := &model.ApplicationComponent{Name: "api", AppID: "app-1", Namespace: "default", Image: "nginx:1.25", Properties: propertiesJSON}
	want := []corev1.EnvVar{{Name: "A_FIRST", Value: "first"}, {Name: "M_MIDDLE", Value: "middle"}, {Name: "Z_LAST", Value: "last"}}

	for _, tc := range []struct {
		name     string
		generate func() []corev1.EnvVar
	}{
		{name: "deployment", generate: func() []corev1.EnvVar {
			result, err := GenerateWebService(component, properties)
			if err != nil {
				t.Fatal(err)
			}
			require.NotNil(t, result)
			return result.Service.(*appsv1.Deployment).Spec.Template.Spec.Containers[0].Env
		}},
		{name: "statefulset", generate: func() []corev1.EnvVar {
			result, err := GenerateStoreService(component)
			if err != nil {
				t.Fatal(err)
			}
			require.NotNil(t, result)
			return result.Service.(*appsv1.StatefulSet).Spec.Template.Spec.Containers[0].Env
		}},
		{name: "job", generate: func() []corev1.EnvVar { return buildJobContainer(component, properties).Env }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for range 32 {
				require.Equal(t, want, tc.generate())
			}
		})
	}
}

func TestGeneratedNestedContainerEnvironmentIsStable(t *testing.T) {
	env := map[string]string{"Z_LAST": "last", "A_FIRST": "first", "M_MIDDLE": "middle"}
	value := "override"
	nestedTraits := spec.Traits{Envs: []spec.SimplifiedEnvSpec{{Name: "A_FIRST", ValueFrom: spec.ValueSource{Static: &value}}}}
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{
		Init:    []spec.InitTraitSpec{{Name: "init", Image: "busybox:1.36", Properties: spec.Properties{Env: env}, Traits: nestedTraits}},
		Sidecar: []spec.SidecarTraitsSpec{{Name: "sidecar", Image: "busybox:1.36", Env: env, Traits: nestedTraits}},
	})
	require.NoError(t, err)
	component := &model.ApplicationComponent{Name: "api", AppID: "app-1", Namespace: "default", Image: "nginx:1.25", Traits: traitsJSON}
	want := []corev1.EnvVar{{Name: "A_FIRST", Value: "first"}, {Name: "M_MIDDLE", Value: "middle"}, {Name: "Z_LAST", Value: "last"}, {Name: "A_FIRST", Value: "override"}}
	for range 32 {
		result, err := GenerateWebService(component, &model.Properties{})
		if err != nil {
			t.Fatal(err)
		}
		require.NotNil(t, result)
		pod := result.Service.(*appsv1.Deployment).Spec.Template.Spec
		require.Equal(t, want, pod.InitContainers[0].Env)
		require.Equal(t, want, pod.Containers[1].Env)
	}
}

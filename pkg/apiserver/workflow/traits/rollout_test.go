package traits

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestRolloutProcessorAppliesDeploymentStrategy(t *testing.T) {
	maxSurge := intstr.FromString("30%")
	maxUnavailable := intstr.FromInt32(0)
	deploy := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "api", Image: "nginx:1.25"}},
				},
			},
		},
	}

	result, err := (&RolloutProcessor{}).Process(NewTraitContext(&model.ApplicationComponent{Name: "api"}, deploy, &spec.RolloutTraitSpec{
		Type: string(appsv1.RollingUpdateDeploymentStrategyType),
		RollingUpdate: &spec.RolloutRollingUpdateSpec{
			MaxSurge:       &maxSurge,
			MaxUnavailable: &maxUnavailable,
		},
	}))
	require.NoError(t, err)
	require.NoError(t, applyWorkloadTraitResult(result, deploy))

	require.Equal(t, appsv1.RollingUpdateDeploymentStrategyType, deploy.Spec.Strategy.Type)
	require.NotNil(t, deploy.Spec.Strategy.RollingUpdate)
	require.Equal(t, "30%", deploy.Spec.Strategy.RollingUpdate.MaxSurge.StrVal)
	require.Equal(t, intstr.Int, deploy.Spec.Strategy.RollingUpdate.MaxUnavailable.Type)
	require.Equal(t, int32(0), deploy.Spec.Strategy.RollingUpdate.MaxUnavailable.IntVal)
}

func TestRolloutProcessorRejectsDeploymentRollingUpdateWithoutConfig(t *testing.T) {
	deploy := &appsv1.Deployment{}

	_, err := (&RolloutProcessor{}).Process(NewTraitContext(&model.ApplicationComponent{Name: "api"}, deploy, &spec.RolloutTraitSpec{
		Type: string(appsv1.RollingUpdateDeploymentStrategyType),
	}))

	require.Error(t, err)
	require.Contains(t, err.Error(), "requires rollingUpdate")
}

func TestRolloutProcessorRejectsDeploymentRollingUpdateMissingFields(t *testing.T) {
	maxSurge := intstr.FromString("25%")
	maxUnavailable := intstr.FromInt32(0)

	testCases := []struct {
		name          string
		rollingUpdate *spec.RolloutRollingUpdateSpec
		expected      string
	}{
		{
			name:          "empty rollingUpdate",
			rollingUpdate: &spec.RolloutRollingUpdateSpec{},
			expected:      "requires rollingUpdate.maxSurge",
		},
		{
			name: "missing maxSurge",
			rollingUpdate: &spec.RolloutRollingUpdateSpec{
				MaxUnavailable: &maxUnavailable,
			},
			expected: "requires rollingUpdate.maxSurge",
		},
		{
			name: "missing maxUnavailable",
			rollingUpdate: &spec.RolloutRollingUpdateSpec{
				MaxSurge: &maxSurge,
			},
			expected: "requires rollingUpdate.maxUnavailable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deploy := &appsv1.Deployment{}

			_, err := (&RolloutProcessor{}).Process(NewTraitContext(&model.ApplicationComponent{Name: "api"}, deploy, &spec.RolloutTraitSpec{
				Type:          string(appsv1.RollingUpdateDeploymentStrategyType),
				RollingUpdate: tc.rollingUpdate,
			}))

			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expected)
		})
	}
}

func TestRolloutProcessorAppliesStatefulSetUpdateStrategy(t *testing.T) {
	partition := int32(2)
	maxUnavailable := intstr.FromInt32(1)
	statefulSet := &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "mysql", Image: "mysql:8"}},
				},
			},
		},
	}

	result, err := (&RolloutProcessor{}).Process(NewTraitContext(&model.ApplicationComponent{Name: "mysql"}, statefulSet, &spec.RolloutTraitSpec{
		Type: string(appsv1.RollingUpdateStatefulSetStrategyType),
		RollingUpdate: &spec.RolloutRollingUpdateSpec{
			Partition:      &partition,
			MaxUnavailable: &maxUnavailable,
		},
	}))
	require.NoError(t, err)
	require.NoError(t, applyWorkloadTraitResult(result, statefulSet))

	require.Equal(t, appsv1.RollingUpdateStatefulSetStrategyType, statefulSet.Spec.UpdateStrategy.Type)
	require.NotNil(t, statefulSet.Spec.UpdateStrategy.RollingUpdate)
	require.Equal(t, int32(2), *statefulSet.Spec.UpdateStrategy.RollingUpdate.Partition)
	require.Equal(t, int32(1), statefulSet.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable.IntVal)
}

func TestRolloutProcessorRejectsUnsupportedWorkload(t *testing.T) {
	_, err := (&RolloutProcessor{}).Process(NewTraitContext(&model.ApplicationComponent{Name: "pod"}, &corev1.Pod{}, &spec.RolloutTraitSpec{
		Type: string(appsv1.RollingUpdateDeploymentStrategyType),
	}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "supports deployment and statefulset")
}

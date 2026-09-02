package job

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func TestGenerateWebService_UsesCommand(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "log-tick-web",
		AppID:     "app-1",
		Namespace: "default",
		Image:     "busybox:1.36",
		Replicas:  1,
	}
	properties := model.Properties{
		Command: []string{"sh", "-c", "echo ok"},
	}

	result := GenerateWebService(component, &properties)
	if result == nil {
		t.Fatalf("expected result, got nil")
	}

	deploy, ok := result.Service.(*appsv1.Deployment)
	if !ok || deploy == nil {
		t.Fatalf("expected deployment service, got %#v", result.Service)
	}
	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("expected at least one container")
	}

	got := deploy.Spec.Template.Spec.Containers[0].Command
	if !reflect.DeepEqual(got, properties.Command) {
		t.Fatalf("expected command %v, got %v", properties.Command, got)
	}
	require.Equal(t, config.DefaultWorkflowImagePullPolicy, deploy.Spec.Template.Spec.Containers[0].ImagePullPolicy)
}

func TestGenerateWebService_AppliesRolloutTrait(t *testing.T) {
	traitsPlu.ResetTraitProcessorsForTest()
	traitsPlu.RegisterAllProcessors()
	t.Cleanup(traitsPlu.ResetTraitProcessorsForTest)

	maxSurge := intstr.FromString("25%")
	maxUnavailable := intstr.FromString("10%")
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{
		Rollout: &spec.RolloutTraitSpec{
			Type: string(appsv1.RollingUpdateDeploymentStrategyType),
			RollingUpdate: &spec.RolloutRollingUpdateSpec{
				MaxSurge:       &maxSurge,
				MaxUnavailable: &maxUnavailable,
			},
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:      "api",
		AppID:     "app-1",
		Namespace: "default",
		Image:     "nginx:1.25",
		Replicas:  2,
		Traits:    traitsJSON,
	}

	result := GenerateWebService(component, &model.Properties{})
	require.NotNil(t, result)

	deploy, ok := result.Service.(*appsv1.Deployment)
	require.True(t, ok)
	require.Equal(t, appsv1.RollingUpdateDeploymentStrategyType, deploy.Spec.Strategy.Type)
	require.NotNil(t, deploy.Spec.Strategy.RollingUpdate)
	require.Equal(t, "25%", deploy.Spec.Strategy.RollingUpdate.MaxSurge.StrVal)
	require.Equal(t, "10%", deploy.Spec.Strategy.RollingUpdate.MaxUnavailable.StrVal)
}

func TestGenerateWebService_BoundsNameAndUsesStableSelector(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:            "m2605081521cctqpk-api-with-extra-suffix-and-a-very-long-label-tail",
		AppID:           "lyhemnnmr48fmmifdf3f1ukl",
		ResourceAppName: "m2605081521cctqpk",
		Namespace:       "default",
		Image:           "nginx:1.25",
		Replicas:        1,
	}
	properties := &model.Properties{
		Labels: map[string]string{
			"tier": "backend",
			"name": "penalty shootout 2026-m2606241344ccufxh-backend",
		},
	}

	result := GenerateWebService(component, properties)
	require.NotNil(t, result)

	deploy, ok := result.Service.(*appsv1.Deployment)
	require.True(t, ok)
	require.LessOrEqual(t, len(deploy.Name), 63)
	require.LessOrEqual(t, len(deploy.Labels[config.LabelManagedBy]), 63)
	require.Equal(t, map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: naming.BoundedLabelValue(component.Name),
	}, deploy.Spec.Selector.MatchLabels)
	require.Equal(t, component.AppID, deploy.Spec.Template.Labels[config.LabelAppID])
	require.Equal(t, naming.BoundedLabelValue(component.Name), deploy.Spec.Template.Labels[config.LabelComponentName])
	require.LessOrEqual(t, len(deploy.Spec.Template.Labels[config.LabelComponentName]), 63)
	require.Equal(t, "backend", deploy.Spec.Template.Labels["tier"])
	require.Equal(t, "penalty-shootout-2026-m2606241344ccufxh-backend", deploy.Labels["name"])
	require.Equal(t, "penalty-shootout-2026-m2606241344ccufxh-backend", deploy.Spec.Template.Labels["name"])
}

func TestIsDeploymentChangedWhenConfiguredStrategyDiffers(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
	})
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{
		Type: appsv1.RecreateDeploymentStrategyType,
	})

	require.True(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedWhenDesiredStrategyRemoved(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{
		Type: appsv1.RecreateDeploymentStrategyType,
	})
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})

	require.True(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedWhenDesiredStrategyRemovedFromCustomRollingUpdate(t *testing.T) {
	maxSurge := intstr.FromString("50%")
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{
			MaxSurge: &maxSurge,
		},
	})
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})

	require.True(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedWhenReplicasDiffer(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Spec.Replicas = int32Ptr(1)
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Spec.Replicas = int32Ptr(3)

	require.True(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedDoesNotResetDefaultRollingUpdateStrategy(t *testing.T) {
	maxSurge := intstr.FromString(defaultDeploymentRollingUpdatePercent)
	maxUnavailable := intstr.FromString(defaultDeploymentRollingUpdatePercent)
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{
			MaxSurge:       &maxSurge,
			MaxUnavailable: &maxUnavailable,
		},
	})
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})

	require.False(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedWhenImagePullPolicyDiffers(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullNever

	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Spec.Template.Spec.Containers[0].ImagePullPolicy = config.DefaultWorkflowImagePullPolicy

	require.True(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedWhenInitContainerImagePullPolicyDiffers(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Spec.Template.Spec.InitContainers = []corev1.Container{{
		Name:            "init",
		Image:           "busybox:1.36",
		ImagePullPolicy: corev1.PullNever,
	}}

	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Spec.Template.Spec.InitContainers = []corev1.Container{{
		Name:            "init",
		Image:           "busybox:1.36",
		ImagePullPolicy: config.DefaultWorkflowImagePullPolicy,
	}}

	require.True(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedIgnoresImmutableSelectorOnlyDrift(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "API",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "API",
	}
	desired := current.DeepCopy()
	desired.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: naming.BoundedLabelValue("API"),
	}}
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: naming.BoundedLabelValue("API"),
	}

	require.False(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedWhenVolumeSourceDiffers(t *testing.T) {
	oldHostPathType := corev1.HostPathDirectory
	newHostPathType := corev1.HostPathFile
	tests := []struct {
		name    string
		current corev1.VolumeSource
		desired corev1.VolumeSource
	}{
		{
			name:    "persistent volume claim",
			current: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "old-claim"}},
			desired: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: "new-claim",
			}},
		},
		{
			name:    "secret",
			current: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "old-secret"}},
			desired: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: "new-secret",
			}},
		},
		{
			name:    "emptydir",
			current: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			desired: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium: corev1.StorageMediumMemory,
			}},
		},
		{
			name: "hostpath",
			current: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: "/data/old",
				Type: &oldHostPathType,
			}},
			desired: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: "/data/new",
				Type: &newHostPathType,
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
			current.Spec.Template.Spec.Volumes = []corev1.Volume{{
				Name:         "runtime-data",
				VolumeSource: tc.current,
			}}
			desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
			desired.Spec.Template.Spec.Volumes = []corev1.Volume{{
				Name:         "runtime-data",
				VolumeSource: tc.desired,
			}}

			require.True(t, isDeploymentChanged(current, desired))
		})
	}
}

func TestIsDeploymentChangedWhenPodTraitFieldDiffers(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(current, desired *appsv1.Deployment)
	}{
		{
			name: "env valueFrom",
			mutate: func(current, desired *appsv1.Deployment) {
				current.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "POD_NAME"}}
				desired.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{
					Name: "POD_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
					},
				}}
			},
		},
		{
			name: "envFrom",
			mutate: func(_, desired *appsv1.Deployment) {
				desired.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
					ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "app-env"},
					},
				}}
			},
		},
		{
			name: "readiness probe",
			mutate: func(_, desired *appsv1.Deployment) {
				desired.Spec.Template.Spec.Containers[0].ReadinessProbe = &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/ready",
							Port: intstr.FromInt(8080),
						},
					},
				}
			},
		},
		{
			name: "security context",
			mutate: func(_, desired *appsv1.Deployment) {
				desired.Spec.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
					RunAsNonRoot: boolPtr(true),
				}
			},
		},
		{
			name: "node selector",
			mutate: func(_, desired *appsv1.Deployment) {
				desired.Spec.Template.Spec.NodeSelector = map[string]string{"disk": "ssd"}
			},
		},
		{
			name: "service account",
			mutate: func(_, desired *appsv1.Deployment) {
				desired.Spec.Template.Spec.ServiceAccountName = "runtime-sa"
			},
		},
		{
			name: "automount token",
			mutate: func(_, desired *appsv1.Deployment) {
				desired.Spec.Template.Spec.AutomountServiceAccountToken = boolPtr(false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
			desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
			tc.mutate(current, desired)

			require.True(t, isDeploymentChanged(current, desired))
		})
	}
}

func TestIsDeploymentChangedIgnoresPodSpecDefaults(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})

	current.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	current.Spec.Template.Spec.DNSPolicy = corev1.DNSClusterFirst
	current.Spec.Template.Spec.SchedulerName = corev1.DefaultSchedulerName
	current.Spec.Template.Spec.TerminationGracePeriodSeconds = int64Ptr(30)
	current.Spec.Template.Spec.EnableServiceLinks = boolPtr(true)
	current.Spec.Template.Spec.Containers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
	current.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	current.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{
		ContainerPort: 8080,
		Protocol:      corev1.ProtocolTCP,
	}}
	desired.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{
		ContainerPort: 8080,
	}}

	current.Spec.Template.Spec.Containers[0].ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/ready",
				Port:   intstr.FromInt(8080),
				Scheme: corev1.URISchemeHTTP,
			},
		},
		TimeoutSeconds:   1,
		PeriodSeconds:    10,
		SuccessThreshold: 1,
		FailureThreshold: 3,
	}
	desired.Spec.Template.Spec.Containers[0].ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/ready",
				Port: intstr.FromInt(8080),
			},
		},
	}

	require.False(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedWhenLiveOnlyCustomMetadataWouldBeRemoved(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Labels = map[string]string{
		config.LabelAppID:   "app-1",
		"external.io/owner": "platform",
	}
	current.Annotations = map[string]string{
		"external.io/revision": "42",
	}
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
		"mesh.injected":           "true",
	}
	current.Spec.Template.Annotations = map[string]string{
		"prometheus.io/scrape": "true",
	}

	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Labels = map[string]string{
		config.LabelAppID: "app-1",
	}
	desired.Spec.Selector = current.Spec.Selector.DeepCopy()
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}

	require.True(t, isDeploymentChanged(current, desired))
}

func TestIsDeploymentChangedIgnoresLiveOnlySystemLabels(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Labels = map[string]string{
		config.LabelAppID:     "app-1",
		config.LabelShareName: "shared",
	}
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
		config.LabelShareStrategy: string(config.ShareStrategyIgnore),
	}

	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Labels = map[string]string{
		config.LabelAppID: "app-1",
	}
	desired.Spec.Selector = current.Spec.Selector.DeepCopy()
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}

	require.False(t, isDeploymentChanged(current, desired))
}

func TestDeploymentForExistingUpdatePreservesImmutableSelector(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.ResourceVersion = "42"
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		"legacy-selector": "true",
	}}
	current.Spec.Template.Labels = map[string]string{
		"legacy-selector": "true",
	}

	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
		"tier":                    "backend",
	}

	update := deploymentForExistingUpdate(current, desired)

	require.Equal(t, "42", update.ResourceVersion)
	require.Equal(t, current.Spec.Selector, update.Spec.Selector)
	require.Equal(t, "true", update.Spec.Template.Labels["legacy-selector"])
	require.Equal(t, "app-1", update.Spec.Template.Labels[config.LabelAppID])
	require.Equal(t, "api", update.Spec.Template.Labels[config.LabelComponentName])
	require.Equal(t, "backend", update.Spec.Template.Labels["tier"])
	require.NotSame(t, desired, update)
	require.Equal(t, "api", desired.Spec.Selector.MatchLabels[config.LabelComponentName])
}

func TestDeploymentForExistingUpdatePreservesSystemLabelsAndRemovesStaleCustomMetadata(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Name = "web-api"
	current.Namespace = "default"
	current.ResourceVersion = "42"
	current.Labels = map[string]string{
		config.LabelManagedBy: config.ManagedByEruun,
		config.LabelShareName: "shared",
		"external.io/owner":   "platform",
	}
	current.Annotations = map[string]string{
		"checksum/config":      "old-top",
		"external.io/revision": "42",
	}
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		"legacy-selector": "true",
	}}
	current.Spec.Template.Labels = map[string]string{
		"legacy-selector":         "true",
		"mesh.injected":           "true",
		"tier":                    "old",
		config.LabelShareStrategy: string(config.ShareStrategyIgnore),
	}
	current.Spec.Template.Annotations = map[string]string{
		"checksum/config":      "old-template",
		"prometheus.io/scrape": "true",
	}

	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Labels = map[string]string{
		config.LabelManagedBy: config.ManagedByEruun,
	}
	desired.Annotations = map[string]string{
		"checksum/config":       "new-top",
		"eruun.io/component-id": "7",
	}
	desired.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
		"tier":                    "backend",
	}
	desired.Spec.Template.Annotations = map[string]string{
		"checksum/config": "new-template",
	}

	update := deploymentForExistingUpdate(current, desired)

	require.Equal(t, "web-api", update.Name)
	require.Equal(t, "default", update.Namespace)
	require.Equal(t, "42", update.ResourceVersion)
	require.Equal(t, config.ManagedByEruun, update.Labels[config.LabelManagedBy])
	require.Equal(t, "shared", update.Labels[config.LabelShareName])
	require.NotContains(t, update.Labels, "external.io/owner")
	require.NotContains(t, update.Annotations, "external.io/revision")
	require.Equal(t, "new-top", update.Annotations["checksum/config"])
	require.Equal(t, "7", update.Annotations["eruun.io/component-id"])
	require.Equal(t, current.Spec.Selector, update.Spec.Selector)
	require.Equal(t, "true", update.Spec.Template.Labels["legacy-selector"])
	require.Equal(t, string(config.ShareStrategyIgnore), update.Spec.Template.Labels[config.LabelShareStrategy])
	require.NotContains(t, update.Spec.Template.Labels, "mesh.injected")
	require.Equal(t, "backend", update.Spec.Template.Labels["tier"])
	require.Equal(t, "api", update.Spec.Template.Labels[config.LabelComponentName])
	require.NotContains(t, update.Spec.Template.Annotations, "prometheus.io/scrape")
	require.Equal(t, "new-template", update.Spec.Template.Annotations["checksum/config"])
}

func TestDeploymentForExistingUpdateReplacesVolumeMounts(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.ResourceVersion = "42"
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		"legacy-selector": "true",
	}}
	current.Spec.Template.Labels = map[string]string{
		"legacy-selector": "true",
		"mesh.injected":   "true",
	}
	current.Spec.Template.Annotations = map[string]string{
		"prometheus.io/scrape": "true",
	}
	current.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name:      "runtime-logs",
		MountPath: "/app/runtime",
	}}
	current.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name:         "runtime-logs",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}

	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}
	desired.Spec.Template.Annotations = map[string]string{
		"checksum/config": "new-template",
	}
	desired.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name:      "logs",
		MountPath: "/app/logs",
	}}
	desired.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name:         "logs",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}

	update := deploymentForExistingUpdate(current, desired)

	require.Equal(t, current.Spec.Selector, update.Spec.Selector)
	require.Equal(t, "true", update.Spec.Template.Labels["legacy-selector"])
	require.NotContains(t, update.Spec.Template.Labels, "mesh.injected")
	require.NotContains(t, update.Spec.Template.Annotations, "prometheus.io/scrape")
	require.Equal(t, "new-template", update.Spec.Template.Annotations["checksum/config"])
	require.Equal(t, []corev1.VolumeMount{{
		Name:      "logs",
		MountPath: "/app/logs",
	}}, update.Spec.Template.Spec.Containers[0].VolumeMounts)
	require.Equal(t, []corev1.Volume{{
		Name:         "logs",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}, update.Spec.Template.Spec.Volumes)
}

func TestDeployJobCtlRunReplacesStaleDeploymentVolumeMounts(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	deployName := buildWebServiceName("api", "app-1")
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Name = deployName
	current.Namespace = "default"
	current.Labels = map[string]string{
		config.LabelShareName: "shared",
		"external.io/owner":   "platform",
	}
	current.Annotations = map[string]string{
		"external.io/revision": "42",
	}
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
		"mesh.injected":           "true",
	}
	current.Spec.Template.Annotations = map[string]string{
		"prometheus.io/scrape": "true",
	}
	current.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name:      "volume-iefct",
		MountPath: "/app/log",
	}}
	current.Spec.Template.Spec.Containers = append(current.Spec.Template.Spec.Containers, corev1.Container{
		Name:  "logs-sidecar",
		Image: "vector:latest",
		VolumeMounts: []corev1.VolumeMount{
			{Name: "vector-config", MountPath: "/etc/vector", ReadOnly: true},
			{Name: "volume-iefct", MountPath: "/app/log"},
		},
	})
	current.Spec.Template.Spec.Volumes = incidentLogVolumes()

	client := fake.NewSimpleClientset(current)
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Namespace = "default"
	desired.Labels = map[string]string{
		config.LabelAppID: "app-1",
	}
	desired.Annotations = map[string]string{
		"eruun.io/component-id": "7",
	}
	desired.Spec.Selector = current.Spec.Selector.DeepCopy()
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}
	desired.Spec.Template.Annotations = map[string]string{
		"checksum/config": "new-template",
	}
	desired.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name:      "volume-iefct",
		MountPath: "/app/conf",
	}}
	desired.Spec.Template.Spec.Containers = append(desired.Spec.Template.Spec.Containers, corev1.Container{
		Name:  "logs-sidecar",
		Image: "vector:latest",
		VolumeMounts: []corev1.VolumeMount{
			{Name: "vector-config", MountPath: "/etc/vector", ReadOnly: true},
			{Name: "volume-iefct", MountPath: "/app/log"},
		},
	})
	desired.Spec.Template.Spec.Volumes = incidentLogVolumes()

	jobTask := &model.JobTask{
		Name:      "api",
		AppID:     "app-1",
		Namespace: "default",
		JobType:   string(config.JobDeploy),
		JobInfo:   desired,
	}
	ctl := NewDeployJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	require.NoError(t, ctl.run(ctx))

	updated, err := client.AppsV1().Deployments("default").Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "app-1", updated.Labels[config.LabelAppID])
	require.Equal(t, "shared", updated.Labels[config.LabelShareName])
	require.NotContains(t, updated.Labels, "external.io/owner")
	require.NotContains(t, updated.Annotations, "external.io/revision")
	require.Equal(t, "7", updated.Annotations["eruun.io/component-id"])
	require.NotContains(t, updated.Spec.Template.Labels, "mesh.injected")
	require.NotContains(t, updated.Spec.Template.Annotations, "prometheus.io/scrape")
	require.Equal(t, "new-template", updated.Spec.Template.Annotations["checksum/config"])
	main := containerByName(updated.Spec.Template.Spec.Containers, "api")
	require.NotNil(t, main)
	require.Equal(t, []corev1.VolumeMount{{
		Name:      "volume-iefct",
		MountPath: "/app/conf",
	}}, main.VolumeMounts)

	sidecar := containerByName(updated.Spec.Template.Spec.Containers, "logs-sidecar")
	require.NotNil(t, sidecar)
	require.Equal(t, []corev1.VolumeMount{
		{Name: "vector-config", MountPath: "/etc/vector", ReadOnly: true},
		{Name: "volume-iefct", MountPath: "/app/log"},
	}, sidecar.VolumeMounts)
	require.Equal(t, incidentLogVolumes(), updated.Spec.Template.Spec.Volumes)
	require.Equal(t, 1, countClientActions(client, "update", "deployments"))
	require.Equal(t, 0, countClientActions(client, "patch", "deployments"))
}

func TestDeployJobCtlRunSkipsDeploymentWithOnlyLiveSystemLabels(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	deployName := buildWebServiceName("api", "app-1")
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Name = deployName
	current.Namespace = "default"
	current.Labels = map[string]string{
		config.LabelAppID:     "app-1",
		config.LabelShareName: "shared",
	}
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
		config.LabelShareStrategy: string(config.ShareStrategyIgnore),
	}

	client := fake.NewSimpleClientset(current)
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Namespace = "default"
	desired.Labels = map[string]string{
		config.LabelAppID: "app-1",
	}
	desired.Spec.Selector = current.Spec.Selector.DeepCopy()
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}

	jobTask := &model.JobTask{
		Name:      "api",
		AppID:     "app-1",
		Namespace: "default",
		JobType:   string(config.JobDeploy),
		JobInfo:   desired,
	}
	ctl := NewDeployJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	require.NoError(t, ctl.run(ctx))

	updated, err := client.AppsV1().Deployments("default").Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "shared", updated.Labels[config.LabelShareName])
	require.Equal(t, string(config.ShareStrategyIgnore), updated.Spec.Template.Labels[config.LabelShareStrategy])
	require.Equal(t, 0, countClientActions(client, "update", "deployments"))
	require.Equal(t, 0, countClientActions(client, "patch", "deployments"))
}

func TestDeployJobCtlRunSkipsDeploymentWithOnlyImmutableSelectorDrift(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	deployName := buildWebServiceName("api", "app-1")
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Name = deployName
	current.Namespace = "default"
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "API",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "API",
	}

	client := fake.NewSimpleClientset(current)
	desired := current.DeepCopy()
	desired.Namespace = "default"
	desired.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: naming.BoundedLabelValue("API"),
	}}
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: naming.BoundedLabelValue("API"),
	}

	jobTask := &model.JobTask{
		Name:      "api",
		AppID:     "app-1",
		Namespace: "default",
		JobType:   string(config.JobDeploy),
		JobInfo:   desired,
	}
	ctl := NewDeployJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	require.NoError(t, ctl.run(ctx))

	updated, err := client.AppsV1().Deployments("default").Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, current.Spec.Selector, updated.Spec.Selector)
	require.Equal(t, "API", updated.Spec.Template.Labels[config.LabelComponentName])
	require.Equal(t, 0, countClientActions(client, "update", "deployments"))
	require.Equal(t, 0, countClientActions(client, "patch", "deployments"))
}

func TestDeployJobCtlRunUpdatesDeploymentReplicasOnly(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	deployName := buildWebServiceName("api", "app-1")
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Name = deployName
	current.Namespace = "default"
	current.Spec.Replicas = int32Ptr(1)
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}

	client := fake.NewSimpleClientset(current)
	desired := current.DeepCopy()
	desired.Namespace = "default"
	desired.Spec.Replicas = int32Ptr(3)

	jobTask := &model.JobTask{
		Name:      "api",
		AppID:     "app-1",
		Namespace: "default",
		TaskID:    "task-replica-only",
		JobType:   string(config.JobDeploy),
		JobInfo:   desired,
	}
	ctl := NewDeployJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	require.NoError(t, ctl.run(ctx))

	updated, err := client.AppsV1().Deployments("default").Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, updated.Spec.Replicas)
	require.Equal(t, int32(3), *updated.Spec.Replicas)
	require.NotContains(t, updated.Spec.Template.Annotations, config.AnnotationJobTaskID)
	require.Empty(t, ctl.expectedPodTemplateAnnotations)
	require.Equal(t, 1, countClientActions(client, "update", "deployments"))
	require.Equal(t, 0, countClientActions(client, "patch", "deployments"))
}

func TestDeployJobCtlRunAnnotatesChangedPodTemplateWithTaskID(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	deployName := buildWebServiceName("api", "app-1")
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Name = deployName
	current.Namespace = "default"
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}
	current.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name:      "logs",
		MountPath: "/app/logs",
	}}

	client := fake.NewSimpleClientset(current)
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Namespace = "default"
	desired.Spec.Selector = current.Spec.Selector.DeepCopy()
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}
	desired.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name:      "logs",
		MountPath: "/app/runtime",
	}}

	jobTask := &model.JobTask{
		Name:      "api",
		AppID:     "app-1",
		Namespace: "default",
		TaskID:    "task-template-1",
		JobType:   string(config.JobDeploy),
		JobInfo:   desired,
	}
	ctl := NewDeployJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	require.NoError(t, ctl.run(ctx))

	updated, err := client.AppsV1().Deployments("default").Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "task-template-1", updated.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, map[string]string{
		config.AnnotationJobTaskID: "task-template-1",
	}, ctl.expectedPodTemplateAnnotations)
	require.Equal(t, 1, countClientActions(client, "update", "deployments"))
}

func TestDeployJobCtlRunRestoresTaskAnnotationForUpToDateDeployment(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	deployName := buildWebServiceName("api", "app-1")
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Name = deployName
	current.Namespace = "default"
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}
	current.Spec.Template.Annotations = map[string]string{
		config.AnnotationJobTaskID: "task-template-1",
	}

	client := fake.NewSimpleClientset(current)
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Namespace = "default"
	desired.Spec.Selector = current.Spec.Selector.DeepCopy()
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}

	jobTask := &model.JobTask{
		Name:      "api",
		AppID:     "app-1",
		Namespace: "default",
		TaskID:    "task-template-1",
		JobType:   string(config.JobDeploy),
		JobInfo:   desired,
	}
	ctl := NewDeployJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	require.NoError(t, ctl.run(ctx))

	updated, err := client.AppsV1().Deployments("default").Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "task-template-1", updated.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, map[string]string{
		config.AnnotationJobTaskID: "task-template-1",
	}, ctl.expectedPodTemplateAnnotations)
	require.Equal(t, 0, countClientActions(client, "update", "deployments"))
	require.Equal(t, 0, countClientActions(client, "patch", "deployments"))
}

func TestDeploymentPodTemplateChangedPreservesExistingTaskIDAnnotation(t *testing.T) {
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Spec.Template.Annotations = map[string]string{
		config.AnnotationJobTaskID: "old-task",
	}
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})

	require.False(t, deploymentPodTemplateChanged(current, desired))
	require.False(t, isDeploymentChanged(current, desired))
}

func TestDeployJobCtlRunReplacesStaleDeploymentVolumeSource(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	deployName := buildWebServiceName("api", "app-1")
	current := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	current.Name = deployName
	current.Namespace = "default"
	current.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}}
	current.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}
	current.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name:      "runtime-data",
		MountPath: "/app/data",
	}}
	current.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name: "runtime-data",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "old-data"},
		},
	}}

	client := fake.NewSimpleClientset(current)
	desired := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	desired.Namespace = "default"
	desired.Spec.Selector = current.Spec.Selector.DeepCopy()
	desired.Spec.Template.Labels = map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}
	desired.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name:      "runtime-data",
		MountPath: "/app/data",
	}}
	desired.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name: "runtime-data",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "new-data"},
		},
	}}

	jobTask := &model.JobTask{
		Name:      "api",
		AppID:     "app-1",
		Namespace: "default",
		JobType:   string(config.JobDeploy),
		JobInfo:   desired,
	}
	ctl := NewDeployJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	require.NoError(t, ctl.run(ctx))

	updated, err := client.AppsV1().Deployments("default").Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, desired.Spec.Template.Spec.Volumes, updated.Spec.Template.Spec.Volumes)
	require.Equal(t, 1, countClientActions(client, "update", "deployments"))
	require.Equal(t, 0, countClientActions(client, "patch", "deployments"))
}

func TestDeployJobCtlWaitTimeoutWithPodAbnormalReturnsFailed(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(newWaitTestPod("app-1", "api", "CrashLoopBackOff"))

	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "api",
				AppID:   "app-1",
				Timeout: 1,
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, statusErr.Status)
	require.Contains(t, err.Error(), "CrashLoopBackOff")
}

func TestDeployJobCtlWaitRequiresResourceWaiter(t *testing.T) {
	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:      "api",
				Namespace: "default",
				AppID:     "app-1",
				Timeout:   1,
			},
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "resource waiter is required")
}

func TestDeployJobCtlWaitUsesBoundedComponentLabel(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(newWaitReadyTestPod("app-1", naming.BoundedLabelValue("API")))

	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "API",
				AppID:   "app-1",
				Timeout: 1,
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.NoError(t, err)
}

func TestDeployJobCtlWaitTimeoutWithPendingPodReturnsTimeout(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(newWaitTestPod("app-1", "api", "ContainerCreating"))

	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "api",
				AppID:   "app-1",
				Timeout: 1,
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, statusErr.Status)
}

func TestDeployJobCtlWaitIgnoresReadyPodWithDifferentImage(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(withWaitPodImage(newWaitReadyTestPod("app-1", "api"), "nginx:1.25"))
	deploy := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	deploy.Spec.Template.Spec.Containers[0].Image = "nginx:1.26"

	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "api",
				AppID:   "app-1",
				Timeout: 1,
				JobInfo: deploy,
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, statusErr.Status)
}

func TestDeployJobCtlWaitUsesExpectedImage(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(withWaitPodImage(newWaitReadyTestPod("app-1", "api"), "nginx:1.26"))
	deploy := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	deploy.Spec.Template.Spec.Containers[0].Image = "nginx:1.26"

	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "api",
				AppID:   "app-1",
				Timeout: 1,
				JobInfo: deploy,
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	require.NoError(t, ctl.wait(context.Background()))
}

func TestDeployJobCtlWaitRequiresExpectedTaskAnnotationWithSameImage(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	oldReady := withWaitPodImage(newWaitReadyTestPod("app-1", "api"), "nginx:1.25")
	oldReady.Name = "api-old"
	newAbnormal := withWaitPodAnnotations(withWaitPodImage(newWaitTestPod("app-1", "api", "CrashLoopBackOff"), "nginx:1.25"), map[string]string{
		config.AnnotationJobTaskID: "task-template-1",
	})
	newAbnormal.Name = "api-new"
	waiter.OnPodAdd(oldReady)
	waiter.OnPodAdd(newAbnormal)

	deploy := deploymentWithStrategy(appsv1.DeploymentStrategy{})
	deploy.Spec.Template.Spec.Containers[0].Image = "nginx:1.25"

	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "api",
				AppID:   "app-1",
				Timeout: 1,
				JobInfo: deploy,
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
		expectedPodTemplateAnnotations: map[string]string{
			config.AnnotationJobTaskID: "task-template-1",
		},
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, statusErr.Status)
	require.Contains(t, err.Error(), "CrashLoopBackOff")
}

func withWaitPodImage(pod *corev1.Pod, image string) *corev1.Pod {
	if pod == nil {
		return nil
	}
	pod.Spec.Containers = []corev1.Container{{
		Name:  "app",
		Image: image,
	}}
	return pod
}

func withWaitPodAnnotations(pod *corev1.Pod, annotations map[string]string) *corev1.Pod {
	if pod == nil {
		return nil
	}
	pod.Annotations = annotations
	return pod
}

func newWaitReadyTestPod(appID, componentName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-pod",
			Namespace: "default",
			Labels: map[string]string{
				config.LabelAppID:         appID,
				config.LabelComponentName: componentName,
				config.LabelComponentID:   "7",
			},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  "app",
					Ready: true,
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{},
					},
				},
			},
		},
	}
}

func newWaitTestPod(appID, componentName, waitingReason string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-pod",
			Namespace: "default",
			Labels: map[string]string{
				config.LabelAppID:         appID,
				config.LabelComponentName: componentName,
				config.LabelComponentID:   "7",
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  waitingReason,
							Message: "waiting",
						},
					},
				},
			},
		},
	}
}

func deploymentWithStrategy(strategy appsv1.DeploymentStrategy) *appsv1.Deployment {
	return &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Strategy: strategy,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "api",
						Image: "nginx:1.25",
					}},
				},
			},
		},
	}
}

func incidentLogVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "vector-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "vector-config"},
				},
			},
		},
		{
			Name:         "volume-iefct",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	}
}

func containerByName(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

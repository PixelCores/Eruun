package traits

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

// TestEnvProcessor comprehensively tests the env trait processing.
func TestEnvProcessor(t *testing.T) {

	// --- Test Data Setup ---

	// Mock Component with a main container and an init container trait
	mockComponent := &model.ApplicationComponent{
		Name:  "main-app",
		Image: "main-app:v1",
		Traits: toJSONStruct(model.Traits{
			// Top-level env trait for the main container
			EnvFrom: []spec.EnvFromSourceSpec{
				{Type: "config", SourceName: "main-app-config"},
				{Type: "secret", SourceName: "main-app-secret"},
			},
			// Init container trait with its own nested env trait
			Init: []spec.InitTraitSpec{
				{
					Name:  "my-init-container",
					Image: "init:v1",
					Traits: model.Traits{
						EnvFrom: []spec.EnvFromSourceSpec{
							{Type: "config", SourceName: "init-container-config"},
						},
					},
				},
			},
		}),
	}

	// Mock base workload (Deployment)
	mockWorkload := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-deployment",
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "main-app", // Name matches component name
							Image: "main-app:v1",
						},
					},
				},
			},
		},
	}

	// --- Test Cases ---

	t.Run("Correctly applies top-level and nested env traits", func(t *testing.T) {
		// --- Arrange ---
		// Deep copy workload to avoid modification across tests
		workload := mockWorkload.DeepCopy()

		// --- Act ---
		_, err := ApplyTraits(mockComponent, workload, nil)

		// --- Assert ---
		assert.NoError(t, err)

		// 1. Verify the main container
		mainContainer := findContainer(workload.Spec.Template.Spec.Containers, "main-app")
		assert.NotNil(t, mainContainer, "Main container should be found")
		assert.Len(t, mainContainer.EnvFrom, 2, "Main container should have 2 envFrom sources")
		assert.Equal(t, "main-app-config", mainContainer.EnvFrom[0].ConfigMapRef.Name)
		assert.Equal(t, "main-app-secret", mainContainer.EnvFrom[1].SecretRef.Name)

		// 2. Verify the init container
		assert.Len(t, workload.Spec.Template.Spec.InitContainers, 1, "Should be one init container")
		initContainer := findContainer(workload.Spec.Template.Spec.InitContainers, "my-init-container")
		assert.NotNil(t, initContainer, "Init container should be found")
		assert.Len(t, initContainer.EnvFrom, 1, "Init container should have 1 envFrom source")
		assert.Equal(t, "init-container-config", initContainer.EnvFrom[0].ConfigMapRef.Name)

		// 3. Marshal and print for visual verification
		yamlBytes, err := yaml.Marshal(workload.Spec.Template.Spec)
		require.NoError(t, err)
		fmt.Println("--- YAML output for TestEnvProcessor ---")
		fmt.Println(string(yamlBytes))
	})

	t.Run("Handles empty traits gracefully", func(t *testing.T) {
		// --- Arrange ---
		workload := mockWorkload.DeepCopy()
		componentWithEmptyTraits := &model.ApplicationComponent{
			Name:   "main-app",
			Image:  "main-app:v1",
			Traits: toJSONStruct(model.Traits{}),
		}

		// --- Act ---
		_, err := ApplyTraits(componentWithEmptyTraits, workload, nil)

		// --- Assert ---
		assert.NoError(t, err)
		mainContainer := findContainer(workload.Spec.Template.Spec.Containers, "main-app")
		assert.Len(t, mainContainer.EnvFrom, 0, "Main container should have no envFrom sources")
		assert.Len(t, workload.Spec.Template.Spec.InitContainers, 0, "Should be no init containers")
	})
}

func TestSecurityPolicyTrait(t *testing.T) {

	component := &model.ApplicationComponent{
		Name:  "main-app",
		Image: "main-app:v1",
		Traits: toJSONStruct(model.Traits{
			SecurityPolicy: &corev1.SecurityContext{
				RunAsUser:                ptr.To[int64](1000),
				RunAsGroup:               ptr.To[int64](1000),
				AllowPrivilegeEscalation: ptr.To(false),
			},
			Init: []spec.InitTraitSpec{
				{
					Name:  "init-task",
					Image: "init:v1",
					Traits: model.Traits{
						SecurityPolicy: &corev1.SecurityContext{
							RunAsUser:  ptr.To[int64](0),
							RunAsGroup: ptr.To[int64](0),
						},
					},
				},
			},
			Sidecar: []spec.SidecarTraitsSpec{
				{
					Name:  "log-agent",
					Image: "log-agent:v1",
					Traits: model.Traits{
						SecurityPolicy: &corev1.SecurityContext{
							RunAsUser: ptr.To[int64](2000),
						},
					},
				},
			},
		}),
	}

	workload := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "test-deployment"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "main-app",
							Image: "main-app:v1",
						},
					},
				},
			},
		},
	}

	_, err := ApplyTraits(component, workload, nil)
	require.NoError(t, err)

	mainContainer := findContainer(workload.Spec.Template.Spec.Containers, "main-app")
	require.NotNil(t, mainContainer)
	require.NotNil(t, mainContainer.SecurityContext)
	require.Equal(t, int64(1000), *mainContainer.SecurityContext.RunAsUser)
	require.Equal(t, int64(1000), *mainContainer.SecurityContext.RunAsGroup)
	require.NotNil(t, mainContainer.SecurityContext.AllowPrivilegeEscalation)
	require.False(t, *mainContainer.SecurityContext.AllowPrivilegeEscalation)

	initContainer := findContainer(workload.Spec.Template.Spec.InitContainers, "init-task")
	require.NotNil(t, initContainer)
	require.NotNil(t, initContainer.SecurityContext)
	require.Equal(t, int64(0), *initContainer.SecurityContext.RunAsUser)
	require.Equal(t, int64(0), *initContainer.SecurityContext.RunAsGroup)

	sidecarContainer := findContainer(workload.Spec.Template.Spec.Containers, "log-agent")
	require.NotNil(t, sidecarContainer)
	require.NotNil(t, sidecarContainer.SecurityContext)
	require.Equal(t, int64(2000), *sidecarContainer.SecurityContext.RunAsUser)
}

func TestNodePoolTrait(t *testing.T) {

	component := &model.ApplicationComponent{
		Name:  "backend",
		Image: "nginx:latest",
		Traits: toJSONStruct(model.Traits{
			NodePool: "batch",
		}),
	}

	workload := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "backend"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "backend",
							Image: "nginx:latest",
						},
					},
				},
			},
		},
	}

	_, err := ApplyTraits(component, workload, &model.NodePool{Name: "batch", NodeSelector: map[string]string{"app": "lab"}, Tolerations: []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}})
	require.NoError(t, err)
	require.Equal(t, "lab", workload.Spec.Template.Spec.NodeSelector["app"])
	require.Len(t, workload.Spec.Template.Spec.Tolerations, 1)
}

// --- Helper Functions ---

// toJSONStruct converts a struct to a model.JSONStruct for easy test setup.
func toJSONStruct(v interface{}) *model.JSONStruct {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var data model.JSONStruct
	if err := json.Unmarshal(b, &data); err != nil {
		panic(err)
	}
	return &data
}

// findContainer is a helper to find a container by name in a slice of containers.
func findContainer(containers []corev1.Container, name string) *corev1.Container {
	for i, c := range containers {
		if c.Name == name {
			return &containers[i]
		}
	}
	return nil
}

func TestApplyTraits_FinalSimplifiedEnvs(t *testing.T) {

	// 1. Define the input component with the final, structured envs spec.
	staticValue := "some_static_value"
	fieldPath := "metadata.namespace"
	traitsStruct := &model.Traits{
		Envs: []spec.SimplifiedEnvSpec{
			{
				Name:      "STATIC_VAR",
				ValueFrom: spec.ValueSource{Static: &staticValue},
			},
			{
				Name: "PASSWORD_FROM_SECRET",
				ValueFrom: spec.ValueSource{Secret: &spec.SecretSelectorSpec{
					Name: "secret/with/slashes",
					Key:  "password",
				}},
			},
			{
				Name: "API_KEY_FROM_CONFIG",
				ValueFrom: spec.ValueSource{Config: &spec.ConfigMapSelectorSpec{
					Name: "config/with/slashes",
					Key:  "api-key",
				}},
			},
			{
				Name:      "MY_POD_NAMESPACE",
				ValueFrom: spec.ValueSource{Field: &fieldPath},
			},
		},
		EnvFrom: []spec.EnvFromSourceSpec{
			{
				Type:       "config",
				SourceName: "another-configmap",
			},
		},
	}
	traitsJSON, err := model.NewJSONStructByStruct(traitsStruct)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:      "test-component",
		Namespace: "test-namespace",
		Traits:    traitsJSON,
	}

	// 2. Define the base workload.
	workload := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: component.Name, Namespace: component.Namespace},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: component.Name, Image: "my-app:1.0"}},
				},
			},
		},
	}

	// 3. Apply the traits.
	_, err = ApplyTraits(component, workload, nil)
	require.NoError(t, err)

	// 4. Assertions
	mainContainer := workload.Spec.Template.Spec.Containers[0]

	// 4.1 Verify EnvFrom sources
	require.Len(t, mainContainer.EnvFrom, 1, "Expected one EnvFrom source")
	require.Equal(t, "another-configmap", mainContainer.EnvFrom[0].ConfigMapRef.Name)

	// 4.2 Verify Envs (translated from SimplifiedEnvSpec)
	require.Len(t, mainContainer.Env, 4, "Expected four environment variables")

	// Static Var
	require.Equal(t, "STATIC_VAR", mainContainer.Env[0].Name)
	require.Equal(t, "some_static_value", mainContainer.Env[0].Value)

	// SecretKeyRef
	require.Equal(t, "PASSWORD_FROM_SECRET", mainContainer.Env[1].Name)
	require.NotNil(t, mainContainer.Env[1].ValueFrom.SecretKeyRef)
	require.Equal(t, "secret/with/slashes", mainContainer.Env[1].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "password", mainContainer.Env[1].ValueFrom.SecretKeyRef.Key)

	// ConfigMapKeyRef
	require.Equal(t, "API_KEY_FROM_CONFIG", mainContainer.Env[2].Name)
	require.NotNil(t, mainContainer.Env[2].ValueFrom.ConfigMapKeyRef)
	require.Equal(t, "config/with/slashes", mainContainer.Env[2].ValueFrom.ConfigMapKeyRef.Name)
	require.Equal(t, "api-key", mainContainer.Env[2].ValueFrom.ConfigMapKeyRef.Key)

	// FieldRef
	require.Equal(t, "MY_POD_NAMESPACE", mainContainer.Env[3].Name)
	require.NotNil(t, mainContainer.Env[3].ValueFrom.FieldRef)
	require.Equal(t, "metadata.namespace", mainContainer.Env[3].ValueFrom.FieldRef.FieldPath)

	// 5. Marshal and print for snapshot verification.
	yamlBytes, err := yaml.Marshal(workload.Spec.Template.Spec.Containers)
	require.NoError(t, err)
	fmt.Println(string(yamlBytes))
}

func TestApplyTraits_InitTrait_WithNestedTraits(t *testing.T) {
	// 1. Define the input component with two init containers sharing a volume.
	traitsStruct := &model.Traits{
		Init: []spec.InitTraitSpec{
			{
				Name:  "init-mysql",
				Image: "kubectl:1.28.5",
				Properties: model.Properties{
					Command: []string{"bash", "-c", ""},
					Env:     map[string]string{"MYSQL_DATABASE": "test"},
				},
				Traits: model.Traits{
					Storage: []spec.StorageTraitSpec{
						{
							Name:      "conf",
							Type:      "config",
							MountPath: "/mnt/conf.d",
						},
						{
							Name:      "config-map",
							Type:      "config",
							MountPath: "/mnt/config-map",
						},
						{
							Name:      "init-scripts",
							Type:      "config",
							MountPath: "/docker-entrypoint-initdb.d",
						},
					},
				},
			},
			{
				Name:  "clone-mysql",
				Image: "xtrabackup:latest",
				Properties: model.Properties{
					Command: []string{"bash", "-c"},
				},
				Traits: model.Traits{
					Storage: []spec.StorageTraitSpec{
						{ //使用稳定存储进行挂载
							Name:      "data",
							Type:      "persistent",
							MountPath: "/var/lib/mysql",
							SubPath:   "mysql",
						},
						{
							Name:      "conf",
							Type:      "config",
							MountPath: "/etc/mysql/conf.d",
						},
					},
				},
			},
		},
	}
	traitsJSON, err := model.NewJSONStructByStruct(traitsStruct)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:      "test-component",
		Namespace: "test-namespace",
		Traits:    traitsJSON,
	}

	// 2. Define the base workload.
	workload := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: component.Name, Namespace: component.Namespace},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: component.Name, Image: "my-app:1.0"}},
				},
			},
		},
	}

	// 3. Apply the traits.
	_, err = ApplyTraits(component, workload, nil)
	require.NoError(t, err)
	require.Len(t, workload.Spec.Template.Spec.InitContainers, 2)
	for _, container := range workload.Spec.Template.Spec.InitContainers {
		require.Equal(t, workflowconfig.DefaultWorkflowImagePullPolicy, container.ImagePullPolicy)
	}

	// 4. Marshal and print for snapshot verification.
	yamlBytes, err := yaml.Marshal(workload.Spec.Template.Spec)
	require.NoError(t, err)
	fmt.Println(string(yamlBytes))
}

func TestResourcesProcessor(t *testing.T) {

	// Test component with resources trait
	component := &model.ApplicationComponent{
		Name:      "test-component",
		Namespace: "test-namespace",
		Traits: toJSONStruct(model.Traits{
			Resources: &spec.ResourceTraitsSpec{
				CPU:         "500m",
				Memory:      "512Mi",
				CPULimit:    "1000m",
				MemoryLimit: "1Gi",
				GPU:         "1",
			},
		}),
	}

	// Base workload
	workload := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: component.Name, Namespace: component.Namespace},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: component.Name, Image: "my-app:1.0"}},
				},
			},
		},
	}

	// Apply traits
	_, err := ApplyTraits(component, workload, nil)
	require.NoError(t, err)

	// Verify resources were applied to the main container
	mainContainer := workload.Spec.Template.Spec.Containers[0]
	assert.NotNil(t, mainContainer.Resources.Requests)
	assert.NotNil(t, mainContainer.Resources.Limits)

	// Check CPU
	cpuReqQty, exists := mainContainer.Resources.Requests[corev1.ResourceCPU]
	assert.True(t, exists)
	assert.Equal(t, resource.MustParse("500m"), cpuReqQty)
	cpuLimitQty, exists := mainContainer.Resources.Limits[corev1.ResourceCPU]
	assert.True(t, exists)
	assert.Equal(t, resource.MustParse("1000m"), cpuLimitQty)

	// Check Memory
	memReqQty, exists := mainContainer.Resources.Requests[corev1.ResourceMemory]
	assert.True(t, exists)
	assert.Equal(t, resource.MustParse("512Mi"), memReqQty)
	memLimitQty, exists := mainContainer.Resources.Limits[corev1.ResourceMemory]
	assert.True(t, exists)
	assert.Equal(t, resource.MustParse("1Gi"), memLimitQty)

	// Check GPU
	gpuQty, exists := mainContainer.Resources.Limits[corev1.ResourceName(spec.ResourceNvidiaGPU)]
	assert.True(t, exists)
	assert.Equal(t, resource.MustParse("1"), gpuQty)
}

func TestResourcesProcessor_WithSidecar(t *testing.T) {

	// Test component with sidecar that has its own resources
	component := &model.ApplicationComponent{
		Name:      "test-component",
		Namespace: "test-namespace",
		Traits: toJSONStruct(model.Traits{
			Sidecar: []spec.SidecarTraitsSpec{
				{
					Name:  "my-sidecar",
					Image: "sidecar:v1",
					Traits: model.Traits{
						Resources: &spec.ResourceTraitsSpec{
							CPU:    "200m",
							Memory: "256Mi",
						},
					},
				},
			},
		}),
	}

	// Base workload
	workload := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: component.Name, Namespace: component.Namespace},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: component.Name, Image: "my-app:1.0"}},
				},
			},
		},
	}

	// Apply traits
	_, err := ApplyTraits(component, workload, nil)
	require.NoError(t, err)

	// Verify sidecar container was created with resources
	require.Len(t, workload.Spec.Template.Spec.Containers, 2, "Should have main container and sidecar")

	sidecarContainer := workload.Spec.Template.Spec.Containers[1]
	assert.Equal(t, "my-sidecar", sidecarContainer.Name)
	assert.NotNil(t, sidecarContainer.Resources.Limits)

	// Check sidecar CPU
	cpuQty, exists := sidecarContainer.Resources.Limits[corev1.ResourceCPU]
	assert.True(t, exists)
	assert.Equal(t, resource.MustParse("200m"), cpuQty)

	// Check sidecar Memory
	memQty, exists := sidecarContainer.Resources.Limits[corev1.ResourceMemory]
	assert.True(t, exists)
	assert.Equal(t, resource.MustParse("256Mi"), memQty)
}

func TestApplyTraits_SidecarTrait_WithNestedTraits(t *testing.T) {
	// 1. Define the input component with a sidecar that has its own storage.
	traitsStruct := &model.Traits{
		Sidecar: []spec.SidecarTraitsSpec{
			{
				Name:  "my-sidecar",
				Image: "sidecar:v1",
				Env:   map[string]string{"SIDECAR_VAR": "value1"},
				Traits: model.Traits{
					Storage: []spec.StorageTraitSpec{
						{
							Name:      "sidecar-data",
							Type:      "ephemeral",
							MountPath: "/data",
						},
					},
				},
			},
		},
	}
	traitsJSON, err := model.NewJSONStructByStruct(traitsStruct)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:      "test-component",
		Namespace: "test-namespace",
		Traits:    traitsJSON,
	}

	// 2. Define the base workload.
	workload := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: component.Name, Namespace: component.Namespace},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: component.Name, Image: "my-app:1.0"}},
				},
			},
		},
	}

	// 3. Apply the traits.
	// Reset processors for a clean test to avoid double registration panics.

	_, err = ApplyTraits(component, workload, nil)
	require.NoError(t, err)

	// 4. Marshal and print for snapshot verification.
	yamlBytes, err := yaml.Marshal(workload.Spec.Template.Spec)
	require.NoError(t, err)
	fmt.Println(string(yamlBytes))

	// 5. Assertions
	require.Len(t, workload.Spec.Template.Spec.Containers, 2, "Expected main container and one sidecar")

	sidecarContainer := findContainer(workload.Spec.Template.Spec.Containers, "my-sidecar")
	require.NotNil(t, sidecarContainer, "Sidecar container should be found")
	require.Equal(t, "sidecar:v1", sidecarContainer.Image)
	require.Equal(t, workflowconfig.DefaultWorkflowImagePullPolicy, sidecarContainer.ImagePullPolicy)

	require.Len(t, sidecarContainer.VolumeMounts, 1, "Sidecar should have one volume mount")
	require.Equal(t, "sidecar-data", sidecarContainer.VolumeMounts[0].Name)
	require.Equal(t, "/data", sidecarContainer.VolumeMounts[0].MountPath)

	require.Len(t, workload.Spec.Template.Spec.Volumes, 1, "Pod should have one volume for the sidecar")
	require.Equal(t, "sidecar-data", workload.Spec.Template.Spec.Volumes[0].Name)
	require.NotNil(t, workload.Spec.Template.Spec.Volumes[0].EmptyDir, "Volume should be an EmptyDir")
}

func TestStorageProcessor_StandalonePVCUsesGivenName(t *testing.T) {
	pvcTrait := spec.StorageTraitSpec{
		Type:      "persistent",
		Name:      "shared-cache",
		TmpCreate: false,
	}
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "worker",
			AppID:     "app-2",
			Namespace: "jobs",
		},
	}

	result, err := processStorage(ctx, []spec.StorageTraitSpec{pvcTrait})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Volumes, 1)

	vol := result.Volumes[0]
	require.Equal(t, "shared-cache", vol.Name)
	require.NotNil(t, vol.VolumeSource.PersistentVolumeClaim)
	require.Equal(t, "shared-cache", vol.VolumeSource.PersistentVolumeClaim.ClaimName)

	require.Len(t, result.AdditionalObjects, 1)
	pvc, ok := result.AdditionalObjects[0].(*corev1.PersistentVolumeClaim)
	require.True(t, ok)
	require.Equal(t, "shared-cache", pvc.Name)
}

func TestStorageProcessor_ExplicitClaimNameCreatesStandalonePVCTarget(t *testing.T) {
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "worker",
			AppID:     "app-2",
			Namespace: "jobs",
		},
	}

	result, err := processStorage(ctx, []spec.StorageTraitSpec{{
		Type:      "persistent",
		Name:      "shared-cache",
		TmpCreate: false,
		ClaimName: "existing-cache",
	}})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Volumes, 1)

	vol := result.Volumes[0]
	require.Equal(t, "shared-cache", vol.Name)
	require.NotNil(t, vol.VolumeSource.PersistentVolumeClaim)
	require.Equal(t, "existing-cache", vol.VolumeSource.PersistentVolumeClaim.ClaimName)

	require.Len(t, result.AdditionalObjects, 1)
	pvc, ok := result.AdditionalObjects[0].(*corev1.PersistentVolumeClaim)
	require.True(t, ok)
	require.Equal(t, "existing-cache", pvc.Name)
}

func TestStorageProcessor_RendersSubPathExpr(t *testing.T) {
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "default",
		},
	}

	result, err := processStorage(ctx, []spec.StorageTraitSpec{{
		Name:        "logs",
		Type:        "persistent",
		ClaimName:   "developer-pvc",
		MountPath:   "/app/log",
		SubPathExpr: "$(TZ)/game/$(INSTANCE_ID)/$(SERVER_NAME)/$(POD_IP)",
	}})
	require.NoError(t, err)
	require.NotNil(t, result)

	mounts := result.VolumeMounts
	require.Len(t, mounts, 1)
	require.Equal(t, "/app/log", mounts[0].MountPath)
	require.Empty(t, mounts[0].SubPath)
	require.Equal(t, "$(TZ)/game/$(INSTANCE_ID)/$(SERVER_NAME)/$(POD_IP)", mounts[0].SubPathExpr)
	require.Len(t, result.AdditionalObjects, 1)
	pvc, ok := result.AdditionalObjects[0].(*corev1.PersistentVolumeClaim)
	require.True(t, ok)
	require.Equal(t, "developer-pvc", pvc.Name)
}

// TestStorageProcessor_StatefulSet_TmpCreate_VolumeNameMatch 验证 StatefulSet 使用 tmpCreate 时
// volumeClaimTemplate 名称与 VolumeMount 名称一致
// 这是修复 "volumeMounts[0].name: Not found" 错误的核心测试
func TestStorageProcessor_StatefulSet_TmpCreate_VolumeNameMatch(t *testing.T) {
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "mysql",
			AppID:     "app-123",
			Namespace: "default",
		},
	}

	result, err := processStorage(ctx, []spec.StorageTraitSpec{
		{
			Name:      "mysql-data",
			Type:      "persistent",
			MountPath: "/var/lib/mysql",
			Size:      "5Gi",
			TmpCreate: true,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// 验证 Volume 名称与 ClaimName 一致
	require.Len(t, result.Volumes, 1)
	assert.Equal(t, "mysql-data", result.Volumes[0].Name,
		"Volume 名称应该是 volumeName")
	require.NotNil(t, result.Volumes[0].PersistentVolumeClaim)
	assert.Equal(t, "mysql-data", result.Volumes[0].PersistentVolumeClaim.ClaimName,
		"ClaimName 应该与 volumeName 一致，以便 StatefulSet 正确处理")

	// 验证 PVC template 名称与 volumeName 一致
	require.Len(t, result.AdditionalObjects, 1)
	pvc, ok := result.AdditionalObjects[0].(*corev1.PersistentVolumeClaim)
	require.True(t, ok)
	assert.Equal(t, "mysql-data", pvc.Name,
		"PVC template 名称应该是 volumeName，以匹配 VolumeMount")

	// 验证 VolumeMount 名称
	mounts := result.VolumeMounts
	require.Len(t, mounts, 1)
	assert.Equal(t, "mysql-data", mounts[0].Name,
		"VolumeMount 名称应该是 volumeName")

	// 核心验证：所有三个名称必须一致
	assert.Equal(t, result.Volumes[0].Name, pvc.Name,
		"Volume.Name 必须等于 PVC template.Name")
	assert.Equal(t, result.Volumes[0].PersistentVolumeClaim.ClaimName, pvc.Name,
		"Volume.ClaimName 必须等于 PVC template.Name")
	assert.Equal(t, mounts[0].Name, pvc.Name,
		"VolumeMount.Name 必须等于 PVC template.Name")
}

// TestStorageProcessor_MultiVolume_TmpCreate 验证多个 tmpCreate volume 的命名
func TestStorageProcessor_MultiVolume_TmpCreate(t *testing.T) {
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "postgres",
			AppID:     "app-456",
			Namespace: "default",
		},
	}

	result, err := processStorage(ctx, []spec.StorageTraitSpec{
		{
			Name:      "pg-data",
			Type:      "persistent",
			MountPath: "/var/lib/postgresql/data",
			Size:      "10Gi",
			TmpCreate: true,
		},
		{
			Name:      "pg-wal",
			Type:      "persistent",
			MountPath: "/var/lib/postgresql/wal",
			Size:      "5Gi",
			TmpCreate: true,
		},
		{
			Name:      "pg-backup",
			Type:      "ephemeral",
			MountPath: "/backup",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// 验证 3 个 volumes (2 PVC + 1 emptyDir)
	require.Len(t, result.Volumes, 3)

	// 验证 2 个 PVC templates
	require.Len(t, result.AdditionalObjects, 2)

	// 验证每个 PVC template 的名称与对应的 volume 名称一致
	pvcNames := make(map[string]bool)
	for _, obj := range result.AdditionalObjects {
		pvc := obj.(*corev1.PersistentVolumeClaim)
		pvcNames[pvc.Name] = true
	}
	assert.True(t, pvcNames["pg-data"], "应该有 pg-data PVC template")
	assert.True(t, pvcNames["pg-wal"], "应该有 pg-wal PVC template")

	// 验证 VolumeMount 名称
	mounts := result.VolumeMounts
	require.Len(t, mounts, 3)
	mountNames := make(map[string]bool)
	for _, m := range mounts {
		mountNames[m.Name] = true
	}
	assert.True(t, mountNames["pg-data"], "应该有 pg-data VolumeMount")
	assert.True(t, mountNames["pg-wal"], "应该有 pg-wal VolumeMount")
	assert.True(t, mountNames["pg-backup"], "应该有 pg-backup VolumeMount")
}

// TestStorageProcessor_MixedMode 验证 tmpCreate 和引用已有 PVC 混合使用
func TestStorageProcessor_MixedMode(t *testing.T) {
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "app-server",
			AppID:     "app-789",
			Namespace: "default",
		},
	}

	result, err := processStorage(ctx, []spec.StorageTraitSpec{
		{
			Name:      "app-data",
			Type:      "persistent",
			MountPath: "/data",
			Size:      "2Gi",
			TmpCreate: true,
		},
		{
			Name:      "shared-config",
			Type:      "persistent",
			MountPath: "/config",
			TmpCreate: false,
			ClaimName: "existing-config-pvc",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// 验证 2 个 volumes
	require.Len(t, result.Volumes, 2)

	// 验证 tmpCreate template 和 claimName standalone PVC 都会作为 additional object 输出。
	require.Len(t, result.AdditionalObjects, 2)

	// 查找 tmpCreate 的 PVC template
	var templatePVC, standalonePVC *corev1.PersistentVolumeClaim
	for _, obj := range result.AdditionalObjects {
		pvc := obj.(*corev1.PersistentVolumeClaim)
		if pvc.GetAnnotations()[config.LabelStorageRole] == "template" {
			templatePVC = pvc
		} else {
			standalonePVC = pvc
		}
	}

	require.NotNil(t, templatePVC, "应该有一个 template PVC")
	require.NotNil(t, standalonePVC, "应该有一个 standalone PVC")

	// 验证 template PVC 名称
	assert.Equal(t, "app-data", templatePVC.Name,
		"template PVC 名称应该是 volumeName")

	// 验证 existing PVC 引用
	require.NotNil(t, result.Volumes[1].PersistentVolumeClaim)
	assert.Equal(t, "existing-config-pvc", result.Volumes[1].PersistentVolumeClaim.ClaimName,
		"existing PVC 应该使用 claimName")
	assert.Equal(t, "existing-config-pvc", standalonePVC.Name,
		"standalone PVC target 应该使用 claimName")
}

// TestApplyStorageToStatefulSet 验证完整的 StatefulSet 处理流程
func TestApplyStorageToStatefulSet(t *testing.T) {
	storage := spec.StorageTraitSpec{Name: "mysql-data", Type: "persistent", MountPath: "/var/lib/mysql", TmpCreate: true, Size: "2Gi"}
	component := &model.ApplicationComponent{Name: "mysql", Namespace: "default", Traits: toJSONStruct(spec.Traits{
		Storage: []spec.StorageTraitSpec{
			storage,
			{Name: "config", Type: "persistent", ClaimName: "mysql-config", MountPath: "/etc/mysql", Size: "1Gi"},
		},
		Init: []spec.InitTraitSpec{{Name: "prepare", Image: "busybox:1.37", Traits: spec.Traits{Storage: []spec.StorageTraitSpec{storage}}}},
	})}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mysql-test",
			Namespace: "default",
		},
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "mysql"},
					},
				},
			},
		},
	}

	objects, err := ApplyTraits(component, sts, nil)
	require.NoError(t, err)
	require.Len(t, sts.Spec.VolumeClaimTemplates, 1)
	template := sts.Spec.VolumeClaimTemplates[0]
	require.Equal(t, "mysql-data", template.Name)
	require.Empty(t, template.Namespace)
	require.Equal(t, resource.MustParse("2Gi"), template.Spec.Resources.Requests[corev1.ResourceStorage])
	require.Equal(t, []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "mysql-config"},
	}}}, sts.Spec.Template.Spec.Volumes, "only the template volume should be removed")
	require.Equal(t, []corev1.VolumeMount{
		{Name: template.Name, MountPath: "/var/lib/mysql"},
		{Name: "config", MountPath: "/etc/mysql"},
	}, sts.Spec.Template.Spec.Containers[0].VolumeMounts)
	require.Len(t, sts.Spec.Template.Spec.InitContainers, 1)
	require.Equal(t, []corev1.VolumeMount{{Name: template.Name, MountPath: "/var/lib/mysql"}}, sts.Spec.Template.Spec.InitContainers[0].VolumeMounts)
	require.Len(t, objects, 1)
	pvc, ok := objects[0].(*corev1.PersistentVolumeClaim)
	require.True(t, ok)
	require.Equal(t, "mysql-config", pvc.Name)
	require.Equal(t, "default", pvc.Namespace)
}

const userInputJSON = `
{
  "storage": [
    {
      "type": "persistent",
      "name": "data",
      "mountPath": "/var/lib/mysql",
      "subPath": "mysql",
      "size": "5Gi",
      "tmpCreate": true
    },
    {
      "type": "ephemeral",
      "name": "conf",
      "mountPath": "/etc/mysql/conf.d"
    }
  ],
  "sidecar": [
    {
      "name": "xtrabackup",
      "traits": {
        "storage": [
          {
            "type": "persistent",
            "name": "data",
            "mountPath": "/var/lib/mysql",
            "subPath": "mysql"
          }
        ]
      }
    }
  ],
  "init": [
    {
      "name": "clone-mysql",
      "traits": {
        "storage": [
          {
            "type": "persistent",
            "name": "data",
            "mountPath": "/var/lib/mysql",
            "subPath": "mysql"
          }
        ]
      }
    }
  ]
}
`

func TestStorageProcessor_DuplicateInput(t *testing.T) {
	// 1. Parse the user's JSON to simulate the input
	var traits spec.Traits
	err := json.Unmarshal([]byte(userInputJSON), &traits)
	assert.NoError(t, err)

	// Combine all storage traits from the input, simulating the recursive discovery
	var allStorageTraits []spec.StorageTraitSpec
	allStorageTraits = append(allStorageTraits, traits.Storage...)
	allStorageTraits = append(allStorageTraits, traits.Sidecar[0].Traits.Storage...)
	allStorageTraits = append(allStorageTraits, traits.Init[0].Traits.Storage...)

	// 2. TmpCreate the processor and the context
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "mysql",
			AppID:     "app-1",
			Namespace: "data",
		},
	}

	// 3. Run the processor
	result, err := processStorage(ctx, allStorageTraits)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// 4. Assert the results are correct

	// 4.1. Check the Volumes list. It should contain the persistent 'data' PVC and the 'conf' disk exactly once each.
	assert.Len(t, result.Volumes, 2, "Should have one PVC volume and one ephemeral volume")
	assert.Equal(t, "data", result.Volumes[0].Name)
	assert.NotNil(t, result.Volumes[0].VolumeSource.PersistentVolumeClaim)
	// For tmpCreate=true, the ClaimName matches the volumeName so VolumeMount references work correctly
	assert.Equal(t, "data", result.Volumes[0].VolumeSource.PersistentVolumeClaim.ClaimName)
	assert.Equal(t, "conf", result.Volumes[1].Name)
	assert.NotNil(t, result.Volumes[1].VolumeSource.EmptyDir)

	// 4.2. Check the AdditionalObjects list. It should contain the 'data' PVC template.
	assert.Len(t, result.AdditionalObjects, 1, "Should be one additional object (the PVC template)")

	pvc, ok := result.AdditionalObjects[0].(*corev1.PersistentVolumeClaim)
	assert.True(t, ok, "The additional object should be a PersistentVolumeClaim")
	// For tmpCreate=true, PVC template name matches volumeName for StatefulSet volumeClaimTemplates compatibility
	assert.Equal(t, "data", pvc.Name, "The PVC template name should match volumeName for VolumeMount references")
	assert.Equal(t, ctx.Component.Namespace, pvc.Namespace, "PVC should inherit component namespace")

	annotations := pvc.GetAnnotations()
	assert.NotNil(t, annotations)
	assert.Equal(t, "template", annotations["eruun.io/pvc-role"], "The PVC should have the 'template' role annotation")
}

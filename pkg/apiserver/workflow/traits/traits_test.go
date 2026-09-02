package traits

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/yaml"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func resetTraitProcessors() {
	ResetTraitProcessorsForTest()
}

// TestEnvProcessor comprehensively tests the env trait processing.
func TestEnvProcessor(t *testing.T) {
	// Register processors needed for the tests.
	// In a real test setup, this might be done in a TestMain or setup function.
	resetTraitProcessors() // Clear existing processors for a clean test
	Register(&InitProcessor{})
	Register(&EnvFromProcessor{})
	Register(&StorageProcessor{}) // Storage is often used alongside other traits

	// --- Test Data Setup ---

	// Mock Component with a main container and an init container trait
	mockComponent := &model.ApplicationComponent{
		Name:  "main-app",
		Image: "main-app:v1",
		Traits: toJSONStruct(model.Traits{
			// Top-level env trait for the main container
			EnvFrom: []model.EnvFromSourceSpec{
				{Type: "config", SourceName: "main-app-config"},
				{Type: "secret", SourceName: "main-app-secret"},
			},
			// Init container trait with its own nested env trait
			Init: []model.InitTrait{
				{
					Name:  "my-init-container",
					Image: "init:v1",
					Traits: model.Traits{
						EnvFrom: []model.EnvFromSourceSpec{
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
		_, err := ApplyTraits(mockComponent, workload)

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
		_, err := ApplyTraits(componentWithEmptyTraits, workload)

		// --- Assert ---
		assert.NoError(t, err)
		mainContainer := findContainer(workload.Spec.Template.Spec.Containers, "main-app")
		assert.Len(t, mainContainer.EnvFrom, 0, "Main container should have no envFrom sources")
		assert.Len(t, workload.Spec.Template.Spec.InitContainers, 0, "Should be no init containers")
	})
}

func TestSecurityPolicyTrait(t *testing.T) {
	resetTraitProcessors()
	Register(&SecurityPolicyProcessor{})
	Register(&InitProcessor{})
	Register(&SidecarProcessor{})

	component := &model.ApplicationComponent{
		Name:  "main-app",
		Image: "main-app:v1",
		Traits: toJSONStruct(model.Traits{
			SecurityPolicy: &corev1.SecurityContext{
				RunAsUser:                pointer.Int64(1000),
				RunAsGroup:               pointer.Int64(1000),
				AllowPrivilegeEscalation: pointer.Bool(false),
			},
			Init: []model.InitTrait{
				{
					Name:  "init-task",
					Image: "init:v1",
					Traits: model.Traits{
						SecurityPolicy: &corev1.SecurityContext{
							RunAsUser:  pointer.Int64(0),
							RunAsGroup: pointer.Int64(0),
						},
					},
				},
			},
			Sidecar: []model.SidecarSpec{
				{
					Name:  "log-agent",
					Image: "log-agent:v1",
					Traits: model.Traits{
						SecurityPolicy: &corev1.SecurityContext{
							RunAsUser: pointer.Int64(2000),
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

	_, err := ApplyTraits(component, workload)
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

func TestTargetWorkEnvTrait(t *testing.T) {
	resetTraitProcessors()
	Register(&TargetWorkEnvProcessor{})

	component := &model.ApplicationComponent{
		Name:  "backend",
		Image: "nginx:latest",
		Traits: toJSONStruct(model.Traits{
			TargetWorkEnv: map[string]string{"app": "lab"},
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

	_, err := ApplyTraits(component, workload)
	require.NoError(t, err)
	require.Equal(t, "lab", workload.Spec.Template.Spec.NodeSelector["app"])
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
	// Register processors needed for the test
	resetTraitProcessors() // Clear existing processors for a clean test
	Register(&EnvsProcessor{})
	Register(&EnvFromProcessor{})

	// 1. Define the input component with the final, structured envs spec.
	staticValue := "some_static_value"
	fieldPath := "metadata.namespace"
	traitsStruct := &model.Traits{
		Envs: []model.SimplifiedEnvSpec{
			{
				Name:      "STATIC_VAR",
				ValueFrom: model.ValueSource{Static: &staticValue},
			},
			{
				Name: "PASSWORD_FROM_SECRET",
				ValueFrom: model.ValueSource{Secret: &model.SecretSelectorSpec{
					Name: "secret/with/slashes",
					Key:  "password",
				}},
			},
			{
				Name: "API_KEY_FROM_CONFIG",
				ValueFrom: model.ValueSource{Config: &model.ConfigMapSelectorSpec{
					Name: "config/with/slashes",
					Key:  "api-key",
				}},
			},
			{
				Name:      "MY_POD_NAMESPACE",
				ValueFrom: model.ValueSource{Field: &fieldPath},
			},
		},
		EnvFrom: []model.EnvFromSourceSpec{
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
	_, err = ApplyTraits(component, workload)
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

func TestBuildIngress_WithAnnotationsAndClass(t *testing.T) {
	trait := &spec.IngressTraitsSpec{
		Name:             "catanddog-2510301134udp0bg-frontend",
		Namespace:        "2505131620u7b9hq",
		IngressClassName: "ingress-nginx",
		Label: map[string]string{
			"frontPurchaserProductId": "31",
			"name":                    "penalty shootout 2026-m2606241344ccufxh-frontend",
		},
		Annotations: map[string]string{
			"nginx.ingress.kubernetes.io/proxy-read-timeout": "60",
			"nginx.ingress.kubernetes.io/proxy-send-timeout": "60",
			"nginx.ingress.kubernetes.io/rewrite-target":     "$1",
			"nginx.ingress.kubernetes.io/use-regex":          "true",
		},
		Routes: []spec.IngressRoutes{
			{
				Path:     "/27d51e4eae211962f00b63622d0274b0(/.*)",
				PathType: "ImplementationSpecific",
				Host:     "game.example.com",
				Backend: spec.IngressRoute{
					ServiceName: "catanddog-2510301134udp0bg-frontend",
					ServicePort: 80,
				},
			},
		},
	}

	ing, err := BuildIngress(trait)
	require.NoError(t, err)

	require.Equal(t, trait.Name, ing.Name)
	require.Equal(t, trait.Namespace, ing.Namespace)
	require.Equal(t, trait.Label["frontPurchaserProductId"], ing.Labels["frontPurchaserProductId"])
	require.Equal(t, "penalty-shootout-2026-m2606241344ccufxh-frontend", ing.Labels["name"])
	require.Equal(t, trait.Annotations["nginx.ingress.kubernetes.io/rewrite-target"], ing.Annotations["nginx.ingress.kubernetes.io/rewrite-target"])
	require.NotNil(t, ing.Spec.IngressClassName)
	require.Equal(t, trait.IngressClassName, *ing.Spec.IngressClassName)

	require.Len(t, ing.Spec.Rules, 1)
	rule := ing.Spec.Rules[0]
	require.Equal(t, trait.Routes[0].Host, rule.Host)
	require.NotNil(t, rule.HTTP)
	require.Len(t, rule.HTTP.Paths, 1)
	ingPath := rule.HTTP.Paths[0]
	require.Equal(t, trait.Routes[0].Path, ingPath.Path)
	require.NotNil(t, ingPath.PathType)
	require.Equal(t, networkingv1.PathTypeImplementationSpecific, *ingPath.PathType)
	require.NotNil(t, ingPath.Backend.Service)
	require.Equal(t, trait.Routes[0].Backend.ServiceName, ingPath.Backend.Service.Name)
	require.Equal(t, trait.Routes[0].Backend.ServicePort, ingPath.Backend.Service.Port.Number)
}

func TestDeterminePathType_Defaults(t *testing.T) {
	spec := &model.IngressTraitsSpec{
		DefaultPathType: "Exact",
	}
	route := model.IngressRoutes{}
	pt := determinePathType(route, spec, nil)
	require.Equal(t, networkingv1.PathTypeExact, pt)

	route.PathType = "ImplementationSpecific"
	pt = determinePathType(route, spec, map[string]string{})
	require.Equal(t, networkingv1.PathTypeImplementationSpecific, pt)
}

func TestApplyIngressDefaultsNamespaceUsesComponent(t *testing.T) {
	component := &model.ApplicationComponent{Namespace: "component-ns"}
	trait := &spec.IngressTraitsSpec{Namespace: "custom-ns"}

	require.NoError(t, applyIngressDefaults(trait, component, 0))
	require.Equal(t, "component-ns", trait.Namespace)
}

func TestApplyIngressDefaultsServiceName(t *testing.T) {
	component := &model.ApplicationComponent{Name: "api", AppID: "app-1"}
	trait := &spec.IngressTraitsSpec{
		Routes: []spec.IngressRoutes{
			{Backend: spec.IngressRoute{}},
			{Backend: spec.IngressRoute{ServiceName: "custom-svc"}},
		},
	}

	require.NoError(t, applyIngressDefaults(trait, component, 0))

	require.Equal(t, naming.ServiceName("api", "app-1"), trait.Routes[0].Backend.ServiceName)
	require.Equal(t, "custom-svc", trait.Routes[1].Backend.ServiceName)
}

func TestApplyIngressDefaultsServiceFromTrait(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:  "api",
		AppID: "app-1",
		Traits: toJSONStruct(model.Traits{
			Service: []spec.ServiceTraitSpec{
				{
					Name: "api-master",
					Type: "internal",
					Ports: []spec.ServicePortTraitSpec{
						{Port: 8080},
					},
				},
			},
		}),
	}

	trait := &spec.IngressTraitsSpec{
		Routes: []spec.IngressRoutes{
			{Backend: spec.IngressRoute{}},
		},
	}

	require.NoError(t, applyIngressDefaults(trait, component, 0))
	require.Equal(t, "api-master", trait.Routes[0].Backend.ServiceName)
	require.Equal(t, int32(8080), trait.Routes[0].Backend.ServicePort)
}

func TestApplyIngressDefaultsRequiresExplicitServiceNameWhenMultipleServices(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:  "api",
		AppID: "app-1",
		Traits: toJSONStruct(model.Traits{
			Service: []spec.ServiceTraitSpec{
				{
					Name: "api-v1",
					Type: "internal",
					Ports: []spec.ServicePortTraitSpec{
						{Port: 8080},
					},
				},
				{
					Name: "api-v2",
					Type: "internal",
					Ports: []spec.ServicePortTraitSpec{
						{Port: 8081},
					},
				},
			},
		}),
	}
	trait := &spec.IngressTraitsSpec{
		Routes: []spec.IngressRoutes{
			{Backend: spec.IngressRoute{}},
		},
	}

	err := applyIngressDefaults(trait, component, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "serviceName is required")
	require.Contains(t, err.Error(), "api")
}

func TestApplyIngressDefaultsAllowsExplicitServiceNameWhenMultipleServices(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:  "api",
		AppID: "app-1",
		Traits: toJSONStruct(model.Traits{
			Service: []spec.ServiceTraitSpec{
				{
					Name: "api-v1",
					Type: "internal",
					Ports: []spec.ServicePortTraitSpec{
						{Port: 8080},
					},
				},
				{
					Name: "api-v2",
					Type: "internal",
					Ports: []spec.ServicePortTraitSpec{
						{Port: 8081},
					},
				},
			},
		}),
	}
	trait := &spec.IngressTraitsSpec{
		Routes: []spec.IngressRoutes{
			{Backend: spec.IngressRoute{ServiceName: "api-v2"}},
		},
	}

	require.NoError(t, applyIngressDefaults(trait, component, 0))
	require.Equal(t, "api-v2", trait.Routes[0].Backend.ServiceName)
	require.Equal(t, int32(8081), trait.Routes[0].Backend.ServicePort)
}

func TestApplyIngressDefaultsServiceFromProperties(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:  "api",
		AppID: "app-1",
		Properties: toJSONStruct(model.Properties{
			Ports: []model.Ports{{Port: 9090}},
		}),
	}

	trait := &spec.IngressTraitsSpec{
		Routes: []spec.IngressRoutes{
			{Backend: spec.IngressRoute{}},
			{Backend: spec.IngressRoute{ServicePort: 8081}},
		},
	}

	require.NoError(t, applyIngressDefaults(trait, component, 0))
	require.Equal(t, naming.ServiceName("api", "app-1"), trait.Routes[0].Backend.ServiceName)
	require.Equal(t, int32(9090), trait.Routes[0].Backend.ServicePort)
	require.Equal(t, int32(8081), trait.Routes[1].Backend.ServicePort)
}

func setupProcessors() {
	resetTraitProcessors()
	RegisterAllProcessors()
}

func TestRegister_Idempotent(t *testing.T) {
	resetTraitProcessors()
	Register(&StorageProcessor{})
	Register(&StorageProcessor{})
	require.Len(t, registeredTraitProcessors, 1)
}

func TestRegisterAllProcessors_UsesOnceAndReset(t *testing.T) {
	resetTraitProcessors()

	RegisterAllProcessors()
	firstCount := len(registeredTraitProcessors)
	require.Greater(t, firstCount, 0)

	RegisterAllProcessors()
	require.Len(t, registeredTraitProcessors, firstCount)

	ResetTraitProcessorsForTest()
	require.Empty(t, registeredTraitProcessors)

	RegisterAllProcessors()
	require.Len(t, registeredTraitProcessors, firstCount)
}

func TestApplyTraits_InitTrait_WithNestedTraits(t *testing.T) {
	setupProcessors()
	// 1. Define the input component with two init containers sharing a volume.
	traitsStruct := &model.Traits{
		Init: []model.InitTrait{
			{
				Name:  "init-mysql",
				Image: "kubectl:1.28.5",
				Properties: model.Properties{
					Command: []string{"bash", "-c", ""},
					Env:     map[string]string{"MYSQL_DATABASE": "test"},
				},
				Traits: model.Traits{
					Storage: []model.StorageTrait{
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
					Storage: []model.StorageTrait{
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
	_, err = ApplyTraits(component, workload)
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

func TestApplyTraitsBindsServiceAccount(t *testing.T) {
	setupProcessors()
	automount := true
	traitsStruct := &spec.Traits{
		RBAC: []spec.RBACPolicySpec{
			{
				ServiceAccount:             "pod-labeler-sa",
				ServiceAccountAutomountSAT: &automount,
				Rules: []spec.RBACRuleSpec{
					{
						Verbs:     []string{"get"},
						Resources: []string{"pods"},
					},
				},
			},
		},
	}
	traitsJSON, err := model.NewJSONStructByStruct(traitsStruct)
	require.NoError(t, err)
	raw, err := json.Marshal(traitsJSON)
	require.NoError(t, err)
	var parsed spec.Traits
	require.NoError(t, json.Unmarshal(raw, &parsed))
	require.NotNil(t, parsed.RBAC[0].ServiceAccountAutomountSAT)
	require.True(t, *parsed.RBAC[0].ServiceAccountAutomountSAT)

	component := &model.ApplicationComponent{
		Name:      "worker",
		Namespace: "demo",
		Traits:    traitsJSON,
	}

	workload := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: component.Name, Namespace: component.Namespace},
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: component.Name, Image: "example.com/worker:latest"}},
				},
			},
		},
	}

	additional, err := ApplyTraits(component, workload)
	require.NoError(t, err)
	require.NotNil(t, additional)
	require.GreaterOrEqual(t, len(additional), 3)
	sa, ok := additional[0].(*corev1.ServiceAccount)
	require.True(t, ok)
	require.NotNil(t, sa.AutomountServiceAccountToken)
	require.True(t, *sa.AutomountServiceAccountToken)
	t.Logf("podSpec SA=%s automount ptr=%v", workload.Spec.Template.Spec.ServiceAccountName, workload.Spec.Template.Spec.AutomountServiceAccountToken)
	require.Equal(t, "pod-labeler-sa", workload.Spec.Template.Spec.ServiceAccountName)
	require.NotNil(t, workload.Spec.Template.Spec.AutomountServiceAccountToken)
	require.True(t, *workload.Spec.Template.Spec.AutomountServiceAccountToken)
}

func TestRBACProcessor_NamespaceRole(t *testing.T) {
	p := &RBACProcessor{}
	component := &model.ApplicationComponent{
		Name:      "backend",
		Namespace: "demo",
	}
	policies := []spec.RBACPolicySpec{
		{
			ServiceAccount: "custom-sa",
			Rules: []spec.RBACRuleSpec{
				{
					APIGroups: []string{""},
					Resources: []string{"pods"},
					Verbs:     []string{"get", "list"},
				},
			},
			ServiceAccountLabels: map[string]string{"app": "Penalty Shootout 2026"},
			RoleLabels:           map[string]string{"role": "Reader Role"},
			BindingLabels:        map[string]string{"binding": "Reader Binding"},
		},
	}

	res, err := p.Process(&TraitContext{Component: component, TraitData: policies})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.AdditionalObjects, 3)
	require.Equal(t, "custom-sa", res.ServiceAccountName)
	require.Nil(t, res.AutomountServiceAccountToken)

	sa, ok := res.AdditionalObjects[0].(*corev1.ServiceAccount)
	require.True(t, ok)
	require.Equal(t, "custom-sa", sa.Name)
	require.Equal(t, "demo", sa.Namespace)
	require.Equal(t, "penalty-shootout-2026", sa.Labels["app"])

	role, ok := res.AdditionalObjects[1].(*rbacv1.Role)
	require.True(t, ok)
	require.Equal(t, "custom-sa-role", role.Name)
	require.Equal(t, "demo", role.Namespace)
	require.Equal(t, 1, len(role.Rules))
	require.Equal(t, []string{"get", "list"}, role.Rules[0].Verbs)
	require.Equal(t, "reader-role", role.Labels["role"])

	binding, ok := res.AdditionalObjects[2].(*rbacv1.RoleBinding)
	require.True(t, ok)
	require.Equal(t, "custom-sa-binding", binding.Name)
	require.Equal(t, "demo", binding.Namespace)
	require.Equal(t, "reader-binding", binding.Labels["binding"])
	require.Equal(t, 1, len(binding.Subjects))
	require.Equal(t, "custom-sa", binding.Subjects[0].Name)
	require.Equal(t, "Role", binding.RoleRef.Kind)
	require.Equal(t, "custom-sa-role", binding.RoleRef.Name)
}

func TestRBACProcessor_ClusterScope(t *testing.T) {
	p := &RBACProcessor{}
	component := &model.ApplicationComponent{
		Name:      "controller",
		Namespace: "system",
	}
	policies := []spec.RBACPolicySpec{
		{
			ClusterScope: true,
			ServiceAccountLabels: map[string]string{
				"app": "Controller App",
			},
			RoleLabels: map[string]string{
				"role": "Cluster Reader",
			},
			BindingLabels: map[string]string{
				"binding": "Cluster Reader Binding",
			},
			Rules: []spec.RBACRuleSpec{
				{
					APIGroups: []string{"apps"},
					Resources: []string{"deployments"},
					Verbs:     []string{"update"},
				},
			},
		},
	}

	res, err := p.Process(&TraitContext{Component: component, TraitData: policies})
	require.NoError(t, err)
	require.Len(t, res.AdditionalObjects, 3)
	require.Equal(t, "controller-sa", res.ServiceAccountName)
	require.Nil(t, res.AutomountServiceAccountToken)

	sa := res.AdditionalObjects[0].(*corev1.ServiceAccount)
	require.Equal(t, "controller-sa", sa.Name)
	require.Equal(t, "system", sa.Namespace)
	require.Equal(t, "controller-app", sa.Labels["app"])

	clusterRole := res.AdditionalObjects[1].(*rbacv1.ClusterRole)
	require.Equal(t, "controller-sa-role", clusterRole.Name)
	require.Equal(t, "cluster-reader", clusterRole.Labels["role"])
	require.Equal(t, []string{"apps"}, clusterRole.Rules[0].APIGroups)

	clusterBinding := res.AdditionalObjects[2].(*rbacv1.ClusterRoleBinding)
	require.Equal(t, "controller-sa-binding", clusterBinding.Name)
	require.Equal(t, "cluster-reader-binding", clusterBinding.Labels["binding"])
	require.Equal(t, "ClusterRole", clusterBinding.RoleRef.Kind)
	require.Equal(t, "controller-sa-role", clusterBinding.RoleRef.Name)
	require.Equal(t, "system", clusterBinding.Subjects[0].Namespace)
}

func TestRBACProcessor_ServiceAccountSelectionAndAutomount(t *testing.T) {
	p := &RBACProcessor{}
	component := &model.ApplicationComponent{
		Name:      "worker",
		Namespace: "ops",
	}
	automount := false
	policies := []spec.RBACPolicySpec{
		{
			ServiceAccount:             "pod-labeler-sa",
			ServiceAccountAutomountSAT: &automount,
			Rules: []spec.RBACRuleSpec{
				{
					Resources: []string{"pods"},
					Verbs:     []string{"patch"},
				},
			},
		},
		{
			ServiceAccount: "secondary-sa",
			Rules: []spec.RBACRuleSpec{
				{
					Resources: []string{"configmaps"},
					Verbs:     []string{"get"},
				},
			},
		},
	}

	res, err := p.Process(&TraitContext{Component: component, TraitData: policies})
	require.NoError(t, err)
	require.Equal(t, "pod-labeler-sa", res.ServiceAccountName, "first policy should determine bound serviceAccount")
	require.NotNil(t, res.AutomountServiceAccountToken)
	require.False(t, *res.AutomountServiceAccountToken)
	require.Len(t, res.AdditionalObjects, 6, "two policies emit two RBAC sets plus serviceAccounts")
}

func TestResourcesProcessor(t *testing.T) {
	// Register processors needed for the test
	resetTraitProcessors() // Clear existing processors for a clean test
	Register(&ResourcesProcessor{})

	// Test component with resources trait
	component := &model.ApplicationComponent{
		Name:      "test-component",
		Namespace: "test-namespace",
		Traits: toJSONStruct(model.Traits{
			Resources: &model.ResourceSpec{
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
	_, err := ApplyTraits(component, workload)
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
	// Register processors needed for the test
	resetTraitProcessors() // Clear existing processors for a clean test
	Register(&ResourcesProcessor{})
	Register(&SidecarProcessor{})

	// Test component with sidecar that has its own resources
	component := &model.ApplicationComponent{
		Name:      "test-component",
		Namespace: "test-namespace",
		Traits: toJSONStruct(model.Traits{
			Sidecar: []model.SidecarSpec{
				{
					Name:  "my-sidecar",
					Image: "sidecar:v1",
					Traits: model.Traits{
						Resources: &model.ResourceSpec{
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
	_, err := ApplyTraits(component, workload)
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
		Sidecar: []model.SidecarSpec{
			{
				Name:  "my-sidecar",
				Image: "sidecar:v1",
				Env:   map[string]string{"SIDECAR_VAR": "value1"},
				Traits: model.Traits{
					Storage: []model.StorageTrait{
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
	resetTraitProcessors()
	Register(&SidecarProcessor{})
	Register(&StorageProcessor{}) // Register dependency trait

	_, err = ApplyTraits(component, workload)
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
	storageProcessor := &StorageProcessor{}
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
		TraitData: []spec.StorageTraitSpec{pvcTrait},
	}

	result, err := storageProcessor.Process(ctx)
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
	storageProcessor := &StorageProcessor{}
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "worker",
			AppID:     "app-2",
			Namespace: "jobs",
		},
		TraitData: []spec.StorageTraitSpec{{
			Type:      "persistent",
			Name:      "shared-cache",
			TmpCreate: false,
			ClaimName: "existing-cache",
		}},
	}

	result, err := storageProcessor.Process(ctx)
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
	storageProcessor := &StorageProcessor{}
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "default",
		},
		TraitData: []spec.StorageTraitSpec{{
			Name:        "logs",
			Type:        "persistent",
			ClaimName:   "developer-pvc",
			MountPath:   "/app/log",
			SubPathExpr: "$(TZ)/game/$(INSTANCE_ID)/$(SERVER_NAME)/$(POD_IP)",
		}},
	}

	result, err := storageProcessor.Process(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)

	mounts := result.VolumeMounts["backend"]
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
	processor := &StorageProcessor{}
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "mysql",
			AppID:     "app-123",
			Namespace: "default",
		},
		TraitData: []spec.StorageTraitSpec{
			{
				Name:      "mysql-data",
				Type:      "persistent",
				MountPath: "/var/lib/mysql",
				Size:      "5Gi",
				TmpCreate: true,
			},
		},
	}

	result, err := processor.Process(ctx)
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
	mounts := result.VolumeMounts["mysql"]
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
	processor := &StorageProcessor{}
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "postgres",
			AppID:     "app-456",
			Namespace: "default",
		},
		TraitData: []spec.StorageTraitSpec{
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
		},
	}

	result, err := processor.Process(ctx)
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
	mounts := result.VolumeMounts["postgres"]
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
	processor := &StorageProcessor{}
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "app-server",
			AppID:     "app-789",
			Namespace: "default",
		},
		TraitData: []spec.StorageTraitSpec{
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
		},
	}

	result, err := processor.Process(ctx)
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
	// 模拟 StorageProcessor 的输出
	result := &TraitResult{
		Volumes: []corev1.Volume{
			{
				Name: "mysql-data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "mysql-data",
					},
				},
			},
		},
		VolumeMounts: map[string][]corev1.VolumeMount{
			"mysql": {
				{Name: "mysql-data", MountPath: "/var/lib/mysql"},
			},
		},
	}

	// 创建 StatefulSet workload
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

	// 应用 volumes 到 StatefulSet
	sts.Spec.Template.Spec.Volumes = result.Volumes

	// 应用 volumeMounts 到容器
	for i := range sts.Spec.Template.Spec.Containers {
		container := &sts.Spec.Template.Spec.Containers[i]
		if mounts, ok := result.VolumeMounts[container.Name]; ok {
			container.VolumeMounts = append(container.VolumeMounts, mounts...)
		}
	}

	// 模拟 processor.go 将 PVC 转换为 volumeClaimTemplates
	templatePVC := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mysql-data", // 使用 volumeName，不是 pvcName
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
	}
	sts.Spec.VolumeClaimTemplates = append(sts.Spec.VolumeClaimTemplates, templatePVC)

	// 移除对应的 volume（StatefulSet 会自动创建）
	var filteredVolumes []corev1.Volume
	for _, vol := range sts.Spec.Template.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == "mysql-data" {
			continue // 移除
		}
		filteredVolumes = append(filteredVolumes, vol)
	}
	sts.Spec.Template.Spec.Volumes = filteredVolumes

	// 验证最终结果
	require.Len(t, sts.Spec.VolumeClaimTemplates, 1,
		"应该有一个 volumeClaimTemplate")
	assert.Equal(t, "mysql-data", sts.Spec.VolumeClaimTemplates[0].Name,
		"volumeClaimTemplate 名称应该与 VolumeMount 名称一致")

	// 验证 Volume 被正确移除
	assert.Len(t, sts.Spec.Template.Spec.Volumes, 0,
		"显式 Volume 应该被移除，StatefulSet 会自动创建")

	// 验证 VolumeMount 仍然存在且名称正确
	require.Len(t, sts.Spec.Template.Spec.Containers[0].VolumeMounts, 1)
	assert.Equal(t, "mysql-data", sts.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name,
		"VolumeMount.Name 应该与 volumeClaimTemplate.Name 一致")

	// 核心验证：VolumeMount.Name == volumeClaimTemplate.Name
	assert.Equal(t,
		sts.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name,
		sts.Spec.VolumeClaimTemplates[0].Name,
		"VolumeMount.Name 必须等于 volumeClaimTemplate.Name，否则 Pod 会创建失败")
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
	storageProcessor := &StorageProcessor{}
	ctx := &TraitContext{
		Component: &model.ApplicationComponent{
			Name:      "mysql",
			AppID:     "app-1",
			Namespace: "data",
		},
		TraitData: allStorageTraits,
	}

	// 3. Run the processor
	result, err := storageProcessor.Process(ctx)
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

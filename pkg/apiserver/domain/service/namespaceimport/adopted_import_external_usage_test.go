package namespaceimport

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestImportNamespaceResources_AdoptedStandalonePodMakesDependenciesShared(t *testing.T) {
	const namespace = "prod"
	objects, root := adoptedExternalUsageBaseObjects(namespace)
	objects = append(objects, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "standalone-consumer",
			Namespace:       namespace,
			UID:             types.UID("uid-standalone-consumer"),
			ResourceVersion: "1",
			Labels:          map[string]string{"app": "api"},
		},
		Spec: adoptedExternalUsagePodSpec(),
	})

	response := runAdoptedExternalUsageDryRun(t, namespace, root.Name, objects...)

	assertAdoptedExternalUsageShared(t, response)
	assert.Contains(t, strings.Join(response.Warnings, "\n"), "Pod/standalone-consumer")
}

func TestImportNamespaceResources_AdoptedExternalReplicaSetMakesDependenciesShared(t *testing.T) {
	const namespace = "prod"
	objects, root := adoptedExternalUsageBaseObjects(namespace)
	controller := true
	objects = append(objects, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "orphaned-api-rs",
			Namespace:       namespace,
			UID:             types.UID("uid-orphaned-api-rs"),
			ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: appsv1.SchemeGroupVersion.String(),
				Kind:       "Deployment",
				Name:       root.Name,
				UID:        types.UID("uid-replaced-deployment"),
				Controller: &controller,
			}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
				Spec:       adoptedExternalUsagePodSpec(),
			},
		},
	})

	response := runAdoptedExternalUsageDryRun(t, namespace, root.Name, objects...)

	assertAdoptedExternalUsageShared(t, response)
	assert.Contains(t, strings.Join(response.Warnings, "\n"), "ReplicaSet/orphaned-api-rs")
}

func TestImportNamespaceResources_AdoptedTargetDeploymentOwnerChainIsNotExternal(t *testing.T) {
	const namespace = "prod"
	objects, root := adoptedExternalUsageBaseObjects(namespace)
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: adoptedTestObjectMeta("api-rs", namespace),
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
				Spec:       adoptedExternalUsagePodSpec(),
			},
		},
	}
	replicaSet.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(root, appsv1.SchemeGroupVersion.WithKind("Deployment")),
	}
	pod := &corev1.Pod{
		ObjectMeta: adoptedTestObjectMeta("api-rs-pod", namespace),
		Spec:       adoptedExternalUsagePodSpec(),
	}
	pod.Labels = map[string]string{"app": "api"}
	pod.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(replicaSet, appsv1.SchemeGroupVersion.WithKind("ReplicaSet")),
	}
	objects = append(objects, replicaSet, pod)

	response := runAdoptedExternalUsageDryRun(t, namespace, root.Name, objects...)

	for _, resourceIdentity := range []struct {
		kind string
		name string
	}{
		{kind: "ConfigMap", name: "api-config"},
		{kind: "Secret", name: "api-secret"},
		{kind: "ServiceAccount", name: "api-sa"},
		{kind: "Service", name: "api-service"},
		{kind: "PodDisruptionBudget", name: "api-pdb"},
		{kind: "NetworkPolicy", name: "api-policy"},
	} {
		result := requireImportResourceResult(t, response, resourceIdentity.kind, resourceIdentity.name)
		assert.Equal(t, adoption.OwnershipExclusive, result.Ownership, resourceIdentity)
		assert.Equal(t, adoption.DispositionManaged, result.Disposition, resourceIdentity)
	}
	pvc := requireImportResourceResult(t, response, "PersistentVolumeClaim", "api-data")
	assert.Equal(t, adoption.OwnershipDataProtected, pvc.Ownership)
	assert.Equal(t, adoption.DispositionDataProtected, pvc.Disposition)
	assert.NotContains(t, strings.Join(response.Warnings, "\n"), "target-external workload ReplicaSet/api-rs")
	assert.NotContains(t, strings.Join(response.Warnings, "\n"), "target-external workload Pod/api-rs-pod")
}

func TestImportNamespaceResources_AdoptedPreservesSourceReplicasIncludingZero(t *testing.T) {
	const namespace = "prod"
	tests := []struct {
		name     string
		kind     string
		replicas *int32
		want     int32
	}{
		{name: "deployment explicit zero", kind: "Deployment", replicas: int32Ptr(0), want: 0},
		{name: "deployment nil defaults one", kind: "Deployment", want: 1},
		{name: "statefulset explicit zero", kind: "StatefulSet", replicas: int32Ptr(0), want: 0},
		{name: "statefulset nil defaults one", kind: "StatefulSet", want: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workloadName := "source-workload"
			var workload runtime.Object
			switch test.kind {
			case "Deployment":
				deployment := adoptedTestDeployment(workloadName, namespace, map[string]string{"app": "api"})
				deployment.Spec.Replicas = test.replicas
				workload = deployment
			case "StatefulSet":
				statefulSet := adoptedTestStatefulSet(
					workloadName,
					namespace,
					map[string]string{"app": "api"},
					workloadName,
					nil,
				)
				statefulSet.Spec.Replicas = test.replicas
				workload = statefulSet
			default:
				require.FailNow(t, "unsupported test workload kind", test.kind)
			}

			client := fake.NewSimpleClientset(workload)
			store := newInMemoryAppStore()
			appService := &namespaceImportAppServiceStub{
				generatedID:  "generated-app-id",
				persistStore: store,
			}
			service := &namespaceImportServiceImpl{
				Cfg:                 adoptedImportTestConfig(),
				KubeClient:          client,
				AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
				ApplicationService:  appService,
				ValidationService:   NewValidationService(),
				AppRepo:             &mockAppRepo{store: store},
				WorkflowRepo:        &mockWorkflowRepo{store: store},
				ComponentRepo:       &mockComponentRepo{store: store},
			}
			request := adoptedSingleWorkloadRequest(
				namespace,
				"api-app",
				"api",
				test.kind,
				workloadName,
			)
			dryRun, err := service.ImportNamespaceResources(context.Background(), request)
			require.NoError(t, err)
			require.NotEmpty(t, dryRun.PlanFingerprint)

			request.Mode = importModeApply
			request.PlanFingerprint = dryRun.PlanFingerprint
			_, err = service.ImportNamespaceResources(context.Background(), request)
			require.NoError(t, err)
			require.Len(t, appService.createReqs, 1)
			require.Len(t, appService.createReqs[0].Component, 1)
			assert.Equal(t, test.want, appService.createReqs[0].Component[0].Replicas)
			require.NotNil(t, store.components["api"])
			assert.Equal(t, test.want, store.components["api"].Replicas)
		})
	}
}

func adoptedExternalUsageBaseObjects(namespace string) ([]runtime.Object, *appsv1.Deployment) {
	root := adoptedTestDeployment("api", namespace, map[string]string{"app": "api"})
	root.Spec.Template.Spec = adoptedExternalUsagePodSpec()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: adoptedTestObjectMeta("api-data", namespace),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	pdbSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}
	return []runtime.Object{
		root,
		&corev1.ConfigMap{
			ObjectMeta: adoptedTestObjectMeta("api-config", namespace),
			Data:       map[string]string{"mode": "prod"},
		},
		&corev1.Secret{
			ObjectMeta: adoptedTestObjectMeta("api-secret", namespace),
			Data:       map[string][]byte{"token": []byte("secret")},
		},
		pvc,
		&corev1.ServiceAccount{ObjectMeta: adoptedTestObjectMeta("api-sa", namespace)},
		adoptedTestService("api-service", namespace, map[string]string{"app": "api"}, false),
		&policyv1.PodDisruptionBudget{
			ObjectMeta: adoptedTestObjectMeta("api-pdb", namespace),
			Spec:       policyv1.PodDisruptionBudgetSpec{Selector: pdbSelector},
		},
		&networkingv1.NetworkPolicy{
			ObjectMeta: adoptedTestObjectMeta("api-policy", namespace),
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
			},
		},
	}, root
}

func adoptedExternalUsagePodSpec() corev1.PodSpec {
	return corev1.PodSpec{
		ServiceAccountName: "api-sa",
		Containers: []corev1.Container{{
			Name:  "app",
			Image: "nginx:1.27",
			EnvFrom: []corev1.EnvFromSource{
				{
					ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "api-config"},
					},
				},
				{
					SecretRef: &corev1.SecretEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "api-secret"},
					},
				},
			},
			VolumeMounts: []corev1.VolumeMount{{
				Name:      "data",
				MountPath: "/data",
			}},
		}},
		Volumes: []corev1.Volume{{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "api-data"},
			},
		}},
	}
}

func runAdoptedExternalUsageDryRun(
	t *testing.T,
	namespace string,
	workloadName string,
	objects ...runtime.Object,
) *apisv1.ImportNamespaceApplicationsResponse {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	store := newInMemoryAppStore()
	service := &namespaceImportServiceImpl{
		Cfg:                adoptedImportTestConfig(),
		KubeClient:         client,
		ApplicationService: &namespaceImportAppServiceStub{},
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		WorkflowRepo:       &mockWorkflowRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}
	response, err := service.ImportNamespaceResources(
		context.Background(),
		adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", workloadName),
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	return response
}

func assertAdoptedExternalUsageShared(
	t *testing.T,
	response *apisv1.ImportNamespaceApplicationsResponse,
) {
	t.Helper()
	for _, resourceIdentity := range []struct {
		kind string
		name string
	}{
		{kind: "ConfigMap", name: "api-config"},
		{kind: "Secret", name: "api-secret"},
		{kind: "PersistentVolumeClaim", name: "api-data"},
		{kind: "ServiceAccount", name: "api-sa"},
		{kind: "Service", name: "api-service"},
		{kind: "PodDisruptionBudget", name: "api-pdb"},
		{kind: "NetworkPolicy", name: "api-policy"},
	} {
		result := requireImportResourceResult(t, response, resourceIdentity.kind, resourceIdentity.name)
		assert.Equal(t, adoption.OwnershipShared, result.Ownership, resourceIdentity)
		assert.Equal(t, adoption.DispositionSharedPreserved, result.Disposition, resourceIdentity)
	}
}

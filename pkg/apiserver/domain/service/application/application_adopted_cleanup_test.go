package application

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestAdoptedCleanupPlanApplyDeletesExclusiveAndRetainsPVC(t *testing.T) {
	deployment := adoptedCleanupDeployment()
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "data-db-0", UID: types.UID("pvc-uid"), ResourceVersion: "4",
	}}
	service, store := adoptedCleanupService(t, deployment, pvc)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "Deployment", deployment.Name, string(deployment.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
		adoptedCleanupSnapshotResource("v1", "PersistentVolumeClaim", pvc.Name, string(pvc.UID), "pvc", importcontract.OwnershipDataProtected, importcontract.DispositionDataProtected),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	require.NotEmpty(t, plan.PlanFingerprint)
	require.Len(t, plan.ResourceResults, 2)

	response, err := service.ApplyApplicationResourceCleanup(context.Background(), "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})
	require.NoError(t, err)
	require.Contains(t, response.DeletedResources, "Deployment/prod/backend")
	require.Contains(t, response.RetainedResources, "PersistentVolumeClaim/prod/data-db-0")

	_, err = service.KubeClient.AppsV1().Deployments("prod").Get(context.Background(), "backend", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	_, err = service.KubeClient.CoreV1().PersistentVolumeClaims("prod").Get(context.Background(), "data-db-0", metav1.GetOptions{})
	require.NoError(t, err)
	foundOrphanDelete := false
	for _, action := range service.KubeClient.(*fake.Clientset).Actions() {
		if action.GetVerb() != "delete" || action.GetResource().Resource != "deployments" {
			continue
		}
		options := action.(k8stesting.DeleteAction).GetDeleteOptions()
		require.NotNil(t, options.PropagationPolicy)
		require.Equal(t, metav1.DeletePropagationOrphan, *options.PropagationPolicy)
		foundOrphanDelete = true
	}
	require.True(t, foundOrphanDelete)
}

func TestAdoptedCleanupWaitsForDependencyDeletionBeforeReportingSuccess(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "prod",
		Name:            "backend-config",
		UID:             types.UID("secret-uid"),
		ResourceVersion: "2",
		Finalizers:      []string{"example.test/cleanup"},
	}}
	service, store := adoptedCleanupService(t, secret)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource(
			"v1",
			"Secret",
			secret.Name,
			string(secret.UID),
			"secret",
			importcontract.OwnershipExclusive,
			importcontract.DispositionManaged,
		),
	})
	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)

	client := service.KubeClient.(*fake.Clientset)
	client.PrependReactor("delete", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	response, err := service.ApplyApplicationResourceCleanup(ctx, "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})

	require.Error(t, err)
	require.NotNil(t, response)
	require.Contains(t, response.FailedResources, "Secret/prod/backend-config")
	require.NotContains(t, response.DeletedResources, "Secret/prod/backend-config")
	_, getErr := client.CoreV1().Secrets("prod").Get(context.Background(), secret.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
}

func TestAdoptedCleanupDeletesSignedRuntimeChildren(t *testing.T) {
	controller := true
	testCases := []struct {
		name                      string
		root                      runtime.Object
		rootSnapshot              importcontract.ResourceSnapshot
		rootResource              string
		runtimeControllerResource string
		children                  []runtime.Object
		expectedChildren          map[string]string
	}{
		{
			name: "deployment ReplicaSet and Pod",
			root: adoptedCleanupDeployment(),
			rootSnapshot: adoptedCleanupSnapshotResource(
				"apps/v1",
				"Deployment",
				"backend",
				"deployment-uid",
				"workload",
				importcontract.OwnershipExclusive,
				importcontract.DispositionManaged,
			),
			rootResource:              "deployments",
			runtimeControllerResource: "replicasets",
			children: []runtime.Object{
				&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
					Namespace:       "prod",
					Name:            "backend-abc",
					UID:             types.UID("replicaset-uid"),
					ResourceVersion: "2",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Name:       "backend",
						UID:        types.UID("deployment-uid"),
						Controller: &controller,
					}},
				}},
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Namespace:       "prod",
					Name:            "backend-abc-123",
					UID:             types.UID("pod-uid"),
					ResourceVersion: "3",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       "backend-abc",
						UID:        types.UID("replicaset-uid"),
						Controller: &controller,
					}},
				}},
			},
			expectedChildren: map[string]string{
				"replicasets": "ReplicaSet/prod/backend-abc",
				"pods":        "Pod/prod/backend-abc-123",
			},
		},
		{
			name: "statefulset ControllerRevision and Pod",
			root: &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
				Namespace:       "prod",
				Name:            "mysql",
				UID:             types.UID("statefulset-uid"),
				ResourceVersion: "4",
			}},
			rootSnapshot: adoptedCleanupSnapshotResource(
				"apps/v1",
				"StatefulSet",
				"mysql",
				"statefulset-uid",
				"workload",
				importcontract.OwnershipExclusive,
				importcontract.DispositionManaged,
			),
			rootResource:              "statefulsets",
			runtimeControllerResource: "controllerrevisions",
			children: []runtime.Object{
				&appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
					Namespace:       "prod",
					Name:            "mysql-abc",
					UID:             types.UID("controller-revision-uid"),
					ResourceVersion: "5",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1",
						Kind:       "StatefulSet",
						Name:       "mysql",
						UID:        types.UID("statefulset-uid"),
						Controller: &controller,
					}},
				}},
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Namespace:       "prod",
					Name:            "mysql-0",
					UID:             types.UID("statefulset-pod-uid"),
					ResourceVersion: "6",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1",
						Kind:       "StatefulSet",
						Name:       "mysql",
						UID:        types.UID("statefulset-uid"),
						Controller: &controller,
					}},
				}},
			},
			expectedChildren: map[string]string{
				"controllerrevisions": "ControllerRevision/prod/mysql-abc",
				"pods":                "Pod/prod/mysql-0",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			objects := append([]runtime.Object{testCase.root}, testCase.children...)
			service, store := adoptedCleanupService(t, objects...)
			store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
				testCase.rootSnapshot,
			})

			plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
			require.NoError(t, err)
			require.Len(t, plan.ResourceResults, 1+len(testCase.children))
			plannedRefs := make([]string, 0, len(plan.ResourceResults))
			for _, resource := range plan.ResourceResults {
				plannedRefs = append(plannedRefs, cleanupResourceRef(resource))
			}
			for _, ref := range testCase.expectedChildren {
				require.Contains(t, plannedRefs, ref)
			}

			response, err := service.ApplyApplicationResourceCleanup(
				context.Background(),
				"app-1",
				apisv1.CleanupApplicationResourcesRequest{PlanFingerprint: plan.PlanFingerprint},
			)
			require.NoError(t, err)
			for _, ref := range testCase.expectedChildren {
				require.Contains(t, response.DeletedResources, ref)
			}

			deletedChildren := make(map[string]bool, len(testCase.expectedChildren))
			deleteIndexes := make(map[string]int, len(testCase.expectedChildren)+1)
			for index, action := range service.KubeClient.(*fake.Clientset).Actions() {
				if action.GetVerb() == "delete" {
					deleteIndexes[action.GetResource().Resource] = index
				}
				ref, expected := testCase.expectedChildren[action.GetResource().Resource]
				if !expected || action.GetVerb() != "delete" {
					continue
				}
				options := action.(k8stesting.DeleteAction).GetDeleteOptions()
				require.NotNil(t, options.PropagationPolicy, ref)
				require.Equal(t, metav1.DeletePropagationOrphan, *options.PropagationPolicy, ref)
				require.NotNil(t, options.Preconditions, ref)
				require.NotNil(t, options.Preconditions.UID, ref)
				require.Nil(t, options.Preconditions.ResourceVersion, ref)
				deletedChildren[action.GetResource().Resource] = true
			}
			for resource, ref := range testCase.expectedChildren {
				require.True(t, deletedChildren[resource], ref)
			}
			require.Contains(t, deleteIndexes, "pods")
			require.Contains(t, deleteIndexes, testCase.runtimeControllerResource)
			require.Contains(t, deleteIndexes, testCase.rootResource)
			require.Less(t, deleteIndexes["pods"], deleteIndexes[testCase.runtimeControllerResource])
			require.Less(t, deleteIndexes[testCase.runtimeControllerResource], deleteIndexes[testCase.rootResource])
		})
	}
}

func TestAdoptedCleanupRejectsFingerprintAfterRuntimeChildDrift(t *testing.T) {
	controller := true
	deployment := adoptedCleanupDeployment()
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "prod",
		Name:            "backend-abc",
		UID:             types.UID("replicaset-uid"),
		ResourceVersion: "2",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       deployment.Name,
			UID:        deployment.UID,
			Controller: &controller,
		}},
	}}
	service, store := adoptedCleanupService(t, deployment, replicaSet)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource(
			"apps/v1",
			"Deployment",
			deployment.Name,
			string(deployment.UID),
			"workload",
			importcontract.OwnershipExclusive,
			importcontract.DispositionManaged,
		),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	live, err := service.KubeClient.AppsV1().ReplicaSets("prod").Get(
		context.Background(),
		replicaSet.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	live.Labels = map[string]string{"drift": "true"}
	live.ResourceVersion = "3"
	_, err = service.KubeClient.AppsV1().ReplicaSets("prod").Update(
		context.Background(),
		live,
		metav1.UpdateOptions{},
	)
	require.NoError(t, err)

	_, err = service.ApplyApplicationResourceCleanup(
		context.Background(),
		"app-1",
		apisv1.CleanupApplicationResourcesRequest{PlanFingerprint: plan.PlanFingerprint},
	)
	require.ErrorIs(t, err, bcode.ErrNamespaceImportPlanDrift)
	_, err = service.KubeClient.AppsV1().Deployments("prod").Get(
		context.Background(),
		deployment.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, err)
}

func TestAdoptedCleanupRescansSharingAfterRootDeletion(t *testing.T) {
	deployment := adoptedCleanupDeployment()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "backend-config", UID: types.UID("secret-uid"), ResourceVersion: "2",
	}}
	service, store := adoptedCleanupService(t, deployment, secret)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "Deployment", deployment.Name, string(deployment.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
		adoptedCleanupSnapshotResource("v1", "Secret", secret.Name, string(secret.UID), "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})

	client := service.KubeClient.(*fake.Clientset)
	client.PrependReactor("delete", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		externalPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod",
				Name:      "late-consumer",
				UID:       types.UID("late-consumer-uid"),
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "consumer",
				EnvFrom: []corev1.EnvFromSource{{
					SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name}},
				}},
			}}},
		}
		require.NoError(t, client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), externalPod, "prod"))
		return false, nil, nil
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	response, err := service.ApplyApplicationResourceCleanup(context.Background(), "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})

	require.NoError(t, err)
	require.Contains(t, response.DeletedResources, "Deployment/prod/backend")
	require.Contains(t, response.RetainedResources, "Secret/prod/backend-config")
	_, err = service.KubeClient.CoreV1().Secrets("prod").Get(context.Background(), secret.Name, metav1.GetOptions{})
	require.NoError(t, err)
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "secrets" {
			t.Fatal("dependency Secret must be retained when a new consumer appears during root deletion")
		}
	}
}

func TestAdoptedCleanupRejectsFingerprintAfterLiveDriftBeforeDeleting(t *testing.T) {
	deployment := adoptedCleanupDeployment()
	service, store := adoptedCleanupService(t, deployment)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "Deployment", deployment.Name, string(deployment.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})
	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)

	live, err := service.KubeClient.AppsV1().Deployments("prod").Get(context.Background(), "backend", metav1.GetOptions{})
	require.NoError(t, err)
	live.Labels = map[string]string{"drift": "true"}
	live.ResourceVersion = "2"
	_, err = service.KubeClient.AppsV1().Deployments("prod").Update(context.Background(), live, metav1.UpdateOptions{})
	require.NoError(t, err)

	_, err = service.ApplyApplicationResourceCleanup(context.Background(), "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})
	require.ErrorIs(t, err, bcode.ErrNamespaceImportPlanDrift)
	_, err = service.KubeClient.AppsV1().Deployments("prod").Get(context.Background(), "backend", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestAdoptedCleanupBlocksStatefulSetWhenDeletedPolicy(t *testing.T) {
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "mysql", UID: types.UID("sts-uid"), ResourceVersion: "8",
		},
		Spec: appsv1.StatefulSetSpec{
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
	service, store := adoptedCleanupService(t, statefulSet)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "StatefulSet", statefulSet.Name, string(statefulSet.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	require.Equal(t, importcontract.DispositionBlocked, plan.ResourceResults[0].Disposition)

	_, err = service.ApplyApplicationResourceCleanup(context.Background(), "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})
	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	_, err = service.KubeClient.AppsV1().StatefulSets("prod").Get(context.Background(), "mysql", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestAdoptedCleanupBlocksStatefulSetWhenScaledPolicy(t *testing.T) {
	replicas := int32(1)
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "mysql", UID: types.UID("sts-uid"), ResourceVersion: "8",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
	service, store := adoptedCleanupService(t, statefulSet)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "StatefulSet", statefulSet.Name, string(statefulSet.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	require.Equal(t, importcontract.DispositionBlocked, plan.ResourceResults[0].Disposition)

	_, err = service.ApplyApplicationResourceCleanup(context.Background(), "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})
	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	live, err := service.KubeClient.AppsV1().StatefulSets("prod").Get(context.Background(), "mysql", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, live.Spec.Replicas)
	require.Equal(t, int32(1), *live.Spec.Replicas)
}

func TestAdoptedCleanupRetainsSecretThatBecameSharedAfterImport(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "database", UID: types.UID("secret-uid"), ResourceVersion: "9",
	}}
	external := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "proxy", UID: types.UID("proxy-uid"), ResourceVersion: "2",
		},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "proxy",
				Env: []corev1.EnvVar{{
					Name: "DATABASE_PASSWORD",
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name},
						Key:                  "password",
					}},
				}},
			}}},
		}},
	}
	service, store := adoptedCleanupService(t, secret, external)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("v1", "Secret", secret.Name, string(secret.UID), "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, plan.ResourceResults, 1)
	require.Equal(t, importcontract.OwnershipShared, plan.ResourceResults[0].Ownership)
	require.Equal(t, importcontract.DispositionSharedPreserved, plan.ResourceResults[0].Disposition)

	response, err := service.ApplyApplicationResourceCleanup(context.Background(), "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})
	require.NoError(t, err)
	require.Contains(t, response.RetainedResources, "Secret/prod/database")
	_, err = service.KubeClient.CoreV1().Secrets("prod").Get(context.Background(), secret.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestAdoptedCleanupRetainsSecretReferencedByExternalAzureFileVolume(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "azure-storage", UID: types.UID("secret-uid"), ResourceVersion: "9",
	}}
	external := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "proxy", UID: types.UID("proxy-uid"), ResourceVersion: "2",
		},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "proxy"}},
				Volumes: []corev1.Volume{{
					Name: "shared-data",
					VolumeSource: corev1.VolumeSource{AzureFile: &corev1.AzureFileVolumeSource{
						SecretName: secret.Name,
						ShareName:  "shared",
					}},
				}},
			},
		}},
	}
	service, store := adoptedCleanupService(t, secret, external)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("v1", "Secret", secret.Name, string(secret.UID), "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, plan.ResourceResults, 1)
	require.Equal(t, importcontract.OwnershipShared, plan.ResourceResults[0].Ownership)
	require.Equal(t, importcontract.DispositionSharedPreserved, plan.ResourceResults[0].Disposition)

	response, err := service.ApplyApplicationResourceCleanup(context.Background(), "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})
	require.NoError(t, err)
	require.Contains(t, response.RetainedResources, "Secret/prod/azure-storage")
	_, err = service.KubeClient.CoreV1().Secrets("prod").Get(context.Background(), secret.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestAdoptedCleanupRetainsSecretReferencedByExternalServiceAccount(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "registry-credentials", UID: types.UID("secret-uid"), ResourceVersion: "9",
	}}
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "shared-runtime", UID: types.UID("service-account-uid"), ResourceVersion: "3",
		},
		Secrets:          []corev1.ObjectReference{{Name: secret.Name}},
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: secret.Name}},
	}
	external := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "proxy", UID: types.UID("proxy-uid"), ResourceVersion: "2",
		},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				ServiceAccountName: serviceAccount.Name,
				Containers:         []corev1.Container{{Name: "proxy"}},
			},
		}},
	}
	service, store := adoptedCleanupService(t, secret, serviceAccount, external)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("v1", "Secret", secret.Name, string(secret.UID), "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
		adoptedCleanupSnapshotResource("v1", "ServiceAccount", serviceAccount.Name, string(serviceAccount.UID), "service-account", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, plan.ResourceResults, 2)
	for _, result := range plan.ResourceResults {
		require.Equal(t, importcontract.OwnershipShared, result.Ownership)
		require.Equal(t, importcontract.DispositionSharedPreserved, result.Disposition)
	}

	response, err := service.ApplyApplicationResourceCleanup(context.Background(), "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})
	require.NoError(t, err)
	require.Contains(t, response.RetainedResources, "Secret/prod/registry-credentials")
	require.Contains(t, response.RetainedResources, "ServiceAccount/prod/shared-runtime")
	_, err = service.KubeClient.CoreV1().Secrets("prod").Get(context.Background(), secret.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestAdoptedCleanupRetainsServiceAccountReferencedByExternalRBAC(t *testing.T) {
	testCases := []struct {
		name    string
		binding func(serviceAccountName string) runtime.Object
	}{
		{
			name: "role binding",
			binding: func(serviceAccountName string) runtime.Object {
				return &rbacv1.RoleBinding{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "prod",
						Name:      "external-reader",
						UID:       types.UID("external-role-binding-uid"),
					},
					Subjects: []rbacv1.Subject{{
						Kind: rbacv1.ServiceAccountKind,
						Name: serviceAccountName,
					}},
					RoleRef: rbacv1.RoleRef{
						APIGroup: rbacv1.GroupName,
						Kind:     "Role",
						Name:     "external-reader",
					},
				}
			},
		},
		{
			name: "cluster role binding",
			binding: func(serviceAccountName string) runtime.Object {
				return &rbacv1.ClusterRoleBinding{
					ObjectMeta: metav1.ObjectMeta{
						Name: "external-cluster-reader",
						UID:  types.UID("external-cluster-role-binding-uid"),
					},
					Subjects: []rbacv1.Subject{{
						Kind:      rbacv1.ServiceAccountKind,
						Namespace: "prod",
						Name:      serviceAccountName,
					}},
					RoleRef: rbacv1.RoleRef{
						APIGroup: rbacv1.GroupName,
						Kind:     "ClusterRole",
						Name:     "external-cluster-reader",
					},
				}
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod",
				Name:      "registry-credentials",
				UID:       types.UID("secret-uid"),
			}}
			serviceAccount := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "prod",
					Name:      "shared-runtime",
					UID:       types.UID("service-account-uid"),
				},
				ImagePullSecrets: []corev1.LocalObjectReference{{Name: secret.Name}},
			}
			service, store := adoptedCleanupService(
				t,
				secret,
				serviceAccount,
				testCase.binding(serviceAccount.Name),
			)
			store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
				adoptedCleanupSnapshotResource(
					"v1",
					"Secret",
					secret.Name,
					string(secret.UID),
					"secret",
					importcontract.OwnershipExclusive,
					importcontract.DispositionManaged,
				),
				adoptedCleanupSnapshotResource(
					"v1",
					"ServiceAccount",
					serviceAccount.Name,
					string(serviceAccount.UID),
					"service-account",
					importcontract.OwnershipExclusive,
					importcontract.DispositionManaged,
				),
			})

			plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
			require.NoError(t, err)
			require.Len(t, plan.ResourceResults, 2)
			for _, result := range plan.ResourceResults {
				require.Equal(t, importcontract.OwnershipShared, result.Ownership)
				require.Equal(t, importcontract.DispositionSharedPreserved, result.Disposition)
			}

			response, err := service.ApplyApplicationResourceCleanup(
				context.Background(),
				"app-1",
				apisv1.CleanupApplicationResourcesRequest{PlanFingerprint: plan.PlanFingerprint},
			)
			require.NoError(t, err)
			require.Contains(t, response.RetainedResources, "Secret/prod/registry-credentials")
			require.Contains(t, response.RetainedResources, "ServiceAccount/prod/shared-runtime")
		})
	}
}

func TestAdoptedCleanupRetainsRoleReferencedThroughNewClusterBinding(t *testing.T) {
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "shared-runtime", UID: types.UID("service-account-uid"),
	}}
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "runtime-reader", UID: types.UID("role-uid"),
	}}
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "runtime-reader", UID: types.UID("role-binding-uid"),
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Namespace: "prod",
			Name:      serviceAccount.Name,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     role.Name,
		},
	}
	clusterBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime-reader", UID: types.UID("cluster-role-binding-uid")},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Namespace: "prod",
			Name:      serviceAccount.Name,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "external-runtime-reader",
		},
	}
	service, store := adoptedCleanupService(t, serviceAccount, role, roleBinding, clusterBinding)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("v1", "ServiceAccount", serviceAccount.Name, string(serviceAccount.UID), "service-account", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
		adoptedCleanupSnapshotResource("rbac.authorization.k8s.io/v1", "Role", role.Name, string(role.UID), "rbac", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
		adoptedCleanupSnapshotResource("rbac.authorization.k8s.io/v1", "RoleBinding", roleBinding.Name, string(roleBinding.UID), "rbac", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, plan.ResourceResults, 3)
	for _, result := range plan.ResourceResults {
		require.Equal(t, importcontract.OwnershipShared, result.Ownership)
		require.Equal(t, importcontract.DispositionSharedPreserved, result.Disposition)
	}
}

func TestAdoptedCleanupRetainsSecretReferencedByStandalonePodOrReplicaSet(t *testing.T) {
	testCases := []struct {
		name     string
		workload func(secretName string) runtime.Object
	}{
		{
			name: "standalone pod",
			workload: func(secretName string) runtime.Object {
				return &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "debug", UID: types.UID("pod-uid")},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name: "debug",
						EnvFrom: []corev1.EnvFromSource{{
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
							},
						}},
					}}},
				}
			},
		},
		{
			name: "standalone replicaset",
			workload: func(secretName string) runtime.Object {
				return &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "worker", UID: types.UID("rs-uid")},
					Spec: appsv1.ReplicaSetSpec{Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{Containers: []corev1.Container{{
							Name: "worker",
							EnvFrom: []corev1.EnvFromSource{{
								SecretRef: &corev1.SecretEnvSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
								},
							}},
						}}},
					}},
				}
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: "shared-runtime", UID: types.UID("secret-uid"), ResourceVersion: "9",
			}}
			service, store := adoptedCleanupService(t, secret, testCase.workload(secret.Name))
			store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
				adoptedCleanupSnapshotResource("v1", "Secret", secret.Name, string(secret.UID), "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
			})

			plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
			require.NoError(t, err)
			require.Len(t, plan.ResourceResults, 1)
			require.Equal(t, importcontract.DispositionSharedPreserved, plan.ResourceResults[0].Disposition)
		})
	}
}

func TestAdoptedCleanupRetainsTLSSecretReferencedByExternalIngress(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "shared-tls", UID: types.UID("secret-uid"), ResourceVersion: "9",
	}}
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "external", UID: types.UID("ingress-uid")},
		Spec: networkingv1.IngressSpec{TLS: []networkingv1.IngressTLS{{
			Hosts:      []string{"external.example.test"},
			SecretName: secret.Name,
		}}},
	}
	service, store := adoptedCleanupService(t, secret, ingress)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("v1", "Secret", secret.Name, string(secret.UID), "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, plan.ResourceResults, 1)
	require.Equal(t, importcontract.DispositionSharedPreserved, plan.ResourceResults[0].Disposition)
}

func TestAdoptedCleanupRetainsTLSSecretWhenAdoptedIngressBecomesShared(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "shared-tls", UID: types.UID("secret-uid"), ResourceVersion: "9",
	}}
	serviceResource := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "backend", UID: types.UID("service-uid"), ResourceVersion: "8",
		},
		Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "backend"}},
	}
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "backend", UID: types.UID("ingress-uid"), ResourceVersion: "7",
		},
		Spec: networkingv1.IngressSpec{
			TLS: []networkingv1.IngressTLS{{
				Hosts:      []string{"backend.example.test"},
				SecretName: secret.Name,
			}},
			Rules: []networkingv1.IngressRule{{
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{Paths: []networkingv1.HTTPIngressPath{{
						Backend: networkingv1.IngressBackend{
							Service: &networkingv1.IngressServiceBackend{Name: serviceResource.Name},
						},
					}}},
				},
			}},
		},
	}
	externalPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "external-backend", UID: types.UID("external-pod-uid"),
			Labels: map[string]string{"app": "backend"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "backend", Image: "nginx"}}},
	}
	service, store := adoptedCleanupService(t, secret, serviceResource, ingress, externalPod)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("v1", "Secret", secret.Name, string(secret.UID), "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
		adoptedCleanupSnapshotResource("v1", "Service", serviceResource.Name, string(serviceResource.UID), "service", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
		adoptedCleanupSnapshotResource("networking.k8s.io/v1", "Ingress", ingress.Name, string(ingress.UID), "ingress", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})

	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, plan.ResourceResults, 3)
	for _, result := range plan.ResourceResults {
		require.Equal(t, importcontract.OwnershipShared, result.Ownership)
		require.Equal(t, importcontract.DispositionSharedPreserved, result.Disposition)
	}
}

func TestAdoptedCleanupRetainsDependenciesWhenRootDeleteFails(t *testing.T) {
	deployment := adoptedCleanupDeployment()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "backend-config", UID: types.UID("secret-uid"), ResourceVersion: "9",
	}}
	service, store := adoptedCleanupService(t, deployment, secret)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "Deployment", deployment.Name, string(deployment.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
		adoptedCleanupSnapshotResource("v1", "Secret", secret.Name, string(secret.UID), "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})
	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	service.KubeClient.(*fake.Clientset).Fake.PrependReactor("delete", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})

	response, err := service.ApplyApplicationResourceCleanup(context.Background(), "app-1", apisv1.CleanupApplicationResourcesRequest{
		PlanFingerprint: plan.PlanFingerprint,
	})
	require.Error(t, err)
	require.Contains(t, response.FailedResources, "Deployment/prod/backend")
	require.Contains(t, response.RetainedResources, "Secret/prod/backend-config")
	_, getErr := service.KubeClient.CoreV1().Secrets("prod").Get(context.Background(), secret.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	for _, action := range service.KubeClient.(*fake.Clientset).Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "secrets" {
			t.Fatal("dependency Secret must be retained when root deletion fails")
		}
	}
}

func TestAdoptedCleanupRetainsDependenciesWhenRuntimeChildDeleteFails(t *testing.T) {
	controller := true
	deployment := adoptedCleanupDeployment()
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "prod",
		Name:            "backend-abc",
		UID:             types.UID("replicaset-uid"),
		ResourceVersion: "2",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       deployment.Name,
			UID:        deployment.UID,
			Controller: &controller,
		}},
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "backend-config", UID: types.UID("secret-uid"), ResourceVersion: "9",
	}}
	service, store := adoptedCleanupService(t, deployment, replicaSet, secret)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "Deployment", deployment.Name, string(deployment.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
		adoptedCleanupSnapshotResource("v1", "Secret", secret.Name, string(secret.UID), "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})
	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	service.KubeClient.(*fake.Clientset).Fake.PrependReactor("delete", "replicasets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})

	response, err := service.ApplyApplicationResourceCleanup(
		context.Background(),
		"app-1",
		apisv1.CleanupApplicationResourcesRequest{PlanFingerprint: plan.PlanFingerprint},
	)
	require.Error(t, err)
	require.Contains(t, response.FailedResources, "ReplicaSet/prod/backend-abc")
	require.Contains(t, response.RetainedResources, "Secret/prod/backend-config")
	liveDeployment, getErr := service.KubeClient.AppsV1().Deployments("prod").Get(
		context.Background(),
		deployment.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, getErr)
	require.True(t, liveDeployment.Spec.Paused)
	require.NotNil(t, liveDeployment.Spec.Replicas)
	require.Zero(t, *liveDeployment.Spec.Replicas)
	liveReplicaSet, getErr := service.KubeClient.AppsV1().ReplicaSets("prod").Get(
		context.Background(),
		replicaSet.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, getErr)
	require.NotNil(t, liveReplicaSet.Spec.Replicas)
	require.Zero(t, *liveReplicaSet.Spec.Replicas)
	_, getErr = service.KubeClient.CoreV1().Secrets("prod").Get(
		context.Background(),
		secret.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, getErr)
	for _, action := range service.KubeClient.(*fake.Clientset).Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "secrets" {
			t.Fatal("dependency Secret must be retained when runtime child deletion fails")
		}
	}
	retryPlan, retryErr := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, retryErr)
	retryRefs := make([]string, 0, len(retryPlan.ResourceResults))
	for _, resource := range retryPlan.ResourceResults {
		retryRefs = append(retryRefs, cleanupResourceRef(resource))
	}
	require.Contains(t, retryRefs, "Deployment/prod/backend")
	require.Contains(t, retryRefs, "ReplicaSet/prod/backend-abc")
}

func TestAdoptedCleanupRetainsRuntimeOwnerChainWhenPodDeleteFails(t *testing.T) {
	controller := true
	deployment := adoptedCleanupDeployment()
	replicaSetReplicas := int32(1)
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "prod",
			Name:            "backend-abc",
			UID:             types.UID("replicaset-uid"),
			ResourceVersion: "2",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment.Name,
				UID:        deployment.UID,
				Controller: &controller,
			}},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: &replicaSetReplicas},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "prod",
		Name:            "backend-abc-123",
		UID:             types.UID("pod-uid"),
		ResourceVersion: "3",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       replicaSet.Name,
			UID:        replicaSet.UID,
			Controller: &controller,
		}},
	}}
	service, store := adoptedCleanupService(t, deployment, replicaSet, pod)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "Deployment", deployment.Name, string(deployment.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})
	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	client := service.KubeClient.(*fake.Clientset)
	client.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})

	response, err := service.ApplyApplicationResourceCleanup(
		context.Background(),
		"app-1",
		apisv1.CleanupApplicationResourcesRequest{PlanFingerprint: plan.PlanFingerprint},
	)
	require.Error(t, err)
	require.Contains(t, response.FailedResources, "Pod/prod/backend-abc-123")
	require.Contains(t, response.RetainedResources, "ReplicaSet/prod/backend-abc")
	liveDeployment, getErr := service.KubeClient.AppsV1().Deployments("prod").Get(
		context.Background(),
		deployment.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, getErr)
	require.True(t, liveDeployment.Spec.Paused)
	require.NotNil(t, liveDeployment.Spec.Replicas)
	require.Zero(t, *liveDeployment.Spec.Replicas)
	liveReplicaSet, getErr := service.KubeClient.AppsV1().ReplicaSets("prod").Get(
		context.Background(),
		replicaSet.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, getErr)
	require.NotNil(t, liveReplicaSet.Spec.Replicas)
	require.Zero(t, *liveReplicaSet.Spec.Replicas)
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "replicasets" {
			t.Fatal("ReplicaSet must be retained when deleting its Pod fails")
		}
	}
	retryPlan, retryErr := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, retryErr)
	retryRefs := make([]string, 0, len(retryPlan.ResourceResults))
	for _, resource := range retryPlan.ResourceResults {
		retryRefs = append(retryRefs, cleanupResourceRef(resource))
	}
	require.Contains(t, retryRefs, "ReplicaSet/prod/backend-abc")
	require.Contains(t, retryRefs, "Pod/prod/backend-abc-123")
}

func TestAdoptedCleanupKeepsRootWhenRuntimeChildAppearsDuringDeletion(t *testing.T) {
	controller := true
	deployment := adoptedCleanupDeployment()
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "prod",
		Name:            "backend-old",
		UID:             types.UID("replicaset-old-uid"),
		ResourceVersion: "2",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       deployment.Name,
			UID:        deployment.UID,
			Controller: &controller,
		}},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "prod",
		Name:            "backend-old-123",
		UID:             types.UID("pod-old-uid"),
		ResourceVersion: "3",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       replicaSet.Name,
			UID:        replicaSet.UID,
			Controller: &controller,
		}},
	}}
	service, store := adoptedCleanupService(t, deployment, replicaSet, pod)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "Deployment", deployment.Name, string(deployment.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})
	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	client := service.KubeClient.(*fake.Clientset)
	injected := false
	client.PrependReactor("delete", "replicasets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if injected || action.(k8stesting.DeleteAction).GetName() != replicaSet.Name {
			return false, nil, nil
		}
		injected = true
		zero := int32(0)
		lateReplicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       "prod",
				Name:            "backend-late",
				UID:             types.UID("replicaset-late-uid"),
				ResourceVersion: "4",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       deployment.Name,
					UID:        deployment.UID,
					Controller: &controller,
				}},
			},
			Spec: appsv1.ReplicaSetSpec{Replicas: &zero},
		}
		latePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace:       "prod",
			Name:            "backend-late-123",
			UID:             types.UID("pod-late-uid"),
			ResourceVersion: "5",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       lateReplicaSet.Name,
				UID:        lateReplicaSet.UID,
				Controller: &controller,
			}},
		}}
		require.NoError(t, client.Tracker().Create(
			appsv1.SchemeGroupVersion.WithResource("replicasets"),
			lateReplicaSet,
			"prod",
		))
		require.NoError(t, client.Tracker().Create(
			corev1.SchemeGroupVersion.WithResource("pods"),
			latePod,
			"prod",
		))
		return false, nil, nil
	})

	response, err := service.ApplyApplicationResourceCleanup(
		context.Background(),
		"app-1",
		apisv1.CleanupApplicationResourcesRequest{PlanFingerprint: plan.PlanFingerprint},
	)
	require.Error(t, err)
	require.True(t, injected)
	require.Contains(t, response.FailedResources, "ReplicaSet/prod/backend-late")
	require.Contains(t, response.FailedResources, "Pod/prod/backend-late-123")
	_, getErr := service.KubeClient.AppsV1().Deployments("prod").Get(
		context.Background(),
		deployment.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, getErr)
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "deployments" {
			t.Fatal("root Deployment must be retained when a new runtime child appears")
		}
	}
	retryPlan, retryErr := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, retryErr)
	retryRefs := make([]string, 0, len(retryPlan.ResourceResults))
	for _, resource := range retryPlan.ResourceResults {
		retryRefs = append(retryRefs, cleanupResourceRef(resource))
	}
	require.Contains(t, retryRefs, "ReplicaSet/prod/backend-late")
	require.Contains(t, retryRefs, "Pod/prod/backend-late-123")
}

func TestAdoptedCleanupKeepsRootWhenPodAppearsWhileControllersQuiesce(t *testing.T) {
	controller := true
	one := int32(1)
	deployment := adoptedCleanupDeployment()
	deployment.Generation = 2
	deployment.Spec.Replicas = &one
	deployment.Status = appsv1.DeploymentStatus{
		ObservedGeneration: 1,
		Replicas:           1,
		UpdatedReplicas:    1,
		ReadyReplicas:      1,
		AvailableReplicas:  1,
	}
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "prod",
			Name:            "backend-old",
			UID:             types.UID("replicaset-old-uid"),
			ResourceVersion: "2",
			Generation:      2,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment.Name,
				UID:        deployment.UID,
				Controller: &controller,
			}},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: &one},
		Status: appsv1.ReplicaSetStatus{
			ObservedGeneration:   1,
			Replicas:             1,
			FullyLabeledReplicas: 1,
			ReadyReplicas:        1,
			AvailableReplicas:    1,
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "prod",
		Name:            "backend-old-123",
		UID:             types.UID("pod-old-uid"),
		ResourceVersion: "3",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       replicaSet.Name,
			UID:        replicaSet.UID,
			Controller: &controller,
		}},
	}}
	service, store := adoptedCleanupService(t, deployment, replicaSet, pod)
	store.apps["app-1"] = adoptedCleanupApplication(t, []importcontract.ResourceSnapshot{
		adoptedCleanupSnapshotResource("apps/v1", "Deployment", deployment.Name, string(deployment.UID), "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged),
	})
	plan, err := service.PlanApplicationResourceCleanup(context.Background(), "app-1")
	require.NoError(t, err)
	client := service.KubeClient.(*fake.Clientset)
	injected := false
	client.PrependReactor("update", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updated := action.(k8stesting.UpdateAction).GetObject().(*appsv1.Deployment).DeepCopy()
		updated.Status = appsv1.DeploymentStatus{ObservedGeneration: updated.Generation}
		require.NoError(t, client.Tracker().Update(
			appsv1.SchemeGroupVersion.WithResource("deployments"),
			updated,
			updated.Namespace,
		))
		if !injected {
			injected = true
			latePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace:       "prod",
				Name:            "backend-late-123",
				UID:             types.UID("pod-late-uid"),
				ResourceVersion: "4",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       replicaSet.Name,
					UID:        replicaSet.UID,
					Controller: &controller,
				}},
			}}
			require.NoError(t, client.Tracker().Create(
				corev1.SchemeGroupVersion.WithResource("pods"),
				latePod,
				latePod.Namespace,
			))
		}
		return true, updated, nil
	})
	client.PrependReactor("update", "replicasets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updated := action.(k8stesting.UpdateAction).GetObject().(*appsv1.ReplicaSet).DeepCopy()
		updated.Status = appsv1.ReplicaSetStatus{ObservedGeneration: updated.Generation}
		require.NoError(t, client.Tracker().Update(
			appsv1.SchemeGroupVersion.WithResource("replicasets"),
			updated,
			updated.Namespace,
		))
		return true, updated, nil
	})

	response, err := service.ApplyApplicationResourceCleanup(
		context.Background(),
		"app-1",
		apisv1.CleanupApplicationResourcesRequest{PlanFingerprint: plan.PlanFingerprint},
	)
	require.Error(t, err)
	require.True(t, injected)
	require.Contains(t, response.FailedResources, "Pod/prod/backend-late-123")
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" &&
			(action.GetResource().Resource == "replicasets" || action.GetResource().Resource == "deployments") {
			t.Fatalf("quiesce drift must retain controller resource %s", action.GetResource().Resource)
		}
	}
}

func adoptedCleanupService(t *testing.T, objects ...runtime.Object) (*applicationsServiceImpl, *inMemoryAppStore) {
	t.Helper()
	store := newInMemoryAppStore()
	service := newMockServiceWithStore(store)
	service.KubeClient = fake.NewSimpleClientset(objects...)
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	service.Cfg = &config.Config{ImportSecretKeyring: `{"activeKeyId":"active","keys":{"active":"` + key + `"}}`}
	return service, store
}

func adoptedCleanupApplication(t *testing.T, resources []importcontract.ResourceSnapshot) *model.Applications {
	t.Helper()
	snapshot := importcontract.NewSnapshot("prod", resources)
	raw, err := model.NewJSONStructByStruct(snapshot)
	require.NoError(t, err)
	return &model.Applications{
		ID:               "app-1",
		Name:             "legacy",
		Namespace:        "prod",
		ManagementMode:   config.ManagementModeAdopted,
		AdoptionSnapshot: raw,
	}
}

func adoptedCleanupSnapshotResource(
	apiVersion, kind, name, uid, role, ownership, disposition string,
) importcontract.ResourceSnapshot {
	return importcontract.ResourceSnapshot{
		Source: importcontract.ResourceIdentity{
			APIVersion: apiVersion,
			Kind:       kind,
			Namespace:  "prod",
			Name:       name,
			UID:        uid,
			SpecDigest: "import-digest",
		},
		ComponentName:  "component",
		DependencyRole: role,
		Ownership:      ownership,
		Disposition:    disposition,
	}
}

func adoptedCleanupDeployment() *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "backend", UID: types.UID("deployment-uid"), ResourceVersion: "1",
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas},
	}
}

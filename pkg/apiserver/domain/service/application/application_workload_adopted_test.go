package application

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestObserveApplicationLifecycleIsReadOnly(t *testing.T) {
	operations := []struct {
		name string
		run  func(*applicationsServiceImpl, string) error
	}{
		{
			name: "stop",
			run: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.StopApplicationDeployments(
					context.Background(),
					appID,
					apisv1.ApplicationLifecycleRequest{},
				)
				return err
			},
		},
		{
			name: "start",
			run: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.StartApplicationDeployments(
					context.Background(),
					appID,
					apisv1.ApplicationLifecycleRequest{},
				)
				return err
			},
		},
		{
			name: "restart",
			run: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.RestartApplicationWorkloads(
					context.Background(),
					appID,
					apisv1.ApplicationLifecycleRequest{},
				)
				return err
			},
		},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			app := &model.Applications{
				ID:             "observe-" + operation.name,
				Name:           "imported-app",
				Namespace:      "production",
				ManagementMode: config.ManagementModeObserve,
			}
			service, _, clientset, queueRepo := newAdoptedLifecycleTestService(t, app, nil)

			err := operation.run(service, app.ID)
			require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
			require.ErrorContains(t, err, "observe applications are read-only")
			require.Empty(t, clientset.Actions())
			require.Empty(t, queueRepo.queues)
		})
	}
}

func TestAdoptedStopUsesExactSourcesAndCapturesReplicaSnapshots(t *testing.T) {
	app := &model.Applications{
		ID:             "adopted-stop",
		Name:           "imported-app",
		Namespace:      "production",
		ManagementMode: config.ManagementModeAdopted,
	}
	deploymentComponent := adoptedTestComponent(
		app.ID,
		"backend",
		config.ServerJob,
		"production-backend",
		"deployment-uid",
	)
	statefulSetComponent := adoptedTestComponent(
		app.ID,
		"mysql",
		config.StoreJob,
		"legacy-mysql",
		"statefulset-uid",
	)
	deploymentReplicas := int32(3)
	statefulSetReplicas := int32(2)
	decoyReplicas := int32(7)
	service, store, clientset, _ := newAdoptedLifecycleTestService(
		t,
		app,
		[]*model.ApplicationComponent{deploymentComponent, statefulSetComponent},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "production-backend",
				Namespace: "production",
				UID:       types.UID("deployment-uid"),
			},
			Spec: appsv1.DeploymentSpec{Replicas: &deploymentReplicas},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "generated-name-decoy", Namespace: "production"},
			Spec:       appsv1.DeploymentSpec{Replicas: &decoyReplicas},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-mysql",
				Namespace: "production",
				UID:       types.UID("statefulset-uid"),
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: &statefulSetReplicas,
				PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenScaled: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				},
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
					{ObjectMeta: metav1.ObjectMeta{Name: "data"}},
				},
			},
		},
		adoptedTestBoundPVC("production", "data-legacy-mysql-0"),
		adoptedTestBoundPVC("production", "data-legacy-mysql-1"),
	)

	response, err := service.StopApplicationDeployments(
		context.Background(),
		app.ID,
		apisv1.ApplicationLifecycleRequest{},
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.ElementsMatch(t, []string{
		"Deployment:production/production-backend",
		"StatefulSet:production/legacy-mysql",
	}, response.StoppedResources)

	deployment, err := clientset.AppsV1().Deployments("production").Get(
		context.Background(),
		"production-backend",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, int32(0), *deployment.Spec.Replicas)
	statefulSet, err := clientset.AppsV1().StatefulSets("production").Get(
		context.Background(),
		"legacy-mysql",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, int32(0), *statefulSet.Spec.Replicas)
	decoy, err := clientset.AppsV1().Deployments("production").Get(
		context.Background(),
		"generated-name-decoy",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, int32(7), *decoy.Spec.Replicas)

	require.Equal(t, int32(3), *store.components["backend"].ResumeReplicas)
	require.Equal(t, int32(2), *store.components["mysql"].ResumeReplicas)
	require.Equal(t, string(config.ComponentStatusStopped), store.components["backend"].Status)
	require.Equal(t, string(config.ComponentStatusStopped), store.components["mysql"].Status)

	retryResponse, err := service.StopApplicationDeployments(
		context.Background(),
		app.ID,
		apisv1.ApplicationLifecycleRequest{},
	)
	require.NoError(t, err)
	require.NotNil(t, retryResponse)
	require.Equal(t, int32(3), *store.components["backend"].ResumeReplicas)
	require.Equal(t, int32(2), *store.components["mysql"].ResumeReplicas)
}

func TestAdoptedLifecycleRejectsLiveHPAAddedAfterImportBeforeAnyWrite(t *testing.T) {
	app := &model.Applications{
		ID:             "adopted-hpa",
		Name:           "imported-app",
		Namespace:      "production",
		ManagementMode: config.ManagementModeAdopted,
	}
	component := adoptedTestComponent(
		app.ID,
		"backend",
		config.ServerJob,
		"production-backend",
		"deployment-uid",
	)
	service, store, clientset, queueRepo := newAdoptedLifecycleTestService(
		t,
		app,
		[]*model.ApplicationComponent{component},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "production-backend",
				Namespace: "production",
				UID:       types.UID("deployment-uid"),
			},
			Spec: appsv1.DeploymentSpec{Replicas: adoptedTestInt32Ptr(3)},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backend-autoscaler",
				Namespace: "production",
			},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: appsv1.SchemeGroupVersion.String(),
					Kind:       "Deployment",
					Name:       "production-backend",
				},
			},
		},
	)

	response, err := service.StopApplicationDeployments(
		context.Background(),
		app.ID,
		apisv1.ApplicationLifecycleRequest{},
	)
	require.ErrorContains(t, err, "HorizontalPodAutoscaler production/backend-autoscaler")
	require.ErrorContains(t, err, "HPA coordination is unsupported")
	require.Nil(t, response)
	requireNoAdoptedLifecycleKubeWrites(t, clientset)
	require.Nil(t, store.components["backend"].ResumeReplicas)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
	require.Empty(t, queueRepo.queues)
}

func TestAdoptedStopRejectsUnsafeStatefulSetBeforeAnyWrite(t *testing.T) {
	app := &model.Applications{
		ID:             "adopted-stop-unsafe",
		Name:           "imported-app",
		Namespace:      "production",
		ManagementMode: config.ManagementModeAdopted,
	}
	deploymentComponent := adoptedTestComponent(
		app.ID,
		"backend",
		config.ServerJob,
		"production-backend",
		"deployment-uid",
	)
	statefulSetComponent := adoptedTestComponent(
		app.ID,
		"mysql",
		config.StoreJob,
		"legacy-mysql",
		"statefulset-uid",
	)
	deploymentReplicas := int32(3)
	statefulSetReplicas := int32(1)
	service, store, clientset, queueRepo := newAdoptedLifecycleTestService(
		t,
		app,
		[]*model.ApplicationComponent{deploymentComponent, statefulSetComponent},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "production-backend",
				Namespace: "production",
				UID:       types.UID("deployment-uid"),
			},
			Spec: appsv1.DeploymentSpec{Replicas: &deploymentReplicas},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-mysql",
				Namespace: "production",
				UID:       types.UID("statefulset-uid"),
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: &statefulSetReplicas,
				PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				},
			},
		},
	)

	response, err := service.StopApplicationDeployments(
		context.Background(),
		app.ID,
		apisv1.ApplicationLifecycleRequest{},
	)
	require.ErrorContains(t, err, "whenScaled=Delete")
	require.Nil(t, response)
	requireNoAdoptedLifecycleKubeWrites(t, clientset)
	require.Nil(t, store.components["backend"].ResumeReplicas)
	require.Nil(t, store.components["mysql"].ResumeReplicas)
	require.Empty(t, queueRepo.queues)

	deployment, err := clientset.AppsV1().Deployments("production").Get(
		context.Background(),
		"production-backend",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, int32(3), *deployment.Spec.Replicas)
	statefulSet, err := clientset.AppsV1().StatefulSets("production").Get(
		context.Background(),
		"legacy-mysql",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, int32(1), *statefulSet.Spec.Replicas)
}

func TestAdoptedStopRejectsUnsafePVCBeforeAnyWrite(t *testing.T) {
	tests := []struct {
		name       string
		wantErr    string
		claimPhase corev1.PersistentVolumeClaimPhase
	}{
		{
			name:    "missing",
			wantErr: "does not exist",
		},
		{
			name:       "pending",
			wantErr:    "must be Bound",
			claimPhase: corev1.ClaimPending,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := &model.Applications{
				ID:             "adopted-pvc-" + test.name,
				Name:           "imported-app",
				Namespace:      "production",
				ManagementMode: config.ManagementModeAdopted,
			}
			component := adoptedTestComponent(
				app.ID,
				"mysql",
				config.StoreJob,
				"legacy-mysql",
				"statefulset-uid",
			)
			replicas := int32(1)
			objects := []runtime.Object{
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "legacy-mysql",
						Namespace: "production",
						UID:       types.UID("statefulset-uid"),
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: &replicas,
						VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
							{ObjectMeta: metav1.ObjectMeta{Name: "data"}},
						},
					},
				},
			}
			if test.claimPhase != "" {
				objects = append(objects, &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "data-legacy-mysql-0",
						Namespace: "production",
					},
					Status: corev1.PersistentVolumeClaimStatus{Phase: test.claimPhase},
				})
			}
			service, store, clientset, _ := newAdoptedLifecycleTestService(
				t,
				app,
				[]*model.ApplicationComponent{component},
				objects...,
			)

			response, err := service.StopApplicationDeployments(
				context.Background(),
				app.ID,
				apisv1.ApplicationLifecycleRequest{},
			)
			require.ErrorContains(t, err, test.wantErr)
			require.Nil(t, response)
			requireNoAdoptedLifecycleKubeWrites(t, clientset)
			require.Nil(t, store.components["mysql"].ResumeReplicas)

			statefulSet, err := clientset.AppsV1().StatefulSets("production").Get(
				context.Background(),
				"legacy-mysql",
				metav1.GetOptions{},
			)
			require.NoError(t, err)
			require.Equal(t, int32(1), *statefulSet.Spec.Replicas)
		})
	}
}

func TestAdoptedStopRevalidatesFreshStatefulSetBeforeScaleDown(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*appsv1.StatefulSet)
		wantErr string
	}{
		{
			name: "retention policy changed to delete",
			mutate: func(statefulSet *appsv1.StatefulSet) {
				statefulSet.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled =
					appsv1.DeletePersistentVolumeClaimRetentionPolicyType
			},
			wantErr: "whenScaled=Delete",
		},
		{
			name: "replicas changed",
			mutate: func(statefulSet *appsv1.StatefulSet) {
				statefulSet.Spec.Replicas = adoptedTestInt32Ptr(2)
			},
			wantErr: "replicas changed after preflight",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := &model.Applications{
				ID:             "adopted-revalidate-" + test.name,
				Name:           "imported-app",
				Namespace:      "production",
				ManagementMode: config.ManagementModeAdopted,
			}
			component := adoptedTestComponent(
				app.ID,
				"mysql",
				config.StoreJob,
				"legacy-mysql",
				"statefulset-uid",
			)
			service, store, clientset, queueRepo := newAdoptedLifecycleTestService(
				t,
				app,
				[]*model.ApplicationComponent{component},
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "legacy-mysql",
						Namespace: "production",
						UID:       types.UID("statefulset-uid"),
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: adoptedTestInt32Ptr(1),
						PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
							WhenScaled: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						},
					},
				},
			)
			getCount := 0
			clientset.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
				getAction := action.(k8stesting.GetAction)
				object, err := clientset.Tracker().Get(
					appsv1.SchemeGroupVersion.WithResource("statefulsets"),
					getAction.GetNamespace(),
					getAction.GetName(),
				)
				if err != nil {
					return true, nil, err
				}
				statefulSet := object.(*appsv1.StatefulSet).DeepCopy()
				getCount++
				if getCount >= 2 {
					test.mutate(statefulSet)
				}
				return true, statefulSet, nil
			})

			response, err := service.StopApplicationDeployments(
				context.Background(),
				app.ID,
				apisv1.ApplicationLifecycleRequest{},
			)
			require.ErrorContains(t, err, test.wantErr)
			require.NotNil(t, response)
			requireNoAdoptedLifecycleKubeWrites(t, clientset)
			require.Equal(t, int32(1), *store.components["mysql"].ResumeReplicas)
			require.Equal(t, string(config.ComponentStatusRunning), store.components["mysql"].Status)
			require.Len(t, queueRepo.queues, 1)
		})
	}
}

func TestAdoptedStopSnapshotUpdatePreservesConcurrentComponentFields(t *testing.T) {
	app := &model.Applications{
		ID:             "adopted-preserve-component",
		Name:           "imported-app",
		Namespace:      "production",
		ManagementMode: config.ManagementModeAdopted,
	}
	component := adoptedTestComponent(
		app.ID,
		"backend",
		config.ServerJob,
		"legacy-backend",
		"deployment-uid",
	)
	component.Image = "original-image"
	component.Replicas = 1
	service, store, _, _ := newAdoptedLifecycleTestService(
		t,
		app,
		[]*model.ApplicationComponent{component},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-backend",
				Namespace: "production",
				UID:       types.UID("deployment-uid"),
			},
			Spec: appsv1.DeploymentSpec{Replicas: adoptedTestInt32Ptr(1)},
		},
	)
	store.beforeTransaction = func(store *inMemoryAppStore) {
		replacement := *store.components["backend"]
		replacement.Image = "concurrent-image"
		replacement.Replicas = 9
		store.components["backend"] = &replacement
	}

	response, err := service.StopApplicationDeployments(
		context.Background(),
		app.ID,
		apisv1.ApplicationLifecycleRequest{},
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, "concurrent-image", store.components["backend"].Image)
	require.Equal(t, int32(9), store.components["backend"].Replicas)
	require.Equal(t, int32(1), *store.components["backend"].ResumeReplicas)
}

func TestAdoptedLifecycleRejectsMissingOrMismatchedSourceBeforeAnyWrite(t *testing.T) {
	tests := []struct {
		name    string
		objects []runtime.Object
		wantErr string
	}{
		{
			name: "uid mismatch",
			objects: []runtime.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "legacy-backend",
						Namespace: "production",
						UID:       types.UID("replacement-uid"),
					},
					Spec: appsv1.DeploymentSpec{Replicas: adoptedTestInt32Ptr(2)},
				},
			},
			wantErr: "source UID mismatch",
		},
		{
			name:    "not found",
			wantErr: "source workload not found",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := &model.Applications{
				ID:             "adopted-identity-" + test.name,
				Name:           "imported-app",
				Namespace:      "production",
				ManagementMode: config.ManagementModeAdopted,
			}
			component := adoptedTestComponent(
				app.ID,
				"backend",
				config.ServerJob,
				"legacy-backend",
				"original-uid",
			)
			service, store, clientset, _ := newAdoptedLifecycleTestService(
				t,
				app,
				[]*model.ApplicationComponent{component},
				test.objects...,
			)

			response, err := service.StopApplicationDeployments(
				context.Background(),
				app.ID,
				apisv1.ApplicationLifecycleRequest{},
			)
			require.ErrorContains(t, err, test.wantErr)
			require.Nil(t, response)
			requireNoAdoptedLifecycleKubeWrites(t, clientset)
			require.Nil(t, store.components["backend"].ResumeReplicas)
		})
	}
}

func TestAdoptedLifecyclePreflightsSkippedSourceIdentityAndStatefulSafety(t *testing.T) {
	t.Run("already running deployment start still validates UID", func(t *testing.T) {
		app := &model.Applications{
			ID:             "adopted-skipped-start",
			Name:           "imported-app",
			Namespace:      "production",
			ManagementMode: config.ManagementModeAdopted,
		}
		component := adoptedTestComponent(
			app.ID,
			"backend",
			config.ServerJob,
			"legacy-backend",
			"original-uid",
		)
		component.Status = string(config.ComponentStatusRunning)
		service, _, clientset, queueRepo := newAdoptedLifecycleTestService(
			t,
			app,
			[]*model.ApplicationComponent{component},
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "legacy-backend",
					Namespace: "production",
					UID:       types.UID("replacement-uid"),
				},
				Spec: appsv1.DeploymentSpec{Replicas: adoptedTestInt32Ptr(1)},
			},
		)

		response, err := service.StartApplicationDeployments(
			context.Background(),
			app.ID,
			apisv1.ApplicationLifecycleRequest{},
		)
		require.ErrorContains(t, err, "source UID mismatch")
		require.Nil(t, response)
		requireNoAdoptedLifecycleKubeWrites(t, clientset)
		require.Empty(t, queueRepo.queues)
	})

	t.Run("stopped statefulset restart still validates data safety", func(t *testing.T) {
		app := &model.Applications{
			ID:             "adopted-skipped-restart",
			Name:           "imported-app",
			Namespace:      "production",
			ManagementMode: config.ManagementModeAdopted,
		}
		component := adoptedTestComponent(
			app.ID,
			"mysql",
			config.StoreJob,
			"legacy-mysql",
			"statefulset-uid",
		)
		component.Status = string(config.ComponentStatusStopped)
		component.ResumeReplicas = adoptedTestInt32Ptr(1)
		service, _, clientset, queueRepo := newAdoptedLifecycleTestService(
			t,
			app,
			[]*model.ApplicationComponent{component},
			&appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "legacy-mysql",
					Namespace: "production",
					UID:       types.UID("statefulset-uid"),
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: adoptedTestInt32Ptr(0),
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
					},
				},
			},
		)

		response, err := service.RestartApplicationWorkloads(
			context.Background(),
			app.ID,
			apisv1.ApplicationLifecycleRequest{},
		)
		require.ErrorContains(t, err, "whenScaled=Delete")
		require.Nil(t, response)
		requireNoAdoptedLifecycleKubeWrites(t, clientset)
		require.Empty(t, queueRepo.queues)
	})
}

func TestAdoptedStartRestoresSnapshotsForDeploymentAndStatefulSet(t *testing.T) {
	app := &model.Applications{
		ID:             "adopted-start",
		Name:           "imported-app",
		Namespace:      "production",
		ManagementMode: config.ManagementModeAdopted,
	}
	deploymentComponent := adoptedTestComponent(
		app.ID,
		"backend",
		config.ServerJob,
		"legacy-backend",
		"deployment-uid",
	)
	deploymentComponent.Status = string(config.ComponentStatusStopped)
	deploymentComponent.ResumeReplicas = adoptedTestInt32Ptr(4)
	statefulSetComponent := adoptedTestComponent(
		app.ID,
		"mysql",
		config.StoreJob,
		"legacy-mysql",
		"statefulset-uid",
	)
	statefulSetComponent.Status = string(config.ComponentStatusStopped)
	statefulSetComponent.ResumeReplicas = adoptedTestInt32Ptr(2)
	zero := int32(0)
	service, store, clientset, _ := newAdoptedLifecycleTestService(
		t,
		app,
		[]*model.ApplicationComponent{deploymentComponent, statefulSetComponent},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-backend",
				Namespace: "production",
				UID:       types.UID("deployment-uid"),
			},
			Spec: appsv1.DeploymentSpec{Replicas: &zero},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-mysql",
				Namespace: "production",
				UID:       types.UID("statefulset-uid"),
			},
			Spec: appsv1.StatefulSetSpec{Replicas: &zero},
		},
	)

	response, err := service.StartApplicationDeployments(
		context.Background(),
		app.ID,
		apisv1.ApplicationLifecycleRequest{},
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.ElementsMatch(t, []string{
		"Deployment:production/legacy-backend",
		"StatefulSet:production/legacy-mysql",
	}, response.StartedResources)

	deployment, err := clientset.AppsV1().Deployments("production").Get(
		context.Background(),
		"legacy-backend",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, int32(4), *deployment.Spec.Replicas)
	statefulSet, err := clientset.AppsV1().StatefulSets("production").Get(
		context.Background(),
		"legacy-mysql",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, int32(2), *statefulSet.Spec.Replicas)
	require.Equal(t, string(config.ComponentStatusStarting), store.components["backend"].Status)
	require.Equal(t, string(config.ComponentStatusStarting), store.components["mysql"].Status)
}

func TestAdoptedStartAndRestartRejectUnsafeStatefulSetBeforeAnyWrite(t *testing.T) {
	tests := []struct {
		name      string
		operation func(*applicationsServiceImpl, string) error
		stateful  *appsv1.StatefulSet
		status    config.ComponentStatus
		wantErr   string
	}{
		{
			name: "start with missing pvc",
			operation: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.StartApplicationDeployments(
					context.Background(),
					appID,
					apisv1.ApplicationLifecycleRequest{},
				)
				return err
			},
			stateful: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "legacy-mysql",
					Namespace: "production",
					UID:       types.UID("statefulset-uid"),
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: adoptedTestInt32Ptr(0),
					VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
						{ObjectMeta: metav1.ObjectMeta{Name: "data"}},
					},
				},
			},
			status:  config.ComponentStatusStopped,
			wantErr: "does not exist",
		},
		{
			name: "restart with delete retention",
			operation: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.RestartApplicationWorkloads(
					context.Background(),
					appID,
					apisv1.ApplicationLifecycleRequest{},
				)
				return err
			},
			stateful: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "legacy-mysql",
					Namespace: "production",
					UID:       types.UID("statefulset-uid"),
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: adoptedTestInt32Ptr(1),
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
					},
				},
			},
			status:  config.ComponentStatusRunning,
			wantErr: "whenScaled=Delete",
		},
		{
			name: "restart with on-delete strategy",
			operation: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.RestartApplicationWorkloads(
					context.Background(),
					appID,
					apisv1.ApplicationLifecycleRequest{},
				)
				return err
			},
			stateful: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "legacy-mysql",
					Namespace: "production",
					UID:       types.UID("statefulset-uid"),
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:       adoptedTestInt32Ptr(1),
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
				},
			},
			status:  config.ComponentStatusRunning,
			wantErr: "OnDelete",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := &model.Applications{
				ID:             "adopted-" + test.name,
				Name:           "imported-app",
				Namespace:      "production",
				ManagementMode: config.ManagementModeAdopted,
			}
			component := adoptedTestComponent(
				app.ID,
				"mysql",
				config.StoreJob,
				"legacy-mysql",
				"statefulset-uid",
			)
			component.Status = string(test.status)
			component.ResumeReplicas = adoptedTestInt32Ptr(1)
			service, _, clientset, queueRepo := newAdoptedLifecycleTestService(
				t,
				app,
				[]*model.ApplicationComponent{component},
				test.stateful,
			)

			require.ErrorContains(t, test.operation(service, app.ID), test.wantErr)
			requireNoAdoptedLifecycleKubeWrites(t, clientset)
			require.Empty(t, queueRepo.queues)
		})
	}
}

func TestAdoptedRestartMutatesExactSourcesAndAddsManagedLabels(t *testing.T) {
	app := &model.Applications{
		ID:             "adopted-restart",
		Name:           "imported-app",
		Namespace:      "production",
		ManagementMode: config.ManagementModeAdopted,
	}
	deploymentComponent := adoptedTestComponent(
		app.ID,
		"backend",
		config.ServerJob,
		"legacy-backend",
		"deployment-uid",
	)
	deploymentComponent.ID = 41
	statefulSetComponent := adoptedTestComponent(
		app.ID,
		"mysql",
		config.StoreJob,
		"legacy-mysql",
		"statefulset-uid",
	)
	statefulSetComponent.ID = 42
	service, store, clientset, _ := newAdoptedLifecycleTestService(
		t,
		app,
		[]*model.ApplicationComponent{deploymentComponent, statefulSetComponent},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-backend",
				Namespace: "production",
				UID:       types.UID("deployment-uid"),
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: adoptedTestInt32Ptr(3),
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"custom": "preserved"}},
				},
			},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-mysql",
				Namespace: "production",
				UID:       types.UID("statefulset-uid"),
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: adoptedTestInt32Ptr(2),
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"custom": "preserved"}},
				},
			},
		},
	)

	response, err := service.RestartApplicationWorkloads(
		context.Background(),
		app.ID,
		apisv1.ApplicationLifecycleRequest{},
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.ElementsMatch(t, []string{
		"Deployment:production/legacy-backend",
		"StatefulSet:production/legacy-mysql",
	}, response.RestartedResources)

	deployment, err := clientset.AppsV1().Deployments("production").Get(
		context.Background(),
		"legacy-backend",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, "preserved", deployment.Spec.Template.Labels["custom"])
	require.Equal(t, app.ID, deployment.Spec.Template.Labels[config.LabelAppID])
	require.Equal(t, "backend", deployment.Spec.Template.Labels[config.LabelComponentName])
	require.Equal(t, "41", deployment.Spec.Template.Labels[config.LabelComponentID])
	require.Equal(t, response.RestartedAt, deployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])

	statefulSet, err := clientset.AppsV1().StatefulSets("production").Get(
		context.Background(),
		"legacy-mysql",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, "preserved", statefulSet.Spec.Template.Labels["custom"])
	require.Equal(t, app.ID, statefulSet.Spec.Template.Labels[config.LabelAppID])
	require.Equal(t, "mysql", statefulSet.Spec.Template.Labels[config.LabelComponentName])
	require.Equal(t, "42", statefulSet.Spec.Template.Labels[config.LabelComponentID])
	require.Equal(t, response.RestartedAt, statefulSet.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	require.Equal(t, string(config.ComponentStatusRestarting), store.components["backend"].Status)
	require.Equal(t, string(config.ComponentStatusRestarting), store.components["mysql"].Status)
}

func TestAdoptedRestartRejectsPausedDeploymentBeforeAnyWrite(t *testing.T) {
	app := &model.Applications{
		ID:             "adopted-paused-restart",
		Name:           "imported-app",
		Namespace:      "production",
		ManagementMode: config.ManagementModeAdopted,
	}
	component := adoptedTestComponent(
		app.ID,
		"backend",
		config.ServerJob,
		"legacy-backend",
		"deployment-uid",
	)
	service, _, clientset, queueRepo := newAdoptedLifecycleTestService(
		t,
		app,
		[]*model.ApplicationComponent{component},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-backend",
				Namespace: "production",
				UID:       types.UID("deployment-uid"),
			},
			Spec: appsv1.DeploymentSpec{
				Paused:   true,
				Replicas: adoptedTestInt32Ptr(0),
			},
		},
	)

	_, err := service.RestartApplicationWorkloads(
		context.Background(),
		app.ID,
		apisv1.ApplicationLifecycleRequest{},
	)
	require.ErrorContains(t, err, "Deployment is paused")
	requireNoAdoptedLifecycleKubeWrites(t, clientset)
	require.Empty(t, queueRepo.queues)
}

func TestFormatWorkloadRestartAtPreservesNanoseconds(t *testing.T) {
	now := time.Date(2026, time.August, 1, 12, 34, 56, 123456789, time.FixedZone("UTC+8", 8*60*60))

	require.Equal(t, "2026-08-01T04:34:56.123456789Z", formatWorkloadRestartAt(now))
}

func TestAdoptedLifecycleSerializesWithWorkflowAndScheduleLock(t *testing.T) {
	operations := []struct {
		name string
		run  func(*applicationsServiceImpl, string) error
	}{
		{
			name: "stop",
			run: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.StopApplicationDeployments(
					context.Background(),
					appID,
					apisv1.ApplicationLifecycleRequest{},
				)
				return err
			},
		},
		{
			name: "start",
			run: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.StartApplicationDeployments(
					context.Background(),
					appID,
					apisv1.ApplicationLifecycleRequest{},
				)
				return err
			},
		},
		{
			name: "restart",
			run: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.RestartApplicationWorkloads(
					context.Background(),
					appID,
					apisv1.ApplicationLifecycleRequest{},
				)
				return err
			},
		},
	}
	blockers := []struct {
		name      string
		prepare   func(*testing.T, *applicationsServiceImpl, *inMemoryAppStore, string) func()
		expectErr error
	}{
		{
			name: "active workflow",
			prepare: func(
				t *testing.T,
				_ *applicationsServiceImpl,
				store *inMemoryAppStore,
				appID string,
			) func() {
				require.NoError(t, store.Add(context.Background(), &model.WorkflowQueue{
					TaskID: "running-workflow-" + appID,
					AppID:  appID,
					Status: config.StatusRunning,
				}))
				return func() {}
			},
			expectErr: bcode.ErrWorkflowTaskRunning,
		},
		{
			name: "schedule lock",
			prepare: func(
				t *testing.T,
				service *applicationsServiceImpl,
				_ *inMemoryAppStore,
				appID string,
			) func() {
				return holdApplicationTestAppScheduleLock(t, service.ScheduleLocker, appID)
			},
			expectErr: bcode.ErrApplicationOperationLocked,
		},
	}

	for _, blocker := range blockers {
		for _, operation := range operations {
			t.Run(blocker.name+"/"+operation.name, func(t *testing.T) {
				app := &model.Applications{
					ID:             "adopted-" + operation.name,
					Name:           "imported-app",
					Namespace:      "production",
					ManagementMode: config.ManagementModeAdopted,
				}
				component := adoptedTestComponent(
					app.ID,
					"backend",
					config.ServerJob,
					"production-backend",
					"deployment-uid",
				)
				if operation.name == "start" {
					component.Status = string(config.ComponentStatusStopped)
					component.ResumeReplicas = adoptedTestInt32Ptr(2)
				}
				replicas := int32(2)
				service, store, clientset, _ := newAdoptedLifecycleTestService(
					t,
					app,
					[]*model.ApplicationComponent{component},
					&appsv1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "production-backend",
							Namespace: "production",
							UID:       types.UID("deployment-uid"),
						},
						Spec: appsv1.DeploymentSpec{Replicas: &replicas},
					},
				)
				release := blocker.prepare(t, service, store, app.ID)
				defer release()

				err := operation.run(service, app.ID)
				require.ErrorIs(t, err, blocker.expectErr)
				requireNoAdoptedLifecycleKubeWrites(t, clientset)
			})
		}
	}
}

func TestAdoptedLifecycleRecordsTaskWhileScheduleLockIsHeld(t *testing.T) {
	operations := []struct {
		name    string
		prepare func(*model.ApplicationComponent, *appsv1.Deployment)
		run     func(*applicationsServiceImpl, string) error
	}{
		{
			name: "stop",
			run: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.StopApplicationDeployments(context.Background(), appID, apisv1.ApplicationLifecycleRequest{})
				return err
			},
		},
		{
			name: "start",
			prepare: func(component *model.ApplicationComponent, deployment *appsv1.Deployment) {
				component.Status = string(config.ComponentStatusStopped)
				component.ResumeReplicas = adoptedTestInt32Ptr(2)
				deployment.Spec.Replicas = adoptedTestInt32Ptr(0)
			},
			run: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.StartApplicationDeployments(context.Background(), appID, apisv1.ApplicationLifecycleRequest{})
				return err
			},
		},
		{
			name: "restart",
			run: func(service *applicationsServiceImpl, appID string) error {
				_, err := service.RestartApplicationWorkloads(context.Background(), appID, apisv1.ApplicationLifecycleRequest{})
				return err
			},
		},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			app := &model.Applications{
				ID:             "adopted-" + operation.name,
				Name:           "imported-app",
				Namespace:      "production",
				ManagementMode: config.ManagementModeAdopted,
			}
			component := adoptedTestComponent(
				app.ID,
				"backend",
				config.ServerJob,
				"production-backend",
				"deployment-uid",
			)
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "production-backend",
					Namespace: "production",
					UID:       types.UID("deployment-uid"),
				},
				Spec: appsv1.DeploymentSpec{Replicas: adoptedTestInt32Ptr(2)},
			}
			if operation.prepare != nil {
				operation.prepare(component, deployment)
			}
			service, _, _, queueRepo := newAdoptedLifecycleTestService(
				t,
				app,
				[]*model.ApplicationComponent{component},
				deployment,
			)

			taskCreateStarted := make(chan struct{})
			releaseTaskCreate := make(chan struct{})
			queueRepo.beforeCreate = func() {
				close(taskCreateStarted)
				<-releaseTaskCreate
			}
			result := make(chan error, 1)
			go func() {
				result <- operation.run(service, app.ID)
			}()

			select {
			case <-taskCreateStarted:
			case err := <-result:
				t.Fatalf("lifecycle returned before recording task: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for lifecycle task persistence")
			}

			lockErr := schedulelock.WithAppScheduleLock(
				context.Background(),
				service.ScheduleLocker,
				app.ID,
				"concurrent-delete",
				true,
				func(context.Context) error {
					return nil
				},
			)
			require.ErrorIs(t, lockErr, bcode.ErrApplicationOperationLocked)
			close(releaseTaskCreate)
			require.NoError(t, <-result)
		})
	}
}

func newAdoptedLifecycleTestService(
	t *testing.T,
	app *model.Applications,
	components []*model.ApplicationComponent,
	objects ...runtime.Object,
) (*applicationsServiceImpl, *inMemoryAppStore, *fake.Clientset, *mockWorkflowQueueRepo) {
	t.Helper()
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), app))
	for _, component := range components {
		require.NoError(t, store.Add(context.Background(), component))
	}
	clientset := fake.NewSimpleClientset(objects...)
	queueRepo := &mockWorkflowQueueRepo{}
	service := &applicationsServiceImpl{
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &mockAppRepo{store: store},
		ComponentRepo:     &mockComponentRepo{store: store},
		WorkflowQueueRepo: queueRepo,
		ScheduleLocker:    locker.NewMemoryLocker("test-adopted-lifecycle"),
	}
	return service, store, clientset, queueRepo
}

func adoptedTestComponent(
	appID string,
	name string,
	componentType config.JobType,
	sourceName string,
	sourceUID string,
) *model.ApplicationComponent {
	return &model.ApplicationComponent{
		AppID:                    appID,
		Name:                     name,
		Namespace:                "production",
		ComponentType:            componentType,
		Status:                   string(config.ComponentStatusRunning),
		SourceWorkloadAPIVersion: appsv1.SchemeGroupVersion.String(),
		SourceWorkloadKind:       adoptedTestSourceKind(componentType),
		SourceWorkloadName:       sourceName,
		SourceWorkloadUID:        adoptedTestStringPtr(sourceUID),
	}
}

func adoptedTestSourceKind(componentType config.JobType) string {
	if componentType == config.StoreJob {
		return "StatefulSet"
	}
	return "Deployment"
}

func adoptedTestBoundPVC(namespace, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func adoptedTestStringPtr(value string) *string {
	return &value
}

func adoptedTestInt32Ptr(value int32) *int32 {
	return &value
}

func requireNoAdoptedLifecycleKubeWrites(t *testing.T, clientset *fake.Clientset) {
	t.Helper()
	for _, action := range clientset.Actions() {
		switch action.GetVerb() {
		case "create", "update", "patch", "delete", "deletecollection":
			t.Fatalf("unexpected Kubernetes write action: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

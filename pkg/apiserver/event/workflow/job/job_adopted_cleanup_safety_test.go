package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type cleanupOwnershipStore struct {
	*adoptedSourceStore
	getErr             error
	getCalls           int
	getStarted         chan struct{}
	resumeGet          <-chan struct{}
	lookupContextErr   error
	lookupContextKey   any
	lookupContextValue any
	lookupDeadline     time.Time
	lookupHasDeadline  bool
	returnedErr        error
}

func (s *cleanupOwnershipStore) Get(ctx context.Context, entity datastore.Entity) error {
	s.getCalls++
	if s.getStarted != nil {
		close(s.getStarted)
	}
	if s.resumeGet != nil {
		<-s.resumeGet
	}
	s.lookupContextErr = ctx.Err()
	if s.lookupContextKey != nil {
		s.lookupContextValue = ctx.Value(s.lookupContextKey)
	}
	s.lookupDeadline, s.lookupHasDeadline = ctx.Deadline()
	if s.lookupContextErr != nil {
		s.returnedErr = s.lookupContextErr
		return s.returnedErr
	}
	if s.getErr != nil {
		s.returnedErr = s.getErr
		return s.returnedErr
	}
	s.returnedErr = s.adoptedSourceStore.Get(ctx, entity)
	return s.returnedErr
}

type cleanupWorkloadCase struct {
	name          string
	kind          domainspec.ResourceKind
	componentName string
	resourceName  string
	resource      string
	newSource     func() runtime.Object
	newController func(runtime.Object, *fake.Clientset, datastore.DataStore) JobCtl
	get           func(*fake.Clientset) (runtime.Object, error)
}

func cleanupWorkloadCases() []cleanupWorkloadCase {
	return []cleanupWorkloadCase{
		{
			name:          "deployment",
			kind:          domainspec.ResourceDeployment,
			componentName: "backend",
			resourceName:  "legacy-backend",
			resource:      "deployments",
			newSource: func() runtime.Object {
				return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "legacy-backend", Namespace: "ops", UID: types.UID("deployment-uid")}}
			},
			newController: func(object runtime.Object, client *fake.Clientset, store datastore.DataStore) JobCtl {
				return NewDeployJobCtl(
					&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeploy), JobInfo: object.(*appsv1.Deployment).DeepCopy()},
					client,
					store,
					func() {},
					locker.NewNoopLocker(shareLockerPrefix),
				)
			},
			get: func(client *fake.Clientset) (runtime.Object, error) {
				return client.AppsV1().Deployments("ops").Get(context.Background(), "legacy-backend", metav1.GetOptions{})
			},
		},
		{
			name:          "statefulset",
			kind:          domainspec.ResourceStatefulSet,
			componentName: "mysql",
			resourceName:  "legacy-mysql",
			resource:      "statefulsets",
			newSource: func() runtime.Object {
				return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "legacy-mysql", Namespace: "ops", UID: types.UID("statefulset-uid")}}
			},
			newController: func(object runtime.Object, client *fake.Clientset, store datastore.DataStore) JobCtl {
				return NewDeployStatefulSetJobCtl(
					&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployStore), JobInfo: object.(*appsv1.StatefulSet).DeepCopy()},
					client,
					store,
					func() {},
					locker.NewNoopLocker(shareLockerPrefix),
				)
			},
			get: func(client *fake.Clientset) (runtime.Object, error) {
				return client.AppsV1().StatefulSets("ops").Get(context.Background(), "legacy-mysql", metav1.GetOptions{})
			},
		},
	}
}

func TestWorkloadFailureCleanupOwnershipLookupSurvivesMidflightCancellation(t *testing.T) {
	for _, workload := range cleanupWorkloadCases() {
		t.Run(workload.name, func(t *testing.T) {
			source := workload.newSource()
			client := fake.NewSimpleClientset(source.DeepCopyObject())
			lookupContextKey := new(int)
			const lookupContextValue = "cleanup-ownership"
			trackedCtx := WithCleanupTracker(context.WithValue(context.Background(), lookupContextKey, lookupContextValue))
			MarkResourceCreated(trackedCtx, workload.kind, "ops", workload.resourceName)
			parentCtx, cancel := context.WithCancel(trackedCtx)
			getStarted := make(chan struct{})
			store := &cleanupOwnershipStore{
				adoptedSourceStore: &adoptedSourceStore{app: &model.Applications{
					ID:             "app-1",
					Namespace:      "ops",
					ManagementMode: config.ManagementModeNative,
				}},
				getStarted:       getStarted,
				resumeGet:        parentCtx.Done(),
				lookupContextKey: lookupContextKey,
			}
			controller := workload.newController(source, client, store)
			require.NotNil(t, controller)

			cleanDone := make(chan struct{})
			go func() {
				defer close(cleanDone)
				controller.Clean(parentCtx)
			}()

			select {
			case <-getStarted:
				require.NoError(t, parentCtx.Err())
			case <-time.After(5 * time.Second):
				cancel()
				<-cleanDone
				t.Fatal("ownership lookup did not start")
			}
			cancel()

			select {
			case <-cleanDone:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup did not finish after parent cancellation")
			}

			require.ErrorIs(t, parentCtx.Err(), context.Canceled)
			require.Equal(t, 1, store.getCalls)
			require.NoError(t, store.lookupContextErr)
			require.Equal(t, lookupContextValue, store.lookupContextValue)
			require.True(t, store.lookupHasDeadline)
			require.True(t, store.lookupDeadline.After(time.Now()))
			require.NoError(t, store.returnedErr)
			_, getErr := workload.get(client)
			require.True(t, k8serrors.IsNotFound(getErr))
			require.Equal(t, 1, countClientActions(client, "delete", workload.resource))
		})
	}
}

func TestAdoptedWorkloadFailureCleanupNeverDeletesSourceResources(t *testing.T) {
	t.Run("deployment", func(t *testing.T) {
		source := &appsv1.Deployment{
			TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-backend",
				Namespace: "ops",
				UID:       types.UID("deployment-uid"),
			},
		}
		snapshot := adoptedSnapshotResource(
			t,
			source,
			"backend",
			"workload",
			adoption.OwnershipExclusive,
			adoption.DispositionManaged,
		)
		client := fake.NewSimpleClientset(source.DeepCopy())
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", snapshot)}
		ctl := NewDeployJobCtl(
			&model.JobTask{
				Name:      "backend",
				AppID:     "app-1",
				Namespace: "ops",
				JobType:   string(config.JobDeploy),
				JobInfo:   source.DeepCopy(),
			},
			client,
			store,
			func() {},
			locker.NewNoopLocker(shareLockerPrefix),
		)

		require.NotNil(t, ctl)
		ctl.Clean(context.Background())

		_, err := client.AppsV1().Deployments("ops").Get(context.Background(), source.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, 0, countClientActions(client, "delete", "deployments"))
	})

	t.Run("statefulset", func(t *testing.T) {
		source := &appsv1.StatefulSet{
			TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-mysql",
				Namespace: "ops",
				UID:       types.UID("statefulset-uid"),
			},
		}
		snapshot := adoptedSnapshotResource(
			t,
			source,
			"mysql",
			"workload",
			adoption.OwnershipExclusive,
			adoption.DispositionManaged,
		)
		client := fake.NewSimpleClientset(source.DeepCopy())
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", snapshot)}
		ctl := NewDeployStatefulSetJobCtl(
			&model.JobTask{
				Name:      "mysql",
				AppID:     "app-1",
				Namespace: "ops",
				JobType:   string(config.JobDeployStore),
				JobInfo:   source.DeepCopy(),
			},
			client,
			store,
			func() {},
			locker.NewNoopLocker(shareLockerPrefix),
		)

		require.NotNil(t, ctl)
		ctl.Clean(context.Background())

		_, err := client.AppsV1().StatefulSets("ops").Get(context.Background(), source.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
	})
}

func TestWorkloadFailureCleanupFailsClosedWhenApplicationOwnershipIsUnavailable(t *testing.T) {
	source := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-backend", Namespace: "ops"},
	}
	client := fake.NewSimpleClientset(source.DeepCopy())
	ctl := NewDeployJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "detached-app",
			Namespace: "ops",
			JobType:   string(config.JobDeploy),
			JobInfo:   source.DeepCopy(),
		},
		client,
		&adoptedSourceStore{},
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NotNil(t, ctl)
	ctl.Clean(context.Background())

	_, err := client.AppsV1().Deployments("ops").Get(context.Background(), source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, 0, countClientActions(client, "delete", "deployments"))
}

func TestWorkloadFailureCleanupResolvesOwnershipWithCancelledParent(t *testing.T) {
	for _, workload := range cleanupWorkloadCases() {
		for _, ownership := range []string{"native", "adopted", "datastore-error"} {
			t.Run(workload.name+"/"+ownership, func(t *testing.T) {
				source := workload.newSource()
				app := &model.Applications{ID: "app-1", Namespace: "ops", ManagementMode: config.ManagementModeNative}
				var datastoreErr error
				if ownership == "adopted" {
					snapshot := adoptedSnapshotResource(t, source, workload.componentName, "workload", adoption.OwnershipExclusive, adoption.DispositionManaged)
					app = adoptedApplication(t, "app-1", "ops", snapshot)
				} else if ownership == "datastore-error" {
					datastoreErr = errors.New("datastore unavailable")
				}
				store := &cleanupOwnershipStore{
					adoptedSourceStore: &adoptedSourceStore{app: app},
					getErr:             datastoreErr,
				}
				client := fake.NewSimpleClientset(source.DeepCopyObject())
				controller := workload.newController(source, client, store)
				require.NotNil(t, controller)

				trackedCtx := WithCleanupTracker(context.Background())
				MarkResourceCreated(trackedCtx, workload.kind, "ops", workload.resourceName)
				cancelledCtx, cancel := context.WithCancel(trackedCtx)
				cancel()
				require.ErrorIs(t, cancelledCtx.Err(), context.Canceled)

				controller.Clean(cancelledCtx)

				require.Equal(t, 1, store.getCalls)
				require.NoError(t, store.lookupContextErr)
				_, getErr := workload.get(client)
				if ownership == "native" {
					require.True(t, k8serrors.IsNotFound(getErr))
					require.Equal(t, 1, countClientActions(client, "delete", workload.resource))
					require.NoError(t, store.returnedErr)
					return
				}
				require.NoError(t, getErr)
				require.Equal(t, 0, countClientActions(client, "delete", workload.resource))
				if ownership == "datastore-error" {
					require.ErrorIs(t, store.returnedErr, datastoreErr)
				} else {
					require.NoError(t, store.returnedErr)
				}
			})
		}
	}
}

package job

import (
	"context"

	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"

	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

func TestCleanupResourcesRejectsSourceBoundComponent(t *testing.T) {
	uid := "deployment-uid"
	component := &model.ApplicationComponent{
		AppID:                    "app-1",
		Name:                     "api",
		Namespace:                "ops",
		ComponentType:            config.ServerJob,
		SourceWorkloadAPIVersion: "apps/v1",
		SourceWorkloadKind:       "Deployment",
		SourceWorkloadName:       "legacy-api",
		SourceWorkloadUID:        &uid,
	}
	ctl := NewCleanupResourcesJobCtl(
		&model.JobTask{AppID: "app-1", Name: "api", JobInfo: component},
		fake.NewSimpleClientset(),
		&noopStore{},
		nil,
	)
	require.NotNil(t, ctl)

	err := ctl.run(context.Background())
	require.ErrorContains(t, err, "explicit cleanup plan fingerprint")
}

func TestCleanupResourcesJobCtlPrioritizesContextTerminationOverOpaqueWorkflowTaskReadError(t *testing.T) {
	tests := []struct {
		name       string
		contextErr error
		wantStatus config.Status
	}{
		{name: "cancelled", contextErr: context.Canceled, wantStatus: config.StatusCancelled},
		{name: "deadline exceeded", contextErr: context.DeadlineExceeded, wantStatus: config.StatusTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component := &model.ApplicationComponent{
				ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
			}
			marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
			gateCtx := &cleanupMutableErrorContext{Context: context.Background()}
			store := &cleanupComponentStore{
				component: component,
				jobInfo: &model.JobInfo{
					ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
					TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
				},
				workflowTaskGetErr: errors.New("opaque workflow task read error"),
			}
			store.beforeWorkflowTaskGet = func() { gateCtx.err = tt.contextErr }
			job := &model.JobTask{
				Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
				TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component, InternalInfo: marker,
			}
			ctl := NewCleanupResourcesJobCtl(job, fake.NewSimpleClientset(), store, nil)
			require.NotNil(t, ctl)

			err := ctl.Run(gateCtx)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.contextErr)
			statusErr, ok := ExtractStatusError(err)
			require.True(t, ok)
			require.Equal(t, tt.wantStatus, statusErr.Status)
			require.Equal(t, tt.wantStatus, job.Status)
			require.False(t, ctl.skipSaveInfo)
			require.NoError(t, ctl.SaveInfo(context.Background()))
			require.Equal(t, string(tt.wantStatus), store.jobInfo.Status)
		})
	}
}

func TestCleanupResourcesJobCtlDoesNotStartWhenCleanupTerminalizesDuringStart(t *testing.T) {
	existing := &model.JobInfo{
		ID:           10,
		Type:         string(config.JobCleanupResources),
		AppID:        "app-1",
		TaskID:       "task-1",
		Status:       string(config.StatusQueued),
		InternalInfo: versionUpdateRemoveCleanupInternalInfo(),
		ServiceName:  "api",
	}
	store := &cleanupComponentStore{jobInfo: existing}
	store.beforeConditionalCAS = func() {
		store.jobInfo.Status = string(config.StatusFailed)
		store.jobInfo.Error = "workflow failed"
	}
	removed := &model.ApplicationComponent{
		ID:            1,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	task := &model.JobTask{
		Name:         removed.Name,
		Namespace:    removed.Namespace,
		AppID:        removed.AppID,
		TaskID:       existing.TaskID,
		JobType:      string(config.JobCleanupResources),
		JobInfo:      removed,
		InternalInfo: existing.InternalInfo,
	}
	ctl := NewCleanupResourcesJobCtl(task, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, ctl)

	skipCleanup, err := ctl.markVersionUpdateCleanupRunning(context.Background())
	require.Error(t, err)
	require.False(t, skipCleanup)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, statusErr.Status)
	require.True(t, ctl.skipSaveInfo)
	require.Nil(t, store.casJobInfo)
	require.Equal(t, string(config.StatusFailed), store.jobInfo.Status)
}

func TestRunJobCleanupResourcesInvalidatesComponentsCache(t *testing.T) {
	component := &model.ApplicationComponent{
		ID:            10,
		Name:          "app-config",
		AppID:         "app-cleanup-cache",
		Namespace:     "default",
		ComponentType: config.ConfJob,
	}
	store := &cleanupComponentStore{component: component}
	cacheStore := cacheutil.NewMemCache(false)
	cacheKey := cacheutil.ApplicationComponentsKey(component.AppID)
	require.NoError(t, cacheStore.Store(cacheKey, "stale"))
	require.True(t, cacheStore.Exists(cacheKey))

	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	runtime := newJobRuntime(cacheStore, nil, nil, nil, nil)

	runJob(context.Background(), task, fake.NewSimpleClientset(), store, func() {}, runtime)

	require.Equal(t, config.StatusCompleted, task.Status)
	require.False(t, cacheStore.Exists(cacheKey))
	require.NotNil(t, store.putComponent)
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.putComponent.Status)
}

func TestCleanupResourcesJobCtlValidatesInputs(t *testing.T) {
	ctl := NewCleanupResourcesJobCtl(nil, nil, nil, nil)
	require.Nil(t, ctl)

	task := &model.JobTask{JobType: string(config.JobCleanupResources), JobInfo: "invalid"}
	ctl = NewCleanupResourcesJobCtl(task, fake.NewSimpleClientset(), &noopStore{}, nil)
	require.NotNil(t, ctl)
	require.Error(t, ctl.Run(context.Background()))
	require.Equal(t, config.StatusFailed, task.Status)
}

func TestCleanupResourcesJobCtlDoesNotRecreateRemovedComponent(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-removed",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: buildWebServiceName(component.Name, component.ResourceNameKey()), Namespace: component.Namespace}},
	)
	store := &cleanupComponentStore{
		jobInfo: &model.JobInfo{
			ID:          3,
			Type:        string(config.JobCleanupResources),
			AppID:       component.AppID,
			TaskID:      "task-removed",
			Status:      string(config.StatusQueued),
			ServiceName: component.Name,
		},
	}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		TaskID:    "task-removed",
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	require.Equal(t, config.StatusCompleted, task.Status)
	require.Nil(t, store.putComponent)

	_, err := client.AppsV1().Deployments(component.Namespace).Get(ctx, buildWebServiceName(component.Name, component.ResourceNameKey()), metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
}

package job

import (
	"context"

	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestCleanupResourcesJobCtlMarksVersionUpdateCleanupRunningBeforeDelete(t *testing.T) {
	executionKey := "task-1:cleanup:api:g1:a1"
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
	removed := &model.ApplicationComponent{
		ID:            1,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	task := &model.JobTask{
		Name:          removed.Name,
		Namespace:     removed.Namespace,
		AppID:         removed.AppID,
		TaskID:        existing.TaskID,
		JobType:       string(config.JobCleanupResources),
		JobInfo:       removed,
		InternalInfo:  existing.InternalInfo,
		ExecutionKey:  executionKey,
		RunGeneration: 1,
		Attempt:       1,
	}
	ctl := NewCleanupResourcesJobCtl(task, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, ctl)

	skipCleanup, err := ctl.markVersionUpdateCleanupRunning(context.Background())
	require.NoError(t, err)
	require.False(t, skipCleanup)
	require.Equal(t, config.StatusRunning, task.Status)
	require.NotZero(t, task.StartTime)
	require.NotNil(t, store.casJobInfo)
	require.Nil(t, store.addedJobInfo)
	require.Equal(t, existing.ID, store.casJobInfo.ID)
	require.Equal(t, string(config.StatusRunning), store.casJobInfo.Status)
	require.Zero(t, store.casJobInfo.EndTime)
	require.Empty(t, store.casJobInfo.Error)
	require.NotNil(t, store.casJobInfo.ExecutionKey)
	require.Equal(t, executionKey, *store.casJobInfo.ExecutionKey)
	require.Equal(t, uint64(1), store.casJobInfo.RunGeneration)
	require.Equal(t, uint(1), store.casJobInfo.Attempt)
}

func TestCleanupResourcesJobCtlDoesNotRestartTerminalizedVersionUpdateCleanup(t *testing.T) {
	existing := &model.JobInfo{
		ID:           10,
		Type:         string(config.JobCleanupResources),
		AppID:        "app-1",
		TaskID:       "task-1",
		Status:       string(config.StatusCancelled),
		InternalInfo: versionUpdateRemoveCleanupInternalInfo(),
		ServiceName:  "api",
	}
	store := &cleanupComponentStore{jobInfo: existing}
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
	require.Equal(t, config.StatusCancelled, statusErr.Status)
	require.True(t, ctl.skipSaveInfo)
	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Nil(t, store.putJobInfo)
	require.Nil(t, store.addedJobInfo)
	require.Equal(t, string(config.StatusCancelled), store.jobInfo.Status)
}

func TestCleanupResourcesJobCtlTreatsCompletedVersionUpdateCleanupAsSuccess(t *testing.T) {
	ctx := context.Background()
	removed := &model.ApplicationComponent{
		ID:            1,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	deployName := buildWebServiceName(removed.Name, removed.ResourceNameKey())
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: removed.Namespace}},
	)
	store := &cleanupComponentStore{
		jobInfo: &model.JobInfo{
			ID:           10,
			Type:         string(config.JobCleanupResources),
			AppID:        removed.AppID,
			TaskID:       "task-1",
			Status:       string(config.StatusCompleted),
			StartTime:    100,
			EndTime:      200,
			InternalInfo: versionUpdateRemoveCleanupInternalInfo(),
			ServiceName:  removed.Name,
		},
	}
	task := &model.JobTask{
		Name:         removed.Name,
		Namespace:    removed.Namespace,
		AppID:        removed.AppID,
		TaskID:       "task-1",
		JobType:      string(config.JobCleanupResources),
		JobInfo:      removed,
		InternalInfo: versionUpdateRemoveCleanupInternalInfo(),
		Timeout:      1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	require.Equal(t, config.StatusCompleted, task.Status)
	require.True(t, ctl.skipSaveInfo)
	require.NoError(t, ctl.SaveInfo(ctx))
	require.Nil(t, store.casJobInfo)
	require.Nil(t, store.putJobInfo)
	require.Nil(t, store.putComponent)

	_, err := client.AppsV1().Deployments(removed.Namespace).Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlBlocksVersionUpdateCleanupForReusedComponent(t *testing.T) {
	ctx := context.Background()
	removed := &model.ApplicationComponent{
		ID:            1,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	current := &model.ApplicationComponent{
		ID:            2,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	deployName := buildWebServiceName(removed.Name, removed.ResourceNameKey())
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: removed.Namespace}},
	)
	store := &cleanupComponentStore{component: current}
	task := &model.JobTask{
		Name:         removed.Name,
		Namespace:    removed.Namespace,
		AppID:        removed.AppID,
		JobType:      string(config.JobCleanupResources),
		JobInfo:      removed,
		InternalInfo: versionUpdateRemoveCleanupInternalInfo(),
		Timeout:      1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	err := ctl.Run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked")
	require.Nil(t, store.putComponent)

	_, err = client.AppsV1().Deployments(removed.Namespace).Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlFailsBeforeDeleteWhenRunningStatusCannotPersist(t *testing.T) {
	ctx := context.Background()
	removed := &model.ApplicationComponent{
		ID:            1,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	deployName := buildWebServiceName(removed.Name, removed.ResourceNameKey())
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: removed.Namespace}},
	)
	store := &cleanupComponentStore{
		jobInfo: &model.JobInfo{
			ID:           10,
			Type:         string(config.JobCleanupResources),
			AppID:        removed.AppID,
			TaskID:       "task-1",
			Status:       string(config.StatusQueued),
			InternalInfo: versionUpdateRemoveCleanupInternalInfo(),
			ServiceName:  removed.Name,
		},
		casErr: errors.New("persist cleanup running status"),
	}
	task := &model.JobTask{
		Name:         removed.Name,
		Namespace:    removed.Namespace,
		AppID:        removed.AppID,
		TaskID:       "task-1",
		JobType:      string(config.JobCleanupResources),
		JobInfo:      removed,
		InternalInfo: versionUpdateRemoveCleanupInternalInfo(),
		Timeout:      1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	err := ctl.Run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "persist cleanup running status")
	require.Equal(t, config.StatusFailed, task.Status)
	require.Nil(t, store.putComponent)

	_, err = client.AppsV1().Deployments(removed.Namespace).Get(ctx, deployName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlMarkComponentNotDeploySkipsDifferentComponentID(t *testing.T) {
	component := &model.ApplicationComponent{
		ID:        1,
		Name:      "api",
		AppID:     "app-1",
		Namespace: "default",
	}
	current := &model.ApplicationComponent{
		ID:            2,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		Status:        string(config.ComponentStatusRunning),
		ReadyReplicas: 1,
	}
	store := &cleanupComponentStore{component: current}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
	}
	ctl := NewCleanupResourcesJobCtl(task, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.markComponentNotDeploy(context.Background(), component))
	require.Nil(t, store.putComponent)
}

func TestCleanupResourcesJobCtlSaveInfoUpdatesVersionUpdateCleanupJob(t *testing.T) {
	existing := &model.JobInfo{
		ID:           7,
		Type:         string(config.JobCleanupResources),
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		TaskID:       "task-1",
		Status:       string(config.StatusQueued),
		InternalInfo: versionUpdateRemoveCleanupInternalInfo(),
		ServiceName:  "api",
	}
	store := &cleanupComponentStore{jobInfo: existing}
	task := &model.JobTask{
		Name:         "api",
		Namespace:    "default",
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		TaskID:       "task-1",
		JobType:      string(config.JobCleanupResources),
		JobInfo:      "invalid cleanup component payload",
		Status:       config.StatusFailed,
		Error:        "delete deployment failed",
		InternalInfo: existing.InternalInfo,
	}
	ctl := NewCleanupResourcesJobCtl(task, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Nil(t, store.addedJobInfo)
	require.NotNil(t, store.putJobInfo)
	require.Equal(t, existing.ID, store.putJobInfo.ID)
	require.Equal(t, string(config.StatusFailed), store.putJobInfo.Status)
	require.Equal(t, "delete deployment failed", store.putJobInfo.Error)
	require.Equal(t, existing.InternalInfo, store.putJobInfo.InternalInfo)
	require.Equal(t, "api", store.putJobInfo.ServiceName)
}

func TestVersionUpdateCleanupJobInfoRejectsStaleGenerationWrite(t *testing.T) {
	oldExecutionKey := "cleanup-generation-1"
	newExecutionKey := "cleanup-generation-2"
	marker := versionUpdateRemoveCleanupInternalInfo()
	store := &cleanupComponentStore{jobInfo: &model.JobInfo{
		ID:            7,
		Type:          string(config.JobCleanupResources),
		WorkflowID:    "wf-1",
		AppID:         "app-1",
		TaskID:        "task-1",
		Status:        string(config.StatusRunning),
		InternalInfo:  marker,
		ServiceName:   "api",
		ExecutionKey:  &oldExecutionKey,
		RunGeneration: 1,
		Attempt:       1,
	}}
	newerTask := &model.JobTask{
		Name:          "api",
		Namespace:     "default",
		WorkflowID:    "wf-1",
		AppID:         "app-1",
		TaskID:        "task-1",
		JobType:       string(config.JobCleanupResources),
		Status:        config.StatusCompleted,
		InternalInfo:  marker,
		ExecutionKey:  newExecutionKey,
		RunGeneration: 2,
		Attempt:       1,
	}
	newerCtl := NewCleanupResourcesJobCtl(newerTask, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, newerCtl)

	require.NoError(t, newerCtl.SaveInfo(context.Background()))
	require.Nil(t, store.putJobInfo)
	require.NotNil(t, store.casJobInfo)
	require.NotNil(t, store.jobInfo.ExecutionKey)
	require.Equal(t, newExecutionKey, *store.jobInfo.ExecutionKey)
	require.Equal(t, uint64(2), store.jobInfo.RunGeneration)
	require.Equal(t, uint(1), store.jobInfo.Attempt)
	require.Equal(t, string(config.StatusCompleted), store.jobInfo.Status)

	store.casJobInfo = nil
	staleTask := &model.JobTask{
		Name:          "api",
		Namespace:     "default",
		WorkflowID:    "wf-1",
		AppID:         "app-1",
		TaskID:        "task-1",
		JobType:       string(config.JobCleanupResources),
		Status:        config.StatusFailed,
		Error:         "late generation failure",
		InternalInfo:  marker,
		ExecutionKey:  oldExecutionKey,
		RunGeneration: 1,
		Attempt:       1,
	}
	staleCtl := NewCleanupResourcesJobCtl(staleTask, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, staleCtl)

	require.NoError(t, staleCtl.SaveInfo(context.Background()))
	require.Nil(t, store.casJobInfo)
	require.Nil(t, store.putJobInfo)
	require.NotNil(t, store.jobInfo.ExecutionKey)
	require.Equal(t, newExecutionKey, *store.jobInfo.ExecutionKey)
	require.Equal(t, uint64(2), store.jobInfo.RunGeneration)
	require.Equal(t, uint(1), store.jobInfo.Attempt)
	require.Equal(t, string(config.StatusCompleted), store.jobInfo.Status)
	require.Empty(t, store.jobInfo.Error)
}

func TestCleanupResourcesJobCtlSaveInfoAddsRegularCleanupJobRecord(t *testing.T) {
	existing := &model.JobInfo{
		ID:           7,
		Type:         string(config.JobCleanupResources),
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		TaskID:       "task-1",
		Status:       string(config.StatusCompleted),
		InternalInfo: `{"name":"api"}`,
		ServiceName:  "api",
	}
	store := &cleanupComponentStore{jobInfo: existing}
	task := &model.JobTask{
		Name:       "api",
		Namespace:  "default",
		WorkflowID: "wf-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobCleanupResources),
		JobInfo:    "regular cleanup payload",
		Status:     config.StatusCompleted,
	}
	ctl := NewCleanupResourcesJobCtl(task, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Nil(t, store.putJobInfo)
	require.NotNil(t, store.addedJobInfo)
	require.Equal(t, string(config.StatusCompleted), store.addedJobInfo.Status)
	require.Equal(t, "api", store.addedJobInfo.ServiceName)
	require.Equal(t, existing, store.jobInfo)
}

func TestCleanupResourcesJobCtlSaveInfoDoesNotUpdateUnmarkedCleanupRecord(t *testing.T) {
	existing := &model.JobInfo{
		ID:           7,
		Type:         string(config.JobCleanupResources),
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		TaskID:       "task-1",
		Status:       string(config.StatusCompleted),
		InternalInfo: `{"source":"other"}`,
		ServiceName:  "api",
	}
	store := &cleanupComponentStore{jobInfo: existing}
	task := &model.JobTask{
		Name:         "api",
		Namespace:    "default",
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		TaskID:       "task-1",
		JobType:      string(config.JobCleanupResources),
		JobInfo:      "version update cleanup payload",
		Status:       config.StatusFailed,
		Error:        "cleanup failed",
		InternalInfo: versionUpdateRemoveCleanupInternalInfo(),
	}
	ctl := NewCleanupResourcesJobCtl(task, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Nil(t, store.putJobInfo)
	require.NotNil(t, store.addedJobInfo)
	require.Equal(t, string(config.StatusFailed), store.addedJobInfo.Status)
	require.Equal(t, "cleanup failed", store.addedJobInfo.Error)
	require.Equal(t, versionUpdateRemoveCleanupInternalInfo(), store.addedJobInfo.InternalInfo)
	require.Equal(t, "api", store.addedJobInfo.ServiceName)
	require.Equal(t, existing, store.jobInfo)
}

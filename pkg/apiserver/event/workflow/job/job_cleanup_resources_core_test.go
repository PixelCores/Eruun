package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
)

func TestRunJobCleanupResourcesInvalidatesComponentsCache(t *testing.T) {
	component := &model.ApplicationComponent{
		ID:            10,
		Name:          "app-config",
		AppID:         "app-cleanup-cache",
		Namespace:     "default",
		ComponentType: config.ConfJob,
	}
	store := &jobtest.CleanupComponentStore{Component: component}
	cacheStore := cacheutil.NewMemCache()
	cacheKey := cacheutil.ApplicationComponentsKey(component.AppID)
	require.NoError(t, cacheStore.Store(context.Background(), cacheKey, "stale"))
	require.NotEmpty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))

	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	runtime := newJobRuntime(&jobexec.Runtime{
		Client: fake.NewSimpleClientset(),
		Store:  store,
		Ack:    func() {},
		Cache:  cacheStore,
	})

	runAdmittedTestJob(context.Background(), task, runtime)

	require.Equal(t, config.StatusCompleted, task.Status)
	require.Empty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))
	require.NotNil(t, store.PutComponent)
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.PutComponent.Status)
}

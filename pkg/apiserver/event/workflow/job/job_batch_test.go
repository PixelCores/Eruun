package job

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobdeploy"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func cachedValue(t *testing.T, c cacheutil.ICache, ctx context.Context, key string) string {
	t.Helper()
	value, err := c.Load(ctx, key)
	require.NoError(t, err)
	return value
}

func TestGenerateInstantJobSetsTTL(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "demo",
		AppID:     "app",
		Namespace: "default",
		Image:     "busybox",
	}
	props := &model.Properties{}

	result, err := jobdeploy.GenerateInstantJob(component, props, "")
	if err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, result)
	jobObj, ok := result.Service.(*batchv1.Job)
	require.True(t, ok)
	require.NotNil(t, jobObj.Spec.TTLSecondsAfterFinished)
	require.Equal(t, config.DefaultJobTTLSeconds, *jobObj.Spec.TTLSecondsAfterFinished)
	require.Equal(t, workflowconfig.DefaultWorkflowImagePullPolicy, jobObj.Spec.Template.Spec.Containers[0].ImagePullPolicy)
}

func TestGenerateScheduledCronJobSetsTTL(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "demo",
		AppID:     "app",
		Namespace: "default",
		Image:     "busybox",
	}
	props := &model.Properties{}

	result, err := jobdeploy.GenerateScheduledCronJob(component, props, "0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, result)
	cronObj, ok := result.Service.(*batchv1.CronJob)
	require.True(t, ok)
	require.NotNil(t, cronObj.Spec.JobTemplate.Spec.TTLSecondsAfterFinished)
	require.Equal(t, config.DefaultJobTTLSeconds, *cronObj.Spec.JobTemplate.Spec.TTLSecondsAfterFinished)
	require.NotNil(t, cronObj.Spec.SuccessfulJobsHistoryLimit)
	require.NotNil(t, cronObj.Spec.FailedJobsHistoryLimit)
	require.Equal(t, config.DefaultCronJobSuccessfulLimit, *cronObj.Spec.SuccessfulJobsHistoryLimit)
	require.Equal(t, config.DefaultCronJobFailedLimit, *cronObj.Spec.FailedJobsHistoryLimit)
	require.Equal(t, workflowconfig.DefaultWorkflowImagePullPolicy, cronObj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].ImagePullPolicy)
}

func TestGenerateScheduledCronJobOverridesHistoryLimit(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "demo",
		AppID:     "app",
		Namespace: "default",
		Image:     "busybox",
	}
	successLimit := int32(1)
	failedLimit := int32(2)
	props := &model.Properties{
		SuccessfulJobsHistoryLimit: &successLimit,
		FailedJobsHistoryLimit:     &failedLimit,
	}

	result, err := jobdeploy.GenerateScheduledCronJob(component, props, "0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, result)
	cronObj, ok := result.Service.(*batchv1.CronJob)
	require.True(t, ok)
	require.NotNil(t, cronObj.Spec.SuccessfulJobsHistoryLimit)
	require.NotNil(t, cronObj.Spec.FailedJobsHistoryLimit)
	require.Equal(t, successLimit, *cronObj.Spec.SuccessfulJobsHistoryLimit)
	require.Equal(t, failedLimit, *cronObj.Spec.FailedJobsHistoryLimit)
}

type jobInfoStore struct {
	addCount  int
	addErr    error
	lastAdded datastore.Entity
}

func (s *jobInfoStore) Add(_ context.Context, entity datastore.Entity) error {
	s.addCount++
	s.lastAdded = entity
	return s.addErr
}

func (s *jobInfoStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (s *jobInfoStore) Put(context.Context, datastore.Entity) error { return nil }

func (s *jobInfoStore) Delete(context.Context, datastore.Entity) error { return nil }

func (s *jobInfoStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *jobInfoStore) Get(context.Context, datastore.Entity) error { return nil }

func (s *jobInfoStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}

func (s *jobInfoStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *jobInfoStore) IsExist(context.Context, datastore.Entity) (bool, error) { return false, nil }

func (s *jobInfoStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *jobInfoStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

var _ datastore.DataStore = (*jobInfoStore)(nil)

func TestRunJob_SkippedWritesJobInfo(t *testing.T) {
	store := &jobInfoStore{}
	jobTask := &model.JobTask{
		Name:       "demo",
		Namespace:  "default",
		WorkflowID: "wf-1",
		ProjectID:  "proj-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDeploy),
		Status:     config.StatusSkipped,
	}

	runJob(context.Background(), jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: jobtest.WithJobTestOwner(store, jobTask), Ack: func() {}})

	require.Equal(t, 1, store.addCount)
	info, ok := store.lastAdded.(*model.JobInfo)
	require.True(t, ok)
	require.Equal(t, string(config.StatusSkipped), info.Status)
	require.NotZero(t, info.StartTime)
	require.NotZero(t, info.EndTime)
}

type componentStatusStore struct {
	components []*model.ApplicationComponent
	updated    *model.ApplicationComponent
	updates    []*model.ApplicationComponent
	statuses   []string
	jobInfos   []*model.JobInfo
	appMissing bool
	putErr     error
	afterCAS   func()
}

func (s *componentStatusStore) Add(_ context.Context, entity datastore.Entity) error {
	if jobInfo, ok := entity.(*model.JobInfo); ok {
		copied := *jobInfo
		s.jobInfos = append(s.jobInfos, &copied)
	}
	return nil
}

func (s *componentStatusStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (s *componentStatusStore) Put(_ context.Context, entity datastore.Entity) error {
	if s.putErr != nil {
		return s.putErr
	}
	if comp, ok := entity.(*model.ApplicationComponent); ok {
		s.updated = comp
		s.updates = append(s.updates, comp)
		s.statuses = append(s.statuses, comp.Status)
	}
	return nil
}

func (s *componentStatusStore) Delete(context.Context, datastore.Entity) error { return nil }

func (s *componentStatusStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *componentStatusStore) Get(_ context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.Applications); ok && s.appMissing {
		return datastore.ErrRecordNotExist
	}
	return nil
}

func (s *componentStatusStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	compQuery, ok := query.(*model.ApplicationComponent)
	if !ok {
		return nil, datastore.ErrRecordNotExist
	}
	var out []datastore.Entity
	for _, comp := range s.components {
		if compQuery.AppID != "" && comp.AppID != compQuery.AppID {
			continue
		}
		out = append(out, comp)
	}
	if len(out) == 0 {
		return nil, datastore.ErrRecordNotExist
	}
	return out, nil
}

func (s *componentStatusStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *componentStatusStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (s *componentStatusStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *componentStatusStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func (s *componentStatusStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if s.putErr != nil {
		return false, s.putErr
	}
	if _, ok := entity.(*model.ApplicationComponent); !ok {
		return false, nil
	}
	for _, comp := range s.components {
		if !jobtest.ComponentMatchesConditions(comp, conditions) {
			continue
		}
		jobtest.ApplyComponentRuntimeUpdateMap(comp, updates)
		copied := *comp
		s.updated = &copied
		s.updates = append(s.updates, &copied)
		s.statuses = append(s.statuses, copied.Status)
		if s.afterCAS != nil {
			s.afterCAS()
		}
		return true, nil
	}
	return false, nil
}

var _ datastore.DataStore = (*componentStatusStore)(nil)

var _ datastore.ConditionalCompareAndSwap = (*componentStatusStore)(nil)

func TestRunJob_ConfigMapUpdatesComponentStatus(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-1",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        "",
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runAdmittedTestJob(context.Background(), jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: store, Ack: func() {}})

	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_ConfigMapSharedDefaultSkippedMapsRunning(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-1",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        string(config.ComponentStatusNotDeploy),
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		Status:    config.StatusSkipped,
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelShareName:     "shared-app-config",
					config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
				},
			},
		},
	}

	runJob(context.Background(), jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: jobtest.WithJobTestOwner(store, jobTask), Ack: func() {}})

	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_ConfigMapSharedIgnoreSkippedDoesNotMapRunning(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-1",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        string(config.ComponentStatusNotDeploy),
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		Status:    config.StatusSkipped,
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelShareName:     "shared-app-config",
					config.LabelShareStrategy: string(domainspec.ShareStrategyIgnore),
				},
			},
		},
	}

	runJob(context.Background(), jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: jobtest.WithJobTestOwner(store, jobTask), Ack: func() {}})

	require.Nil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.components[0].Status)
}

func TestRunJob_ConfigMapUpdatesComponentStatusWithWorkflowID(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "wf-1",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        "",
			},
		},
	}
	jobTask := &model.JobTask{
		Name:       "app-config",
		Namespace:  "default",
		WorkflowID: "wf-1",
		JobType:    string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runAdmittedTestJob(context.Background(), jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: store, Ack: func() {}})

	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_ConfigMapDoesNotSetPending(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-2",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        string(config.ComponentStatusNotDeploy),
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-2",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runAdmittedTestJob(context.Background(), jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: store, Ack: func() {}})

	require.NotEmpty(t, store.statuses)
	for _, status := range store.statuses {
		require.NotEqual(t, string(config.ComponentStatusPending), status)
	}
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
}

func TestRunJob_ConfigMapInvalidatesComponentsCache(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-3",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
			},
		},
	}
	cacheStore := cacheutil.NewMemCache()
	cacheKey := cacheutil.ApplicationComponentsKey("app-3")
	require.NoError(t, cacheStore.Store(context.Background(), cacheKey, "stale"))
	require.NotEmpty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-3",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runtime := newJobRuntime(&jobexec.Runtime{
		Client: fake.NewSimpleClientset(),
		Store:  store,
		Ack:    func() {},
		Cache:  cacheStore,
	})
	runAdmittedTestJob(context.Background(), jobTask, runtime)

	require.Empty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))
	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
}

func TestConfigComponentStatusInvalidatesCacheAfterCommittedWriteCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{{
			ID: 7, AppID: "app-3", Name: "app-config", Namespace: "default",
			ComponentType: config.ConfJob, Status: string(config.ComponentStatusNotDeploy),
		}},
		afterCAS: cancel,
	}
	cacheStore := cacheutil.NewMemCache()
	cacheKey := cacheutil.ApplicationComponentsKey("app-3")
	require.NoError(t, cacheStore.Store(t.Context(), cacheKey, "stale components"))
	jobTask := &model.JobTask{
		Name: "app-config", Namespace: "default", AppID: "app-3",
		JobType: string(config.JobDeployConfigMap), Status: config.StatusCompleted,
	}
	runtime := newJobRuntime(&jobexec.Runtime{
		Cache: cacheStore,
	})
	defer runtime.Close()

	err := jobexec.SyncConfigComponentStatusIfWorkflowOwned(ctx, jobTask, jobtest.WithJobTestOwner(store, jobTask), runtime)

	require.NoError(t, err)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
	require.Len(t, store.updates, 1)
	require.Empty(t, cachedValue(t, cacheStore, t.Context(), cacheKey), "committed config status must invalidate the old snapshot after caller cancellation")
}

func TestRunJob_SecretFailureInvalidatesComponentsCache(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-4",
				Name:          "app-secret",
				Namespace:     "default",
				ComponentType: config.SecretJob,
			},
		},
	}
	cacheStore := cacheutil.NewMemCache()
	cacheKey := cacheutil.ApplicationComponentsKey("app-4")
	require.NoError(t, cacheStore.Store(context.Background(), cacheKey, "stale"))
	require.NotEmpty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-secret",
		Namespace: "default",
		AppID:     "app-4",
		JobType:   string(config.JobDeploySecret),
		JobInfo:   "bad-job-info-type",
	}

	runtime := newJobRuntime(&jobexec.Runtime{
		Client: fake.NewSimpleClientset(),
		Store:  store,
		Ack:    func() {},
		Cache:  cacheStore,
	})
	runAdmittedTestJob(context.Background(), jobTask, runtime)

	require.Empty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))
	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusFailed), store.updated.Status)
	require.NotEmpty(t, store.updated.LastAbnormal)
}

func TestRunJob_SecretSharedDefaultSkippedMapsRunning(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-4",
				Name:          "app-secret",
				Namespace:     "default",
				ComponentType: config.SecretJob,
				Status:        string(config.ComponentStatusNotDeploy),
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-secret",
		Namespace: "default",
		AppID:     "app-4",
		JobType:   string(config.JobDeploySecret),
		Status:    config.StatusSkipped,
		JobInfo: &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-secret",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelShareName:     "shared-app-secret",
					config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
				},
			},
		},
	}

	runJob(context.Background(), jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: jobtest.WithJobTestOwner(store, jobTask), Ack: func() {}})

	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_ConfigMapDoesNotInvalidateCacheWhenStatusPersistFails(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-5",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
			},
		},
		putErr: errors.New("persist failed"),
	}
	cacheStore := cacheutil.NewMemCache()
	cacheKey := cacheutil.ApplicationComponentsKey("app-5")
	require.NoError(t, cacheStore.Store(context.Background(), cacheKey, "stale"))
	require.NotEmpty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-5",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runtime := newJobRuntime(&jobexec.Runtime{
		Client: fake.NewSimpleClientset(),
		Store:  store,
		Ack:    func() {},
		Cache:  cacheStore,
	})
	runAdmittedTestJob(context.Background(), jobTask, runtime)

	require.NotEmpty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))
	require.Nil(t, store.updated)
}

func TestRunJob_DeployStartInvalidatesComponentsCache(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-6",
				Name:          "app-web",
				Namespace:     "default",
				ComponentType: config.ServerJob,
				Status:        string(config.ComponentStatusRunning),
				ReadyReplicas: 1,
			},
		},
	}
	cacheStore := cacheutil.NewMemCache()
	cacheKey := cacheutil.ApplicationComponentsKey("app-6")
	require.NoError(t, cacheStore.Store(context.Background(), cacheKey, "stale"))
	require.NotEmpty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-web",
		Namespace: "default",
		AppID:     "app-6",
		JobType:   string(config.JobDeploy),
		JobInfo:   "bad-deploy-job-info",
	}

	runtime := newJobRuntime(&jobexec.Runtime{
		Client: fake.NewSimpleClientset(),
		Store:  store,
		Ack:    func() {},
		Cache:  cacheStore,
	})
	runAdmittedTestJob(context.Background(), jobTask, runtime)

	require.Empty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))
	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusPending), store.updated.Status)
	require.Equal(t, int32(0), store.updated.ReadyReplicas)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_DeployStartDoesNotInvalidateCacheWhenStatusPersistFails(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-7",
				Name:          "app-web",
				Namespace:     "default",
				ComponentType: config.ServerJob,
			},
		},
		putErr: errors.New("persist failed"),
	}
	cacheStore := cacheutil.NewMemCache()
	cacheKey := cacheutil.ApplicationComponentsKey("app-7")
	require.NoError(t, cacheStore.Store(context.Background(), cacheKey, "stale"))
	require.NotEmpty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-web",
		Namespace: "default",
		AppID:     "app-7",
		JobType:   string(config.JobDeploy),
		JobInfo:   "bad-deploy-job-info",
	}

	runtime := newJobRuntime(&jobexec.Runtime{
		Client: fake.NewSimpleClientset(),
		Store:  store,
		Ack:    func() {},
		Cache:  cacheStore,
	})
	runAdmittedTestJob(context.Background(), jobTask, runtime)

	require.NotEmpty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))
	require.Nil(t, store.updated)
}

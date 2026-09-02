package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

func newTestApplicationDeleteCancelSignalCache(t *testing.T) cache.ICache {
	t.Helper()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(server.Close)

	redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = redisClient.Close()
	})

	return cache.NewWithClient(false, cache.CacheTypeMem, redisClient)
}

func TestDeleteApplicationCascadeSuccess(t *testing.T) {
	store := newCascadeDeleteStore()
	seedCascadeStoreData(store)
	store.apps["app-1"].Namespace = "tenant-a"
	store.components["cron-task"].Namespace = "tenant-a"
	queueRepo := &mockWorkflowQueueRepo{}
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tenant-a",
			Annotations: map[string]string{
				config.AnnotationNamespaceOwnerAppID:  "app-1",
				config.AnnotationNamespaceAutoCreated: "true",
			},
		},
	}
	clientset := fake.NewSimpleClientset(namespace)
	svc := &applicationsServiceImpl{
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &cascadeAppRepo{store: store},
		ComponentRepo:     &cascadeComponentRepo{store: store},
		WorkflowQueueRepo: queueRepo,
		ScheduleLocker:    locker.NewNoopLocker("test-app-schedule"),
		Cache:             newTestApplicationDeleteCancelSignalCache(t),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})
	if err != nil {
		t.Fatalf("delete cascade returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected response, got nil")
	}
	if resp.AppID != "app-1" {
		t.Fatalf("unexpected appId: %s", resp.AppID)
	}
	if resp.DeletedCounts.Schedules != 1 || resp.DeletedCounts.Workflows != 1 || resp.DeletedCounts.Components != 1 {
		t.Fatalf("unexpected deleted counts: %+v", resp.DeletedCounts)
	}
	if resp.DeletedCounts.Tasks != 1 || resp.DeletedCounts.Apps != 1 {
		t.Fatalf("unexpected task/app counts: %+v", resp.DeletedCounts)
	}
	if resp.DeletedCounts.Jobs < 1 {
		t.Fatalf("expected at least one deleted job record, got %+v", resp.DeletedCounts)
	}
	if _, ok := store.apps["app-1"]; ok {
		t.Fatalf("app should be deleted")
	}
	if len(store.workflows) != 0 || len(store.components) != 0 || len(store.schedules) != 0 {
		t.Fatalf("workflow/component/schedule should be deleted")
	}
	if len(store.tasks) != 0 || len(store.jobs) != 0 {
		t.Fatalf("task/job should be deleted")
	}
	if _, err := clientset.CoreV1().Namespaces().Get(context.Background(), "tenant-a", metav1.GetOptions{}); err != nil {
		t.Fatalf("namespace should be preserved: %v", err)
	}
}

func TestDeleteApplicationCascadeReloadsManagementModeAfterLock(t *testing.T) {
	store := newCascadeDeleteStore()
	seedCascadeStoreData(store)
	appRepo := &transitioningCascadeAppRepo{
		cascadeAppRepo: &cascadeAppRepo{store: store},
		transitionMode: config.ManagementModeObserve,
	}
	svc := &applicationsServiceImpl{
		KubeClient:        fake.NewSimpleClientset(),
		Store:             store,
		AppRepo:           appRepo,
		ComponentRepo:     &cascadeComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		Cache:             newTestApplicationDeleteCancelSignalCache(t),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})
	if err != nil {
		t.Fatalf("delete observe application: %v", err)
	}
	if resp == nil || !resp.ResourcesRetained {
		t.Fatalf("expected retained resources response, got %+v", resp)
	}
	if len(resp.DeletedResources) != 0 {
		t.Fatalf("observe delete must not delete Kubernetes resources: %+v", resp.DeletedResources)
	}
	if appRepo.findCalls < 2 {
		t.Fatalf("expected mode reload after lock, got %d reads", appRepo.findCalls)
	}
}

func TestDeleteApplicationCascadeContinuesWhenActiveTasksRemain(t *testing.T) {
	store := newCascadeDeleteStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.tasks["task-1"] = &model.WorkflowQueue{
		TaskID:       "task-1",
		AppID:        "app-1",
		WorkflowID:   "wf-1",
		WorkflowName: "deploy",
		Status:       config.StatusRunning,
	}
	store.jobs[1] = &model.JobInfo{
		ID:         1,
		AppID:      "app-1",
		TaskID:     "task-1",
		WorkflowID: "wf-1",
		Status:     string(config.StatusRunning),
	}
	store.nextJobID = 1

	svc := &applicationsServiceImpl{
		KubeClient:        fake.NewSimpleClientset(),
		Store:             store,
		AppRepo:           &cascadeAppRepo{store: store},
		ComponentRepo:     &cascadeComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
		ScheduleLocker:    locker.NewNoopLocker("test-app-schedule"),
		Cache:             newTestApplicationDeleteCancelSignalCache(t),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})
	if err == nil {
		t.Fatalf("expected warning error, got nil")
	}
	if resp == nil {
		t.Fatalf("expected response when warning occurred")
	}
	if len(resp.ActiveTaskIDs) != 1 || resp.ActiveTaskIDs[0] != "task-1" {
		t.Fatalf("expected active task warning, got %+v", resp.ActiveTaskIDs)
	}
	if len(resp.Warnings) == 0 {
		t.Fatalf("expected warnings, got none")
	}
	if _, ok := store.apps["app-1"]; ok {
		t.Fatalf("app should still be deleted on warning")
	}
	if len(store.tasks) != 0 || len(store.jobs) != 0 {
		t.Fatalf("task/job should be deleted despite warnings")
	}
}

func TestDeleteApplicationCascadeCountsExcludeCleanupOperationLogs(t *testing.T) {
	store := newCascadeDeleteStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	queueRepo := &storeBackedWorkflowQueueRepo{store: store}

	svc := &applicationsServiceImpl{
		KubeClient:        fake.NewSimpleClientset(),
		Store:             store,
		AppRepo:           &cascadeAppRepo{store: store},
		ComponentRepo:     &cascadeComponentRepo{store: store},
		WorkflowQueueRepo: queueRepo,
		ScheduleLocker:    locker.NewNoopLocker("test-app-schedule"),
		Cache:             newTestApplicationDeleteCancelSignalCache(t),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})
	if err != nil {
		t.Fatalf("delete cascade returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected response, got nil")
	}
	if len(queueRepo.createdTaskIDs) == 0 {
		t.Fatalf("expected cleanup operation task to be recorded")
	}
	if resp.DeletedCounts.Tasks != 0 || resp.DeletedCounts.Jobs != 0 {
		t.Fatalf("cleanup operation logs should not affect deleted counts: %+v", resp.DeletedCounts)
	}
	if resp.DeletedCounts.Apps != 1 {
		t.Fatalf("unexpected app count: %+v", resp.DeletedCounts)
	}
}

func TestDeleteApplicationCascadeFailFastWithoutTransactionalStore(t *testing.T) {
	base := newCascadeDeleteStore()
	base.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	base.tasks["task-1"] = &model.WorkflowQueue{
		TaskID:       "task-1",
		AppID:        "app-1",
		WorkflowID:   "wf-1",
		WorkflowName: "deploy",
		Status:       config.StatusRunning,
	}
	base.jobs[1] = &model.JobInfo{
		ID:         1,
		AppID:      "app-1",
		TaskID:     "task-1",
		WorkflowID: "wf-1",
		Status:     string(config.StatusRunning),
	}
	base.nextJobID = 1

	store := &nonTransactionalCascadeStore{store: base}
	queueRepo := &storeBackedWorkflowQueueRepo{store: store}
	svc := &applicationsServiceImpl{
		KubeClient:        fake.NewSimpleClientset(),
		Store:             store,
		AppRepo:           &cascadeAppRepo{store: base},
		ComponentRepo:     &cascadeComponentRepo{store: base},
		WorkflowQueueRepo: queueRepo,
		ScheduleLocker:    locker.NewNoopLocker("test-app-schedule"),
		Cache:             cache.NewMemCacheWithClient(false, nil),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})
	if err == nil {
		t.Fatalf("expected error for non-transactional datastore")
	}
	if resp != nil {
		t.Fatalf("expected nil response on fail-fast error, got %+v", resp)
	}
	if !strings.Contains(err.Error(), "datastore does not support transactional delete") {
		t.Fatalf("unexpected error: %v", err)
	}
	if base.tasks["task-1"].Status != config.StatusRunning {
		t.Fatalf("task status should remain unchanged before fail-fast, got %s", base.tasks["task-1"].Status)
	}
	if len(queueRepo.createdTaskIDs) != 0 {
		t.Fatalf("no cleanup side effects expected on fail-fast")
	}
	if _, ok := base.apps["app-1"]; !ok {
		t.Fatalf("app metadata should remain unchanged")
	}
}

func TestDeleteApplicationCascadeFailsWhenLockUnavailable(t *testing.T) {
	store := newCascadeDeleteStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	svc := &applicationsServiceImpl{
		KubeClient:        fake.NewSimpleClientset(),
		Store:             store,
		AppRepo:           &cascadeAppRepo{store: store},
		ComponentRepo:     &cascadeComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
		Cache:             cache.NewMemCacheWithClient(false, nil),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})
	if resp != nil {
		t.Fatalf("expected nil response, got %+v", resp)
	}
	if !errors.Is(err, bcode.ErrDistributedLockUnavailable) {
		t.Fatalf("expected distributed lock unavailable error, got %v", err)
	}
	if _, ok := store.apps["app-1"]; !ok {
		t.Fatalf("app metadata should stay unchanged on lock failure")
	}
}

func TestDeleteApplicationCascadeDeletesSchedulesBeforeCleanup(t *testing.T) {
	store := newCascadeDeleteStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.schedules["sch-1"] = &model.WorkflowSchedule{
		ID:         "sch-1",
		AppID:      "app-1",
		WorkflowID: "wf-1",
		Cron:       "*/5 * * * *",
		Enabled:    true,
		NextRun:    1,
	}
	queueRepo := &storeBackedWorkflowQueueRepo{
		store:         store,
		scheduleStore: store,
	}

	svc := &applicationsServiceImpl{
		KubeClient:        fake.NewSimpleClientset(),
		Store:             store,
		AppRepo:           &cascadeAppRepo{store: store},
		ComponentRepo:     &cascadeComponentRepo{store: store},
		WorkflowQueueRepo: queueRepo,
		ScheduleLocker:    locker.NewNoopLocker("test-app-schedule"),
		Cache:             cache.NewMemCacheWithClient(false, nil),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})
	if err != nil {
		t.Fatalf("delete cascade returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected response, got nil")
	}
	if len(queueRepo.scheduleCountsAtCreate) == 0 {
		t.Fatalf("expected cleanup operation task creation to be observed")
	}
	if queueRepo.scheduleCountsAtCreate[0] != 0 {
		t.Fatalf("schedules should be deleted before cleanup, got count=%d", queueRepo.scheduleCountsAtCreate[0])
	}
}

func TestDeleteApplicationCascadeCancelsLateTasksAfterCleanup(t *testing.T) {
	store := newCascadeDeleteStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	queueRepo := &storeBackedWorkflowQueueRepo{
		store:                    store,
		injectLateTaskOnCreate:   true,
		injectLateTaskID:         "late-task-1",
		injectLateTaskWorkflowID: "wf-late",
	}

	svc := &applicationsServiceImpl{
		KubeClient:        fake.NewSimpleClientset(),
		Store:             store,
		AppRepo:           &cascadeAppRepo{store: store},
		ComponentRepo:     &cascadeComponentRepo{store: store},
		WorkflowQueueRepo: queueRepo,
		ScheduleLocker:    locker.NewNoopLocker("test-app-schedule"),
		Cache:             newTestApplicationDeleteCancelSignalCache(t),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})
	if err != nil {
		t.Fatalf("delete cascade returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected response, got nil")
	}
	if len(queueRepo.injectedLateTaskIDs) != 1 || queueRepo.injectedLateTaskIDs[0] != "late-task-1" {
		t.Fatalf("expected a late task injection, got %+v", queueRepo.injectedLateTaskIDs)
	}
	if !containsString(resp.CancelledTaskIDs, "late-task-1") {
		t.Fatalf("expected late task to be cancelled in second pass, got %+v", resp.CancelledTaskIDs)
	}
	if containsString(resp.ActiveTaskIDs, "late-task-1") {
		t.Fatalf("late task should not remain active: %+v", resp.ActiveTaskIDs)
	}
}

func TestCancelTaskForAppDeleteFailsBeforeStateChangeWithoutCancelSignal(t *testing.T) {
	store := newCascadeDeleteStore()
	task := &model.WorkflowQueue{
		TaskID:       "task-no-signal",
		AppID:        "app-1",
		WorkflowID:   "wf-1",
		WorkflowName: "deploy",
		Status:       config.StatusRunning,
	}
	store.tasks[task.TaskID] = task

	svc := &applicationsServiceImpl{Store: store}
	err := svc.cancelTaskForAppDelete(context.Background(), task, "delete app")
	if !errors.Is(err, bcode.ErrWorkflowCancelSignalUnavailable) {
		t.Fatalf("expected cancel signal unavailable error, got %v", err)
	}
	if task.Status != config.StatusRunning {
		t.Fatalf("task status should remain unchanged, got %s", task.Status)
	}
	if task.TaskRevoker != "" || task.CancelSource != "" {
		t.Fatalf("task cancel metadata should remain unchanged: revoker=%q source=%q", task.TaskRevoker, task.CancelSource)
	}
}

func TestDeleteApplicationCascadeDeletesRecreatedSchedulesInFinalTx(t *testing.T) {
	store := newCascadeDeleteStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.workflows["wf-1"] = &model.Workflow{ID: "wf-1", AppID: "app-1", Name: "deploy"}
	store.schedules["sch-init"] = &model.WorkflowSchedule{
		ID:         "sch-init",
		AppID:      "app-1",
		WorkflowID: "wf-1",
		Cron:       "*/5 * * * *",
		Enabled:    true,
		NextRun:    1,
	}
	queueRepo := &storeBackedWorkflowQueueRepo{
		store:                      store,
		scheduleStore:              store,
		injectLateScheduleOnCreate: true,
		injectLateScheduleID:       "sch-late",
		injectLateScheduleWorkflow: "wf-1",
	}

	svc := &applicationsServiceImpl{
		KubeClient:        fake.NewSimpleClientset(),
		Store:             store,
		AppRepo:           &cascadeAppRepo{store: store},
		ComponentRepo:     &cascadeComponentRepo{store: store},
		WorkflowQueueRepo: queueRepo,
		ScheduleLocker:    locker.NewNoopLocker("test-app-schedule"),
		Cache:             cache.NewMemCacheWithClient(false, nil),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})
	if err != nil {
		t.Fatalf("delete cascade returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected response, got nil")
	}
	if len(queueRepo.injectedLateScheduleIDs) != 1 || queueRepo.injectedLateScheduleIDs[0] != "sch-late" {
		t.Fatalf("expected late schedule recreation, got %+v", queueRepo.injectedLateScheduleIDs)
	}
	if len(store.schedules) != 0 {
		t.Fatalf("all schedules should be deleted in final tx, got %+v", store.schedules)
	}
}

type cascadeDeleteStore struct {
	apps       map[string]*model.Applications
	workflows  map[string]*model.Workflow
	components map[string]*model.ApplicationComponent
	schedules  map[string]*model.WorkflowSchedule
	tasks      map[string]*model.WorkflowQueue
	jobs       map[int]*model.JobInfo
	nextJobID  int
}

func newCascadeDeleteStore() *cascadeDeleteStore {
	return &cascadeDeleteStore{
		apps:       map[string]*model.Applications{},
		workflows:  map[string]*model.Workflow{},
		components: map[string]*model.ApplicationComponent{},
		schedules:  map[string]*model.WorkflowSchedule{},
		tasks:      map[string]*model.WorkflowQueue{},
		jobs:       map[int]*model.JobInfo{},
	}
}

func seedCascadeStoreData(store *cascadeDeleteStore) {
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.workflows["wf-1"] = &model.Workflow{ID: "wf-1", AppID: "app-1", Name: "deploy"}
	store.schedules["sch-1"] = &model.WorkflowSchedule{
		ID:         "sch-1",
		AppID:      "app-1",
		WorkflowID: "wf-1",
		Cron:       "*/5 * * * *",
		Enabled:    true,
		NextRun:    1,
	}
	store.components["cron-task"] = &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "cron-task",
		Namespace:     "default",
		Image:         "busybox:latest",
		ComponentType: config.ScheduledJob,
		Properties:    mustJSONStruct(model.Properties{Schedule: "*/5 * * * *"}),
	}
	store.tasks["task-1"] = &model.WorkflowQueue{
		TaskID:       "task-1",
		AppID:        "app-1",
		WorkflowID:   "wf-1",
		WorkflowName: "deploy",
		Status:       config.StatusCompleted,
	}
	store.jobs[1] = &model.JobInfo{
		ID:         1,
		AppID:      "app-1",
		TaskID:     "task-1",
		WorkflowID: "wf-1",
		Status:     string(config.StatusCompleted),
	}
	store.nextJobID = 1
}

func (s *cascadeDeleteStore) clone() *cascadeDeleteStore {
	cp := newCascadeDeleteStore()
	cp.nextJobID = s.nextJobID
	for k, v := range s.apps {
		tmp := *v
		cp.apps[k] = &tmp
	}
	for k, v := range s.workflows {
		tmp := *v
		cp.workflows[k] = &tmp
	}
	for k, v := range s.components {
		tmp := *v
		cp.components[k] = &tmp
	}
	for k, v := range s.schedules {
		tmp := *v
		cp.schedules[k] = &tmp
	}
	for k, v := range s.tasks {
		tmp := *v
		cp.tasks[k] = &tmp
	}
	for k, v := range s.jobs {
		tmp := *v
		cp.jobs[k] = &tmp
	}
	return cp
}

func (s *cascadeDeleteStore) Add(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		cp := *v
		s.apps[v.ID] = &cp
	case *model.Workflow:
		cp := *v
		s.workflows[v.ID] = &cp
	case *model.ApplicationComponent:
		cp := *v
		s.components[v.Name] = &cp
	case *model.WorkflowSchedule:
		cp := *v
		s.schedules[v.ID] = &cp
	case *model.WorkflowQueue:
		cp := *v
		s.tasks[v.TaskID] = &cp
	case *model.JobInfo:
		cp := *v
		if cp.ID == 0 {
			s.nextJobID++
			cp.ID = s.nextJobID
		}
		s.jobs[cp.ID] = &cp
	default:
		return fmt.Errorf("unsupported entity type: %T", entity)
	}
	return nil
}

func (s *cascadeDeleteStore) BatchAdd(ctx context.Context, entities []datastore.Entity) error {
	for _, entity := range entities {
		if err := s.Add(ctx, entity); err != nil {
			return err
		}
	}
	return nil
}

func (s *cascadeDeleteStore) Put(ctx context.Context, entity datastore.Entity) error {
	return s.Add(ctx, entity)
}

func (s *cascadeDeleteStore) Delete(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		if _, ok := s.apps[v.ID]; !ok {
			return datastore.ErrRecordNotExist
		}
		delete(s.apps, v.ID)
	case *model.Workflow:
		if _, ok := s.workflows[v.ID]; !ok {
			return datastore.ErrRecordNotExist
		}
		delete(s.workflows, v.ID)
	case *model.ApplicationComponent:
		if _, ok := s.components[v.Name]; !ok {
			return datastore.ErrRecordNotExist
		}
		delete(s.components, v.Name)
	case *model.WorkflowSchedule:
		if _, ok := s.schedules[v.ID]; !ok {
			return datastore.ErrRecordNotExist
		}
		delete(s.schedules, v.ID)
	case *model.WorkflowQueue:
		if _, ok := s.tasks[v.TaskID]; !ok {
			return datastore.ErrRecordNotExist
		}
		delete(s.tasks, v.TaskID)
	case *model.JobInfo:
		if _, ok := s.jobs[v.ID]; !ok {
			return datastore.ErrRecordNotExist
		}
		delete(s.jobs, v.ID)
	default:
		return fmt.Errorf("unsupported entity type: %T", entity)
	}
	return nil
}

func (s *cascadeDeleteStore) DeleteByFilter(_ context.Context, entity datastore.Entity, options *datastore.FilterOptions) error {
	switch q := entity.(type) {
	case *model.WorkflowSchedule:
		for id, item := range s.schedules {
			if q.AppID != "" && item.AppID != q.AppID {
				continue
			}
			if q.WorkflowID != "" && item.WorkflowID != q.WorkflowID {
				continue
			}
			delete(s.schedules, id)
		}
	case *model.WorkflowQueue:
		for id, item := range s.tasks {
			if q.AppID != "" && item.AppID != q.AppID {
				continue
			}
			delete(s.tasks, id)
		}
	case *model.JobInfo:
		for id, item := range s.jobs {
			if matchInFilter(options, "app_id", item.AppID) {
				delete(s.jobs, id)
			}
		}
	default:
		return fmt.Errorf("unsupported entity type: %T", entity)
	}
	return nil
}

func (s *cascadeDeleteStore) Get(_ context.Context, entity datastore.Entity) error {
	switch q := entity.(type) {
	case *model.Applications:
		if q.ID != "" {
			if item, ok := s.apps[q.ID]; ok {
				*q = *item
				return nil
			}
			return datastore.ErrRecordNotExist
		}
		for _, item := range s.apps {
			if item.Name == q.Name {
				*q = *item
				return nil
			}
		}
		return datastore.ErrRecordNotExist
	case *model.Workflow:
		if item, ok := s.workflows[q.ID]; ok {
			*q = *item
			return nil
		}
		return datastore.ErrRecordNotExist
	case *model.ApplicationComponent:
		if q.Name != "" {
			if item, ok := s.components[q.Name]; ok {
				*q = *item
				return nil
			}
			return datastore.ErrRecordNotExist
		}
		for _, item := range s.components {
			if q.AppID != "" && item.AppID == q.AppID {
				*q = *item
				return nil
			}
		}
		return datastore.ErrRecordNotExist
	case *model.WorkflowSchedule:
		if item, ok := s.schedules[q.ID]; ok {
			*q = *item
			return nil
		}
		return datastore.ErrRecordNotExist
	case *model.WorkflowQueue:
		if item, ok := s.tasks[q.TaskID]; ok {
			*q = *item
			return nil
		}
		return datastore.ErrRecordNotExist
	case *model.JobInfo:
		if item, ok := s.jobs[q.ID]; ok {
			*q = *item
			return nil
		}
		return datastore.ErrRecordNotExist
	default:
		return datastore.ErrRecordNotExist
	}
}

func (s *cascadeDeleteStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	var out []datastore.Entity
	switch q := query.(type) {
	case *model.Workflow:
		for _, item := range s.workflows {
			if q.AppID != "" && item.AppID != q.AppID {
				continue
			}
			out = append(out, item)
		}
	case *model.ApplicationComponent:
		for _, item := range s.components {
			if q.AppID != "" && item.AppID != q.AppID {
				continue
			}
			out = append(out, item)
		}
	case *model.WorkflowSchedule:
		for _, item := range s.schedules {
			if q.AppID != "" && item.AppID != q.AppID {
				continue
			}
			if q.WorkflowID != "" && item.WorkflowID != q.WorkflowID {
				continue
			}
			if q.Enabled && !item.Enabled {
				continue
			}
			out = append(out, item)
		}
	case *model.WorkflowQueue:
		for _, item := range s.tasks {
			if q.AppID != "" && item.AppID != q.AppID {
				continue
			}
			if q.Status != "" && item.Status != q.Status {
				continue
			}
			out = append(out, item)
		}
	case *model.JobInfo:
		for _, item := range s.jobs {
			if q.TaskID != "" && item.TaskID != q.TaskID {
				continue
			}
			if q.WorkflowID != "" && item.WorkflowID != q.WorkflowID {
				continue
			}
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *cascadeDeleteStore) Count(ctx context.Context, entity datastore.Entity, filterOptions *datastore.FilterOptions) (int64, error) {
	switch entity.(type) {
	case *model.JobInfo:
		var count int64
		for _, item := range s.jobs {
			if matchInFilter(filterOptions, "app_id", item.AppID) {
				count++
			}
		}
		return count, nil
	default:
		items, err := s.List(ctx, entity, nil)
		if err != nil {
			return 0, err
		}
		return int64(len(items)), nil
	}
}

func (s *cascadeDeleteStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (s *cascadeDeleteStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *cascadeDeleteStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func (s *cascadeDeleteStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if _, ok := entity.(*model.ApplicationComponent); !ok {
		return false, nil
	}
	for _, component := range s.components {
		if !matchesComponentRuntimeConditions(component, conditions) {
			continue
		}
		applyComponentRuntimeUpdates(component, updates)
		return true, nil
	}
	return false, nil
}

func (s *cascadeDeleteStore) WithTransaction(ctx context.Context, fn func(tx datastore.DataStore) error) error {
	txStore := s.clone()
	if err := fn(txStore); err != nil {
		return err
	}
	*s = *txStore
	return nil
}

func matchInFilter(filterOptions *datastore.FilterOptions, key, value string) bool {
	if filterOptions == nil || len(filterOptions.In) == 0 {
		return true
	}
	for _, in := range filterOptions.In {
		if strings.TrimSpace(in.Key) != key {
			continue
		}
		for _, candidate := range in.Values {
			if candidate == value {
				return true
			}
		}
		return false
	}
	return true
}

func int64Ptr(v int64) *int64 {
	return &v
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type cascadeAppRepo struct {
	store *cascadeDeleteStore
}

type transitioningCascadeAppRepo struct {
	*cascadeAppRepo
	findCalls      int
	transitionMode config.ManagementMode
}

func (r *transitioningCascadeAppRepo) FindByID(ctx context.Context, id string) (*model.Applications, error) {
	app, err := r.cascadeAppRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	r.findCalls++
	if r.findCalls == 1 {
		r.store.apps[id].ManagementMode = r.transitionMode
	}
	return app, nil
}

var _ repository.ApplicationRepository = (*transitioningCascadeAppRepo)(nil)

func (r *cascadeAppRepo) FindByID(ctx context.Context, id string) (*model.Applications, error) {
	app := &model.Applications{ID: id}
	if err := r.store.Get(ctx, app); err != nil {
		return nil, err
	}
	return app, nil
}

func (r *cascadeAppRepo) FindByIDs(_ context.Context, ids []string) ([]*model.Applications, error) {
	applications := make([]*model.Applications, 0, len(ids))
	for _, id := range ids {
		if app, ok := r.store.apps[id]; ok {
			applications = append(applications, app)
		}
	}
	return applications, nil
}

func (r *cascadeAppRepo) FindByName(ctx context.Context, name string) (*model.Applications, error) {
	app := &model.Applications{Name: name}
	if err := r.store.Get(ctx, app); err != nil {
		return nil, err
	}
	return app, nil
}

func (r *cascadeAppRepo) Create(ctx context.Context, app *model.Applications) error {
	return r.store.Add(ctx, app)
}

func (r *cascadeAppRepo) Update(ctx context.Context, app *model.Applications) error {
	return r.store.Put(ctx, app)
}

func (r *cascadeAppRepo) Delete(ctx context.Context, app *model.Applications) error {
	return r.store.Delete(ctx, app)
}

func (r *cascadeAppRepo) List(_ context.Context, _ datastore.ListOptions) ([]*model.Applications, error) {
	out := make([]*model.Applications, 0, len(r.store.apps))
	for _, app := range r.store.apps {
		out = append(out, app)
	}
	return out, nil
}

func (r *cascadeAppRepo) ListByQuery(ctx context.Context, options *model.Applications, listOptions datastore.ListOptions) ([]*model.Applications, error) {
	return r.List(ctx, listOptions)
}

var _ repository.ApplicationRepository = (*cascadeAppRepo)(nil)

type cascadeComponentRepo struct {
	store *cascadeDeleteStore
}

func (r *cascadeComponentRepo) Create(ctx context.Context, component *model.ApplicationComponent) error {
	return r.store.Add(ctx, component)
}

func (r *cascadeComponentRepo) Update(ctx context.Context, component *model.ApplicationComponent) error {
	return r.store.Put(ctx, component)
}

func (r *cascadeComponentRepo) Delete(ctx context.Context, component *model.ApplicationComponent) error {
	return r.store.Delete(ctx, component)
}

func (r *cascadeComponentRepo) BatchAdd(ctx context.Context, components []*model.ApplicationComponent) error {
	for _, component := range components {
		if err := r.store.Add(ctx, component); err != nil {
			return err
		}
	}
	return nil
}

func (r *cascadeComponentRepo) DeleteByAppID(_ context.Context, appID string) error {
	for name, component := range r.store.components {
		if component != nil && component.AppID == appID {
			delete(r.store.components, name)
		}
	}
	return nil
}

func (r *cascadeComponentRepo) FindByAppID(_ context.Context, appID string) ([]*model.ApplicationComponent, error) {
	out := make([]*model.ApplicationComponent, 0)
	for _, component := range r.store.components {
		if component != nil && component.AppID == appID {
			out = append(out, component)
		}
	}
	return out, nil
}

func (r *cascadeComponentRepo) FindByName(_ context.Context, appID, name string) (*model.ApplicationComponent, error) {
	component, ok := r.store.components[name]
	if !ok || component == nil || component.AppID != appID {
		return nil, datastore.ErrRecordNotExist
	}
	return component, nil
}

type storeBackedWorkflowQueueRepo struct {
	store                      datastore.DataStore
	scheduleStore              *cascadeDeleteStore
	createdTaskIDs             []string
	scheduleCountsAtCreate     []int
	injectLateTaskOnCreate     bool
	injectLateTaskID           string
	injectLateTaskWorkflowID   string
	injectedLateTaskIDs        []string
	injectLateScheduleOnCreate bool
	injectLateScheduleID       string
	injectLateScheduleWorkflow string
	injectedLateScheduleIDs    []string
}

func (r *storeBackedWorkflowQueueRepo) Create(ctx context.Context, queue *model.WorkflowQueue) error {
	if queue == nil {
		return nil
	}
	if r.scheduleStore != nil {
		r.scheduleCountsAtCreate = append(r.scheduleCountsAtCreate, len(r.scheduleStore.schedules))
	}
	r.createdTaskIDs = append(r.createdTaskIDs, queue.TaskID)
	if err := r.store.Add(ctx, queue); err != nil {
		return err
	}
	if r.injectLateTaskOnCreate {
		r.injectLateTaskOnCreate = false
		taskID := strings.TrimSpace(r.injectLateTaskID)
		if taskID == "" {
			taskID = "late-task"
		}
		workflowID := strings.TrimSpace(r.injectLateTaskWorkflowID)
		if workflowID == "" {
			workflowID = queue.WorkflowID
		}
		lateTask := &model.WorkflowQueue{
			TaskID:       taskID,
			AppID:        queue.AppID,
			WorkflowID:   workflowID,
			WorkflowName: "late-dispatch",
			Status:       config.StatusRunning,
		}
		if err := r.store.Add(ctx, lateTask); err != nil {
			return err
		}
		r.injectedLateTaskIDs = append(r.injectedLateTaskIDs, taskID)
	}
	if r.injectLateScheduleOnCreate && r.scheduleStore != nil {
		r.injectLateScheduleOnCreate = false
		scheduleID := strings.TrimSpace(r.injectLateScheduleID)
		if scheduleID == "" {
			scheduleID = "sch-late"
		}
		workflowID := strings.TrimSpace(r.injectLateScheduleWorkflow)
		if workflowID == "" {
			workflowID = queue.WorkflowID
		}
		r.scheduleStore.schedules[scheduleID] = &model.WorkflowSchedule{
			ID:         scheduleID,
			AppID:      queue.AppID,
			WorkflowID: workflowID,
			Cron:       "*/5 * * * *",
			Enabled:    true,
			NextRun:    time.Now().Unix(),
		}
		r.injectedLateScheduleIDs = append(r.injectedLateScheduleIDs, scheduleID)
	}
	return nil
}

func (r *storeBackedWorkflowQueueRepo) Update(context.Context, *model.WorkflowQueue) error {
	return nil
}

func (r *storeBackedWorkflowQueueRepo) FindByID(context.Context, string) (*model.WorkflowQueue, error) {
	return nil, datastore.ErrRecordNotExist
}

func (r *storeBackedWorkflowQueueRepo) FindWaiting(context.Context) ([]*model.WorkflowQueue, error) {
	return nil, nil
}

func (r *storeBackedWorkflowQueueRepo) FindRunning(context.Context) ([]*model.WorkflowQueue, error) {
	return nil, nil
}

func (r *storeBackedWorkflowQueueRepo) UpdateStatus(context.Context, string, config.Status, config.Status) (bool, error) {
	return false, nil
}

type nonTransactionalCascadeStore struct {
	store *cascadeDeleteStore
}

func (s *nonTransactionalCascadeStore) Add(ctx context.Context, entity datastore.Entity) error {
	return s.store.Add(ctx, entity)
}

func (s *nonTransactionalCascadeStore) BatchAdd(ctx context.Context, entities []datastore.Entity) error {
	return s.store.BatchAdd(ctx, entities)
}

func (s *nonTransactionalCascadeStore) Put(ctx context.Context, entity datastore.Entity) error {
	return s.store.Put(ctx, entity)
}

func (s *nonTransactionalCascadeStore) Delete(ctx context.Context, entity datastore.Entity) error {
	return s.store.Delete(ctx, entity)
}

func (s *nonTransactionalCascadeStore) DeleteByFilter(ctx context.Context, entity datastore.Entity, options *datastore.FilterOptions) error {
	return s.store.DeleteByFilter(ctx, entity, options)
}

func (s *nonTransactionalCascadeStore) Get(ctx context.Context, entity datastore.Entity) error {
	return s.store.Get(ctx, entity)
}

func (s *nonTransactionalCascadeStore) List(ctx context.Context, query datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	return s.store.List(ctx, query, options)
}

func (s *nonTransactionalCascadeStore) Count(ctx context.Context, entity datastore.Entity, options *datastore.FilterOptions) (int64, error) {
	return s.store.Count(ctx, entity, options)
}

func (s *nonTransactionalCascadeStore) IsExist(ctx context.Context, entity datastore.Entity) (bool, error) {
	return s.store.IsExist(ctx, entity)
}

func (s *nonTransactionalCascadeStore) IsExistByCondition(ctx context.Context, table string, cond map[string]interface{}, dest interface{}) (bool, error) {
	return s.store.IsExistByCondition(ctx, table, cond, dest)
}

func (s *nonTransactionalCascadeStore) CompareAndSwap(ctx context.Context, entity datastore.Entity, conditionField string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	return s.store.CompareAndSwap(ctx, entity, conditionField, conditionValue, updates)
}

func (s *nonTransactionalCascadeStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	return s.store.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

var _ repository.ComponentRepository = (*cascadeComponentRepo)(nil)
var _ repository.WorkflowQueueRepository = (*storeBackedWorkflowQueueRepo)(nil)

var _ datastore.DataStore = (*cascadeDeleteStore)(nil)
var _ datastore.Transactional = (*cascadeDeleteStore)(nil)
var _ datastore.DataStore = (*nonTransactionalCascadeStore)(nil)

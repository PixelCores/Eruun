package workflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type workflowAckTestStore struct {
	mu                      sync.RWMutex
	workflow                *model.Workflow
	task                    *model.WorkflowQueue
	taskGets                int
	failGetAt               int
	taskGetErr              error
	taskGetHook             func(*model.WorkflowQueue)
	compareAndSwapCalls     int
	failCompareAndSwapAt    int
	failCompareAndSwapError error
}

func (s *workflowAckTestStore) Add(context.Context, datastore.Entity) error { return nil }
func (s *workflowAckTestStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func (s *workflowAckTestStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (s *workflowAckTestStore) Put(_ context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, ok := entity.(*model.WorkflowQueue); ok && task != nil {
		cp := *task
		s.task = &cp
	}
	return nil
}
func (s *workflowAckTestStore) Delete(context.Context, datastore.Entity) error { return nil }
func (s *workflowAckTestStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *workflowAckTestStore) Get(ctx context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch e := entity.(type) {
	case *model.Workflow:
		if s.workflow == nil {
			return fmt.Errorf("workflow not configured")
		}
		*e = *s.workflow
	case *model.WorkflowQueue:
		s.taskGets++
		if s.failGetAt > 0 && s.taskGets == s.failGetAt {
			return fmt.Errorf("injected task get failure")
		}
		if s.taskGetErr != nil {
			return s.taskGetErr
		}
		if s.task == nil {
			return datastore.ErrRecordNotExist
		}
		if s.taskGetHook != nil {
			s.taskGetHook(s.task)
		}
		*e = *s.task
	default:
	}
	return nil
}

func (s *workflowAckTestStore) List(ctx context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	if _, ok := query.(*model.ApplicationComponent); ok {
		return []datastore.Entity{}, nil
	}
	return nil, nil
}

func (s *workflowAckTestStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *workflowAckTestStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (s *workflowAckTestStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *workflowAckTestStore) CompareAndSwap(_ context.Context, entity datastore.Entity, field string, condition interface{}, updates map[string]interface{}) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compareAndSwapWithConditionsLocked(entity, map[string]interface{}{field: condition}, updates)
}

func (s *workflowAckTestStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compareAndSwapWithConditionsLocked(entity, conditions, updates)
}

func (s *workflowAckTestStore) compareAndSwapWithConditionsLocked(entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	task, ok := entity.(*model.WorkflowQueue)
	if !ok || task == nil || s.task == nil || task.TaskID != s.task.TaskID {
		return false, nil
	}
	ownershipProbe := len(updates) == 0
	if !ownershipProbe {
		s.compareAndSwapCalls++
	}
	if !ownershipProbe && s.failCompareAndSwapAt > 0 && s.compareAndSwapCalls == s.failCompareAndSwapAt {
		err := s.failCompareAndSwapError
		if err == nil {
			err = fmt.Errorf("injected compare and swap failure")
		}
		return false, err
	}
	for field, condition := range conditions {
		if !matchWorkflowQueueField(s.task, field, condition) {
			return false, nil
		}
	}
	for key, value := range updates {
		applyWorkflowQueueUpdate(s.task, key, value)
	}
	return true, nil
}

func (s *workflowAckTestStore) mutateTask(mutator func(*model.WorkflowQueue)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.task == nil {
		return
	}
	mutator(s.task)
}

func (s *workflowAckTestStore) taskSnapshot() *model.WorkflowQueue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.task == nil {
		return nil
	}
	cp := *s.task
	return &cp
}

func (s *workflowAckTestStore) taskGetsCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.taskGets
}

func (s *workflowAckTestStore) setFailGetAt(at int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failGetAt = at
}

func (s *workflowAckTestStore) compareAndSwapCallsCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.compareAndSwapCalls
}

type stubWorkflowService struct {
	updateOK     bool
	updateCalled bool
	store        *workflowAckTestStore
}

func (s *stubWorkflowService) WaitingTasks(context.Context) ([]*model.WorkflowQueue, error) {
	return nil, nil
}
func (s *stubWorkflowService) UpdateTask(context.Context, *model.WorkflowQueue) bool {
	s.updateCalled = true
	return s.updateOK
}
func (s *stubWorkflowService) TaskRunning(context.Context) ([]*model.WorkflowQueue, error) {
	return nil, nil
}
func (s *stubWorkflowService) MarkTaskStatus(ctx context.Context, taskID string, from, to config.Status) (bool, error) {
	if s.store == nil {
		return true, nil
	}
	return s.store.CompareAndSwap(ctx, &model.WorkflowQueue{TaskID: taskID}, "status", from, map[string]interface{}{"status": to})
}
func (s *stubWorkflowService) DispatchWorkflowSchedules(context.Context) (int, error) {
	return 0, nil
}

var _ workflowRuntimeService = (*stubWorkflowService)(nil)

func newWorkflowForAckTests(t testing.TB, updateOK bool) *Workflow {
	t.Helper()
	steps, _ := model.NewJSONStructByStruct(&model.WorkflowSteps{})
	store := &workflowAckTestStore{
		workflow: &model.Workflow{
			ID:        "wf-1",
			Namespace: "default",
			Steps:     steps,
		},
		task: &model.WorkflowQueue{
			TaskID:        "task-1",
			WorkflowID:    "wf-1",
			AppID:         "app-1",
			ProjectID:     "proj-1",
			WorkflowName:  "demo",
			Status:        config.StatusQueued,
			RunGeneration: 1,
			RunToken:      "run-token-1",
			WorkerID:      "test-worker",
		},
	}

	return &Workflow{
		KubeClient:                fake.NewSimpleClientset(),
		Store:                     store,
		WorkflowService:           &stubWorkflowService{updateOK: updateOK, store: store},
		Queue:                     nil,
		Cfg:                       &config.Config{},
		taskRunLocker:             locker.NewMemoryLocker(workflowTaskRunLockerPrefix),
		URLSecurityPolicyProvider: newTestURLSecurityPolicyProvider(t, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}),
	}
}

func mustTestTaskDispatch(t testing.TB) []byte {
	t.Helper()
	payload, err := MarshalTaskDispatch(TaskDispatch{
		Version:       taskDispatchVersion,
		TaskID:        "task-1",
		WorkflowID:    "wf-1",
		ProjectID:     "proj-1",
		AppID:         "app-1",
		RunGeneration: 1,
		RunToken:      "run-token-1",
	})
	require.NoError(t, err)
	return payload
}

func configureWorkflowAckTestCancelCache(t *testing.T, w *Workflow) {
	t.Helper()
	redisServer, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(redisServer.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() {
		_ = redisClient.Close()
	})
	w.Cache = cacheutil.NewMemCacheWithClient(false, redisClient)
}

func TestWorkflowTaskPersistenceDistinguishesInfrastructureStopFromUserCancel(t *testing.T) {
	t.Run("infrastructure stop leaves task recoverable", func(t *testing.T) {
		w := newWorkflowForAckTests(t, true)
		store := w.Store.(*workflowAckTestStore)
		store.mutateTask(func(task *model.WorkflowQueue) {
			task.Status = config.StatusRunning
		})
		controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(signal.ErrInfrastructureStop)
		controller.ctx = ctx
		controller.setTerminalStatus(config.StatusCancelled, "worker drain timeout")

		controller.updateWorkflowTask()

		require.Equal(t, 0, store.compareAndSwapCallsCount())
		require.Equal(t, config.StatusRunning, store.taskSnapshot().Status)
		stopped, persistenceErr := controller.workflowRunStopResult()
		require.True(t, stopped)
		require.ErrorIs(t, persistenceErr, errWorkflowTaskPersistenceUncertain)
		require.True(t, controller.terminalCallbackSuppressed())
	})

	t.Run("user cancel persists terminal status", func(t *testing.T) {
		w := newWorkflowForAckTests(t, true)
		store := w.Store.(*workflowAckTestStore)
		store.mutateTask(func(task *model.WorkflowQueue) {
			task.Status = config.StatusRunning
		})
		controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		controller.ctx = ctx
		controller.setTerminalStatus(config.StatusCancelled, "cancelled by user")

		controller.updateWorkflowTask()

		require.Equal(t, 1, store.compareAndSwapCallsCount())
		require.Equal(t, config.StatusCancelled, store.taskSnapshot().Status)
	})
}

func TestWorkflowRunSuppressesCallbackAfterOwnershipChanges(t *testing.T) {
	var callbackCount int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()
	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: callbackServer.URL})
	require.NoError(t, err)

	w := newWorkflowForAckTests(t, true)
	configureWorkflowAckTestCancelCache(t, w)
	store := w.Store.(*workflowAckTestStore)
	store.mutateTask(func(task *model.WorkflowQueue) {
		task.Status = config.StatusRunning
		task.RunGeneration = 3
		task.RunToken = "token-3"
		task.WorkerID = "worker-a"
		task.Callback = callback
	})

	controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
	controller.Cache = w.Cache
	authoritative := store.taskSnapshot()
	authoritative.Status = config.StatusCompleted
	authoritative.RunGeneration = 4
	authoritative.RunToken = "token-4"
	authoritative.WorkerID = "worker-b"
	controller.ack = func() {
		controller.stopTaskPersistence(authoritative, false, true)
	}

	err = controller.Run(context.Background(), 1)

	require.NoError(t, err)
	require.Equal(t, int32(0), atomic.LoadInt32(&callbackCount))
	require.True(t, controller.terminalCallbackSuppressed())
	require.ErrorIs(t, context.Cause(controller.ctx), signal.ErrInfrastructureStop)
}

func TestStopTaskPersistencePropagatesInfrastructureCause(t *testing.T) {
	tests := []struct {
		name              string
		uncertain         bool
		authoritativeTask func(*model.WorkflowQueue) *model.WorkflowQueue
	}{
		{
			name:      "persistence uncertainty",
			uncertain: true,
		},
		{
			name: "execution ownership changed",
			authoritativeTask: func(task *model.WorkflowQueue) *model.WorkflowQueue {
				authoritative := *task
				authoritative.RunGeneration++
				authoritative.RunToken = "token-next"
				authoritative.WorkerID = "worker-next"
				return &authoritative
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &model.WorkflowQueue{
				TaskID:        "task-1",
				RunGeneration: 3,
				RunToken:      "token-3",
				WorkerID:      "worker-a",
			}
			controller := &WorkflowCtl{workflowTask: task}
			ctx, cancel := context.WithCancelCause(context.Background())
			controller.ctx = ctx
			controller.registerRunCancel(cancel)
			var authoritative *model.WorkflowQueue
			if test.authoritativeTask != nil {
				authoritative = test.authoritativeTask(task)
			}

			controller.stopTaskPersistence(authoritative, test.uncertain, test.authoritativeTask != nil)

			require.ErrorIs(t, context.Cause(ctx), signal.ErrInfrastructureStop)
		})
	}
}

func TestRunWorkflowControllerRecoversTransientPersistenceFailure(t *testing.T) {
	for _, status := range []config.Status{config.StatusQueued, config.StatusRunning} {
		t.Run(string(status), func(t *testing.T) {
			w := newWorkflowForAckTests(t, true)
			w.Cfg.Workflow.WorkerBackoffMin = time.Millisecond
			w.Cfg.Workflow.WorkerBackoffMax = 5 * time.Millisecond
			store := w.Store.(*workflowAckTestStore)
			store.mutateTask(func(task *model.WorkflowQueue) {
				task.Status = status
			})
			store.failCompareAndSwapAt = 1
			store.failCompareAndSwapError = errors.New("temporary database failure")

			controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
			err := w.runWorkflowControllerWithPersistenceRecovery(context.Background(), controller, 1)

			require.NoError(t, err)
			task := store.taskSnapshot()
			require.NotNil(t, task)
			require.Equal(t, config.StatusCompleted, task.Status)
			require.Greater(t, store.compareAndSwapCallsCount(), 1)
		})
	}
}

func TestRunWorkflowControllerStopsRecoveryWhenOwnershipChanges(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	store := w.Store.(*workflowAckTestStore)
	store.mutateTask(func(task *model.WorkflowQueue) {
		task.Status = config.StatusRunning
		task.RunGeneration = 3
		task.RunToken = "token-3"
		task.WorkerID = "worker-a"
	})
	store.failCompareAndSwapAt = 1
	store.failCompareAndSwapError = errors.New("temporary database failure")
	store.taskGetHook = func(task *model.WorkflowQueue) {
		task.Status = config.StatusRunning
		task.RunGeneration = 4
		task.RunToken = "token-4"
		task.WorkerID = "worker-b"
	}

	controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
	err := w.runWorkflowControllerWithPersistenceRecovery(context.Background(), controller, 1)

	require.ErrorIs(t, err, repository.ErrWorkflowOwnershipLost)
	task := store.taskSnapshot()
	require.NotNil(t, task)
	require.Equal(t, config.StatusRunning, task.Status)
	require.Equal(t, uint64(4), task.RunGeneration)
	require.Equal(t, "token-4", task.RunToken)
	require.Equal(t, "worker-b", task.WorkerID)
	require.Equal(t, 1, store.compareAndSwapCallsCount())
}

func TestWorkflowRunSkipsDeferredExitAckAfterCompletedPersistence(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	store := w.Store.(*workflowAckTestStore)
	store.failCompareAndSwapAt = 3
	store.failCompareAndSwapError = errors.New("unexpected redundant exit ack")

	controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
	err := controller.Run(context.Background(), 1)

	require.NoError(t, err)
	require.Equal(t, 2, store.compareAndSwapCallsCount())
	task := store.taskSnapshot()
	require.NotNil(t, task)
	require.Equal(t, config.StatusCompleted, task.Status)
	stopped, persistenceErr := controller.workflowRunStopResult()
	require.False(t, stopped)
	require.NoError(t, persistenceErr)
	require.False(t, controller.terminalCallbackSuppressed())
}

func TestWorkflowRunSendsCompletedCallbackWithoutWorkflowAck(t *testing.T) {
	var callbackCount int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()
	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: callbackServer.URL})
	require.NoError(t, err)

	w := newWorkflowForAckTests(t, true)
	configureWorkflowAckTestCancelCache(t, w)
	store := w.Store.(*workflowAckTestStore)
	store.failCompareAndSwapAt = 3
	store.failCompareAndSwapError = errors.New("unexpected callback workflow ack")
	store.mutateTask(func(task *model.WorkflowQueue) {
		task.Callback = callback
	})

	controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
	controller.Cache = w.Cache
	err = controller.Run(context.Background(), 1)

	require.NoError(t, err)
	require.Equal(t, 2, store.compareAndSwapCallsCount())
	task := store.taskSnapshot()
	require.NotNil(t, task)
	require.Equal(t, config.StatusCompleted, task.Status)
	stopped, persistenceErr := controller.workflowRunStopResult()
	require.False(t, stopped)
	require.NoError(t, persistenceErr)
	require.False(t, controller.terminalCallbackSuppressed())
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
}

func TestRunWorkflowControllerRecoversDeferredExitAckPersistenceFailure(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	configureWorkflowAckTestCancelCache(t, w)
	w.Cfg.Workflow.WorkerBackoffMin = time.Millisecond
	w.Cfg.Workflow.WorkerBackoffMax = 5 * time.Millisecond
	store := w.Store.(*workflowAckTestStore)
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "unsupported-step",
			WorkflowType: config.JobType("unsupported"),
		}},
	})
	require.NoError(t, err)
	store.workflow.Steps = steps
	store.failCompareAndSwapAt = 4
	store.failCompareAndSwapError = errors.New("temporary deferred exit ack failure")

	controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
	controller.Cache = w.Cache
	err = w.runWorkflowControllerWithPersistenceRecovery(context.Background(), controller, 1)

	require.Error(t, err)
	require.NotErrorIs(t, err, errWorkflowTaskPersistenceUncertain)
	task := store.taskSnapshot()
	require.NotNil(t, task)
	require.Equal(t, config.StatusFailed, task.Status)
	require.Greater(t, store.compareAndSwapCallsCount(), 4)
}

func TestRunWorkflowControllerAcceptsAuthoritativeCancellationFromDeferredExitAck(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	configureWorkflowAckTestCancelCache(t, w)
	store := w.Store.(*workflowAckTestStore)
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "unsupported-step",
			WorkflowType: config.JobType("unsupported"),
		}},
	})
	require.NoError(t, err)
	store.workflow.Steps = steps

	controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
	controller.Cache = w.Cache
	persistAck := controller.ack
	ackCalls := 0
	controller.ack = func() {
		ackCalls++
		if ackCalls != 4 {
			persistAck()
			return
		}
		authoritative := store.taskSnapshot()
		require.NotNil(t, authoritative)
		authoritative.Status = config.StatusCancelled
		authoritative.CancelSource = config.CancelSourceUser
		store.mutateTask(func(task *model.WorkflowQueue) {
			*task = *authoritative
		})
		controller.stopTaskPersistence(authoritative, false, true)
	}

	err = w.runWorkflowControllerWithPersistenceRecovery(context.Background(), controller, 1)

	require.NoError(t, err)
	task := store.taskSnapshot()
	require.NotNil(t, task)
	require.Equal(t, config.StatusCancelled, task.Status)
	require.Equal(t, 4, ackCalls)
	require.Equal(t, 3, store.compareAndSwapCallsCount())
}

func TestRunWorkflowControllerStopsRecoveryAtNonRunnableState(t *testing.T) {
	for _, status := range []config.Status{config.StatusCompleted, config.StatusWaitingApprove} {
		t.Run(string(status), func(t *testing.T) {
			var callbackCount int32
			callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				atomic.AddInt32(&callbackCount, 1)
				w.WriteHeader(http.StatusOK)
			}))
			defer callbackServer.Close()
			callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: callbackServer.URL})
			require.NoError(t, err)

			w := newWorkflowForAckTests(t, true)
			store := w.Store.(*workflowAckTestStore)
			store.mutateTask(func(task *model.WorkflowQueue) {
				task.Callback = callback
			})
			store.failCompareAndSwapAt = 1
			store.failCompareAndSwapError = errors.New("temporary database failure")
			store.taskGetHook = func(task *model.WorkflowQueue) {
				task.Status = status
			}

			controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
			err = w.runWorkflowControllerWithPersistenceRecovery(context.Background(), controller, 1)

			require.NoError(t, err)
			task := store.taskSnapshot()
			require.NotNil(t, task)
			require.Equal(t, status, task.Status)
			require.Equal(t, 1, store.compareAndSwapCallsCount())
			require.Equal(t, int32(0), atomic.LoadInt32(&callbackCount))
		})
	}
}

func TestRunWorkflowControllerReportsMissingTaskDuringPersistenceRecovery(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	store := w.Store.(*workflowAckTestStore)
	store.failCompareAndSwapAt = 1
	store.failCompareAndSwapError = errors.New("temporary database failure")
	store.taskGetErr = datastore.ErrRecordNotExist

	controller := newTestWorkflowController(t, store.taskSnapshot(), w.KubeClient, store)
	err := w.runWorkflowControllerWithPersistenceRecovery(context.Background(), controller, 1)

	require.ErrorIs(t, err, errWorkflowTaskPersistenceUncertain)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	require.Equal(t, 1, store.compareAndSwapCallsCount())
}

func TestRunWorkflowTaskRetainsLeaseWhilePersistenceRecoveryWaits(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	w.Cfg.Workflow.WorkerBackoffMin = 5 * time.Millisecond
	w.Cfg.Workflow.WorkerBackoffMax = 10 * time.Millisecond
	store := w.Store.(*workflowAckTestStore)
	store.failCompareAndSwapAt = 1
	store.failCompareAndSwapError = errors.New("temporary database failure")
	store.taskGetErr = errors.New("database unavailable")

	ctx, cancel := context.WithCancel(context.Background())
	task := store.taskSnapshot()
	require.NotNil(t, task)
	started, err := w.runWorkflowTask(ctx, nil, task, 1)
	require.NoError(t, err)
	require.True(t, started)
	require.Eventually(t, func() bool {
		return store.taskGetsCount() > 0
	}, time.Second, 5*time.Millisecond)

	secondLease, acquired, err := w.tryAcquireTaskRunLease(context.Background(), context.Background(), task.TaskID)
	require.NoError(t, err)
	require.False(t, acquired)
	require.Nil(t, secondLease)

	cancel()
	require.Eventually(t, func() bool {
		lease, ok, acquireErr := w.tryAcquireTaskRunLease(context.Background(), context.Background(), task.TaskID)
		if acquireErr != nil || !ok {
			return false
		}
		lease.release()
		return true
	}, time.Second, 10*time.Millisecond)
}

func TestProcessDispatchMessageAckOnSuccess(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	payload := mustTestTaskDispatch(t)

	ack, taskID := w.processDispatchMessage(context.Background(), nil, msg.Message{ID: "1-0", Payload: payload})
	require.True(t, ack)
	require.Equal(t, "task-1", taskID)
}

func TestProcessDispatchMessageAckOnFailure(t *testing.T) {
	// In pass/fail system, we always ack messages even on execution failure
	// Task state is tracked in database, not message queue
	w := newWorkflowForAckTests(t, false)
	payload := mustTestTaskDispatch(t)

	ack, taskID := w.processDispatchMessage(context.Background(), nil, msg.Message{ID: "1-0", Payload: payload})
	require.True(t, ack) // Always ack - no retry in pass/fail system
	require.Equal(t, "task-1", taskID)
}

func TestProcessDispatchMessageMarksTaskFailedWhenURLPolicyUnavailable(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	w.URLSecurityPolicyProvider = nil
	store := w.Store.(*workflowAckTestStore)
	payload := mustTestTaskDispatch(t)

	ack, taskID := w.processDispatchMessage(context.Background(), nil, msg.Message{ID: "1-0", Payload: payload})
	require.True(t, ack)
	require.Equal(t, "task-1", taskID)

	task := store.taskSnapshot()
	require.NotNil(t, task)
	require.Equal(t, config.StatusFailed, task.Status)
}

func TestProcessDispatchMessageKeepsPendingWhenURLPolicyLoadCanceled(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	w.URLSecurityPolicyProvider = newFailingURLSecurityPolicyProvider(context.Canceled)
	store := w.Store.(*workflowAckTestStore)
	payload := mustTestTaskDispatch(t)

	ack, taskID := w.processDispatchMessage(context.Background(), nil, msg.Message{ID: "1-0", Payload: payload})
	require.False(t, ack)
	require.Equal(t, "task-1", taskID)

	task := store.taskSnapshot()
	require.NotNil(t, task)
	require.Equal(t, config.StatusRunning, task.Status)
}

func TestProcessDispatchMessageAckOnDecodeError(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	ack, taskID := w.processDispatchMessage(context.Background(), nil, msg.Message{ID: "1-0", Payload: []byte("oops")})
	require.True(t, ack)
	require.Equal(t, "", taskID)
}

func TestProcessDispatchMessageKeepsPendingOnDatastoreFailure(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	store := w.Store.(*workflowAckTestStore)
	store.taskGetErr = errors.New("datastore temporarily unavailable")
	payload := mustTestTaskDispatch(t)

	ack, taskID := w.processDispatchMessage(context.Background(), nil, msg.Message{ID: "1-0", Payload: payload})
	require.False(t, ack)
	require.Equal(t, "task-1", taskID)
}

func TestProcessDispatchMessageAckOnMissingTask(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	store := w.Store.(*workflowAckTestStore)
	store.taskGetErr = datastore.ErrRecordNotExist
	payload := mustTestTaskDispatch(t)

	ack, taskID := w.processDispatchMessage(context.Background(), nil, msg.Message{ID: "1-0", Payload: payload})
	require.True(t, ack)
	require.Equal(t, "task-1", taskID)
}

func TestProcessDispatchMessageSkipsUserCancelledTask(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:       "task-1",
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		ProjectID:    "proj-1",
		WorkflowName: "demo",
		Status:       config.StatusCancelled,
		CancelSource: config.CancelSourceUser,
	}
	store := &workflowAckTestStore{
		task: task,
	}
	svc := &stubWorkflowService{updateOK: true}
	w := &Workflow{
		KubeClient:      fake.NewSimpleClientset(),
		Store:           store,
		WorkflowService: svc,
		Queue:           nil,
		Cfg:             &config.Config{},
	}
	payload := mustTestTaskDispatch(t)

	ack, taskID := w.processDispatchMessage(context.Background(), nil, msg.Message{ID: "1-0", Payload: payload})
	require.True(t, ack)
	require.Equal(t, "task-1", taskID)
	require.False(t, svc.updateCalled)
}

func TestProcessDispatchMessageKeepsPendingWhenRunningLeaseHeld(t *testing.T) {
	w := newWorkflowForAckTests(t, true)
	store := w.Store.(*workflowAckTestStore)
	task := store.taskSnapshot()
	require.NotNil(t, task)

	lockProvider := locker.NewMemoryLocker(workflowTaskRunLockerPrefix)
	w.taskRunLocker = lockProvider
	mutex := lockProvider.NewMutex(w.taskRunLeaseKey(task.TaskID), locker.WithTTL(time.Minute), locker.WithRetryCount(0))
	require.NoError(t, mutex.Lock(context.Background()))
	defer func() {
		require.NoError(t, mutex.Unlock(context.Background()))
	}()

	payload := mustTestTaskDispatch(t)
	ack, taskID := w.processDispatchMessage(context.Background(), nil, msg.Message{ID: "1-0", Payload: payload})
	require.False(t, ack)
	require.Equal(t, "task-1", taskID)
}

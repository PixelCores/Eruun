package job

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type ownedCheckpointFailureStore struct {
	*jobInfoStore
	transactionErr error
}

func (s *ownedCheckpointFailureStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	if s.transactionErr != nil {
		return s.transactionErr
	}
	return fn(s)
}

func (s *ownedCheckpointFailureStore) CompareAndSwapWithConditions(
	context.Context,
	datastore.Entity,
	map[string]interface{},
	map[string]interface{},
) (bool, error) {
	return true, nil
}

func TestRunJobsSerialContinuesWhenStopOnFailureFalse(t *testing.T) {
	jobs := []*model.JobTask{
		{Name: "first", JobType: "unknown"},
		{Name: "second", JobType: "unknown"},
	}

	RunJobs(context.Background(), jobs, &Runtime{
		Concurrency: 1,
		Store:       &noopStore{},
		Ack:         func() {},
	})

	require.Equal(t, config.StatusFailed, jobs[0].Status)
	require.Equal(t, config.StatusFailed, jobs[1].Status)
}

func TestRunJobsSerialStopsWhenStopOnFailureTrue(t *testing.T) {
	jobs := []*model.JobTask{
		{Name: "first", JobType: "unknown"},
		{Name: "second", JobType: "unknown"},
	}

	RunJobs(context.Background(), jobs, &Runtime{
		Concurrency:   1,
		Store:         &noopStore{},
		Ack:           func() {},
		StopOnFailure: true,
	})

	require.Equal(t, config.StatusFailed, jobs[0].Status)
	require.Empty(t, jobs[1].Status)
}

func TestRunJobsSerialStopsWhenAckCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{{
			AppID:         "app-1",
			Name:          "app-config",
			Namespace:     "default",
			ComponentType: config.ConfJob,
		}},
	}
	jobs := []*model.JobTask{
		{
			Name:      "app-config",
			Namespace: "default",
			AppID:     "app-1",
			JobType:   string(config.JobDeployConfigMap),
			JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
				Name: "app-config", Namespace: "default",
			}},
		},
		{Name: "second", JobType: "unknown"},
	}

	RunJobs(ctx, jobs, &Runtime{
		Concurrency: 1,
		Client:      fake.NewSimpleClientset(),
		Store:       withJobTestOwner(store, jobs[0]),
		Ack:         cancel,
	})

	require.Equal(t, config.StatusCancelled, jobs[0].Status)
	require.Empty(t, jobs[1].Status)
	require.Len(t, store.jobInfos, 1)
	require.Equal(t, string(config.StatusCancelled), store.jobInfos[0].Status)
	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusFailed), store.updated.Status)
}

func TestRunJobsReturnsInfrastructureStopWhenDistributedCheckpointFails(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
			checkpointErr := errors.New("injected job info persistence failure")
			store := &ownedCheckpointFailureStore{
				jobInfoStore: &jobInfoStore{addErr: checkpointErr},
			}
			queue := &enqueueCaptureQueue{enqueueID: "delay-checkpoint"}
			task := &model.JobTask{
				Name:          "delayed-job",
				Namespace:     "default",
				TaskID:        "task-delayed",
				JobType:       string(config.JobDeployInstant),
				ExecutionKey:  "execution-delayed",
				RunGeneration: 3,
				RunToken:      "run-3",
				WorkerID:      "worker-3",
				JobInfo: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name:      "delayed-job",
					Namespace: "default",
					Annotations: map[string]string{
						config.AnnotationJobStartTime: "4102444800",
					},
				}},
			}

			err := RunJobs(context.Background(), []*model.JobTask{task}, &Runtime{
				Concurrency: concurrency,
				Client:      fake.NewSimpleClientset(),
				Store:       store,
				Ack:         func() {},
				DelayQueue:  queue,
			})

			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			require.ErrorIs(t, err, checkpointErr)
			require.Equal(t, config.StatusPrepare, task.Status)
			require.Empty(t, task.Error)
			require.Empty(t, queue.enqueued, "queue notification must not precede the durable checkpoint")
			require.Equal(t, 1, store.addCount)
		})
	}
}

func TestRunJobsReturnsInfrastructureStopWhenStartOwnershipTransactionFails(t *testing.T) {
	transactionErr := errors.New("injected ownership transaction failure")
	store := &ownedCheckpointFailureStore{
		jobInfoStore:   &jobInfoStore{},
		transactionErr: transactionErr,
	}
	task := &model.JobTask{
		Name:          "app-config",
		Namespace:     "default",
		AppID:         "app-1",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployConfigMap),
		ExecutionKey:  "execution-1",
		RunGeneration: 2,
		RunToken:      "run-2",
		WorkerID:      "worker-2",
		JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: "app-config", Namespace: "default",
		}},
	}
	client := fake.NewSimpleClientset()
	ackCount := 0

	err := RunJobs(context.Background(), []*model.JobTask{task}, &Runtime{
		Concurrency:   1,
		Client:        client,
		Store:         store,
		Ack:           func() { ackCount++ },
		StopOnFailure: true,
	})

	require.ErrorIs(t, err, signal.ErrInfrastructureStop)
	require.ErrorIs(t, err, transactionErr)
	require.Equal(t, config.StatusPrepare, task.Status)
	require.Equal(t, 1, ackCount, "the failed ownership transaction must not add another ack")
	require.Empty(t, client.Actions())
}

func TestRunJobsReturnsInfrastructureStopWhenTerminalPersistenceFails(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
			persistErr := errors.New("injected terminal persistence failure")
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "terminal.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
			require.NoError(t, err)
			connection, err := db.DB()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })
			require.NoError(t, db.AutoMigrate(&model.JobInfo{}, &model.WorkflowQueue{}, &model.Applications{}, &model.ApplicationComponent{}))
			store := &sqlstore.Driver{Client: *db}
			task := &model.JobTask{
				Name:          "app-config",
				Namespace:     "default",
				WorkspaceID:   "test-space",
				TaskID:        "task-1",
				JobType:       string(config.JobDeployConfigMap),
				ExecutionKey:  "execution-1",
				RunGeneration: 2,
				RunToken:      "run-2",
				WorkerID:      "worker-2",
				JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
					Name: "app-config", Namespace: "default",
				}},
			}
			now := time.Now().UTC()
			lease := now.Add(time.Minute)
			require.NoError(t, store.Add(context.Background(), &model.WorkflowQueue{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Status: config.StatusRunning, RunGeneration: task.RunGeneration, RunToken: task.RunToken, WorkerID: task.WorkerID, LeaseExpiresAt: &lease}))
			record := buildJobInfoRecord(task)
			record.Status = string(config.StatusPrepare)
			record.SchedulingState = "admitted"
			record.SchedulingOwnerStatus = config.StatusRunning
			record.SchedulingGeneration = task.RunGeneration
			record.SchedulingQueuedAt = &now
			require.NoError(t, store.Add(context.Background(), &record))
			require.NoError(t, db.Callback().Update().Before("gorm:update").Register("fail-terminal-job-write", func(tx *gorm.DB) {
				if _, ok := tx.Statement.Model.(*model.JobInfo); !ok {
					return
				}
				if updates, ok := tx.Statement.Dest.(map[string]interface{}); ok && fmt.Sprint(updates["status"]) == string(config.StatusCompleted) {
					tx.AddError(persistErr)
				}
			}))
			err = RunJobs(context.Background(), []*model.JobTask{task}, &Runtime{
				Concurrency:   concurrency,
				Client:        fake.NewSimpleClientset(),
				Store:         store,
				Ack:           func() {},
				StopOnFailure: true,
			})

			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			require.ErrorContains(t, err, persistErr.Error())
			require.Equal(t, config.StatusCompleted, task.Status)
			require.NoError(t, store.Get(context.Background(), &record))
			require.NotEqual(t, string(config.StatusCompleted), record.Status)
		})
	}
}

func TestRunJobReturnsInfrastructureStopWhenEarlyTerminalPersistenceFails(t *testing.T) {
	persistErr := errors.New("injected early terminal persistence failure")
	for _, tc := range []struct {
		name   string
		status config.Status
		ctx    func() (context.Context, context.CancelFunc)
	}{
		{
			name:   "skipped",
			status: config.StatusSkipped,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.Background(), func() {}
			},
		},
		{
			name: "cancelled before start",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
		},
		{
			name: "cancellation watcher unavailable",
			ctx: func() (context.Context, context.CancelFunc) {
				return WithTaskMetadata(context.Background(), "task-early-terminal"), func() {}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &ownedCheckpointFailureStore{jobInfoStore: &jobInfoStore{addErr: persistErr}}
			task := &model.JobTask{
				Name:          "app-config",
				Namespace:     "default",
				TaskID:        "task-early-terminal",
				JobType:       string(config.JobDeployConfigMap),
				Status:        tc.status,
				ExecutionKey:  "execution-1",
				RunGeneration: 1,
				RunToken:      "run-1",
				WorkerID:      "worker-1",
				JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
					Name: "app-config", Namespace: "default",
				}},
			}
			ctx, cancel := tc.ctx()
			defer cancel()

			client := fake.NewSimpleClientset()
			ack := func() {}
			err := runJob(ctx, task, &Runtime{Client: client, Store: store, Ack: ack})

			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			require.ErrorIs(t, err, persistErr)
			require.Equal(t, 1, store.addCount)
		})
	}
}

func TestRunJobsRejectsMissingOwnershipBeforeSideEffects(t *testing.T) {
	persistErr := errors.New("unexpected persistence attempt")
	store := &ownedCheckpointFailureStore{
		jobInfoStore: &jobInfoStore{addErr: persistErr},
	}
	task := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		TaskID:    "task-unclaimed",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: "app-config", Namespace: "default",
		}},
	}

	client := fake.NewSimpleClientset()
	err := RunJobs(context.Background(), []*model.JobTask{task}, &Runtime{
		Concurrency:   1,
		Client:        client,
		Store:         store,
		Ack:           func() {},
		StopOnFailure: true,
	})

	require.ErrorIs(t, err, signal.ErrInfrastructureStop)
	require.ErrorIs(t, err, repository.ErrWorkflowOwnershipRequired)
	require.NotEqual(t, config.StatusCompleted, task.Status)
	require.Zero(t, store.addCount)
	require.Empty(t, client.Actions())
}

func TestRunJobsReturnsTerminalCallbackPersistenceFailureWithoutWorker(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "callback.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
			require.NoError(t, err)
			connection, err := db.DB()
			require.NoError(t, err)
			connection.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })
			require.NoError(t, db.AutoMigrate(&model.JobInfo{}, &model.WorkflowQueue{}, &model.Applications{}, &model.ApplicationComponent{}, &model.SystemSetting{}))
			store := &sqlstore.Driver{Client: *db}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, store))
			owner := &model.WorkflowQueue{TaskID: "cancelled-before-worker", WorkspaceID: "workspace", Status: config.StatusCancelled}
			require.NoError(t, store.Add(ctx, owner))
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			task := &model.JobTask{Name: "callback", WorkspaceID: owner.WorkspaceID, TaskID: owner.TaskID,
				ExecutionKey: TerminalCallbackExecutionKey(owner.TaskID, 0, "cancelled"), OwnerStatus: owner.Status,
				JobType: string(config.JobDeployCallback), JobInfo: &CallbackJobInfo{Event: "cancelled", URL: server.URL}}
			persistErr := errors.New("injected callback terminal persistence failure")
			require.NoError(t, db.Callback().Update().Before("gorm:update").Register("fail-terminal-callback-write", func(tx *gorm.DB) {
				if _, ok := tx.Statement.Model.(*model.JobInfo); !ok {
					return
				}
				if updates, ok := tx.Statement.Dest.(map[string]interface{}); ok && fmt.Sprint(updates["status"]) == string(config.StatusCompleted) {
					tx.AddError(persistErr)
				}
			}))
			result := make(chan error, 1)
			go func() {
				result <- RunJobs(ctx, []*model.JobTask{task}, &Runtime{
					Concurrency:       concurrency,
					Store:             store,
					Ack:               func() {},
					StopOnFailure:     true,
					URLSecurityPolicy: &spec.URLSecurityPolicySpec{AllowPrivateByDefault: true},
				})
			}()
			require.Eventually(t, func() bool {
				var count int64
				return db.Model(&model.JobInfo{}).Where("execution_key = ? AND scheduling_state = ?", task.ExecutionKey, "queued").Count(&count).Error == nil && count == 1
			}, time.Second, 10*time.Millisecond)
			admitted, err := repository.AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, admitted)
			select {
			case err = <-result:
			case <-ctx.Done():
				t.Fatal("terminal callback did not finish")
			}
			require.Equal(t, int32(1), requests.Load())
			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			require.ErrorContains(t, err, persistErr.Error())
			require.Equal(t, config.StatusCompleted, task.Status)
			var stored model.JobInfo
			require.NoError(t, db.Where("execution_key = ?", task.ExecutionKey).First(&stored).Error)
			require.Equal(t, string(config.StatusPrepare), stored.Status, "a failed write must not fabricate a committed callback result")
			require.Equal(t, "released", stored.SchedulingState)
		})
	}
}

func TestRunJobsInfrastructureStopDoesNotPersistCancelledState(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{{
			AppID:         "app-1",
			Name:          "app-config",
			Namespace:     "default",
			ComponentType: config.ConfJob,
		}},
	}
	jobs := []*model.JobTask{{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: "app-config", Namespace: "default",
		}},
	}}
	ack := func() {
		cancel(signal.ErrInfrastructureStop)
	}

	RunJobs(ctx, jobs, &Runtime{
		Concurrency: 1,
		Client:      fake.NewSimpleClientset(),
		Store:       store,
		Ack:         ack,
	})

	require.Equal(t, config.StatusPrepare, jobs[0].Status)
	require.Empty(t, jobs[0].Error)
	require.Empty(t, store.jobInfos)
	require.Nil(t, store.updated)
}

func TestRunJobsParallelDoesNotStartJobsWithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	jobs := []*model.JobTask{
		{Name: "first", JobType: "unknown"},
		{Name: "second", JobType: "unknown"},
	}

	RunJobs(ctx, jobs, &Runtime{
		Concurrency: 2,
		Store:       &noopStore{},
		Ack:         func() {},
	})

	require.Empty(t, jobs[0].Status)
	require.Empty(t, jobs[1].Status)
}

type blockingManagementModeStore struct {
	*componentStatusStore
	started chan struct{}
	once    sync.Once
}

func (s *blockingManagementModeStore) Get(ctx context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.Applications); !ok {
		return s.componentStatusStore.Get(ctx, entity)
	}
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	return ctx.Err()
}

type blockingRedisGetHook struct {
	started chan struct{}
	once    sync.Once
}

func (h *blockingRedisGetHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *blockingRedisGetHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "get" {
			return next(ctx, cmd)
		}
		h.once.Do(func() { close(h.started) })
		<-ctx.Done()
		return ctx.Err()
	}
}

func (h *blockingRedisGetHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestRunJobInfrastructureStopDuringManagementModeCheckDoesNotPersistFailure(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	store := &blockingManagementModeStore{
		componentStatusStore: &componentStatusStore{},
		started:              make(chan struct{}),
	}
	job := infrastructureStopTestJob()
	ackCount := 0
	client := fake.NewSimpleClientset()
	ack := func() { ackCount++ }
	done := make(chan struct{})
	go func() {
		runJob(ctx, job, &Runtime{Client: client, Store: store, Ack: ack})
		close(done)
	}()

	requireClosed(t, store.started)
	cancel(signal.ErrInfrastructureStop)
	requireClosed(t, done)

	require.Equal(t, config.StatusPrepare, job.Status)
	require.Empty(t, job.Error)
	require.Equal(t, 1, ackCount)
	require.Empty(t, store.jobInfos)
	require.Nil(t, store.updated)
}

func TestRunJobInfrastructureStopDuringCancellationWatcherSetupDoesNotPersistFailure(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	ctx = WithTaskMetadata(ctx, "task-infrastructure-stop")
	store := &componentStatusStore{}
	job := infrastructureStopTestJob()
	hook := &blockingRedisGetHook{started: make(chan struct{})}
	redisClient := redis.NewClient(&redis.Options{Addr: "unused:0"})
	redisClient.AddHook(hook)
	defer redisClient.Close()
	client := fake.NewSimpleClientset()
	ackCount := 0
	ack := func() { ackCount++ }
	runtime := &Runtime{Client: client, Store: store, Ack: ack, RedisClient: redisClient}
	done := make(chan struct{})
	go func() {
		runJob(ctx, job, runtime)
		close(done)
	}()

	requireClosed(t, hook.started)
	cancel(signal.ErrInfrastructureStop)
	requireClosed(t, done)

	require.Equal(t, config.StatusPrepare, job.Status)
	require.Empty(t, job.Error)
	require.Equal(t, 1, ackCount)
	require.Empty(t, store.jobInfos)
	require.Nil(t, store.updated)
}

func TestJobRuntimeDoesNotReusePersistenceGateAcrossRuns(t *testing.T) {
	config := &Runtime{}
	config.adoptionPersistenceOnce.Do(func() {
		config.adoptionPersistenceGate = make(chan struct{}, 1)
	})

	first := newJobRuntime(config)
	second := newJobRuntime(config)
	require.NoError(t, first.withAdoptionPersistenceContext(context.Background(), func() error { return nil }))
	require.NoError(t, second.withAdoptionPersistenceContext(context.Background(), func() error { return nil }))
	require.NotEqual(t, config.adoptionPersistenceGate, first.adoptionPersistenceGate)
	require.NotEqual(t, first.adoptionPersistenceGate, second.adoptionPersistenceGate)
}

func infrastructureStopTestJob() *model.JobTask {
	return &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		Status:    config.StatusQueued,
		JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: "app-config", Namespace: "default",
		}},
	}
}

package job

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
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

	RunJobs(context.Background(), jobs, 1, nil, nil, &noopStore{}, func() {}, false, nil, nil, nil, nil, nil)

	require.Equal(t, config.StatusFailed, jobs[0].Status)
	require.Equal(t, config.StatusFailed, jobs[1].Status)
}

func TestRunJobsSerialStopsWhenStopOnFailureTrue(t *testing.T) {
	jobs := []*model.JobTask{
		{Name: "first", JobType: "unknown"},
		{Name: "second", JobType: "unknown"},
	}

	RunJobs(context.Background(), jobs, 1, nil, nil, &noopStore{}, func() {}, true, nil, nil, nil, nil, nil)

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

	RunJobs(ctx, jobs, 1, fake.NewSimpleClientset(), nil, store, cancel, false, nil, nil, nil, nil, nil)

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

			err := RunJobs(
				context.Background(),
				[]*model.JobTask{task},
				concurrency,
				fake.NewSimpleClientset(),
				nil,
				store,
				func() {},
				false,
				nil,
				nil,
				queue,
				nil,
				nil,
			)

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

	err := RunJobs(context.Background(), []*model.JobTask{task}, 1, client, nil, store, func() { ackCount++ }, true, nil, nil, nil, nil, nil)

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
			store := &ownedCheckpointFailureStore{
				jobInfoStore: &jobInfoStore{addErr: persistErr},
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

			err := RunJobs(context.Background(), []*model.JobTask{task}, concurrency, fake.NewSimpleClientset(), nil, store, func() {}, true, nil, nil, nil, nil, nil)

			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			require.ErrorIs(t, err, persistErr)
			require.Equal(t, config.StatusCompleted, task.Status)
			require.Equal(t, 1, store.addCount)
		})
	}
}

func TestRunJobsKeepsLegacyTerminalPersistenceBestEffort(t *testing.T) {
	persistErr := errors.New("injected legacy terminal persistence failure")
	store := &ownedCheckpointFailureStore{
		jobInfoStore: &jobInfoStore{addErr: persistErr},
	}
	task := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		TaskID:    "task-legacy",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: "app-config", Namespace: "default",
		}},
	}

	err := RunJobs(context.Background(), []*model.JobTask{task}, 1, fake.NewSimpleClientset(), nil, store, func() {}, true, nil, nil, nil, nil, nil)

	require.NoError(t, err)
	require.Equal(t, config.StatusCompleted, task.Status)
	require.Equal(t, 1, store.addCount)
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

	RunJobs(ctx, jobs, 1, fake.NewSimpleClientset(), nil, store, ack, false, nil, nil, nil, nil, nil)

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

	RunJobs(ctx, jobs, 2, nil, nil, &noopStore{}, func() {}, false, nil, nil, nil, nil, nil)

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
	done := make(chan struct{})
	go func() {
		runJob(ctx, job, fake.NewSimpleClientset(), store, func() { ackCount++ }, nil)
		close(done)
	}()

	requireClosed(t, store.started)
	cancel(signal.ErrInfrastructureStop)
	requireClosed(t, done)

	require.Equal(t, config.StatusQueued, job.Status)
	require.Empty(t, job.Error)
	require.Equal(t, 0, ackCount)
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
	runtime := &jobRuntime{redisClient: redisClient}
	ackCount := 0
	done := make(chan struct{})
	go func() {
		runJob(ctx, job, fake.NewSimpleClientset(), store, func() { ackCount++ }, runtime)
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

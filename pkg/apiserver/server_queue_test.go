package apiserver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/event"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type testServerQueue struct {
	ensureGroupErr error
	ensureCalls    int
}

func (q *testServerQueue) EnsureGroup(context.Context, string) error {
	q.ensureCalls++
	return q.ensureGroupErr
}

func (q *testServerQueue) Enqueue(context.Context, []byte) (string, error) { return "", nil }
func (q *testServerQueue) ReadGroup(context.Context, string, string, int, time.Duration) ([]msg.Message, error) {
	return nil, nil
}
func (q *testServerQueue) Ack(context.Context, string, ...string) error { return nil }
func (q *testServerQueue) AutoClaim(context.Context, string, string, time.Duration, int) ([]msg.Message, error) {
	return nil, nil
}
func (q *testServerQueue) Close(context.Context) error                         { return nil }
func (q *testServerQueue) Stats(context.Context, string) (int64, int64, error) { return 0, 0, nil }

type testServerWorker struct {
	starts     atomic.Int64
	subscribes atomic.Int64
}

func (w *testServerWorker) Start(context.Context, chan error) {
	w.starts.Add(1)
}

func (w *testServerWorker) StartWorker(consumerCtx context.Context, _ context.Context, _ chan error, ready, stopped func()) {
	w.subscribes.Add(1)
	if ready != nil {
		ready()
	}
	if stopped != nil {
		defer stopped()
	}
	<-consumerCtx.Done()
}

type testWorkerContexts struct {
	consumer  context.Context
	execution context.Context
}

type contextTrackingServerWorker struct {
	contexts         chan testWorkerContexts
	leaderContexts   chan context.Context
	waitForExecution bool
}

func (w *contextTrackingServerWorker) Start(ctx context.Context, _ chan error) {
	if w.leaderContexts == nil {
		return
	}
	w.leaderContexts <- ctx
	<-ctx.Done()
}

func (w *contextTrackingServerWorker) StartWorker(consumerCtx, executionCtx context.Context, _ chan error, ready, stopped func()) {
	w.contexts <- testWorkerContexts{consumer: consumerCtx, execution: executionCtx}
	if ready != nil {
		ready()
	}
	if stopped != nil {
		defer stopped()
	}
	<-consumerCtx.Done()
	if w.waitForExecution {
		<-executionCtx.Done()
	}
}

type blockingServerWorker struct {
	started chan struct{}
	release chan struct{}
}

func (w *blockingServerWorker) Start(context.Context, chan error) {}

func (w *blockingServerWorker) StartWorker(_ context.Context, _ context.Context, _ chan error, ready, stopped func()) {
	close(w.started)
	if ready != nil {
		ready()
	}
	if stopped != nil {
		defer stopped()
	}
	<-w.release
}

type readinessServerWorker struct {
	started          chan struct{}
	allowReady       chan struct{}
	stopConsuming    chan struct{}
	consumingStopped chan struct{}
	release          chan struct{}
}

func (w *readinessServerWorker) Start(context.Context, chan error) {}

func (w *readinessServerWorker) StartWorker(_ context.Context, _ context.Context, _ chan error, ready, stopped func()) {
	close(w.started)
	<-w.allowReady
	if ready != nil {
		ready()
	}
	<-w.stopConsuming
	if stopped != nil {
		stopped()
	}
	close(w.consumingStopped)
	<-w.release
}

func newThreeReplicaTestServer(t *testing.T, worker event.Worker) *restServer {
	t.Helper()
	const (
		podName   = "eruun-0"
		namespace = "default"
	)
	t.Setenv("POD_NAME", podName)
	t.Setenv("POD_NAMESPACE", namespace)

	replicas := int32(3)
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "eruun", Namespace: namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "StatefulSet",
				Name: statefulSet.Name,
			}},
		},
	}
	return &restServer{
		KubeClient:   fake.NewSimpleClientset(statefulSet, pod),
		eventWorkers: []event.Worker{worker},
	}
}

type testLeaderElectionLock struct {
	identity  string
	record    *resourcelock.LeaderElectionRecord
	getErr    error
	updateErr error
	getFunc   func(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error)
	updates   []resourcelock.LeaderElectionRecord
}

func (l *testLeaderElectionLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	if l.getFunc != nil {
		return l.getFunc(ctx)
	}
	if l.getErr != nil {
		return nil, nil, l.getErr
	}
	if l.record == nil {
		return &resourcelock.LeaderElectionRecord{}, nil, nil
	}
	record := *l.record
	return &record, nil, nil
}

func (l *testLeaderElectionLock) Create(context.Context, resourcelock.LeaderElectionRecord) error {
	return nil
}

func (l *testLeaderElectionLock) Update(_ context.Context, record resourcelock.LeaderElectionRecord) error {
	if l.updateErr != nil {
		return l.updateErr
	}
	l.updates = append(l.updates, record)
	return nil
}

func (l *testLeaderElectionLock) RecordEvent(string) {}

func (l *testLeaderElectionLock) Identity() string { return l.identity }

func (l *testLeaderElectionLock) Describe() string { return "test-lock" }

func TestEnsureQueueGroup(t *testing.T) {
	t.Run("skip when queue is nil", func(t *testing.T) {
		server := &restServer{}
		require.NoError(t, server.ensureQueueGroup(context.Background()))
		require.EqualValues(t, 0, server.ensureQueueGroupFailures.Load())
	})

	t.Run("propagate ensure group error", func(t *testing.T) {
		expectedErr := errors.New("queue unavailable")
		server := &restServer{
			Queue: &testServerQueue{ensureGroupErr: expectedErr},
		}
		err := server.ensureQueueGroup(context.Background())
		require.ErrorIs(t, err, expectedErr)
		require.EqualValues(t, 1, server.ensureQueueGroupFailures.Load())
	})

	t.Run("ignore cancellation error for failure counter", func(t *testing.T) {
		server := &restServer{
			Queue: &testServerQueue{ensureGroupErr: context.Canceled},
		}
		err := server.ensureQueueGroup(context.Background())
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 0, server.ensureQueueGroupFailures.Load())
	})
}

func TestOnStartedSchedulerLeadingReportsQueueGroupError(t *testing.T) {
	expectedErr := errors.New("ensure group failed")
	server := &restServer{
		Queue: &testServerQueue{ensureGroupErr: expectedErr},
	}
	t.Cleanup(server.stopSchedulerRun)
	errChan := make(chan error, 1)

	server.onStartedSchedulerLeading(context.Background(), errChan)

	select {
	case err := <-errChan:
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
		require.ErrorContains(t, err, "ensure queue group "+config.WorkflowWorkerQueueGroup)
		require.EqualValues(t, 1, server.ensureQueueGroupFailures.Load())
	default:
		t.Fatal("expected scheduler startup to report ensureQueueGroup error")
	}
}

func TestReportableInformerStartError(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), 0)
	defer deadlineCancel()
	<-deadlineCtx.Done()

	testCases := []struct {
		name   string
		ctx    context.Context
		err    error
		report bool
	}{
		{name: "nil error", ctx: context.Background()},
		{name: "nil context with informer canceled", err: context.Canceled, report: true},
		{name: "active leader context with informer canceled", ctx: context.Background(), err: context.Canceled, report: true},
		{name: "active leader context with informer deadline exceeded", ctx: context.Background(), err: context.DeadlineExceeded, report: true},
		{name: "canceled leader context", ctx: canceledCtx, err: errors.New("informer unavailable")},
		{name: "expired leader context", ctx: deadlineCtx, err: context.DeadlineExceeded},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := reportableInformerStartError(tc.ctx, tc.err)
			if !tc.report {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorIs(t, err, tc.err)
			require.ErrorContains(t, err, "start informer manager")
		})
	}
}

func TestOnStartedSchedulerLeadingIgnoresQueueGroupErrorWhenContextCanceled(t *testing.T) {
	server := &restServer{
		Queue: &testServerQueue{ensureGroupErr: context.Canceled},
	}
	t.Cleanup(server.stopSchedulerRun)
	errChan := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server.onStartedSchedulerLeading(ctx, errChan)
	require.EqualValues(t, 0, server.ensureQueueGroupFailures.Load())

	select {
	case err := <-errChan:
		t.Fatalf("expected canceled context error to be ignored, got: %v", err)
	default:
	}
}

func TestBeginControllerRunStopsPreviousInformerRuntimeSynchronously(t *testing.T) {
	manager := informer.NewManager(fake.NewSimpleClientset())
	t.Cleanup(func() {
		manager.Stop()
		manager.GetWaiter().Close()
	})

	previousRun := newWorkerRun(context.Background())
	previousRun.markStarted()
	require.NoError(t, manager.Start(previousRun.ctx))
	require.True(t, manager.IsStarted())

	server := &restServer{
		InformerManager: manager,
		controllerRun:   previousRun,
	}
	currentRun := server.beginControllerRun(context.Background())
	require.NotNil(t, currentRun)
	require.False(t, manager.IsStarted(), "beginning a controller run must wait for the previous informer runtime to stop")

	currentRun.markStarted()
	t.Cleanup(server.stopControllerRun)
	require.NoError(t, manager.Start(currentRun.ctx))
	require.True(t, manager.IsStarted())
}

func TestStartWorkersUsesServerScopedEventWorkers(t *testing.T) {
	firstWorker := &testServerWorker{}
	laterWorker := &testServerWorker{}
	firstServer := &restServer{eventWorkers: []event.Worker{firstWorker}}
	laterServer := &restServer{eventWorkers: []event.Worker{laterWorker}}
	require.Len(t, laterServer.eventWorkers, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() {
		firstServer.stopWorkers(context.Background())
	})

	firstServer.startWorkers(ctx, nil)
	firstServer.startWorkers(ctx, nil)

	waitForServerWorkerCount(t, "first server subscriber", firstWorker.subscribes.Load, 1)
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, firstWorker.subscribes.Load())
	require.EqualValues(t, 0, laterWorker.subscribes.Load())
	require.EqualValues(t, 0, laterWorker.starts.Load())
}

func TestStartWorkersSeparatesConsumerAndExecutionContexts(t *testing.T) {
	worker := &contextTrackingServerWorker{contexts: make(chan testWorkerContexts, 1)}
	server := &restServer{eventWorkers: []event.Worker{worker}}
	executionCtx, cancelExecution := context.WithCancel(context.Background())
	defer cancelExecution()

	server.startWorkers(executionCtx, nil)
	var contexts testWorkerContexts
	select {
	case contexts = <-worker.contexts:
	case <-time.After(time.Second):
		t.Fatal("worker subscriber did not start")
	}
	require.NotSame(t, contexts.execution, contexts.consumer)
	require.NotSame(t, executionCtx, contexts.execution)
	require.NoError(t, contexts.execution.Err())

	server.stopWorkers(context.Background())
	require.ErrorIs(t, contexts.consumer.Err(), context.Canceled)
	require.ErrorIs(t, contexts.execution.Err(), context.Canceled)
	require.NoError(t, executionCtx.Err())
}

func TestStartWorkersIgnoresCanceledExecutionContext(t *testing.T) {
	worker := &testServerWorker{}
	server := &restServer{eventWorkers: []event.Worker{worker}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server.startWorkers(ctx, nil)

	server.workersMu.Lock()
	started := server.workersStarted
	server.workersMu.Unlock()
	require.False(t, started)
	require.EqualValues(t, 0, worker.subscribes.Load())
}

func TestStopWorkersReturnsWhenDrainTimeoutExpires(t *testing.T) {
	worker := &blockingServerWorker{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	server := &restServer{eventWorkers: []event.Worker{worker}}
	server.startWorkers(context.Background(), nil)
	select {
	case <-worker.started:
	case <-time.After(time.Second):
		t.Fatal("worker subscriber did not start")
	}

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelDrain()
	stopped := make(chan struct{})
	go func() {
		server.stopWorkers(drainCtx)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(250 * time.Millisecond):
		close(worker.release)
		t.Fatal("worker drain exceeded its hard timeout")
	}

	server.workersMu.Lock()
	require.Len(t, server.drainingWorkerRuns, 1)
	server.workersMu.Unlock()
	close(worker.release)
	require.Eventually(t, func() bool {
		server.workersMu.Lock()
		defer server.workersMu.Unlock()
		return len(server.drainingWorkerRuns) == 0
	}, time.Second, time.Millisecond)
}

func TestRenewDeadlineForLeaseDuration(t *testing.T) {
	require.Equal(t, 4*time.Second*2/3, renewDeadlineForLeaseDuration(4*time.Second))
	require.Greater(t, renewDeadlineForLeaseDuration(4*time.Second), leaderElectionRetryPeriod*6/5)
	require.Equal(t, 10*time.Second, renewDeadlineForLeaseDuration(15*time.Second))
	require.Equal(t, 200*time.Second, renewDeadlineForLeaseDuration(5*time.Minute))
	require.Equal(t, 10*time.Second, renewDeadlineForLeaseDuration(12*time.Second))
}

func TestBestEffortReleaseLeaderLockReleasesCurrentHolder(t *testing.T) {
	lock := &testLeaderElectionLock{
		identity: "server-1",
		record: &resourcelock.LeaderElectionRecord{
			HolderIdentity:       "server-1",
			LeaseDurationSeconds: 300,
			LeaderTransitions:    7,
		},
	}

	released := bestEffortReleaseLeaderLock(lock, time.Second)

	require.True(t, released)
	require.Len(t, lock.updates, 1)
	update := lock.updates[0]
	require.Empty(t, update.HolderIdentity)
	require.Equal(t, 1, update.LeaseDurationSeconds)
	require.Equal(t, 7, update.LeaderTransitions)
	require.False(t, update.AcquireTime.IsZero())
	require.False(t, update.RenewTime.IsZero())
}

func TestBestEffortReleaseLeaderLockSkipsChangedHolderAndFailures(t *testing.T) {
	t.Run("holder_changed", func(t *testing.T) {
		lock := &testLeaderElectionLock{
			identity: "server-1",
			record: &resourcelock.LeaderElectionRecord{
				HolderIdentity:       "server-2",
				LeaseDurationSeconds: 300,
			},
		}

		released := bestEffortReleaseLeaderLock(lock, time.Second)

		require.False(t, released)
		require.Empty(t, lock.updates)
	})

	t.Run("get_not_found", func(t *testing.T) {
		lock := &testLeaderElectionLock{
			identity: "server-1",
			getErr: apierrors.NewNotFound(schema.GroupResource{
				Group:    "coordination.k8s.io",
				Resource: "leases",
			}, "apiserver-lock"),
		}

		released := bestEffortReleaseLeaderLock(lock, time.Second)

		require.False(t, released)
		require.Empty(t, lock.updates)
	})

	t.Run("update_error", func(t *testing.T) {
		lock := &testLeaderElectionLock{
			identity: "server-1",
			record: &resourcelock.LeaderElectionRecord{
				HolderIdentity:       "server-1",
				LeaseDurationSeconds: 300,
			},
			updateErr: errors.New("update failed"),
		}

		released := bestEffortReleaseLeaderLock(lock, time.Second)

		require.False(t, released)
		require.Empty(t, lock.updates)
	})

	t.Run("get_timeout", func(t *testing.T) {
		lock := &testLeaderElectionLock{
			identity: "server-1",
			getFunc: func(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
				<-ctx.Done()
				return nil, nil, ctx.Err()
			},
		}

		start := time.Now()
		released := bestEffortReleaseLeaderLock(lock, 20*time.Millisecond)
		elapsed := time.Since(start)

		require.False(t, released)
		require.Less(t, elapsed, 200*time.Millisecond)
		require.Empty(t, lock.updates)
	})
}

func TestBuildQueueRejectsInvalidMessagingType(t *testing.T) {
	server := &restServer{
		cfg: config.Config{
			Messaging: config.MessagingConfig{Type: "noop"},
		},
	}
	queue, err := server.buildQueue("demo", nil)
	require.Nil(t, queue)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported messaging type")
}

func TestBuildRedisQueueRequiresClient(t *testing.T) {
	server := &restServer{
		cfg: config.Config{
			Messaging: config.MessagingConfig{Type: "redis"},
		},
	}
	queue, err := server.buildRedisQueue("demo", nil)
	require.Nil(t, queue)
	require.Error(t, err)
	require.Contains(t, err.Error(), "redis client is not initialized")
}

func TestInitRedisClientForConfiguredBackendsFailsRedisCache(t *testing.T) {
	expectedErr := errors.New("redis unavailable")
	oldNewRedisClient := newRedisClient
	newRedisClient = func(config.RedisCacheConfig) (*redis.Client, error) {
		return nil, expectedErr
	}
	t.Cleanup(func() {
		newRedisClient = oldNewRedisClient
	})

	server := &restServer{
		cfg: config.Config{
			Cache:     config.RedisCacheConfig{CacheType: "redis"},
			Messaging: config.MessagingConfig{Type: "kafka"},
		},
	}
	client, err := server.initRedisClientForConfiguredBackends()
	require.Nil(t, client)
	require.ErrorIs(t, err, expectedErr)
	require.Contains(t, err.Error(), "init redis client for configured redis backend")
}

func TestInitRedisClientForConfiguredBackendsSkipsRedisWhenUnused(t *testing.T) {
	oldNewRedisClient := newRedisClient
	called := false
	newRedisClient = func(config.RedisCacheConfig) (*redis.Client, error) {
		called = true
		return redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}), nil
	}
	t.Cleanup(func() {
		newRedisClient = oldNewRedisClient
	})

	server := &restServer{
		cfg: config.Config{
			Cache:     config.RedisCacheConfig{CacheType: "memory"},
			Messaging: config.MessagingConfig{Type: "kafka"},
		},
	}
	client, err := server.initRedisClientForConfiguredBackends()
	require.NoError(t, err)
	require.Nil(t, client)
	require.False(t, called)
}

func TestInitRedisClientForConfiguredBackendsUsesRedisForCache(t *testing.T) {
	expectedClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() {
		_ = expectedClient.Close()
	})
	oldNewRedisClient := newRedisClient
	newRedisClient = func(config.RedisCacheConfig) (*redis.Client, error) {
		return expectedClient, nil
	}
	t.Cleanup(func() {
		newRedisClient = oldNewRedisClient
	})

	server := &restServer{
		cfg: config.Config{
			Cache:     config.RedisCacheConfig{CacheType: "redis"},
			Messaging: config.MessagingConfig{Type: "kafka"},
		},
	}
	client, err := server.initRedisClientForConfiguredBackends()
	require.NoError(t, err)
	require.Same(t, expectedClient, client)
}

func TestEnsureKafkaMessagingReadySkipsNonKafka(t *testing.T) {
	server := &restServer{
		cfg: config.Config{
			Messaging: config.MessagingConfig{Type: "redis"},
		},
	}
	require.NoError(t, server.ensureKafkaMessagingReady())
}

func TestEnsureKafkaMessagingReadyUsesOnlyRoleTopics(t *testing.T) {
	oldEnsureKafka := ensureKafkaMessaging
	var captured clients.KafkaConfig
	calls := 0
	ensureKafkaMessaging = func(cfg clients.KafkaConfig) (*kafka.Dialer, error) {
		calls++
		captured = cfg
		return &kafka.Dialer{}, nil
	}
	t.Cleanup(func() {
		ensureKafkaMessaging = oldEnsureKafka
	})

	base := config.Config{Messaging: config.MessagingConfig{
		Type:                        "kafka",
		ChannelPrefix:               "tenant-a",
		KafkaBrokers:                []string{"broker-1:9092"},
		KafkaTopicPartitions:        3,
		KafkaTopicReplicationFactor: 2,
	}}
	tests := []struct {
		role   config.RuntimeRole
		topics []string
	}{
		{role: config.RuntimeRoleAPI},
		{role: config.RuntimeRoleController, topics: []string{"tenant-a.job.delay", "tenant-a.job.result"}},
		{role: config.RuntimeRoleScheduler, topics: []string{"tenant-a.workflow.dispatch"}},
		{role: config.RuntimeRoleWorker, topics: []string{"tenant-a.workflow.dispatch", "tenant-a.job.delay"}},
	}
	for _, tc := range tests {
		t.Run(string(tc.role), func(t *testing.T) {
			cfg := base
			cfg.Role = tc.role
			server := &restServer{cfg: cfg}
			before := calls
			require.NoError(t, server.ensureKafkaMessagingReady())
			if len(tc.topics) == 0 {
				require.Equal(t, before, calls)
				return
			}
			require.Equal(t, before+1, calls)
			require.Equal(t, tc.topics, captured.Topics)
			require.Equal(t, []string{"broker-1:9092"}, captured.Brokers)
			require.Equal(t, 3, captured.TopicPartitions)
			require.Equal(t, 2, captured.TopicReplicationFactor)
		})
	}
}

func TestServerLifecycleRejectsNilContext(t *testing.T) {
	server := &restServer{}

	require.PanicsWithValue(t, "create worker run: nil context", func() {
		newWorkerRun(nil)
	})
	require.PanicsWithValue(t, "start workers: nil context", func() {
		server.startWorkers(nil, nil)
	})
	require.PanicsWithValue(t, "stop workers: nil context", func() {
		server.stopWorkers(nil)
	})
}

func waitForServerWorkerCount(t *testing.T, label string, got func() int64, want int64) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if got() == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("expected %s count %d, got %d", label, want, got())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

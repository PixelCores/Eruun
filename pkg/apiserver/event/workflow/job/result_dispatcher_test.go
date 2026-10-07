package job

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestResultDispatcherClaimsPendingAtomically(t *testing.T) {
	ctx := context.Background()
	payload, live, pod, store := completedResultFixture(t)
	pending := buildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(ctx, pending))
	var wg sync.WaitGroup
	winners := make(chan *model.JobResultOutbox, 20)
	errs := make(chan error, 20)
	start := make(chan struct{})
	for range 20 {
		snapshot := *pending
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claimed, err := claimResultOutbox(ctx, store, &snapshot)
			errs <- err
			if claimed {
				winners <- &snapshot
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, winners, 1, "only one dispatcher may own a pending snapshot")
	owner := <-winners
	require.NotEmpty(t, owner.ClaimToken)
	require.NotNil(t, owner.LeaseExpiresAt)
	oldOwner := *owner
	require.NoError(t, bindResultOutboxJobUID(ctx, store, owner, string(live.UID)))
	require.NoError(t, retryResultOutbox(ctx, store, owner, "retry cleanup"))
	claimed, err := claimResultOutbox(ctx, store, pending)
	require.NoError(t, err)
	require.False(t, claimed, "a scan predating the previous attempt must not discard its persisted UID or attempt count")
	current, err := getJobResultOutboxByID(ctx, store, pending.ID)
	require.NoError(t, err)
	require.Equal(t, string(live.UID), current.JobUID)
	claimed, err = claimResultOutbox(ctx, store, current)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotEqual(t, oldOwner.ClaimToken, current.ClaimToken)
	client := fake.NewSimpleClientset(live, pod)
	dispatcher := NewResultDispatcher(client, store)
	require.ErrorIs(t, dispatcher.processResult(ctx, &oldOwner), errResultOutboxOwnershipLost)
	for _, action := range client.Actions() {
		require.NotEqual(t, "delete", action.GetVerb())
	}
	require.Equal(t, string(config.StatusDistributed), store.jobInfoByTaskID(payload.TaskID).Status)
	actual, err := getJobResultOutboxByID(ctx, store, pending.ID)
	require.NoError(t, err)
	require.Equal(t, current.ClaimToken, actual.ClaimToken)
	require.NoError(t, dispatcher.processResult(ctx, current))
	claimed, err = claimResultOutbox(ctx, store, pending)
	require.NoError(t, err)
	require.False(t, claimed, "a deleted outbox cannot be resurrected by a stale scan")
}

type resultPendingQueryStore struct {
	*resultOutboxTestStore
	pendingQueries []*datastore.ListOptions
	recoveryErr    error
}

func (s *resultPendingQueryStore) List(ctx context.Context, entity datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	if outbox, ok := entity.(*model.JobResultOutbox); ok {
		if outbox.State == config.JobResultOutboxStateResultPending {
			s.pendingQueries = append(s.pendingQueries, opts)
		} else if s.recoveryErr != nil {
			return nil, s.recoveryErr
		}
	}
	return s.resultOutboxTestStore.List(ctx, entity, opts)
}

func TestResultDispatcherCursorRotatesPastRetriesAndNewArrivals(t *testing.T) {
	ctx := context.Background()
	payload, _, _, base := completedResultFixture(t)
	store := &resultPendingQueryStore{resultOutboxTestStore: base}
	add := func(id string) {
		next := *payload
		next.TaskID = id
		outbox := buildJobResultOutbox(&next, config.JobResultOutboxStateResultPending)
		outbox.ID = id
		outbox.UpdateTime = time.Now().Add(24 * time.Hour)
		if id == "c" {
			outbox.UpdateTime = time.Now().Add(-24 * time.Hour)
		}
		require.NoError(t, base.Add(ctx, outbox))
	}
	for _, id := range []string{"a", "b", "c"} {
		add(id)
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("temporary read failure")
	})
	dispatcher := NewResultDispatcher(client, store)
	slots := make(chan struct{}, 1)
	var wg sync.WaitGroup
	for _, want := range []string{"c", "b", "a"} {
		require.NoError(t, dispatcher.dispatchPendingResults(ctx, slots, &wg))
		wg.Wait()
		current, err := getJobResultOutboxByID(ctx, store, want)
		require.NoError(t, err)
		require.Equal(t, 1, current.Attempts, "retry and clock-skewed timestamps must not hide the next pending row")
		require.Equal(t, config.JobResultOutboxStateResultPending, current.State)
		if want == "c" {
			add("z")
		}
	}
	require.NoError(t, dispatcher.dispatchPendingResults(ctx, slots, &wg)) // reach the end, wrap
	require.NoError(t, dispatcher.dispatchPendingResults(ctx, slots, &wg))
	wg.Wait()
	newest, err := getJobResultOutboxByID(ctx, store, "z")
	require.NoError(t, err)
	require.Equal(t, 1, newest.Attempts, "new arrivals are visited in the next bounded pass")
	for _, query := range store.pendingQueries {
		require.Equal(t, 1, query.PageSize)
		require.Equal(t, "id", query.SortBy[0].Key)
	}
	require.Equal(t, "c", store.pendingQueries[1].LessThan[0].Value)
}

func TestResultDispatcherRunBoundsProcessingAndRecoversWhileFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, _, store := completedResultFixture(t)
	client := fake.NewSimpleClientset()
	for i := range defaultResultProcessingConcurrency + 1 {
		taskID := fmt.Sprintf("long-result-%02d", i)
		live := jobForPodFallback(taskID, nil)
		stampTestResultJob(live, taskID)
		require.NoError(t, client.Tracker().Add(live))
		payload := &JobResultPayload{TaskID: taskID, ExecutionKey: testResultExecutionKey(taskID), RunGeneration: 1, Namespace: live.Namespace, Name: live.Name, TimeoutSeconds: 120}
		record := testResultJobInfo(i+2, payload)
		record.Status = string(config.StatusDistributed)
		require.NoError(t, store.Add(ctx, record))
		_, err := createJobResultOutbox(ctx, store, payload, config.JobResultOutboxStateResultPending)
		require.NoError(t, err)
	}
	dispatcher := NewResultDispatcher(client, store)
	dispatcher.pollInterval = 5 * time.Millisecond
	dispatcher.heartbeatInterval = 5 * time.Millisecond
	done := make(chan struct{})
	go func() { dispatcher.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); requireClosed(t, done) })
	require.Eventually(t, func() bool {
		rows, err := listJobResultOutboxesByStates(ctx, store, []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessing}, 100)
		return err == nil && len(rows) == defaultResultProcessingConcurrency
	}, time.Second, time.Millisecond)
	pending, err := listJobResultOutboxesByStates(ctx, store, []config.JobResultOutboxState{config.JobResultOutboxStateResultPending}, 100)
	require.NoError(t, err)
	require.Len(t, pending, 1, "a full pool must not lease work it cannot start")
	expired := *pending[0]
	expired.ID = "abandoned-owner"
	expired.State = config.JobResultOutboxStateResultProcessing
	expired.ClaimToken = "dead-owner"
	deadline := time.Now().Add(-time.Minute)
	expired.LeaseExpiresAt = &deadline
	require.NoError(t, store.Add(ctx, &expired))
	require.Eventually(t, func() bool {
		row, err := getJobResultOutboxByID(ctx, store, expired.ID)
		return err == nil && row.State == config.JobResultOutboxStateResultPending && row.Attempts == 1
	}, time.Second, time.Millisecond)
	active, err := listJobResultOutboxesByStates(ctx, store, []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessing}, 100)
	require.NoError(t, err)
	require.Len(t, active, defaultResultProcessingConcurrency)
	first := active[0]
	require.Eventually(t, func() bool {
		row, err := getJobResultOutboxByID(ctx, store, first.ID)
		return err == nil && row.LeaseExpiresAt.After(*first.LeaseExpiresAt)
	}, time.Second, time.Millisecond, "long tasks must renew their independent processing leases")
	cancel()
	requireClosed(t, done)
	active, err = listJobResultOutboxesByStates(context.Background(), store, []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessing}, 100)
	require.NoError(t, err)
	require.Empty(t, active, "Run must join processing and release its claims before returning")
}

func TestResultDispatcherLongWaitDoesNotBlockCompletedResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payload, live, pod, store := completedResultFixture(t)
	completed, err := createJobResultOutbox(ctx, store, payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, err)
	long := jobForPodFallback("long-wait", nil)
	stampTestResultJob(long, "long-task")
	longPayload := &JobResultPayload{TaskID: "long-task", ExecutionKey: testResultExecutionKey("long-task"), RunGeneration: 1, Namespace: long.Namespace, Name: long.Name, TimeoutSeconds: 120}
	longOutbox, err := createJobResultOutbox(ctx, store, longPayload, config.JobResultOutboxStateResultPending)
	require.NoError(t, err)
	dispatcher := NewResultDispatcher(fake.NewSimpleClientset(long, live, pod), store)
	dispatcher.processingConcurrency = 2
	dispatcher.pollInterval = time.Millisecond
	done := make(chan struct{})
	go func() { dispatcher.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); requireClosed(t, done) })
	require.Eventually(t, func() bool {
		_, err := getJobResultOutboxByID(ctx, store, completed.ID)
		return errors.Is(err, datastore.ErrRecordNotExist)
	}, time.Second, time.Millisecond)
	require.Equal(t, string(config.StatusCompleted), store.jobInfoByTaskID(payload.TaskID).Status)
	waiting, err := getJobResultOutboxByID(ctx, store, longOutbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultProcessing, waiting.State)
	requireStillRunning(t, done)
}

func TestResultDispatcherRecoveryErrorDoesNotBlockPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payload, live, pod, base := completedResultFixture(t)
	store := &resultPendingQueryStore{resultOutboxTestStore: base, recoveryErr: errors.New("active-row query failed")}
	outbox, err := createJobResultOutbox(ctx, store, payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, err)
	dispatcher := NewResultDispatcher(fake.NewSimpleClientset(live, pod), store)
	dispatcher.pollInterval = time.Hour
	done := make(chan struct{})
	go func() { dispatcher.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); requireClosed(t, done) })
	require.Eventually(t, func() bool {
		_, err := getJobResultOutboxByID(ctx, store, outbox.ID)
		return errors.Is(err, datastore.ErrRecordNotExist)
	}, time.Second, time.Millisecond)
	require.Equal(t, string(config.StatusCompleted), base.jobInfoByTaskID(payload.TaskID).Status)
}

func TestResultDispatcherInvalidPersistedPayloadFailsWithoutKubernetesIO(t *testing.T) {
	ctx := context.Background()
	store := newResultOutboxTestStore()
	outbox := &model.JobResultOutbox{ID: "invalid-payload", State: config.JobResultOutboxStateResultPending}
	require.NoError(t, store.Add(ctx, outbox))
	client := fake.NewSimpleClientset()
	require.ErrorIs(t, processPendingTestResult(t, ctx, NewResultDispatcher(client, store), outbox.ID), errResultDispatchNoRetry)
	actual, err := getJobResultOutboxByID(ctx, store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateFailed, actual.State)
	require.NotEmpty(t, actual.LastError)
	require.Empty(t, client.Actions())
}

type renewingResultSnapshotStore struct {
	*resultOutboxTestStore
	deadline time.Time
}

func (s *renewingResultSnapshotStore) List(ctx context.Context, entity datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	rows, err := s.resultOutboxTestStore.List(ctx, entity, opts)
	if err == nil && len(rows) > 0 {
		current := *(rows[0].(*model.JobResultOutbox))
		current.LeaseExpiresAt = &s.deadline
		err = s.resultOutboxTestStore.Put(ctx, &current)
	}
	return rows, err
}
func TestResultRecoveryCannotReclaimLeaseRenewedAfterScan(t *testing.T) {
	ctx := context.Background()
	payload, _, _, base := completedResultFixture(t)
	outbox := buildLeasedTestResultOutbox(t, base, payload, config.JobResultOutboxStateResultProcessing)
	expired := time.Now().Add(-time.Second)
	outbox.LeaseExpiresAt = &expired
	require.NoError(t, base.Add(ctx, outbox))
	store := &renewingResultSnapshotStore{resultOutboxTestStore: base, deadline: time.Now().Add(time.Minute)}
	require.NoError(t, NewResultDispatcher(nil, store).recoverResultOutboxes(ctx))
	actual, err := getJobResultOutboxByID(ctx, store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultProcessing, actual.State)
	require.Equal(t, outbox.ClaimToken, actual.ClaimToken)
	require.Equal(t, store.deadline, *actual.LeaseExpiresAt)
}

type failingResultClaimStore struct {
	*resultOutboxTestStore
	failID string
}

func (s *failingResultClaimStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if outbox, ok := entity.(*model.JobResultOutbox); ok && outbox.ID == s.failID && updates["state"] == config.JobResultOutboxStateResultProcessing {
		return false, errors.New("claim unavailable")
	}
	return s.resultOutboxTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}
func TestResultDispatcherClaimErrorAdvancesOnlyAttemptedRow(t *testing.T) {
	ctx := context.Background()
	payload, live, pod, base := completedResultFixture(t)
	healthy := buildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	healthy.ID = "a-healthy"
	require.NoError(t, base.Add(ctx, healthy))
	failing := *healthy
	failing.ID = "z-failing"
	require.NoError(t, base.Add(ctx, &failing))
	store := &failingResultClaimStore{resultOutboxTestStore: base, failID: failing.ID}
	dispatcher := NewResultDispatcher(fake.NewSimpleClientset(live, pod), store)
	slots := make(chan struct{}, 2)
	var wg sync.WaitGroup
	require.ErrorContains(t, dispatcher.dispatchPendingResults(ctx, slots, &wg), "claim unavailable")
	require.Equal(t, failing.ID, dispatcher.pendingBeforeID)
	require.Empty(t, slots, "a failed claim must release its local processing slot")
	require.NoError(t, dispatcher.dispatchPendingResults(ctx, slots, &wg))
	wg.Wait()
	_, err := getJobResultOutboxByID(ctx, store, healthy.ID)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist, "the next scan must visit the unattempted healthy row")
	current, err := getJobResultOutboxByID(ctx, store, failing.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, current.State)
	require.Empty(t, dispatcher.pendingBeforeID, "the exhausted short batch wraps for the next scan")
}

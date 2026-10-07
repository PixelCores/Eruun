package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type failingResultCommitStore struct {
	*resultOutboxTestStore
	fail bool
}

func (s *failingResultCommitStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if _, ok := entity.(*model.JobInfo); ok && s.fail {
		return false, errors.New("result commit unavailable")
	}
	return s.resultOutboxTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}
func (s *failingResultCommitStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func completedResultFixture(t *testing.T) (*JobResultPayload, *batchv1.Job, *corev1.Pod, *resultOutboxTestStore) {
	t.Helper()
	live := jobForPodFallback("durable-result", nil)
	stampTestResultJob(live, "durable-result-task")
	live.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	pod := resultTestPod(live)
	payload := &JobResultPayload{TaskID: "durable-result-task", ExecutionKey: testResultExecutionKey("durable-result-task"), RunGeneration: 1, Name: live.Name, Namespace: live.Namespace, JobType: string(config.JobDeployInstant), TimeoutSeconds: 1}
	store := newResultOutboxTestStore()
	record := testResultJobInfo(1, payload)
	record.Status = string(config.StatusDistributed)
	require.NoError(t, store.Add(context.Background(), record))
	return payload, live, pod, store
}

func TestResultRecoveryRepublishesLostQueuedNotification(t *testing.T) {
	payload, _, _, store := completedResultFixture(t)
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "lost-message"
	outbox.UpdateTime = time.Now().Add(-time.Hour)
	expired := time.Now().Add(-time.Minute)
	outbox.LeaseExpiresAt = &expired
	require.NoError(t, store.Add(context.Background(), outbox))
	queue := &enqueueCaptureQueue{enqueueID: "recovered-message"}
	dispatcher := NewResultOutboxDispatcher(queue, fake.NewSimpleClientset(), store)
	require.NoError(t, dispatcher.processOnce(context.Background()))
	require.Len(t, queue.enqueued, 1, "a durable queued outbox must recover even when the broker has no message")
}

func TestResultRecoveryDuplicateCannotTakeActiveProcessing(t *testing.T) {
	payload, live, pod, store := completedResultFixture(t)
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultProcessingQueue)
	outbox.MessageID = "active-delivery"
	require.NoError(t, store.Add(context.Background(), outbox))
	queue := &dispatcherAckQueue{}
	dispatcher := NewResultDispatcher(queue, fake.NewSimpleClientset(live, pod), store, "results", "consumer")
	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)
	dispatcher.handleMessage(context.Background(), msg.Message{ID: outbox.MessageID, Payload: raw})
	record, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err, "redelivery must not delete an active consumer's outbox")
	require.Equal(t, config.JobResultOutboxStateResultProcessingQueue, record.State)
	require.Equal(t, string(config.StatusDistributed), store.jobInfoByTaskID(payload.TaskID).Status)
}

func TestResultRecoveryCommitFailureKeepsSuccessEvidence(t *testing.T) {
	payload, live, pod, base := completedResultFixture(t)
	store := &failingResultCommitStore{resultOutboxTestStore: base, fail: true}
	client := fake.NewSimpleClientset(live, pod)
	require.ErrorContains(t, processJobResult(context.Background(), client, store, payload), "result commit unavailable")
	_, err := client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
	require.NoError(t, err, "successful workload must remain until its result commits")
	_, err = client.CoreV1().Pods(live.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err, "logs must remain available after failed result persistence")
}

func TestResultRecoveryCleanupFailureIsRetryableWithoutLosingSuccess(t *testing.T) {
	payload, live, pod, store := completedResultFixture(t)
	client := fake.NewSimpleClientset(live, pod)
	client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("cleanup unavailable")
	})
	require.ErrorContains(t, processJobResult(context.Background(), client, store, payload), "cleanup unavailable")
	require.Equal(t, string(config.StatusCompleted), store.jobInfoByTaskID(payload.TaskID).Status, "cleanup failure must not erase a committed success")
}

func TestResultRecoveryCleanupRetriesCommittedSuccess(t *testing.T) {
	payload, live, pod, store := completedResultFixture(t)
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "first"
	require.NoError(t, store.Add(context.Background(), outbox))
	client := fake.NewSimpleClientset(live, pod)
	cleanupFails := true
	client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		if cleanupFails {
			return true, nil, errors.New("cleanup unavailable")
		}
		return false, nil, nil
	})
	queue := &dispatcherAckQueue{}
	dispatcher := NewResultDispatcher(queue, client, store, "result", "consumer")
	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)
	require.False(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: "first", Payload: raw}))
	require.Empty(t, queue.ackCalls)
	committed := store.jobInfoByTaskID(payload.TaskID)
	require.Equal(t, string(config.StatusCompleted), committed.Status)
	pending, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, string(live.UID), pending.JobUID)
	require.Equal(t, config.JobResultOutboxStateResultPending, pending.State)
	cleanupFails = false
	publisher := NewResultOutboxDispatcher(&enqueueCaptureQueue{enqueueID: "retry"}, client, store)
	require.NoError(t, publisher.dispatchPendingOutbox(context.Background(), pending))
	require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: "retry", Payload: raw}))
	require.Equal(t, committed, store.jobInfoByTaskID(payload.TaskID), "cleanup-only replay must not rewrite the terminal result")
	_, err = getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	require.Len(t, queue.ackCalls, 1)
}

func TestResultRecoveryRetainsSameNameReplacementAndLegacyUnknownUID(t *testing.T) {
	for _, knownUID := range []bool{false, true} {
		t.Run(fmt.Sprint(knownUID), func(t *testing.T) {
			payload, live, pod, store := completedResultFixture(t)
			record := store.jobInfoByTaskID(payload.TaskID)
			record.Status = string(config.StatusCompleted)
			record.Info = "committed logs"
			require.NoError(t, store.Put(context.Background(), record))
			outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
			outbox.MessageID = "cleanup"
			if knownUID {
				outbox.JobUID = string(live.UID)
			}
			require.NoError(t, store.Add(context.Background(), outbox))
			replacement := live.DeepCopy()
			replacement.UID = "replacement-uid"
			client := fake.NewSimpleClientset(replacement, pod)
			client.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
				options := action.(k8stesting.DeleteAction).GetDeleteOptions()
				require.NotNil(t, options.Preconditions)
				require.NotNil(t, options.Preconditions.UID)
				require.Equal(t, live.UID, *options.Preconditions.UID)
				return true, nil, k8serrors.NewConflict(schema.GroupResource{Group: "batch", Resource: "jobs"}, live.Name, errors.New("UID precondition failed"))
			})
			queue := &dispatcherAckQueue{}
			dispatcher := NewResultDispatcher(queue, client, store, "result", "consumer")
			raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
			require.NoError(t, err)
			require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: "cleanup", Payload: raw}))
			current, err := client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, replacement.UID, current.UID)
			require.Equal(t, "committed logs", store.jobInfoByTaskID(payload.TaskID).Info)
		})
	}
}

type uncertainResultCommitStore struct {
	*resultOutboxTestStore
	failOnce bool
}

func (s *uncertainResultCommitStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	updated, err := s.resultOutboxTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
	if _, ok := entity.(*model.JobInfo); ok && updated && err == nil && s.failOnce {
		s.failOnce = false
		return false, errors.New("commit response lost")
	}
	return updated, err
}
func (s *uncertainResultCommitStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func TestResultRecoveryUncertainCommitReplaysCleanupOnly(t *testing.T) {
	payload, live, pod, base := completedResultFixture(t)
	store := &uncertainResultCommitStore{resultOutboxTestStore: base, failOnce: true}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "first"
	require.NoError(t, store.Add(context.Background(), outbox))
	client := fake.NewSimpleClientset(live, pod)
	queue := &dispatcherAckQueue{}
	dispatcher := NewResultDispatcher(queue, client, store, "result", "consumer")
	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)
	require.False(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: "first", Payload: raw}))
	require.Empty(t, queue.ackCalls)
	_, err = client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	committed := store.jobInfoByTaskID(payload.TaskID)
	require.Equal(t, string(config.StatusCompleted), committed.Status)
	pending, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	publisher := NewResultOutboxDispatcher(&enqueueCaptureQueue{enqueueID: "retry"}, client, store)
	require.NoError(t, publisher.dispatchPendingOutbox(context.Background(), pending))
	require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: "retry", Payload: raw}))
	require.Equal(t, committed, store.jobInfoByTaskID(payload.TaskID))
}

type resultLeaseClockStore struct {
	*resultOutboxTestStore
	now      time.Time
	clockErr error
}

func (s *resultLeaseClockStore) CurrentDatabaseTime(context.Context) (time.Time, error) {
	return s.now, s.clockErr
}
func (s *resultLeaseClockStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func TestResultRecoveryLeaseRenewalAndStaleOwner(t *testing.T) {
	payload, _, _, base := completedResultFixture(t)
	store := &resultLeaseClockStore{resultOutboxTestStore: base, now: time.Now().Add(2 * time.Hour)}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "delivery"
	require.NoError(t, store.Add(context.Background(), outbox))
	claimed, err := claimResultOutbox(context.Background(), store, outbox, "delivery")
	require.NoError(t, err)
	require.True(t, claimed)
	firstLease := *outbox.LeaseExpiresAt
	store.now = store.now.Add(20 * time.Second)
	require.NoError(t, renewResultOutboxLease(context.Background(), store, outbox))
	store.now = firstLease.Add(time.Second)
	recovery := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, fake.NewSimpleClientset(), store)
	require.NoError(t, recovery.processOnce(context.Background()))
	active, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultProcessingQueue, active.State, "renewed consumer must survive a recovery scan past its original lease")
	require.Equal(t, outbox.MessageID, active.MessageID)
	store.now = active.LeaseExpiresAt.Add(time.Second)
	require.NoError(t, recovery.processOnce(context.Background()))
	require.ErrorIs(t, renewResultOutboxLease(context.Background(), store, outbox), errResultOutboxOwnershipLost)
	called := false
	require.ErrorIs(t, withResultOutboxOwnership(context.Background(), store, outbox, func(datastore.DataStore, *model.JobResultOutbox) error { called = true; return nil }), errResultOutboxOwnershipLost)
	require.False(t, called, "a recovered consumer cannot write results or delete the new outbox")
}

func TestResultRecoveryLegacyRowsReceiveBoundedGrace(t *testing.T) {
	for _, state := range []config.JobResultOutboxState{config.JobResultOutboxStateResultQueued, config.JobResultOutboxStateResultProcessingQueue} {
		t.Run(string(state), func(t *testing.T) {
			payload, _, _, base := completedResultFixture(t)
			store := &resultLeaseClockStore{resultOutboxTestStore: base, now: time.Now().UTC()}
			outbox := buildJobResultOutbox(payload, state)
			outbox.MessageID = "legacy"
			outbox.UpdateTime = store.now.Add(-24 * time.Hour)
			require.NoError(t, store.Add(context.Background(), outbox))
			queue := &enqueueCaptureQueue{}
			recovery := NewResultOutboxDispatcher(queue, fake.NewSimpleClientset(), store)
			require.NoError(t, recovery.processOnce(context.Background()))
			require.Empty(t, queue.enqueued)
			preserved, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
			require.NoError(t, err)
			require.Equal(t, state, preserved.State)
			require.NotNil(t, preserved.LeaseExpiresAt)
			require.True(t, preserved.LeaseExpiresAt.After(store.now))
			store.now = preserved.LeaseExpiresAt.Add(time.Second)
			require.NoError(t, recovery.processOnce(context.Background()))
			require.Len(t, queue.enqueued, 1)
		})
	}
}

func TestResultRecoveryHeartbeatFailureCancelsBeforeResultWrite(t *testing.T) {
	payload, live, _, base := completedResultFixture(t)
	live.Status.Conditions = nil
	store := &resultLeaseClockStore{resultOutboxTestStore: base, now: time.Now().UTC()}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "delivery"
	require.NoError(t, store.Add(context.Background(), outbox))
	claimed, err := claimResultOutbox(context.Background(), store, outbox, "delivery")
	require.NoError(t, err)
	require.True(t, claimed)
	outbox.JobUID = string(live.UID)
	current, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	current.JobUID = string(live.UID)
	require.NoError(t, store.Put(context.Background(), current))
	store.clockErr = errors.New("heartbeat database unavailable")
	dispatcher := NewResultDispatcher(&dispatcherAckQueue{}, fake.NewSimpleClientset(live), store, "result", "consumer")
	dispatcher.heartbeatInterval = time.Millisecond
	started := time.Now()
	err = dispatcher.processOwnedResult(context.Background(), outbox, payload)
	require.ErrorContains(t, err, "heartbeat database unavailable")
	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, string(config.StatusDistributed), store.jobInfoByTaskID(payload.TaskID).Status)
}

func TestResultRecoveryExpiredRowsAreNotBlockedByActiveBatch(t *testing.T) {
	payload, _, _, base := completedResultFixture(t)
	now := time.Now().UTC()
	store := &resultLeaseClockStore{resultOutboxTestStore: base, now: now}
	for i := 0; i < resultOutboxBatchSize+1; i++ {
		next := *payload
		next.TaskID = fmt.Sprintf("mixed-result-%d", i)
		outbox := buildJobResultOutbox(&next, config.JobResultOutboxStateResultProcessingQueue)
		outbox.MessageID = fmt.Sprintf("owner-%d", i)
		deadline := now.Add(time.Hour)
		outbox.LeaseExpiresAt = &deadline
		outbox.UpdateTime = now.Add(-time.Hour)
		if i == resultOutboxBatchSize {
			deadline = now.Add(-time.Second)
			outbox.UpdateTime = now
		}
		require.NoError(t, store.Add(context.Background(), outbox))
	}
	dispatcher := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, fake.NewSimpleClientset(), store)
	require.NoError(t, dispatcher.recoverResultOutboxes(context.Background(), []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessingQueue}))
	next := *payload
	next.TaskID = fmt.Sprintf("mixed-result-%d", resultOutboxBatchSize)
	expired, err := getJobResultOutboxByPayload(context.Background(), store, &next)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, expired.State, "a full batch of active leases must not hide a newer expired lease")
}

func TestResultRecoveryDetectsUIDReplacementAfterCompletion(t *testing.T) {
	payload, live, pod, store := completedResultFixture(t)
	client := fake.NewSimpleClientset(live, pod)
	calls := 0
	replacement := live.DeepCopy()
	replacement.UID = "replacement-after-completion"
	client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls >= 3 {
			return true, replacement, nil
		}
		return false, nil, nil
	})
	require.NoError(t, processJobResult(context.Background(), client, store, payload))
	require.Equal(t, string(config.StatusDistributed), store.jobInfoByTaskID(payload.TaskID).Status)
	for _, action := range client.Actions() {
		require.NotEqual(t, "delete", action.GetVerb(), "same annotations do not authorize deleting a different UID")
	}
}

func TestResultRecoveryLogFailureRetainsEvidenceAndRetries(t *testing.T) {
	payload, live, pod, store := completedResultFixture(t)
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "first"
	require.NoError(t, store.Add(context.Background(), outbox))
	client := fake.NewSimpleClientset(live, pod)
	failLogs := true
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failLogs {
			return true, nil, errors.New("pod logs unavailable")
		}
		return false, nil, nil
	})
	queue := &dispatcherAckQueue{}
	dispatcher := NewResultDispatcher(queue, client, store, "result", "consumer")
	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)
	require.False(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: "first", Payload: raw}))
	require.Empty(t, queue.ackCalls)
	require.Equal(t, string(config.StatusDistributed), store.jobInfoByTaskID(payload.TaskID).Status)
	_, err = client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	failLogs = false
	pending, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	publisher := NewResultOutboxDispatcher(&enqueueCaptureQueue{enqueueID: "retry"}, client, store)
	require.NoError(t, publisher.dispatchPendingOutbox(context.Background(), pending))
	require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: "retry", Payload: raw}))
	require.Equal(t, string(config.StatusCompleted), store.jobInfoByTaskID(payload.TaskID).Status)
	require.NotEmpty(t, store.jobInfoByTaskID(payload.TaskID).Info)
}

func TestResultRecoveryTransientReadFailureDoesNotSettleResult(t *testing.T) {
	for _, failedRead := range []int{2, 3} {
		t.Run(fmt.Sprint(failedRead), func(t *testing.T) {
			payload, live, pod, store := completedResultFixture(t)
			client := fake.NewSimpleClientset(live, pod)
			calls := 0
			client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
				calls++
				if calls == failedRead {
					return true, nil, errors.New("temporary API failure")
				}
				return false, nil, nil
			})
			require.ErrorContains(t, processJobResult(context.Background(), client, store, payload), "temporary API failure")
			require.Equal(t, string(config.StatusDistributed), store.jobInfoByTaskID(payload.TaskID).Status)
			require.NoError(t, processJobResult(context.Background(), client, store, payload))
			require.Equal(t, string(config.StatusCompleted), store.jobInfoByTaskID(payload.TaskID).Status)
		})
	}
}

func TestResultRecoveryMissingJobAfterCompletionKeepsObservedUID(t *testing.T) {
	payload, live, pod, store := completedResultFixture(t)
	replacement := live.DeepCopy()
	replacement.UID = "replacement-while-cleaning"
	client := fake.NewSimpleClientset(replacement, pod)
	calls := 0
	client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		switch calls {
		case 1, 2:
			return true, live, nil
		case 3:
			return true, nil, k8serrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, live.Name)
		}
		return false, nil, nil
	})
	client.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		options := action.(k8stesting.DeleteAction).GetDeleteOptions()
		require.NotNil(t, options.Preconditions)
		require.Equal(t, live.UID, *options.Preconditions.UID)
		return true, nil, k8serrors.NewConflict(schema.GroupResource{Group: "batch", Resource: "jobs"}, live.Name, errors.New("UID precondition failed"))
	})
	require.NoError(t, processJobResult(context.Background(), client, store, payload))
	current, err := client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, replacement.UID, current.UID)
	require.Equal(t, string(config.StatusCompleted), store.jobInfoByTaskID(payload.TaskID).Status)
}

type resultInterleavingQueue struct {
	enqueueCaptureQueue
	afterEnqueue func()
}

func (q *resultInterleavingQueue) Enqueue(ctx context.Context, raw []byte) (string, error) {
	id, err := q.enqueueCaptureQueue.Enqueue(ctx, raw)
	if err == nil && q.afterEnqueue != nil {
		q.afterEnqueue()
	}
	return id, err
}

func TestResultRecoveryProducerCannotOverwriteEarlyConsumer(t *testing.T) {
	payload, _, _, store := completedResultFixture(t)
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))
	queue := &resultInterleavingQueue{enqueueCaptureQueue: enqueueCaptureQueue{enqueueID: "delivered-before-confirmation"}}
	owner := ""
	queue.afterEnqueue = func() {
		current, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
		require.NoError(t, err)
		claimed, err := claimResultOutbox(context.Background(), store, current, queue.enqueueID)
		require.NoError(t, err)
		require.True(t, claimed)
		owner = current.MessageID
	}
	dispatcher := NewResultOutboxDispatcher(queue, fake.NewSimpleClientset(), store)
	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), outbox))
	current, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultProcessingQueue, current.State)
	require.Equal(t, owner, current.MessageID)
	require.NotEqual(t, queue.enqueueID, current.MessageID)
}

func TestResultRecoveryOldDeliveryCannotInvalidateNewNotification(t *testing.T) {
	payload, live, pod, store := completedResultFixture(t)
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "current-delivery"
	require.NoError(t, store.Add(context.Background(), outbox))
	queue := &dispatcherAckQueue{}
	dispatcher := NewResultDispatcher(queue, fake.NewSimpleClientset(live, pod), store, "result", "consumer")
	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)
	require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: "old-delivery", Payload: raw}))
	current, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, "current-delivery", current.MessageID)
	require.Equal(t, config.JobResultOutboxStateResultQueued, current.State)
	require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: "current-delivery", Payload: raw}))
	require.Equal(t, string(config.StatusCompleted), store.jobInfoByTaskID(payload.TaskID).Status)
}

func TestResultRecoveryMissingResultRowCannotAuthorizeCleanup(t *testing.T) {
	payload, live, pod, store := completedResultFixture(t)
	store.jobInfos = make(map[int]*model.JobInfo)
	client := fake.NewSimpleClientset(live, pod)
	require.ErrorContains(t, processJobResult(context.Background(), client, store, payload), "result status was not persisted")
	_, err := client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestResultRecoveryDistinguishesJobTimeoutFromConsumerCancellation(t *testing.T) {
	for _, cancelConsumer := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelConsumer), func(t *testing.T) {
			payload, live, _, store := completedResultFixture(t)
			live.Status.Conditions = nil
			client := fake.NewSimpleClientset(live)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelConsumer {
				calls := 0
				client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
					calls++
					if calls == 2 {
						cancel()
					}
					return false, nil, nil
				})
			}
			err := processJobResult(ctx, client, store, payload)
			if cancelConsumer {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, string(config.StatusDistributed), store.jobInfoByTaskID(payload.TaskID).Status)
			} else {
				require.NoError(t, err)
				require.Equal(t, string(config.StatusTimeout), store.jobInfoByTaskID(payload.TaskID).Status)
			}
		})
	}
}

type settlingResultCommitStore struct {
	*resultOutboxTestStore
	settle func()
}

func (s *settlingResultCommitStore) beforeResultWrite(entity datastore.Entity) {
	if _, ok := entity.(*model.JobInfo); ok && s.settle != nil {
		settle := s.settle
		s.settle = nil
		settle()
	}
}
func (s *settlingResultCommitStore) Put(ctx context.Context, entity datastore.Entity) error {
	s.beforeResultWrite(entity)
	return s.resultOutboxTestStore.Put(ctx, entity)
}
func (s *settlingResultCommitStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	s.beforeResultWrite(entity)
	return s.resultOutboxTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}
func (s *settlingResultCommitStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func TestResultRecoveryPreservesConcurrentTerminalResult(t *testing.T) {
	for _, terminal := range []config.Status{config.StatusCancelled, config.StatusFailed, config.StatusTimeout, config.StatusReject, config.StatusCompleted, config.StatusPassed, config.StatusSkipped} {
		for _, window := range []string{"during logs", "immediately before write"} {
			t.Run(string(terminal)+"/"+window, func(t *testing.T) {
				payload, live, pod, base := completedResultFixture(t)
				store := &settlingResultCommitStore{resultOutboxTestStore: base}
				settled := base.jobInfoByTaskID(payload.TaskID)
				settled.Status = string(terminal)
				settled.Error = "previously committed terminal reason"
				settled.EndTime = 123
				settled.Info = "previously committed evidence"
				settle := func() { require.NoError(t, base.Put(context.Background(), settled)) }
				outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
				outbox.MessageID = "result-delivery"
				require.NoError(t, store.Add(context.Background(), outbox))
				client := fake.NewSimpleClientset(live, pod)
				if window == "during logs" {
					client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
						if settle != nil {
							settle()
							settle = nil
						}
						return false, nil, nil
					})
				} else {
					store.settle = settle
				}
				queue := &dispatcherAckQueue{}
				dispatcher := NewResultDispatcher(queue, client, store, "result", "consumer")
				raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
				require.NoError(t, err)
				require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{ID: outbox.MessageID, Payload: raw}), "a settled result should finish without an endless retry")
				actual := base.jobInfoByTaskID(payload.TaskID)
				require.Equal(t, settled.Status, actual.Status)
				require.Equal(t, settled.Error, actual.Error)
				require.Equal(t, settled.Info, actual.Info)
				require.Equal(t, settled.EndTime, actual.EndTime)
				if terminal == config.StatusCompleted {
					_, err := client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
					require.True(t, k8serrors.IsNotFound(err), "the committed Completed result authorizes cleanup")
				} else {
					for _, action := range client.Actions() {
						require.NotEqual(t, "delete", action.GetVerb(), "only the final committed Completed result can authorize cleanup")
					}
				}
				require.Len(t, queue.ackCalls, 1)
			})
		}
	}
}

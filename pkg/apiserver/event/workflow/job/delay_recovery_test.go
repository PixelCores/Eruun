package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

func seedDelayRecoveryCheckpoint(t *testing.T, store *resultOutboxTestStore, id int, executeAt int64) *DelayJobPayload {
	t.Helper()
	name := fmt.Sprintf("delay-recovery-%d", id)
	payload := &DelayJobPayload{
		ExecuteAt: executeAt, Namespace: "default", JobType: string(config.JobDeployInstant),
		TaskID: name, ExecutionKey: name, RunGeneration: 1, ServiceName: name,
		Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}},
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	store.jobInfos[id] = &model.JobInfo{
		ID: id, Type: payload.JobType, TaskID: payload.TaskID, ServiceName: name,
		Status: string(config.StatusDistributed), ExecutionKey: &payload.ExecutionKey, RunGeneration: 1,
		DelayState: config.JobDelayStatePending, DelayExecuteAt: executeAt, DelayPayload: string(raw),
	}
	return payload
}

func TestDelayDispatcherRecoveryAdvancesPastFirstBatch(t *testing.T) {
	for _, firstBatchState := range []string{"retrying", "dispatched", "invalid_payload"} {
		t.Run(firstBatchState, func(t *testing.T) {
			ctx := context.Background()
			store := newResultOutboxTestStore()
			now := time.Now().Unix()
			healthy := seedDelayRecoveryCheckpoint(t, store, 1, now-30)
			for id := 2; id <= delayRecoveryBatchSize+1; id++ {
				seedDelayRecoveryCheckpoint(t, store, id, now-60)
				if firstBatchState == "invalid_payload" {
					store.jobInfos[id].DelayPayload = "{"
				}
			}
			client := fake.NewSimpleClientset()
			dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
			err := dispatcher.recoverDueCheckpoints(ctx)
			if firstBatchState == "invalid_payload" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, dispatcher.items, delayRecoveryBatchSize)
				for _, item := range dispatcher.items {
					item.executeAt = now + 60
				}
				if firstBatchState == "dispatched" {
					for id := 2; id <= delayRecoveryBatchSize+1; id++ {
						store.jobInfos[id].DelayState = config.JobDelayStateDispatched
					}
				}
			}
			// A new higher ID must not restart a scan before it reaches the older job.
			newArrival := seedDelayRecoveryCheckpoint(t, store, delayRecoveryBatchSize+2, now-15)
			require.NoError(t, dispatcher.recoverDueCheckpoints(ctx))
			item, wait := dispatcher.nextItem()
			require.NotNil(t, item)
			require.Zero(t, wait)
			require.Equal(t, healthy.ExecutionKey, item.payload.ExecutionKey)
			require.NoError(t, dispatcher.dispatchJob(ctx, item, client))
			dispatcher.finish(ctx, item)
			require.Equal(t, config.JobDelayStateDispatched, store.jobInfos[1].DelayState)

			// After reaching the end, a new pass must pick up arrivals above the cursor.
			err = dispatcher.recoverDueCheckpoints(ctx)
			if firstBatchState == "invalid_payload" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			item, wait = dispatcher.nextItem()
			require.NotNil(t, item)
			require.Zero(t, wait)
			require.Equal(t, newArrival.ExecutionKey, item.payload.ExecutionKey)
		})
	}
}

type delayMessageLifecycleQueue struct {
	dispatcherAckQueue
	inFlight map[string]bool
}

func (q *delayMessageLifecycleQueue) MarkMessageHandlingStart(id string) { q.inFlight[id] = true }
func (q *delayMessageLifecycleQueue) MarkMessageHandlingDone(id string, _ bool) {
	q.inFlight[id] = false
}

func TestDelayDispatcherDeduplicatedMessagesRemainRetryable(t *testing.T) {
	for _, source := range []string{"database", "queue"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			store := newResultOutboxTestStore()
			payload := seedDelayRecoveryCheckpoint(t, store, 1, time.Now().Add(-time.Minute).Unix())
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			queue := &delayMessageLifecycleQueue{inFlight: make(map[string]bool)}
			client := fake.NewSimpleClientset()
			createErr := errors.New("kubernetes unavailable")
			createAttempts := 0
			client.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
				createAttempts++
				if createAttempts == 1 {
					return true, nil, createErr
				}
				return false, nil, nil
			})
			dispatcher := NewDelayDispatcher(queue, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
			if source == "database" {
				require.NoError(t, dispatcher.recoverDueCheckpoints(ctx))
			} else {
				msg.MarkMessageHandlingStart(queue, "original")
				dispatcher.handleMessage(ctx, msg.Message{ID: "original", Payload: raw})
			}
			duplicate := msg.Message{ID: "duplicate", Payload: raw}
			msg.MarkMessageHandlingStart(queue, duplicate.ID)
			dispatcher.handleMessage(ctx, duplicate)
			require.False(t, queue.inFlight[duplicate.ID], "Kafka must be able to reclaim the deduplicated message")
			require.Empty(t, queue.ackCalls, "deduplication must not acknowledge uncompleted work")
			require.Len(t, dispatcher.items, 1)

			item, _ := dispatcher.nextItem()
			require.ErrorIs(t, dispatcher.dispatchJob(ctx, item, client), createErr)
			dispatcher.requeue(item)
			// Reclaiming while dispatch is retrying must remain safe and retryable.
			msg.MarkMessageHandlingStart(queue, duplicate.ID)
			dispatcher.handleMessage(ctx, duplicate)
			require.False(t, queue.inFlight[duplicate.ID])
			require.Empty(t, queue.ackCalls)
			require.Len(t, dispatcher.items, 1)
			item, _ = dispatcher.nextItem()
			require.NoError(t, dispatcher.dispatchJob(ctx, item, client))
			dispatcher.finish(ctx, item)

			// Once dispatch finishes, reclaiming the notification must ACK it without another Job create.
			msg.MarkMessageHandlingStart(queue, duplicate.ID)
			dispatcher.handleMessage(ctx, duplicate)
			item, _ = dispatcher.nextItem()
			require.NotNil(t, item)
			require.NoError(t, dispatcher.dispatchJob(ctx, item, client))
			dispatcher.finish(ctx, item)
			require.False(t, queue.inFlight[duplicate.ID])
			require.Equal(t, []string{duplicate.ID}, queue.ackCalls[len(queue.ackCalls)-1].ids)
			require.Equal(t, 2, createAttempts)
			require.Len(t, store.outboxes, 1)
			require.Empty(t, dispatcher.pending)
		})
	}
}

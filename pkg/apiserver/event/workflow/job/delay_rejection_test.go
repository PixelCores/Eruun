package job

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

type checkpointAckQueue struct {
	dispatcherAckQueue
	failures int
	calls    int
	acked    chan string
}

func (q *checkpointAckQueue) Ack(_ context.Context, _ string, ids ...string) error {
	q.calls++
	if q.failures > 0 {
		q.failures--
		return errors.New("ack unavailable")
	}
	q.acked <- ids[0]
	return nil
}
func runDelayedScheduleUntilAck(t *testing.T, d *DelayDispatcher, p *DelayJobPayload) *checkpointAckQueue {
	t.Helper()
	q, ok := d.queue.(*checkpointAckQueue)
	if !ok {
		q = &checkpointAckQueue{acked: make(chan string, 1)}
		d.queue = q
	}
	d.backoffMin, d.backoffMax = time.Millisecond, time.Millisecond
	require.True(t, d.addPending(&delayItem{payload: p, key: p.ExecutionKey, msgID: "delivery", executeAt: time.Now().Unix() - 1}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); d.scheduleLoop(ctx) }()
	select {
	case <-q.acked:
	case <-time.After(3 * time.Second):
		cancel()
		<-done
		t.Fatal("schedule did not acknowledge the delivery")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("schedule did not stop")
	}
	return q
}
func changeDelayedNamespaceOwner(t *testing.T, m *workspace.Manager) {
	t.Helper()
	_, err := m.Client.CoreV1().Namespaces().Update(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "namespace-1", Labels: map[string]string{workspace.OwnerLabel: "space-2"}}}, metav1.UpdateOptions{})
	require.NoError(t, err)
	m.Client.(*fake.Clientset).ClearActions()
}

func TestRejectedDelayedCheckpointReachesTerminalState(t *testing.T) {
	tests := []struct {
		name   string
		change func(*delayedWorkspaceStore, *workspace.Manager)
	}{
		{"namespace ownership changed", func(_ *delayedWorkspaceStore, m *workspace.Manager) { changeDelayedNamespaceOwner(t, m) }},
		{"application namespace changed", func(s *delayedWorkspaceStore, _ *workspace.Manager) { s.apps["app-1"].Namespace = "namespace-2" }},
		{"application removed", func(s *delayedWorkspaceStore, _ *workspace.Manager) { delete(s.apps, "app-1") }},
		{"workspace removed", func(s *delayedWorkspaceStore, _ *workspace.Manager) { delete(s.spaces, "space-1") }},
		{"application ownership missing", func(s *delayedWorkspaceStore, _ *workspace.Manager) { s.jobInfos[1].AppID = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, manager, payloads := delayedWorkspaceFixture(t)
			delete(store.jobInfos, 2)
			tt.change(store, manager)
			dispatcher := NewDelayDispatcher(nil, manager, access.NewStore(store), "", "")
			runDelayedScheduleUntilAck(t, dispatcher, payloads[0])
			record := *store.jobInfos[1]
			require.Equal(t, string(config.StatusFailed), record.Status)
			require.Equal(t, config.JobDelayStatePending, record.DelayState, "a rejected Job was never dispatched")
			require.NotEmpty(t, record.Error)
			require.Positive(t, record.EndTime)
			require.Empty(t, store.outboxes)
			require.NoError(t, dispatcher.recoverDueCheckpoints(context.Background()))
			require.Empty(t, dispatcher.items)
			runDelayedScheduleUntilAck(t, dispatcher, payloads[0])
			require.Equal(t, record, *store.jobInfos[1], "duplicate delivery must not modify the terminal checkpoint")
			for _, action := range manager.Client.(*fake.Clientset).Actions() {
				require.Equal(t, "get", action.GetVerb())
			}
		})
	}
}

func TestRejectedPersistedWorkloadReachesTerminalState(t *testing.T) {
	store, manager, payloads := delayedWorkspaceFixture(t)
	payloads[0].Job.Spec.Template.Spec.InitContainers[0].SecurityContext = &corev1.SecurityContext{Privileged: ptr.To(true)}
	raw, err := json.Marshal(payloads[0])
	require.NoError(t, err)
	store.jobInfos[1].DelayPayload = string(raw)
	dispatcher := NewDelayDispatcher(nil, manager, access.NewStore(store), "", "")
	runDelayedScheduleUntilAck(t, dispatcher, payloads[0])
	require.Equal(t, string(config.StatusFailed), store.jobInfos[1].Status)
	require.Equal(t, config.JobDelayStatePending, store.jobInfos[1].DelayState)
	require.Empty(t, manager.Client.(*fake.Clientset).Actions())
	require.Empty(t, store.outboxes)
}

func TestUntrustedDelayedNotificationDoesNotSettleCheckpoint(t *testing.T) {
	store, manager, payloads := delayedWorkspaceFixture(t)
	delete(store.jobInfos, 2)
	payloads[0].Namespace = "namespace-2"
	before := *store.jobInfos[1]
	dispatcher := NewDelayDispatcher(nil, manager, access.NewStore(store), "", "")
	runDelayedScheduleUntilAck(t, dispatcher, payloads[0])
	require.Equal(t, before, *store.jobInfos[1])
	require.Empty(t, store.outboxes)
	require.Empty(t, manager.Client.(*fake.Clientset).Actions())
	require.NoError(t, dispatcher.recoverDueCheckpoints(context.Background()))
	item, _ := dispatcher.nextItem()
	require.NotNil(t, item)
	require.Equal(t, "namespace-1", item.payload.Namespace, "the original checkpoint remains recoverable")
}

type rejectTransitionStore struct {
	*delayedWorkspaceStore
	beforeReject func() (bool, error)
}

func (s *rejectTransitionStore) CompareAndSwapWithConditions(ctx context.Context, e datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if updates["status"] == string(config.StatusFailed) && s.beforeReject != nil {
		proceed, err := s.beforeReject()
		if !proceed || err != nil {
			return false, err
		}
	}
	return s.delayedWorkspaceStore.CompareAndSwapWithConditions(ctx, e, conditions, updates)
}

func TestDelayedRejectionRetriesFailedPersistenceAndAck(t *testing.T) {
	for _, failure := range []string{"database", "ack"} {
		t.Run(failure, func(t *testing.T) {
			base, manager, payloads := delayedWorkspaceFixture(t)
			changeDelayedNamespaceOwner(t, manager)
			store := &rejectTransitionStore{delayedWorkspaceStore: base}
			attempts := 0
			store.beforeReject = func() (bool, error) {
				attempts++
				if failure == "database" && attempts == 1 {
					return false, errors.New("database unavailable")
				}
				return true, nil
			}
			queue := &checkpointAckQueue{acked: make(chan string, 1)}
			if failure == "ack" {
				queue.failures = 1
			}
			dispatcher := NewDelayDispatcher(queue, manager, access.NewStore(store), "", "")
			runDelayedScheduleUntilAck(t, dispatcher, payloads[0])
			require.Equal(t, string(config.StatusFailed), store.jobInfos[1].Status)
			require.Equal(t, config.JobDelayStatePending, store.jobInfos[1].DelayState)
			if failure == "database" {
				require.Equal(t, 2, attempts)
				require.Equal(t, 1, queue.calls)
			} else {
				require.Equal(t, 1, attempts)
				require.Equal(t, 2, queue.calls)
			}
			require.Empty(t, store.outboxes)
		})
	}
}

func TestDelayedRejectionPreservesConcurrentTransitions(t *testing.T) {
	for _, transition := range []string{"completed", "dispatched", "new generation"} {
		t.Run(transition, func(t *testing.T) {
			base, manager, payloads := delayedWorkspaceFixture(t)
			changeDelayedNamespaceOwner(t, manager)
			store := &rejectTransitionStore{delayedWorkspaceStore: base}
			var expected model.JobInfo
			store.beforeReject = func() (bool, error) {
				base.mu.Lock()
				defer base.mu.Unlock()
				record := base.jobInfos[1]
				switch transition {
				case "completed":
					record.Status = string(config.StatusCompleted)
				case "dispatched":
					record.DelayState = config.JobDelayStateDispatched
				case "new generation":
					record.RunGeneration++
				}
				expected = *record
				// The conditional write must reject its now-stale snapshot.
				return true, nil
			}
			dispatcher := NewDelayDispatcher(nil, manager, access.NewStore(store), "", "")
			runDelayedScheduleUntilAck(t, dispatcher, payloads[0])
			require.Equal(t, expected, *store.jobInfos[1])
		})
	}
}

func TestDelayedRejectionDoesNotFailPendingResult(t *testing.T) {
	store, manager, payloads := delayedWorkspaceFixture(t)
	changeDelayedNamespaceOwner(t, manager)
	result := newJobResultPayloadFromDelay(payloads[0], payloads[0].Job)
	require.NoError(t, store.Add(context.Background(), buildJobResultOutbox(result, config.JobResultOutboxStateResultPending)))
	dispatcher := NewDelayDispatcher(nil, manager, access.NewStore(store), "", "")
	runDelayedScheduleUntilAck(t, dispatcher, payloads[0])
	require.Equal(t, string(config.StatusDistributed), store.jobInfos[1].Status)
	require.Equal(t, config.JobDelayStateDispatched, store.jobInfos[1].DelayState)
	require.Empty(t, manager.Client.(*fake.Clientset).Actions(), "result processing already owns the created Job")
}

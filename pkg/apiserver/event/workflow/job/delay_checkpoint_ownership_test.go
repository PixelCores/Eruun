package job

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type checkpointOnlyDelayStore struct {
	*resultOutboxTestStore
	parentReads     int
	checkpointReads int
	failAt          int
	settleAt        int
}

func (s *checkpointOnlyDelayStore) Get(ctx context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.parentReads++
		return errors.New("parent workflow lookup must not authorize delayed execution")
	}
	return s.resultOutboxTestStore.Get(ctx, entity)
}
func (s *checkpointOnlyDelayStore) List(ctx context.Context, entity datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	if _, ok := entity.(*model.JobInfo); !ok {
		return s.resultOutboxTestStore.List(ctx, entity, opts)
	}
	s.checkpointReads++
	if s.checkpointReads == s.failAt {
		return nil, errors.New("checkpoint database unavailable")
	}
	records, err := s.resultOutboxTestStore.List(ctx, entity, opts)
	if s.checkpointReads == s.settleAt {
		s.mu.Lock()
		s.jobInfos[1].Status = string(config.StatusCancelled)
		s.mu.Unlock()
	}
	return records, err
}

func TestDelayDispatcherRequiresCommittedCheckpoint(t *testing.T) {
	for _, state := range []string{"missing", "wrong execution", "wrong generation", string(config.StatusWaiting), string(config.StatusQueued), string(config.StatusRunning), string(config.StatusCancelled), string(config.StatusCompleted)} {
		for _, entry := range []string{"notification", "prepared workload"} {
			t.Run(state+"/"+entry, func(t *testing.T) {
				store := &checkpointOnlyDelayStore{resultOutboxTestStore: newResultOutboxTestStore()}
				payload := seedDelayRecoveryCheckpoint(t, store.resultOutboxTestStore, 1, time.Now().Add(-time.Minute).Unix())
				notification := *payload
				payload = &notification
				switch state {
				case "missing":
					delete(store.jobInfos, 1)
				case "wrong execution":
					payload.ExecutionKey = "unknown-execution"
				case "wrong generation":
					payload.RunGeneration++
				default:
					store.jobInfos[1].Status = state
				}
				client := fake.NewSimpleClientset()
				dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
				var err error
				if entry == "notification" {
					err = dispatcher.dispatch(context.Background(), &delayItem{payload: payload})
				} else {
					err = dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client)
				}
				require.NoError(t, err, "uncommitted or obsolete work must be discarded without consulting parent ownership")
				require.Zero(t, store.parentReads)
				require.Empty(t, client.Actions())
				require.Empty(t, store.outboxes)
			})
		}
	}
}

func TestDelayDispatcherRechecksCheckpointBeforeAdmission(t *testing.T) {
	for _, failure := range []string{"database", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			store := &checkpointOnlyDelayStore{resultOutboxTestStore: newResultOutboxTestStore()}
			payload := seedDelayRecoveryCheckpoint(t, store.resultOutboxTestStore, 1, time.Now().Add(-time.Minute).Unix())
			if failure == "database" {
				store.failAt = 2
			} else {
				store.settleAt = 1
			}
			client := fake.NewSimpleClientset()
			dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
			err := dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client)
			if failure == "database" {
				require.ErrorContains(t, err, "checkpoint database unavailable")
				require.NotErrorIs(t, err, errDelayDispatchNoRetry, "checkpoint read errors must remain retryable")
				require.Equal(t, string(config.StatusDistributed), store.jobInfos[1].Status)
			} else {
				require.NoError(t, err)
				require.Equal(t, string(config.StatusCancelled), store.jobInfos[1].Status)
			}
			require.Equal(t, 2, store.checkpointReads)
			require.Zero(t, store.parentReads)
			require.Empty(t, client.Actions(), "the repeated checkpoint check must precede Kubernetes effects")
			require.Empty(t, store.outboxes)
		})
	}
}

func TestPersistDelayCheckpointStillRequiresParentOwnership(t *testing.T) {
	for _, ownership := range []string{"current", "stale generation", "stale token", "stale worker"} {
		t.Run(ownership, func(t *testing.T) {
			parent := &model.WorkflowQueue{TaskID: "delayed-task", Status: config.StatusRunning, RunGeneration: 2, RunToken: "current-token", WorkerID: "current-worker"}
			store := &workflowOwnedJobInfoStore{workflowTask: parent}
			task := &model.JobTask{TaskID: parent.TaskID, Name: "delayed-job", ExecutionKey: "delayed-execution", RunGeneration: parent.RunGeneration, RunToken: parent.RunToken, WorkerID: parent.WorkerID}
			switch ownership {
			case "stale generation":
				task.RunGeneration--
			case "stale token":
				task.RunToken = "previous-token"
			case "stale worker":
				task.WorkerID = "previous-worker"
			}
			payload := &DelayJobPayload{TaskID: task.TaskID, ExecutionKey: task.ExecutionKey, RunGeneration: task.RunGeneration, ExecuteAt: time.Now().Add(time.Hour).Unix(), Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: task.Name, Namespace: "default"}}}
			err := persistDelayJobCheckpoint(context.Background(), store, task, payload)
			require.Equal(t, 1, store.transactionCalls)
			if ownership != "current" {
				require.ErrorIs(t, err, repository.ErrWorkflowOwnershipLost)
				require.ErrorIs(t, err, signal.ErrInfrastructureStop)
				require.Nil(t, store.addedJobInfo, "removing the notification token must not authorize stale checkpoint writes")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, store.addedJobInfo)
			require.Equal(t, string(config.StatusDistributed), store.addedJobInfo.Status)
			require.Equal(t, config.JobDelayStatePending, store.addedJobInfo.DelayState)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(store.addedJobInfo.DelayPayload), &fields))
			require.NotContains(t, fields, "runToken")
			require.Equal(t, parent.RunToken, task.RunToken, "the current Worker still owns the checkpoint transaction")
		})
	}
}

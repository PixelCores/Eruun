package jobresult

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestUpdateJobInfoStatusUsesResultOutboxFence(t *testing.T) {
	for _, claimState := range []string{"current", "stale token", "expired lease", "missing lease", "missing claim"} {
		t.Run(claimState, func(t *testing.T) {
			ctx := context.Background()
			store := &resultLeaseClockStore{resultOutboxTestStore: newResultOutboxTestStore(), now: time.Now().UTC()}
			payload := &JobResultPayload{TaskID: "task-result", Name: "job-svc-a", Namespace: "default", JobType: string(config.JobDeployScheduled), ServiceName: "svc-a", ExecutionKey: "execution-current", RunGeneration: 2}
			for id := 1; id <= 3; id++ {
				record := testResultJobInfo(id, payload)
				record.Status = string(config.StatusDistributed)
				if id == 1 {
					record.RunGeneration = 1
				} else if id == 2 {
					otherKey := "execution-other"
					record.ExecutionKey = &otherKey
				}
				require.NoError(t, store.Add(ctx, record))
			}
			outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultQueued)
			outbox.MessageID = "delivery"
			require.NoError(t, store.Add(ctx, outbox))
			claimed, err := claimResultOutbox(ctx, store, outbox, "delivery")
			require.NoError(t, err)
			require.True(t, claimed)
			switch claimState {
			case "stale token":
				outbox.MessageID = "stale-processing-owner"
			case "expired lease":
				store.now = outbox.LeaseExpiresAt.Add(time.Second)
			case "missing lease":
				outbox.LeaseExpiresAt = nil
				require.NoError(t, store.Put(ctx, outbox))
			case "missing claim":
				outbox = nil
			}
			called := false
			err = withResultOutboxOwnership(ctx, store, outbox, func(tx datastore.DataStore, _ *model.JobResultOutbox) error {
				called = true
				return UpdateJobInfoStatus(ctx, tx, payload, config.StatusCompleted, "", 0, 1, "logs")
			})
			if claimState == "current" {
				require.NoError(t, err, "result ownership survives the parent Workflow finishing")
				require.True(t, called)
			} else {
				require.ErrorIs(t, err, errResultOutboxOwnershipLost)
				require.False(t, called)
			}
			for id := 1; id <= 3; id++ {
				record := &model.JobInfo{ID: id}
				require.NoError(t, store.Get(ctx, record))
				if claimState == "current" && id == 3 {
					require.Equal(t, string(config.StatusCompleted), record.Status)
					require.Equal(t, "logs", record.Info)
				} else {
					require.Equal(t, string(config.StatusDistributed), record.Status)
					require.Empty(t, record.Info)
				}
			}
		})
	}
}

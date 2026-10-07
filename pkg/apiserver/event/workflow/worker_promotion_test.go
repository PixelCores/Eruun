package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

type promotionPolicyStore struct {
	workflowURLPolicyStore
	beforeLoad func(context.Context) error
}

func (s *promotionPolicyStore) Get(ctx context.Context, entity datastore.Entity) error {
	if err := s.beforeLoad(ctx); err != nil {
		return err
	}
	return s.workflowURLPolicyStore.Get(ctx, entity)
}

func TestClaimedWorkflowPreparationUsesExecutionContextAfterPromotion(t *testing.T) {
	for _, action := range []string{"complete", "stop execution", "ownership replaced"} {
		t.Run(action, func(t *testing.T) {
			w := newWorkflowForAckTests(t, true)
			configureWorkflowAckTestCancelClient(t, w)
			store := w.Store.(*workflowAckTestStore)
			w.Store = &workspaceControllerTestStore{DataStore: store, appID: store.taskSnapshot().AppID}
			w.Cfg.Accounts = &spec.AccountConfig{}
			w.KubeConfig = &rest.Config{Host: "https://kubernetes.example.invalid"}
			consumerCtx, stopIntake := context.WithCancel(context.Background())
			executionCtx, stopExecution := context.WithCancel(context.Background())
			run := newWorkflowWorkerRun(executionCtx, nil)
			policyEntered := make(chan context.Context, 1)
			continuePolicy := make(chan struct{})
			w.URLSecurityPolicyProvider = urlpolicy.NewProvider(&promotionPolicyStore{
				workflowURLPolicyStore: workflowURLPolicyStore{item: &model.SystemSetting{
					Type: model.SystemSettingTypeURLSecurityPolicy, Value: []byte(`{}`),
				}},
				beforeLoad: func(ctx context.Context) error {
					// Policy loading starts only after the database execution claim.
					// Promotion stops intake while the acquired task remains alive.
					stopIntake()
					policyEntered <- ctx
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-continuePolicy:
						return nil
					}
				},
			}, 0)
			type dispatchResult struct {
				ack    bool
				taskID string
			}
			result := make(chan dispatchResult, 1)
			dispatched := make(chan struct{})
			payload := mustTestTaskDispatch(t)
			go func() {
				defer close(dispatched)
				ack, taskID := w.processDispatchMessage(consumerCtx, run, msg.Message{ID: "1-0", Payload: payload})
				result <- dispatchResult{ack: ack, taskID: taskID}
			}()
			t.Cleanup(func() {
				stopIntake()
				stopExecution()
				<-dispatched
				_ = run.wait()
			})
			var policyCtx context.Context
			select {
			case policyCtx = <-policyEntered:
			case <-time.After(time.Second):
				t.Fatal("claimed task did not reach policy loading")
			}
			require.ErrorIs(t, consumerCtx.Err(), context.Canceled)
			require.NoError(t, policyCtx.Err(), "promotion must preserve claimed-task preparation")
			claimed := store.taskSnapshot()
			require.Equal(t, config.StatusRunning, claimed.Status)
			require.Equal(t, "worker claimed dispatch", claimed.SchedulingReason)
			require.NotNil(t, claimed.LeaseExpiresAt)
			require.True(t, claimed.LeaseExpiresAt.After(time.Now()), "promotion must not expire the execution lease")

			if action == "complete" {
				close(continuePolicy)
			} else {
				if action == "ownership replaced" {
					store.mutateTask(func(task *model.WorkflowQueue) {
						task.RunGeneration++
						task.RunToken = "successor-token"
						task.WorkerID = "successor-worker"
					})
				}
				stopExecution()
			}
			select {
			case outcome := <-result:
				require.Equal(t, action == "complete", outcome.ack)
				require.Equal(t, "task-1", outcome.taskID)
			case <-time.After(time.Second):
				t.Fatal("task preparation did not finish")
			}
			require.NoError(t, run.wait())
			final := store.taskSnapshot()
			switch action {
			case "complete":
				require.Equal(t, config.StatusCompleted, final.Status)
				require.NotEqual(t, "worker execution stopped", final.SchedulingReason)
			case "stop execution":
				require.Equal(t, config.StatusRunning, final.Status)
				require.Equal(t, "worker execution stopped", final.SchedulingReason)
				require.False(t, final.LeaseExpiresAt.After(time.Now()))
			case "ownership replaced":
				require.Equal(t, config.StatusRunning, final.Status)
				require.Equal(t, claimed.RunGeneration+1, final.RunGeneration)
				require.Equal(t, "successor-token", final.RunToken)
				require.Equal(t, "successor-worker", final.WorkerID)
				require.Equal(t, claimed.LeaseExpiresAt, final.LeaseExpiresAt, "old task cleanup must not expire successor ownership")
			}
		})
	}
}

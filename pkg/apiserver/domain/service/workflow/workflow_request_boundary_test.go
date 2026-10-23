package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// The service, not the transports, owns these request rules. The service has
// no ScheduleLocker, so a rule checked after taking the lock would surface as
// ErrDistributedLockUnavailable instead.
func TestWorkflowRequestRulesAtServiceBoundary(t *testing.T) {
	svc := &workflowServiceImpl{Store: &statusDataStore{}}
	ctx := context.Background()

	require.ErrorIs(t, svc.CancelWorkflowTaskForApp(ctx, "app-1", "", "", ""), bcode.ErrWorkflowConfig)
	require.ErrorIs(t, svc.CancelDelayedVersionTaskForApp(ctx, "app-1", "", "", ""), bcode.ErrWorkflowConfig)

	_, err := svc.ExecWorkflowTaskForApp(ctx, "app-1", "Bad ID", 0, "")
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)

	for _, req := range []apis.UpsertWorkflowScheduleRequest{
		{WorkflowID: "Bad ID", Cron: "0 * * * *"},
		{WorkflowID: " wf-1 ", Cron: "0 * * * *"},
		{WorkflowID: "wf-1"},
		{WorkflowID: "wf-1", Cron: "not a cron"},
	} {
		_, err = svc.UpsertWorkflowSchedule(ctx, "app-1", req)
		require.ErrorIs(t, err, bcode.ErrWorkflowConfig, "%+v", req)
	}
}

// Blank users are recorded as the default revoker on every cancellation path.
func TestBlankUserCancellationsRecordDefaultRevoker(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{AllowPrivateURLTargets: true}
	approvalTask := func(taskID string) *statusDataStore {
		return &statusDataStore{task: &model.WorkflowQueue{
			TaskID:              taskID,
			AppID:               "app-1",
			Status:              config.StatusWaitingApprove,
			CurrentStep:         2,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		}}
	}

	store := approvalTask("task-cancel-blank")
	svc := &workflowServiceImpl{Store: store, Cfg: cfg}
	require.NoError(t, svc.CancelWorkflowTaskForApp(ctx, "app-1", " ", store.task.TaskID, "manual"))
	require.Equal(t, config.DefaultTaskRevoker, store.task.TaskRevoker)

	store = approvalTask("task-approve-blank")
	svc = &workflowServiceImpl{Store: store, Cfg: cfg}
	_, err := svc.ApproveWorkflowTask(ctx, store.task.TaskID, "cancel", " ", "")
	require.NoError(t, err)
	require.Equal(t, config.DefaultTaskRevoker, store.task.TaskRevoker)

	store = &statusDataStore{task: &model.WorkflowQueue{
		TaskID:    "task-delay-blank",
		AppID:     "app-1",
		Status:    config.StatusWaiting,
		ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
	}}
	svc = &workflowServiceImpl{Store: store, Cache: cache.NewMemCache(), RedisClient: newTestWorkflowCancelSignalClient(t), Cfg: cfg}
	require.NoError(t, svc.CancelDelayedVersionTaskForApp(ctx, "app-1", " ", store.task.TaskID, "manual"))
	require.Equal(t, config.DefaultTaskRevoker, store.task.TaskRevoker)

	running := &model.WorkflowQueue{TaskID: "task-running-blank", AppID: "app-1", Status: config.StatusRunning}
	svc = &workflowServiceImpl{Store: &statusDataStore{tasks: []*model.WorkflowQueue{running}}, RedisClient: newTestWorkflowCancelSignalClient(t), Cfg: cfg}
	_, err = svc.CancelAllWorkflowTasksForApp(ctx, "app-1", " ", "manual")
	require.NoError(t, err)
	require.Equal(t, config.DefaultTaskRevoker, running.TaskRevoker)
}

package workflow

import (
	"context"

	"sync/atomic"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"

	approvaltimeout "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/approvaltimeout"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestApproveWorkflowTaskContinue(t *testing.T) {
	var timeoutCancelled int32
	timerID := approvaltimeout.Register("task-approve-1", func() {
		atomic.AddInt32(&timeoutCancelled, 1)
	})
	require.NotZero(t, timerID)
	t.Cleanup(func() {
		approvaltimeout.Cancel("task-approve-1")
	})

	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-approve-1",
			AppID:               "app-1",
			Status:              config.StatusWaitingApprove,
			CurrentStep:         1,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	resp, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-1", "continue", "approver", "looks good")
	require.NoError(t, err)
	require.Equal(t, "continue", resp.Action)
	require.Equal(t, string(config.StatusWaiting), resp.Status)
	require.Equal(t, config.StatusWaiting, store.task.Status)
	require.Equal(t, 2, store.task.CurrentStep)
	require.False(t, store.task.ApprovalPending)
	require.Equal(t, "", store.task.PendingApprovalStep)
	require.Equal(t, int32(1), atomic.LoadInt32(&timeoutCancelled))
}

func TestApproveWorkflowTaskRejectsInvalidAction(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:          "task-approve-3",
			Status:          config.StatusWaitingApprove,
			ApprovalPending: true,
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	_, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-3", "invalid", "approver", "")
	require.ErrorIs(t, err, bcode.ErrWorkflowApprovalActionInvalid)
}

func TestApproveWorkflowTaskRejectsNonApprovalState(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:          "task-approve-4",
			Status:          config.StatusRunning,
			ApprovalPending: false,
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	_, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-4", "continue", "approver", "")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskNotAwaitingApproval)
}

func TestApproveWorkflowTaskRejectsQueuedState(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:          "task-approve-5",
			Status:          config.StatusQueued,
			ApprovalPending: true,
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	_, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-5", "continue", "approver", "")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskNotAwaitingApproval)
}

func TestApproveWorkflowTaskAllowsWaitingStateWhenApprovalPending(t *testing.T) {
	testCases := []struct {
		name               string
		action             string
		reason             string
		expectedStatus     config.Status
		expectedStep       int
		expectedRevoker    string
		expectedCancelFrom string
	}{
		{
			name:           "continue",
			action:         "continue",
			expectedStatus: config.StatusWaiting,
			expectedStep:   3,
		},
		{
			name:               "cancel",
			action:             "cancel",
			reason:             "reject",
			expectedStatus:     config.StatusCancelled,
			expectedStep:       2,
			expectedRevoker:    "approver",
			expectedCancelFrom: config.CancelSourceUser,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &statusDataStore{
				task: &model.WorkflowQueue{
					TaskID:              "task-approve-waiting-" + tc.name,
					AppID:               "app-1",
					Status:              config.StatusWaiting,
					CurrentStep:         2,
					ApprovalPending:     true,
					PendingApprovalStep: "manual-approval",
				},
			}
			svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

			resp, err := svc.ApproveWorkflowTask(context.Background(), store.task.TaskID, tc.action, "approver", tc.reason)
			require.NoError(t, err)
			require.Equal(t, tc.action, resp.Action)
			require.Equal(t, string(tc.expectedStatus), resp.Status)
			require.Equal(t, tc.expectedStatus, store.task.Status)
			require.False(t, store.task.ApprovalPending)
			require.Equal(t, "", store.task.PendingApprovalStep)
			require.Equal(t, tc.expectedStep, store.task.CurrentStep)
			require.Equal(t, tc.expectedRevoker, store.task.TaskRevoker)
			require.Equal(t, tc.expectedCancelFrom, store.task.CancelSource)
		})
	}
}

func TestApproveWorkflowTaskContinueCASConflict(t *testing.T) {
	// Simulate: ApprovalPending was already cleared by another concurrent operation
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:          "task-approve-cas-1",
			AppID:           "app-1",
			Status:          config.StatusWaitingApprove,
			ApprovalPending: false, // Already processed by timeout or another approve
			CurrentStep:     1,
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	_, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-cas-1", "continue", "approver", "")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskNotAwaitingApproval)
}

func TestApproveWorkflowTaskContinueCASRejectsQueuedRace(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-approve-race-1",
			AppID:               "app-1",
			Status:              config.StatusWaitingApprove,
			CurrentStep:         2,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		beforeCAS: func(task *model.WorkflowQueue) {
			// Simulate another actor changing status between read and CAS.
			task.Status = config.StatusQueued
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	_, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-race-1", "continue", "approver", "")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskNotAwaitingApproval)
	require.Equal(t, config.StatusQueued, store.task.Status)
	require.True(t, store.task.ApprovalPending)
	require.Equal(t, 2, store.task.CurrentStep)
}

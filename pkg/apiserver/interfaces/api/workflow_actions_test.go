package api

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestWorkflowTaskAllowedActionsAreExecutableDescriptors(t *testing.T) {
	active := workflowTaskAllowedActions("task-1", "app-1", string(config.StatusRunning), "")
	require.Len(t, active, 1)
	require.Equal(t, "cancel", active[0].Name)
	require.Equal(t, "POST", active[0].Method)
	require.Equal(t, "/api/v1/applications/app-1/workflow/cancel", active[0].Path)
	require.Equal(t, "task-1", active[0].Body["taskId"])

	approval := workflowTaskAllowedActions("task-2", "app-1", string(config.StatusWaitingApprove), "approve-release")
	require.Len(t, approval, 2)
	require.Equal(t, "continue", approval[0].Name)
	require.Equal(t, "cancel", approval[1].Name)
	require.Equal(t, "/api/v1/workflow/tasks/task-2/approval", approval[0].Path)

	terminal := workflowTaskAllowedActions("task-3", "app-1", string(config.StatusCompleted), "")
	require.Empty(t, terminal)
	require.NotNil(t, terminal)
}

func TestWorkflowTaskAllowedActionsPreserveLifecycleBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name    string
		taskID  string
		appID   string
		status  string
		pending string
		actions []string
	}{
		{name: "empty status remains cancellable", taskID: "task", appID: "app", actions: []string{"cancel"}},
		{name: "blocked task remains cancellable", taskID: "task", appID: "app", status: string(config.StatusBlocked), actions: []string{"cancel"}},
		{name: "trim transport values", taskID: " task ", appID: " app ", status: " running ", actions: []string{"cancel"}},
		{name: "unknown status has no actions", taskID: "task", appID: "app", status: "unknown"},
		{name: "cancelled status has no advertised retry", taskID: "task", appID: "app", status: string(config.StatusCancelled)},
		{name: "no task identity", appID: "app", status: string(config.StatusRunning)},
		{name: "no application cancel route", taskID: "task", status: string(config.StatusRunning)},
		{name: "approval route has task identity", taskID: "task", status: string(config.StatusWaiting), pending: "approve", actions: []string{"continue", "cancel"}},
		{name: "terminal ignores stale approval step", taskID: "task", appID: "app", status: string(config.StatusCompleted), pending: "approve"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			actions := workflowTaskAllowedActions(tt.taskID, tt.appID, tt.status, tt.pending)
			require.NotNil(t, actions)
			require.Len(t, actions, len(tt.actions))
			for i, name := range tt.actions {
				require.Equal(t, name, actions[i].Name)
			}
		})
	}
}

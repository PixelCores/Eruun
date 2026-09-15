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

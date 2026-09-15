package v1

import (
	"net/url"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

// WorkflowTaskAllowedActions is shared response assembly for HTTP and gRPC.
func WorkflowTaskAllowedActions(taskID, appID, status, pendingApprovalStep string) []AllowedAction {
	taskID = strings.TrimSpace(taskID)
	appID = strings.TrimSpace(appID)
	statusValue := config.Status(strings.TrimSpace(status))
	if taskID == "" {
		return []AllowedAction{}
	}
	if strings.TrimSpace(pendingApprovalStep) != "" && (statusValue == config.StatusWaitingApprove || statusValue == config.StatusWaiting) {
		path := "/api/v1/workflow/tasks/" + url.PathEscape(taskID) + "/approval"
		return []AllowedAction{
			{Name: "continue", Method: "POST", Path: path, Body: map[string]any{"action": "continue"}},
			{Name: "cancel", Method: "POST", Path: path, Body: map[string]any{"action": "cancel"}},
		}
	}
	if appID == "" || !WorkflowStatusCancellable(statusValue) {
		return []AllowedAction{}
	}
	return []AllowedAction{{
		Name: "cancel", Method: "POST", Path: "/api/v1/applications/" + url.PathEscape(appID) + "/workflow/cancel",
		Body: map[string]any{"taskId": taskID},
	}}
}

func WorkflowStatusCancellable(status config.Status) bool {
	switch status {
	case "", config.StatusCreated, config.StatusRunning, config.StatusWaiting,
		config.StatusQueued, config.StatusBlocked, config.QueueItemPending,
		config.StatusPrepare, config.StatusWaitingApprove, config.StatusDistributed,
		config.StatusDebugBefore, config.StatusDebugAfter:
		return true
	default:
		return false
	}
}

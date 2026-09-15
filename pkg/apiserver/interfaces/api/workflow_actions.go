package api

import (
	"net/url"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func workflowTaskAllowedActions(taskID, appID, status, pendingApprovalStep string) []apis.AllowedAction {
	taskID = strings.TrimSpace(taskID)
	appID = strings.TrimSpace(appID)
	statusValue := config.Status(strings.TrimSpace(status))
	if taskID == "" {
		return []apis.AllowedAction{}
	}
	if strings.TrimSpace(pendingApprovalStep) != "" && (statusValue == config.StatusWaitingApprove || statusValue == config.StatusWaiting) {
		path := "/api/v1/workflow/tasks/" + url.PathEscape(taskID) + "/approval"
		return []apis.AllowedAction{
			{Name: "continue", Method: "POST", Path: path, Body: map[string]any{"action": "continue"}},
			{Name: "cancel", Method: "POST", Path: path, Body: map[string]any{"action": "cancel"}},
		}
	}
	if appID == "" || !isWorkflowStatusCancellable(statusValue) {
		return []apis.AllowedAction{}
	}
	return []apis.AllowedAction{{
		Name:   "cancel",
		Method: "POST",
		Path:   "/api/v1/applications/" + url.PathEscape(appID) + "/workflow/cancel",
		Body:   map[string]any{"taskId": taskID},
	}}
}

func isWorkflowStatusCancellable(status config.Status) bool {
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

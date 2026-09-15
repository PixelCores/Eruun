package api

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func workflowTaskAllowedActions(taskID, appID, status, pendingApprovalStep string) []apis.AllowedAction {
	return apis.WorkflowTaskAllowedActions(taskID, appID, status, pendingApprovalStep)
}

func isWorkflowStatusCancellable(status config.Status) bool {
	return apis.WorkflowStatusCancellable(status)
}

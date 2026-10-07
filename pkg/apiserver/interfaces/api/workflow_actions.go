package api

import (
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func workflowTaskAllowedActions(taskID, appID, status, pendingApprovalStep string) []apis.AllowedAction {
	return apis.WorkflowTaskAllowedActions(taskID, appID, status, pendingApprovalStep)
}

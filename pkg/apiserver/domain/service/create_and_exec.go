package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"k8s.io/klog/v2"
)

// ExecuteCreateAndExecApplication owns the shared HTTP/gRPC create-then-execute
// orchestration. Execution failures are partial successful results because the
// application already exists; each transport chooses its safe client message.
func ExecuteCreateAndExecApplication(
	ctx context.Context,
	applications ApplicationsService,
	workflows WorkflowService,
	req apis.CreateAndExecApplicationRequest,
	idempotencyKey string,
	execFailureMessage func(error) string,
) (*apis.CreateAndExecApplicationResponse, error) {
	created, err := applications.CreateApplications(ctx, req.CreateApplicationsRequest)
	if err != nil {
		return nil, err
	}
	if created == nil {
		return nil, fmt.Errorf("create application returned empty response")
	}
	workflowID := strings.TrimSpace(req.WorkflowID)
	if workflowID == "" {
		workflowID = strings.TrimSpace(created.WorkflowID)
	}
	resp := &apis.CreateAndExecApplicationResponse{
		Application: created, WorkflowID: workflowID,
		ExecStatus:     apis.CreateAndExecStatusQueued,
		AllowedActions: []apis.AllowedAction{},
	}
	if workflowID == "" {
		resp.ExecStatus = apis.CreateAndExecStatusFailed
		resp.ExecError = execFailureMessage(bcode.ErrWorkflowNotExist)
		return resp, nil
	}
	if req.ExecuteAt < 0 {
		resp.ExecStatus = apis.CreateAndExecStatusFailed
		resp.ExecError = execFailureMessage(bcode.ErrWorkflowConfig)
		return resp, nil
	}
	executed, execErr := workflows.ExecWorkflowTaskForApp(ctx, created.ID, workflowID, req.ExecuteAt, idempotencyKey)
	if execErr != nil {
		// Do not log a domain error verbatim: it may contain request material.
		klog.InfoS("create and exec workflow failed", "appID", created.ID, "workflowID", workflowID)
		resp.ExecStatus = apis.CreateAndExecStatusFailed
		resp.ExecError = execFailureMessage(execErr)
		return resp, nil
	}
	if executed != nil {
		resp.TaskID = executed.TaskID
		resp.AllowedActions = apis.WorkflowTaskAllowedActions(executed.TaskID, created.ID, executed.Status, executed.PendingApprovalStep)
	}
	if resp.TaskID != "" && (req.ExecuteAt == 0 || req.ExecuteAt <= time.Now().Unix()) {
		if markErr := applications.MarkInitialDeployingWorkflowComponents(ctx, created.ID, workflowID); markErr != nil {
			klog.InfoS("mark initial deploy workflow components deploying failed",
				"appID", created.ID, "workflowID", workflowID, "taskID", resp.TaskID)
		}
	}
	return resp, nil
}

package grpcapi

import (
	"context"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (s *ApplicationsServer) ExecApplicationWorkflow(ctx context.Context, req *eruunv1.ExecWorkflowRPCRequest) (*eruunv1.AppDTOExecWorkflowResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	input, err := decodeTypedRequest[apis.ExecWorkflowRequest](req.Request)
	if err != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	key, err := requestIdempotencyKey(ctx, bcode.ErrWorkflowConfig)
	if err != nil {
		return nil, rpcError(err)
	}
	resp, err := s.Workflow.ExecWorkflowTaskForApp(ctx, req.AppId, input.WorkflowID, input.ExecuteAt, key)
	if resp != nil {
		resp.AllowedActions = apis.WorkflowTaskAllowedActions(resp.TaskID, req.AppId, resp.Status, resp.PendingApprovalStep)
	}
	return typedResult(resp, err, &eruunv1.AppDTOExecWorkflowResponse{})
}

func (s *ApplicationsServer) cancelWorkflow(ctx context.Context, req *eruunv1.CancelWorkflowRPCRequest, delayed bool) (*eruunv1.AppDTOCancelWorkflowResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	input, err := decodeTypedRequest[apis.CancelWorkflowRequest](req.Request)
	if err != nil || input.TaskID == "" {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	user := input.User
	if delayed {
		user = strings.TrimSpace(user)
	}
	if user == "" {
		user = config.DefaultTaskRevoker
	}
	if delayed {
		err = s.Workflow.CancelDelayedVersionTaskForApp(ctx, req.AppId, user, input.TaskID, input.Reason)
	} else {
		err = s.Workflow.CancelWorkflowTaskForApp(ctx, req.AppId, user, input.TaskID, input.Reason)
	}
	return typedResult(apis.CancelWorkflowResponse{TaskID: input.TaskID, Status: string(config.StatusCancelled)}, err, &eruunv1.AppDTOCancelWorkflowResponse{})
}

func (s *ApplicationsServer) CancelApplicationWorkflow(ctx context.Context, req *eruunv1.CancelWorkflowRPCRequest) (*eruunv1.AppDTOCancelWorkflowResponse, error) {
	return s.cancelWorkflow(ctx, req, false)
}

func (s *ApplicationsServer) CancelDelayedVersionUpdate(ctx context.Context, req *eruunv1.CancelWorkflowRPCRequest) (*eruunv1.AppDTOCancelWorkflowResponse, error) {
	return s.cancelWorkflow(ctx, req, true)
}

func (s *ApplicationsServer) CancelAllApplicationWorkflows(ctx context.Context, req *eruunv1.ApplicationIDRequest) (*eruunv1.AppDTOCancelAllApplicationWorkflowsResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	ids, err := s.Workflow.CancelAllWorkflowTasksForApp(ctx, req.AppId, config.DefaultTaskRevoker, "")
	return typedResult(apis.CancelAllApplicationWorkflowsResponse{AppID: req.AppId, CancelledTaskIDs: ids}, err, &eruunv1.AppDTOCancelAllApplicationWorkflowsResponse{})
}

func (s *ApplicationsServer) ListApplicationTasks(ctx context.Context, req *eruunv1.ApplicationIDRequest) (*eruunv1.AppDTOListApplicationTasksResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	tasks, err := s.Applications.ListApplicationTasks(ctx, req.AppId)
	if err != nil {
		return nil, rpcError(err)
	}
	result := make([]*apis.ApplicationTask, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		result = append(result, &apis.ApplicationTask{
			TaskID: task.TaskID, AppID: task.AppID, WorkflowID: task.WorkflowID,
			WorkflowName: task.WorkflowName, WorkflowDisplayName: task.WorkflowDisplayName,
			Status: string(task.Status), Type: task.Type, TaskCreator: task.TaskCreator,
			TaskRevoker: task.TaskRevoker, CreateTime: task.CreateTime, UpdateTime: task.UpdateTime,
			AllowedActions: apis.WorkflowTaskAllowedActions(task.TaskID, task.AppID, string(task.Status), task.PendingApprovalStep),
		})
	}
	return typedResult(apis.ListApplicationTasksResponse{Tasks: result}, nil, &eruunv1.AppDTOListApplicationTasksResponse{})
}

func (s *ApplicationsServer) ApproveWorkflowTask(ctx context.Context, req *eruunv1.TaskApprovalRPCRequest) (*eruunv1.AppDTOTaskApprovalResponse, error) {
	if req == nil || req.TaskId == "" {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	input, err := decodeTypedRequest[apis.TaskApprovalRequest](req.Request)
	if err != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	input.Action = strings.ToLower(strings.TrimSpace(input.Action))
	if input.Action != "continue" && input.Action != "cancel" {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	user := strings.TrimSpace(input.User)
	if user == "" {
		user = config.DefaultTaskRevoker
	}
	resp, err := s.Workflow.ApproveWorkflowTask(ctx, req.TaskId, input.Action, user, input.Reason)
	return typedResult(resp, err, &eruunv1.AppDTOTaskApprovalResponse{})
}

func (s *ApplicationsServer) GetWorkflowTaskStatus(ctx context.Context, req *eruunv1.WorkflowTaskIDRequest) (*eruunv1.AppDTOTaskStatusResponse, error) {
	if req == nil || req.TaskId == "" {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	resp, err := s.Workflow.GetTaskStatus(ctx, req.TaskId)
	if resp != nil {
		resp.AllowedActions = apis.WorkflowTaskAllowedActions(resp.TaskID, resp.AppID, resp.Status, resp.PendingApprovalStep)
	}
	return typedResult(resp, err, &eruunv1.AppDTOTaskStatusResponse{})
}

func (s *ApplicationsServer) GetWorkflowTaskStages(ctx context.Context, req *eruunv1.WorkflowTaskIDRequest) (*eruunv1.AppDTOTaskStagesResponse, error) {
	if req == nil || req.TaskId == "" {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	resp, err := s.Workflow.GetTaskStages(ctx, req.TaskId)
	if resp != nil {
		resp.AllowedActions = apis.WorkflowTaskAllowedActions(resp.TaskID, resp.AppID, resp.Status, resp.PendingApprovalStep)
	}
	return typedResult(resp, err, &eruunv1.AppDTOTaskStagesResponse{})
}

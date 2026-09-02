package api

import (
	"context"
	"strings"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (app *applications) listApplicationWorkflows(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	workflows, err := app.ApplicationService.ListApplicationWorkflows(c.Request.Context(), appID)
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	resp, err := convertDTOList(workflows, assembler.ConvertWorkflowModelToDTO, func(wf *model.Workflow, err error) error {
		klog.ErrorS(err, "convert workflow dto failed", "appID", appID, "workflowID", wf.ID)
		return err
	})
	respondWithResult(c, apis.ListApplicationWorkflowsResponse{Workflows: resp}, err)
}

func convertDTOList[S any, D any](items []*S, convert func(*S) (*D, error), onConvertErr func(*S, error) error) ([]*D, error) {
	result := make([]*D, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		dto, err := convert(item)
		if err != nil {
			if onConvertErr != nil {
				return nil, onConvertErr(item, err)
			}
			return nil, err
		}
		if dto != nil {
			result = append(result, dto)
		}
	}
	return result, nil
}

func (app *applications) updateApplicationWorkflow(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	req, ok := bindRequest[apis.UpdateApplicationWorkflowRequest](c, bcode.ErrWorkflowConfig, true)
	if !ok {
		return
	}
	req.WorkflowType = config.WorkflowTaskType(strings.ToLower(strings.TrimSpace(string(req.WorkflowType))))
	normalizeWorkflowSteps(req.Workflow)
	if err := validate.Struct(req); err != nil {
		bcode.ReturnError(c, bcode.ErrWorkflowConfig)
		return
	}
	ctx := c.Request.Context()
	klog.InfoS("update workflow request received", "appID", appID, "workflowID", req.WorkflowID, "name", req.Name)
	resp, err := app.ApplicationService.UpdateApplicationWorkflow(ctx, appID, *req)
	if err != nil {
		klog.ErrorS(err, "update workflow failed", "appID", appID, "workflowID", req.WorkflowID)
		bcode.ReturnError(c, err)
		return
	}
	klog.InfoS("update workflow succeeded", "appID", appID, "workflowID", resp.WorkflowID)
	bcode.ReturnSuccess(c, resp)
}

func (app *applications) listWorkflowSchedules(c *gin.Context) {
	handlePathResult(
		c,
		appIDPathParam,
		func(ctx context.Context, appID string) (apis.ListWorkflowSchedulesResponse, error) {
			schedules, err := app.WorkflowService.ListWorkflowSchedules(ctx, appID)
			return apis.ListWorkflowSchedulesResponse{Schedules: schedules}, err
		},
	)
}

func (app *applications) upsertWorkflowSchedule(c *gin.Context) {
	handlePathBoundResult(
		c,
		appIDPathParam,
		validatedRequestBody[apis.UpsertWorkflowScheduleRequest](bcode.ErrWorkflowConfig, true),
		func(ctx context.Context, appID string, req *apis.UpsertWorkflowScheduleRequest) (*apis.UpsertWorkflowScheduleResponse, error) {
			return app.WorkflowService.UpsertWorkflowSchedule(ctx, appID, *req)
		},
	)
}

func (app *applications) deleteWorkflowSchedule(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	workflowID, ok := workflowIDPathParam(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()
	if err := app.WorkflowService.DeleteWorkflowSchedule(ctx, appID, workflowID); err != nil {
		bcode.ReturnError(c, err)
		return
	}
	bcode.ReturnSuccess(c, apis.DeleteWorkflowScheduleResponse{WorkflowID: workflowID})
}

func normalizeWorkflowSteps(steps []apis.CreateWorkflowStepRequest) {
	for i := range steps {
		step := &steps[i]
		normalizeWorkflowStepNode(&step.Name, step.Components, step.Properties.Policies)
		step.Properties.Path = strings.TrimSpace(step.Properties.Path)
		step.Properties.Container = strings.TrimSpace(step.Properties.Container)
		step.StepType = config.WorkflowStepType(strings.ToLower(strings.TrimSpace(string(step.StepType))))
		if step.Approval != nil {
			step.Approval.NotifyURL = strings.TrimSpace(step.Approval.NotifyURL)
			step.Approval.Message = strings.TrimSpace(step.Approval.Message)
			step.Approval.Method = strings.ToUpper(strings.TrimSpace(step.Approval.Method))
		}
		for j := range step.SubSteps {
			subStep := &step.SubSteps[j]
			normalizeWorkflowStepNode(&subStep.Name, subStep.Components, subStep.Properties.Policies)
			subStep.Properties.Path = strings.TrimSpace(subStep.Properties.Path)
			subStep.Properties.Container = strings.TrimSpace(subStep.Properties.Container)
		}
	}
}

func normalizeWorkflowStepNode(name *string, components []string, policies []string) {
	*name = strings.ToLower(*name)
	trimWorkflowStringList(components)
	trimWorkflowStringList(policies)
}

func trimWorkflowStringList(values []string) {
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
	}
}

func (app *applications) execApplicationWorkflow(c *gin.Context) {
	handlePathBoundResult(
		c,
		appIDPathParam,
		validatedRequestBody[apis.ExecWorkflowRequest](bcode.ErrWorkflowConfig, true),
		func(ctx context.Context, appID string, req *apis.ExecWorkflowRequest) (*apis.ExecWorkflowResponse, error) {
			return app.WorkflowService.ExecWorkflowTaskForApp(ctx, appID, req.WorkflowID, req.ExecuteAt)
		},
	)
}

func (app *applications) cancelApplicationWorkflow(c *gin.Context) {
	app.cancelWorkflow(c, app.WorkflowService.CancelWorkflowTaskForApp, func(user string) string {
		return user
	})
}

func (app *applications) cancelAllApplicationWorkflows(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	cancelledTaskIDs, err := app.WorkflowService.CancelAllWorkflowTasksForApp(
		c.Request.Context(),
		appID,
		config.DefaultTaskRevoker,
		"",
	)
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	bcode.ReturnSuccess(c, apis.CancelAllApplicationWorkflowsResponse{
		AppID:            appID,
		CancelledTaskIDs: cancelledTaskIDs,
	})
}

func (app *applications) approveWorkflowTask(c *gin.Context) {
	taskID, ok := taskIDPathParam(c)
	if !ok {
		return
	}
	req, ok := bindRequest[apis.TaskApprovalRequest](c, bcode.ErrWorkflowConfig, true)
	if !ok {
		return
	}
	req.Action = strings.ToLower(strings.TrimSpace(req.Action))
	if err := validate.Struct(req); err != nil {
		bcode.ReturnError(c, bcode.ErrWorkflowConfig)
		return
	}
	user := strings.TrimSpace(req.User)
	if user == "" {
		user = config.DefaultTaskRevoker
	}
	resp, err := app.WorkflowService.ApproveWorkflowTask(c.Request.Context(), taskID, req.Action, user, req.Reason)
	respondWithResult(c, resp, err)
}

func (app *applications) listApplicationTasks(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	tasks, err := app.ApplicationService.ListApplicationTasks(c.Request.Context(), appID)
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	resp := make([]*apis.ApplicationTask, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		resp = append(resp, &apis.ApplicationTask{
			TaskID:              task.TaskID,
			AppID:               task.AppID,
			WorkflowID:          task.WorkflowID,
			WorkflowName:        task.WorkflowName,
			WorkflowDisplayName: task.WorkflowDisplayName,
			Status:              string(task.Status),
			Type:                task.Type,
			TaskCreator:         task.TaskCreator,
			TaskRevoker:         task.TaskRevoker,
			CreateTime:          task.CreateTime,
			UpdateTime:          task.UpdateTime,
		})
	}
	bcode.ReturnSuccess(c, apis.ListApplicationTasksResponse{Tasks: resp})
}

func (app *applications) getWorkflowTaskStatus(c *gin.Context) {
	handlePathResult(c, taskIDPathParam, app.WorkflowService.GetTaskStatus)
}

func (app *applications) getWorkflowTaskStages(c *gin.Context) {
	handlePathResult(c, taskIDPathParam, app.WorkflowService.GetTaskStages)
}

func (app *applications) cancelDelayedVersionUpdate(c *gin.Context) {
	app.cancelWorkflow(c, app.WorkflowService.CancelDelayedVersionTaskForApp, strings.TrimSpace)
}

func (app *applications) cancelWorkflow(
	c *gin.Context,
	cancelFn func(context.Context, string, string, string, string) error,
	normalizeUser func(string) string,
) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	req, ok := bindAndValidate[apis.CancelWorkflowRequest](c, bcode.ErrWorkflowConfig, true)
	if !ok {
		return
	}
	user := req.User
	if normalizeUser != nil {
		user = normalizeUser(user)
	}
	if user == "" {
		user = config.DefaultTaskRevoker
	}
	ctx := c.Request.Context()
	if err := cancelFn(ctx, appID, user, req.TaskID, req.Reason); err != nil {
		bcode.ReturnError(c, err)
		return
	}
	bcode.ReturnSuccess(c, apis.CancelWorkflowResponse{TaskID: req.TaskID, Status: string(config.StatusCancelled)})
}

// tryApplication validates an application creation request without actually creating it
// @Summary Try/DryRun application creation
// @Description Validates application configuration against naming rules, traits rules, and workflow component references without creating the application
// @Tags applications
// @Accept json
// @Produce json
// @Param request body apis.TryApplicationRequest true "Application configuration to validate (optional appId to validate workflow against an existing application)"
// @Success 200 {object} apis.TryApplicationResponse "Validation result with detailed errors if any"
// @Router /applications/try [post]
func (app *applications) tryApplication(c *gin.Context) {
	req, ok := bindStrictJSON[apis.TryApplicationRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}

	for i := range req.Component {
		req.Component[i].Name = strings.ToLower(strings.TrimSpace(req.Component[i].Name))
	}
	normalizeWorkflowSteps(req.WorkflowSteps)

	ctx := c.Request.Context()
	if strings.TrimSpace(req.AppID) != "" {
		appID := strings.TrimSpace(req.AppID)
		klog.V(2).InfoS("try validation request received", "appID", appID, "steps", len(req.WorkflowSteps))

		wfResp := app.ValidationService.TryWorkflow(ctx, appID, apis.TryWorkflowRequest{
			FailurePolicy: req.WorkflowFailurePolicy,
			Workflow:      req.WorkflowSteps,
		})
		bcode.ReturnSuccess(c, apis.TryApplicationResponse{Valid: wfResp.Valid, Errors: wfResp.Errors})
		return
	}

	klog.V(2).InfoS("try application validation request received", "name", req.Name, "components", len(req.Component), "workflows", len(req.WorkflowSteps))

	resp := app.ValidationService.TryApplication(ctx, req.CreateApplicationsRequest)

	klog.V(2).InfoS("try application validation completed", "name", req.Name, "valid", resp.Valid, "errorCount", len(resp.Errors))

	bcode.ReturnSuccess(c, resp)
}

// tryWorkflow validates a workflow update request without actually updating it
// @Summary Try/DryRun workflow update
// @Description Validates workflow configuration against existing components without updating the workflow
// @Tags applications
// @Accept json
// @Produce json
// @Param appID path string true "Application ID"
// @Param request body apis.UpdateApplicationWorkflowRequest true "Workflow configuration to validate"
// @Success 200 {object} apis.TryWorkflowResponse "Validation result with detailed errors if any"
// @Router /applications/{appID}/workflow/try [post]
func (app *applications) tryWorkflow(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}

	req, ok := bindRequest[apis.UpdateApplicationWorkflowRequest](c, bcode.ErrWorkflowConfig, true)
	if !ok {
		return
	}

	normalizeWorkflowSteps(req.Workflow)
	tryReq := apis.TryWorkflowRequest{
		WorkflowID:    req.WorkflowID,
		Name:          req.Name,
		Alias:         req.Alias,
		WorkflowType:  req.WorkflowType,
		Callback:      req.Callback,
		FailurePolicy: req.FailurePolicy,
		Workflow:      req.Workflow,
	}

	ctx := c.Request.Context()
	klog.V(2).InfoS("try workflow validation request received", "appID", appID, "workflowID", tryReq.WorkflowID, "name", tryReq.Name, "steps", len(tryReq.Workflow))

	resp := app.ValidationService.TryWorkflow(ctx, appID, tryReq)

	klog.V(2).InfoS("try workflow validation completed", "appID", appID, "valid", resp.Valid, "errorCount", len(resp.Errors))

	bcode.ReturnSuccess(c, resp)
}

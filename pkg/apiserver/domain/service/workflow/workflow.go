package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/cancelsignal"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	approvaltimeout "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/approvaltimeout"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/urlpolicy"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	wf "github.com/PixelCores/Eruun/pkg/apiserver/workflow"
)

type WorkflowService interface {
	CreateWorkflowTask(ctx context.Context, workflow apis.CreateWorkflowRequest) (*apis.CreateWorkflowResponse, error)
	ExecWorkflowTask(ctx context.Context, workflowID string, executeAt int64) (*apis.ExecWorkflowResponse, error)
	ExecWorkflowTaskForApp(ctx context.Context, appID, workflowID string, executeAt int64) (*apis.ExecWorkflowResponse, error)
	WaitingTasks(ctx context.Context) ([]*model.WorkflowQueue, error)
	UpdateTask(ctx context.Context, queue *model.WorkflowQueue) bool
	TaskRunning(ctx context.Context) ([]*model.WorkflowQueue, error)
	CancelWorkflowTask(ctx context.Context, userName, taskID, reason string) error
	CancelWorkflowTaskForApp(ctx context.Context, appID, userName, taskID, reason string) error
	CancelAllWorkflowTasksForApp(ctx context.Context, appID, userName, reason string) ([]string, error)
	CancelDelayedVersionTaskForApp(ctx context.Context, appID, userName, taskID, reason string) error
	ApproveWorkflowTask(ctx context.Context, taskID, action, userName, reason string) (*apis.TaskApprovalResponse, error)
	MarkTaskStatus(ctx context.Context, taskID string, from, to config.Status) (bool, error)
	GetTaskStatus(ctx context.Context, taskID string) (*apis.TaskStatusResponse, error)
	GetTaskStages(ctx context.Context, taskID string) (*apis.TaskStagesResponse, error)
	UpsertWorkflowSchedule(ctx context.Context, appID string, req apis.UpsertWorkflowScheduleRequest) (*apis.UpsertWorkflowScheduleResponse, error)
	ListWorkflowSchedules(ctx context.Context, appID string) ([]apis.WorkflowSchedule, error)
	DeleteWorkflowSchedule(ctx context.Context, appID, workflowID string) error
	DispatchWorkflowSchedules(ctx context.Context) (int, error)
}

const cancelWorkflowTaskCASMaxAttempts = 3

type workflowServiceImpl struct {
	Store                     datastore.DataStore  `inject:"datastore"`
	KubeClient                kubernetes.Interface `inject:"kubeClient"`
	KubeConfig                *rest.Config         `inject:"kubeConfig"`
	Cache                     cache.ICache         `inject:"cache"`
	Cfg                       *config.Config       `inject:""`
	URLSecurityPolicyProvider *urlpolicy.Provider  `inject:""`
	ScheduleLocker            locker.Locker
}

var workflowScheduleParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
)

var errScheduleNotClaimed = errors.New("schedule not claimed")

const (
	workflowApprovalActionContinue = "continue"
	workflowApprovalActionCancel   = "cancel"
)

// NewWorkflowService new workflow service
func NewWorkflowService() WorkflowService {
	return &workflowServiceImpl{}
}

// CreateWorkflowTask 创建工作流任务(执行)
func (w *workflowServiceImpl) CreateWorkflowTask(ctx context.Context, req apis.CreateWorkflowRequest) (*apis.CreateWorkflowResponse, error) {
	workflow := &model.Workflow{
		Name: req.Name,
	}
	exist, err := w.Store.IsExist(ctx, workflow)
	if err != nil {
		klog.Errorf("check workflow existence failure: %v", err)
		return nil, fmt.Errorf("check workflow existence: %w", err)
	}
	if exist {
		return nil, bcode.ErrWorkflowExist
	}
	workflow = ConvertWorkflow(&req)

	// 校验工作流信息
	if err = wf.LintWorkflow(workflow); err != nil {
		return nil, err
	}

	if err = repository.CreateWorkflow(ctx, w.Store, workflow); err != nil {
		return nil, bcode.ErrCreateWorkflow
	}

	// 创建组件
	for _, component := range req.Component {
		nComponent := ConvertComponent(&component, workflow.ID)
		properties, err := model.NewJSONStructByStruct(component.Properties)
		if err != nil {
			w.rollbackWorkflowCreation(ctx, workflow)
			klog.Errorf("new trait failure,%s", err.Error())
			return nil, bcode.ErrInvalidProperties
		}

		nComponent.Properties = properties

		err = repository.CreateComponents(ctx, w.Store, nComponent)
		if err != nil {
			w.rollbackWorkflowCreation(ctx, workflow)
			klog.Errorf("TmpCreate Components err: %s", err)
			return nil, bcode.ErrCreateComponents
		}
	}
	return &apis.CreateWorkflowResponse{
		WorkflowID: workflow.ID,
	}, nil
}

func ConvertWorkflow(req *apis.CreateWorkflowRequest) *model.Workflow {
	return &model.Workflow{
		ID:          utils.RandStringByNumLowercase(24),
		Name:        strings.ToLower(req.Name),
		Alias:       req.Alias,
		Disabled:    true,
		ProjectID:   strings.ToLower(req.Project),
		Description: req.Description,
	}
}

func ConvertComponent(req *apis.CreateComponentRequest, appID string) *model.ApplicationComponent {
	if req.Replicas <= 0 {
		req.Replicas = 1
	}

	return &model.ApplicationComponent{
		Name:          req.Name,
		AppID:         appID,
		Namespace:     req.Namespace,
		Image:         req.Image,
		Replicas:      req.Replicas,
		ComponentType: req.ComponentType,
	}
}

// ExecWorkflowTask 执行工作流的任务
func (w *workflowServiceImpl) ExecWorkflowTask(ctx context.Context, workflowID string, executeAt int64) (*apis.ExecWorkflowResponse, error) {
	workflow, err := repository.WorkflowByID(ctx, w.Store, workflowID)
	if err != nil {
		return nil, err
	}
	return w.execWorkflowTaskForAppLocked(ctx, workflow.AppID, workflowID, executeAt)
}

func (w *workflowServiceImpl) GetTaskStatus(ctx context.Context, taskID string) (*apis.TaskStatusResponse, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, bcode.ErrWorkflowTaskNotExist
	}
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrWorkflowTaskNotExist
		}
		return nil, err
	}

	componentAggregates := make(map[string]*apis.ComponentTaskStatus)
	jobEntities, err := w.Store.List(ctx, &model.JobInfo{TaskID: taskID}, &datastore.ListOptions{
		SortBy: []datastore.SortOption{
			{Key: "update_time", Order: datastore.SortOrderAscending},
			{Key: "create_time", Order: datastore.SortOrderAscending},
		},
	})
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		klog.Errorf("list job info for task %s failed: %v", taskID, err)
	} else {
		for _, entity := range jobEntities {
			j, ok := entity.(*model.JobInfo)
			if !ok {
				continue
			}
			key := strings.ToLower(j.ServiceName)
			agg, exists := componentAggregates[key]
			if !exists {
				agg = &apis.ComponentTaskStatus{
					Name:      j.ServiceName,
					Type:      j.Type,
					Status:    j.Status,
					Error:     j.Error,
					StartTime: j.StartTime,
					EndTime:   j.EndTime,
				}
				componentAggregates[key] = agg
				continue
			}
			// Aggregate: prefer the most severe status; capture first error message.
			agg.Status = chooseAggStatus(agg.Status, j.Status)
			if agg.Error == "" && j.Error != "" {
				agg.Error = j.Error
			}
			if agg.StartTime == 0 || (j.StartTime != 0 && j.StartTime < agg.StartTime) {
				agg.StartTime = j.StartTime
			}
			if j.EndTime > agg.EndTime {
				agg.EndTime = j.EndTime
			}
		}
	}

	// Fill in missing components from workflow definition so the caller can see
	// waiting/queued/cancelled components even before job records exist.
	if workflowID := strings.TrimSpace(task.WorkflowID); workflowID != "" {
		if workflow, wfErr := repository.WorkflowByID(ctx, w.Store, workflowID); wfErr == nil {
			names := collectWorkflowComponentNames(ctx, w.Store, task.AppID, workflow)
			defaultStatus := defaultComponentStatus(task.Status)
			for _, name := range names {
				key := strings.ToLower(name)
				if _, exists := componentAggregates[key]; exists {
					continue
				}
				componentAggregates[key] = &apis.ComponentTaskStatus{
					Name:   name,
					Status: defaultStatus,
				}
			}
		} else if !errors.Is(wfErr, datastore.ErrRecordNotExist) {
			klog.V(4).Infof("load workflow %s for task %s failed: %v", workflowID, taskID, wfErr)
		}
	}

	componentStatuses := make([]apis.ComponentTaskStatus, 0, len(componentAggregates))
	for _, cs := range componentAggregates {
		componentStatuses = append(componentStatuses, *cs)
	}

	return &apis.TaskStatusResponse{
		TaskID:              task.TaskID,
		Status:              string(task.Status),
		WorkflowID:          task.WorkflowID,
		WorkflowName:        task.WorkflowName,
		AppID:               task.AppID,
		Type:                task.Type,
		PendingApprovalStep: task.PendingApprovalStep,
		Components:          componentStatuses,
	}, nil
}

func (w *workflowServiceImpl) GetTaskStages(ctx context.Context, taskID string) (*apis.TaskStagesResponse, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, bcode.ErrWorkflowTaskNotExist
	}
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrWorkflowTaskNotExist
		}
		return nil, err
	}

	jobEntities, err := w.Store.List(ctx, &model.JobInfo{TaskID: taskID}, &datastore.ListOptions{
		SortBy: []datastore.SortOption{
			{Key: "update_time", Order: datastore.SortOrderAscending},
			{Key: "create_time", Order: datastore.SortOrderAscending},
		},
	})
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		klog.Errorf("list job info for task %s failed: %v", taskID, err)
	}

	jobInfos := make([]*model.JobInfo, 0, len(jobEntities))
	stageOrder := make([]string, 0, len(jobEntities))
	stageAggregates := make(map[string]*stageAggregate)
	for _, entity := range jobEntities {
		j, ok := entity.(*model.JobInfo)
		if !ok || j == nil {
			continue
		}
		jobInfos = append(jobInfos, j)
		name := stageNameForJob(j)
		key := strings.ToLower(name)
		agg, exists := stageAggregates[key]
		if !exists {
			agg = newStageAggregate(name)
			stageAggregates[key] = agg
			stageOrder = append(stageOrder, key)
		}
		agg.add(j)
	}

	stages := make([]apis.TaskStageDetail, 0, len(stageOrder))
	for _, key := range stageOrder {
		if agg := stageAggregates[key]; agg != nil {
			stages = append(stages, agg.finalize())
		}
	}

	taskStatus := string(task.Status)
	if task.Status == config.StatusRunning {
		if aggStatus, ok := aggregateTerminalJobStatus(jobInfos); ok {
			taskStatus = aggStatus
		}
	}

	return &apis.TaskStagesResponse{
		TaskID:              task.TaskID,
		Status:              taskStatus,
		WorkflowID:          task.WorkflowID,
		WorkflowName:        task.WorkflowName,
		AppID:               task.AppID,
		Type:                task.Type,
		PendingApprovalStep: task.PendingApprovalStep,
		Stages:              stages,
	}, nil
}

// chooseAggStatus merges two statuses, preferring failure/timeouts over running, over waiting.
func chooseAggStatus(current, incoming string) string {
	priority := func(status string) int {
		switch config.Status(status) {
		case config.StatusFailed, config.StatusTimeout, config.StatusReject:
			return 4
		case config.StatusCancelled:
			return 3
		case config.StatusRunning, config.StatusPrepare, config.StatusDistributed, config.StatusDebugBefore, config.StatusDebugAfter:
			return 2
		case config.StatusCompleted, config.StatusPassed:
			return 1
		default:
			return 0
		}
	}
	if priority(incoming) > priority(current) {
		return incoming
	}
	return current
}

func aggregateTerminalJobStatus(jobs []*model.JobInfo) (string, bool) {
	if len(jobs) == 0 {
		return "", false
	}
	status := ""
	for _, job := range jobs {
		if job == nil {
			continue
		}
		jobStatus := config.Status(strings.TrimSpace(job.Status))
		if jobStatus == "" || !isJobTerminalStatus(jobStatus) {
			return "", false
		}
		status = chooseAggStatus(status, normalizeTerminalJobStatus(jobStatus))
	}
	if status == "" {
		return "", false
	}
	return status, true
}

func normalizeTerminalJobStatus(status config.Status) string {
	switch status {
	case config.StatusSkipped, config.StatusPassed:
		return string(config.StatusCompleted)
	default:
		return string(status)
	}
}

// collectWorkflowComponentNames extracts all unique component names declared or implied in a workflow.
func collectWorkflowComponentNames(ctx context.Context, store datastore.DataStore, appID string, workflow *model.Workflow) []string {
	if workflow == nil || workflow.Steps == nil {
		return nil
	}
	raw, err := json.Marshal(workflow.Steps)
	if err != nil {
		klog.Errorf("marshal workflow steps for %s failed: %v", workflow.ID, err)
		return nil
	}
	var steps model.WorkflowSteps
	if err := json.Unmarshal(raw, &steps); err != nil {
		klog.Errorf("unmarshal workflow steps for %s failed: %v", workflow.ID, err)
		return nil
	}
	seen := make(map[string]struct{})
	var names []string
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		names = append(names, name)
	}
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		for _, n := range step.ComponentNames() {
			add(n)
		}
		for _, sub := range step.SubSteps {
			if sub == nil {
				continue
			}
			for _, n := range sub.ComponentNames() {
				add(n)
			}
		}
	}
	return names
}

func workflowRequiresComponents(workflow *model.Workflow) bool {
	if workflow == nil || workflow.Steps == nil {
		return false
	}
	var steps model.WorkflowSteps
	if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
		klog.Errorf("unmarshal workflow steps for %s failed: %v", workflow.ID, err)
		return false
	}
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeComponent {
			return true
		}
	}
	return false
}

func workflowHasNoSteps(workflow *model.Workflow) bool {
	if workflow == nil || workflow.Steps == nil {
		return false
	}
	var steps model.WorkflowSteps
	if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
		klog.Errorf("unmarshal workflow steps for %s failed: %v", workflow.ID, err)
		return false
	}
	return len(steps.Steps) == 0
}

func decodeJSONStruct(raw *model.JSONStruct, target interface{}) error {
	if raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	if string(data) == "null" {
		return nil
	}
	return json.Unmarshal(data, target)
}

func defaultComponentStatus(taskStatus config.Status) string {
	switch taskStatus {
	case config.StatusCancelled:
		return string(config.StatusCancelled)
	case config.StatusFailed, config.StatusTimeout, config.StatusReject:
		return string(taskStatus)
	case config.StatusCompleted:
		return string(config.StatusCompleted)
	default:
		return string(config.StatusWaiting)
	}
}

func (w *workflowServiceImpl) ExecWorkflowTaskForApp(ctx context.Context, appID, workflowID string, executeAt int64) (*apis.ExecWorkflowResponse, error) {
	return w.execWorkflowTaskForAppLocked(ctx, appID, workflowID, executeAt)
}

func (w *workflowServiceImpl) execWorkflowTaskForAppLocked(ctx context.Context, appID, workflowID string, executeAt int64) (*apis.ExecWorkflowResponse, error) {
	normalized, err := normalizeExecuteAt(executeAt)
	if err != nil {
		return nil, err
	}
	lockProvider, err := w.appScheduleLocker()
	if err != nil {
		return nil, err
	}
	var resp *apis.ExecWorkflowResponse
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, appID, "exec-workflow", true, func(lockCtx context.Context) error {
		txStore, ok := w.Store.(datastore.Transactional)
		if !ok {
			return fmt.Errorf("%w: workflow execution requires transactional datastore", bcode.ErrExecWorkflow)
		}
		return txStore.WithTransaction(lockCtx, func(tx datastore.DataStore) error {
			workflow, err := repository.WorkflowByID(lockCtx, tx, workflowID)
			if err != nil {
				return err
			}
			if workflow.AppID == "" || workflow.AppID != appID {
				return bcode.ErrWorkflowNotExist
			}
			if err := EnsureAppWorkflowIdle(lockCtx, tx, appID); err != nil {
				return err
			}
			if err := EnsureNoPendingStatefulSetCleanup(lockCtx, tx, appID); err != nil {
				return err
			}
			resp, err = w.enqueueWorkflowTaskWithStore(lockCtx, tx, workflow, normalized)
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (w *workflowServiceImpl) UpsertWorkflowSchedule(ctx context.Context, appID string, req apis.UpsertWorkflowScheduleRequest) (*apis.UpsertWorkflowScheduleResponse, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	lockProvider, err := w.appScheduleLocker()
	if err != nil {
		return nil, err
	}
	var resp *apis.UpsertWorkflowScheduleResponse
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, appID, "upsert-workflow-schedule", false, func(lockCtx context.Context) error {
		var lockErr error
		resp, lockErr = w.upsertWorkflowScheduleUnlocked(lockCtx, appID, req)
		return lockErr
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (w *workflowServiceImpl) upsertWorkflowScheduleUnlocked(ctx context.Context, appID string, req apis.UpsertWorkflowScheduleRequest) (*apis.UpsertWorkflowScheduleResponse, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	workflowID := strings.TrimSpace(req.WorkflowID)
	if workflowID == "" {
		return nil, bcode.ErrWorkflowConfig
	}
	app, err := repository.ApplicationByID(ctx, w.Store, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	if app.EffectiveManagementMode() == config.ManagementModeObserve {
		return nil, fmt.Errorf("%w: observe applications are read-only", bcode.ErrApplicationManagementMode)
	}
	normalizedCron, err := utils.NormalizeCronSchedule(req.Cron)
	if err != nil {
		return nil, bcode.ErrWorkflowConfig
	}
	workflow, err := repository.WorkflowByID(ctx, w.Store, workflowID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrWorkflowNotExist
		}
		return nil, err
	}
	if workflow.AppID == "" || workflow.AppID != appID {
		return nil, bcode.ErrWorkflowNotExist
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	var nextRun int64
	if enabled {
		nextRun, err = nextWorkflowScheduleRun(normalizedCron, time.Now().UTC())
		if err != nil {
			return nil, bcode.ErrWorkflowConfig
		}
	}

	schedule, err := repository.FindWorkflowScheduleByAppAndWorkflowID(ctx, w.Store, appID, workflowID)
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}
	if schedule == nil {
		schedule = &model.WorkflowSchedule{
			ID:         utils.RandStringByNumLowercase(24),
			AppID:      appID,
			WorkflowID: workflowID,
			Cron:       normalizedCron,
			Enabled:    enabled,
			NextRun:    nextRun,
			LastRun:    0,
		}
		if err := repository.CreateWorkflowSchedule(ctx, w.Store, schedule); err != nil {
			return nil, err
		}
	} else {
		schedule.Cron = normalizedCron
		schedule.Enabled = enabled
		if enabled {
			schedule.NextRun = nextRun
		} else {
			schedule.NextRun = 0
		}
		if err := repository.UpdateWorkflowSchedule(ctx, w.Store, schedule); err != nil {
			return nil, err
		}
	}

	dto := workflowScheduleToDTO(schedule, workflow)
	return &apis.UpsertWorkflowScheduleResponse{Schedule: dto}, nil
}

func (w *workflowServiceImpl) ListWorkflowSchedules(ctx context.Context, appID string) ([]apis.WorkflowSchedule, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	if _, err := repository.ApplicationByID(ctx, w.Store, appID); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	schedules, err := repository.FindWorkflowSchedulesByAppID(ctx, w.Store, appID)
	if err != nil {
		return nil, err
	}
	if len(schedules) == 0 {
		return []apis.WorkflowSchedule{}, nil
	}
	workflows, err := repository.FindWorkflowsByAppID(ctx, w.Store, appID)
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}
	workflowByID := make(map[string]*model.Workflow, len(workflows))
	for _, wf := range workflows {
		if wf == nil {
			continue
		}
		workflowByID[wf.ID] = wf
	}
	resp := make([]apis.WorkflowSchedule, 0, len(schedules))
	for _, schedule := range schedules {
		if schedule == nil {
			continue
		}
		resp = append(resp, workflowScheduleToDTO(schedule, workflowByID[schedule.WorkflowID]))
	}
	return resp, nil
}

func (w *workflowServiceImpl) DeleteWorkflowSchedule(ctx context.Context, appID, workflowID string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return bcode.ErrApplicationNotExist
	}
	lockProvider, err := w.appScheduleLocker()
	if err != nil {
		return err
	}
	return schedulelock.WithAppScheduleLock(ctx, lockProvider, appID, "delete-workflow-schedule", false, func(lockCtx context.Context) error {
		return w.deleteWorkflowScheduleUnlocked(lockCtx, appID, workflowID)
	})
}

func (w *workflowServiceImpl) deleteWorkflowScheduleUnlocked(ctx context.Context, appID, workflowID string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return bcode.ErrApplicationNotExist
	}
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		return bcode.ErrWorkflowConfig
	}
	schedule, err := repository.FindWorkflowScheduleByAppAndWorkflowID(ctx, w.Store, appID, workflowID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return bcode.ErrWorkflowNotExist
		}
		return err
	}
	return repository.DeleteWorkflowSchedule(ctx, w.Store, schedule)
}

func (w *workflowServiceImpl) DispatchWorkflowSchedules(ctx context.Context) (int, error) {
	txStore, ok := w.Store.(datastore.Transactional)
	if !ok {
		return 0, fmt.Errorf("workflow schedule dispatch requires transactional datastore")
	}
	schedules, err := repository.FindEnabledWorkflowSchedules(ctx, w.Store)
	if err != nil {
		return 0, err
	}
	if len(schedules) == 0 {
		return 0, nil
	}
	now := time.Now().UTC()
	processed := 0
	var dispatchErrs []error
	for _, schedule := range schedules {
		queued, err := w.dispatchWorkflowSchedule(ctx, txStore, schedule, now)
		if err != nil {
			if queued {
				processed++
			}
			scheduleID := "<nil>"
			if schedule != nil {
				scheduleID = schedule.ID
			}
			scheduleErr := fmt.Errorf("dispatch workflow schedule %s: %w", scheduleID, err)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return processed, scheduleErr
			}
			dispatchErrs = append(dispatchErrs, scheduleErr)
			continue
		}
		if queued {
			processed++
		}
	}
	if len(dispatchErrs) > 0 {
		return processed, errors.Join(dispatchErrs...)
	}
	return processed, nil
}

func (w *workflowServiceImpl) dispatchWorkflowSchedule(ctx context.Context, txStore datastore.Transactional, schedule *model.WorkflowSchedule, now time.Time) (bool, error) {
	if schedule == nil || !schedule.Enabled {
		return false, nil
	}
	if schedule.Cron == "" {
		return false, nil
	}
	nowUnix := now.Unix()
	if schedule.NextRun <= 0 {
		nextRun, err := nextWorkflowScheduleRun(schedule.Cron, now)
		if err != nil {
			return false, err
		}
		err = txStore.WithTransaction(ctx, func(tx datastore.DataStore) error {
			claimed, err := repository.UpdateWorkflowScheduleNextRun(ctx, tx, schedule.ID, schedule.NextRun, nextRun)
			if err != nil {
				return err
			}
			if !claimed {
				return errScheduleNotClaimed
			}
			return nil
		})
		if errors.Is(err, errScheduleNotClaimed) {
			return false, nil
		}
		return false, err
	}
	if schedule.NextRun > nowUnix {
		return false, nil
	}
	claimedNextRun := schedule.NextRun
	nextRun, err := nextWorkflowScheduleRun(schedule.Cron, now)
	if err != nil {
		return false, err
	}
	lockProvider, err := w.appScheduleLocker()
	if err != nil {
		return false, err
	}

	queued := false
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, schedule.AppID, "dispatch-workflow-schedule", true, func(lockCtx context.Context) error {
		return txStore.WithTransaction(lockCtx, func(tx datastore.DataStore) error {
			claimed, err := repository.UpdateWorkflowScheduleNextRun(lockCtx, tx, schedule.ID, schedule.NextRun, nextRun)
			if err != nil {
				return err
			}
			if !claimed {
				return errScheduleNotClaimed
			}
			if err := EnsureAppWorkflowIdle(lockCtx, tx, schedule.AppID); err != nil {
				if errors.Is(err, bcode.ErrWorkflowTaskRunning) || errors.Is(err, bcode.ErrWorkflowTaskCancelling) {
					return nil
				}
				return err
			}
			if err := EnsureNoPendingStatefulSetCleanup(lockCtx, tx, schedule.AppID); err != nil {
				if errors.Is(err, bcode.ErrApplicationConfig) ||
					errors.Is(err, bcode.ErrWorkflowTaskRunning) ||
					errors.Is(err, bcode.ErrWorkflowTaskCancelling) {
					return nil
				}
				return err
			}
			workflow, err := repository.WorkflowByID(lockCtx, tx, schedule.WorkflowID)
			if err != nil {
				return err
			}
			idempotencyKey := workflowScheduleIdempotencyKey(schedule.ID, claimedNextRun)
			if _, err := w.enqueueWorkflowTaskWithStoreAndIdempotencyKey(lockCtx, tx, workflow, 0, idempotencyKey); err != nil {
				return err
			}
			if err := repository.UpdateWorkflowScheduleLastRun(lockCtx, tx, schedule.ID, nowUnix); err != nil {
				return err
			}
			queued = true
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, errScheduleNotClaimed) || errors.Is(err, bcode.ErrApplicationOperationLocked) {
			return false, nil
		}
		return false, err
	}
	return queued, nil
}

func nextWorkflowScheduleRun(spec string, base time.Time) (int64, error) {
	next, err := workflowScheduleParser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return next.Next(base.UTC()).Unix(), nil
}

func workflowScheduleIdempotencyKey(scheduleID string, nextRun int64) string {
	return fmt.Sprintf("workflow-schedule:%s:%d", strings.TrimSpace(scheduleID), nextRun)
}

func workflowScheduleToDTO(schedule *model.WorkflowSchedule, workflow *model.Workflow) apis.WorkflowSchedule {
	dto := apis.WorkflowSchedule{
		ID:         schedule.ID,
		AppID:      schedule.AppID,
		WorkflowID: schedule.WorkflowID,
		Cron:       schedule.Cron,
		Enabled:    schedule.Enabled,
		NextRun:    schedule.NextRun,
		LastRun:    schedule.LastRun,
		CreateTime: schedule.CreateTime,
		UpdateTime: schedule.UpdateTime,
	}
	if workflow != nil {
		dto.WorkflowName = workflow.Name
		dto.WorkflowAlias = workflow.Alias
	}
	return dto
}

func (w *workflowServiceImpl) WaitingTasks(ctx context.Context) ([]*model.WorkflowQueue, error) {
	list, err := repository.WaitingTasks(ctx, w.Store)
	if err != nil {
		return nil, err
	}
	return list, err
}

func (w *workflowServiceImpl) UpdateTask(ctx context.Context, task *model.WorkflowQueue) bool {
	err := repository.UpdateTask(ctx, w.Store, task)
	if err != nil {
		klog.Errorf("%s:%s update t status error", task.WorkflowName, task.TaskID)
		return false
	}
	return true
}

func (w *workflowServiceImpl) MarkTaskStatus(ctx context.Context, taskID string, from, to config.Status) (bool, error) {
	if from == config.StatusWaiting && to == config.StatusQueued {
		return w.claimWaitingTaskForDispatch(ctx, taskID)
	}
	return repository.UpdateTaskStatus(ctx, w.Store, taskID, from, to)
}

func (w *workflowServiceImpl) claimWaitingTaskForDispatch(ctx context.Context, taskID string) (bool, error) {
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return false, nil
		}
		return false, err
	}
	if task.Status != config.StatusWaiting {
		return false, nil
	}
	// Immediate and cron tasks are created while holding this same application
	// lock and remain active for idle checks. Only delayed tasks can outlive the
	// safety decision made when they were originally enqueued.
	if task.ExecuteAt <= 0 {
		return repository.UpdateTaskStatus(ctx, w.Store, taskID, config.StatusWaiting, config.StatusQueued)
	}
	lockProvider, err := w.appScheduleLocker()
	if err != nil {
		return false, err
	}
	claimed := false
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, task.AppID, "dispatch-workflow-task", true, func(lockCtx context.Context) error {
		current, err := repository.TaskByID(lockCtx, w.Store, taskID)
		if err != nil {
			if errors.Is(err, datastore.ErrRecordNotExist) {
				return nil
			}
			return err
		}
		if current.Status != config.StatusWaiting {
			return nil
		}
		if current.AppID == "" || current.AppID != task.AppID {
			return fmt.Errorf("workflow task %s changed application from %q to %q", taskID, task.AppID, current.AppID)
		}
		_, migrationTask, err := statefulSetCleanupInfoForFence(current)
		if err != nil {
			return err
		}
		// A v2/v3 migration task is the operation that resolves the fence. Its
		// cleanup contract was validated while the version update held this lock.
		if !migrationTask {
			if err := EnsureNoPendingStatefulSetCleanup(lockCtx, w.Store, current.AppID); err != nil {
				return err
			}
		}
		claimed, err = repository.UpdateTaskStatus(lockCtx, w.Store, taskID, config.StatusWaiting, config.StatusQueued)
		return err
	})
	if err != nil {
		return false, err
	}
	return claimed, nil
}

// TaskRunning 所有正在运行的Task
func (w *workflowServiceImpl) TaskRunning(ctx context.Context) ([]*model.WorkflowQueue, error) {
	list, err := repository.TaskRunning(ctx, w.Store)
	if err != nil {
		return nil, err
	}
	return list, err
}

func (w *workflowServiceImpl) CancelWorkflowTask(ctx context.Context, userName, taskID, reason string) error {
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		return err
	}
	return w.cancelWorkflowTask(ctx, task, userName, reason)
}

func (w *workflowServiceImpl) CancelWorkflowTaskForApp(ctx context.Context, appID, userName, taskID, reason string) error {
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		return err
	}
	if task.AppID == "" || task.AppID != appID {
		return bcode.ErrWorkflowNotExist
	}
	return w.cancelWorkflowTask(ctx, task, userName, reason)
}

func (w *workflowServiceImpl) CancelAllWorkflowTasksForApp(ctx context.Context, appID, userName, reason string) ([]string, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	userName = strings.TrimSpace(userName)
	if userName == "" {
		userName = config.DefaultTaskRevoker
	}

	tasks, err := repository.FindWorkflowTasksByAppID(ctx, w.Store, appID)
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}
	cancelledTaskIDs := make([]string, 0, len(tasks))
	for _, task := range tasks {
		shouldCancel, err := w.shouldCancelAppTask(ctx, appID, task)
		if err != nil {
			return cancelledTaskIDs, err
		}
		if !shouldCancel {
			continue
		}
		if err := w.cancelWorkflowTask(ctx, task, userName, reason); err != nil {
			if errors.Is(err, bcode.ErrWorkflowTaskNotCancellable) {
				continue
			}
			return cancelledTaskIDs, fmt.Errorf("cancel workflow task %s: %w", task.TaskID, err)
		}
		cancelledTaskIDs = append(cancelledTaskIDs, task.TaskID)
	}
	return cancelledTaskIDs, nil
}

func (w *workflowServiceImpl) shouldCancelAppTask(ctx context.Context, appID string, task *model.WorkflowQueue) (bool, error) {
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return false, nil
	}
	if task.AppID == "" || task.AppID != appID {
		return false, nil
	}
	return w.shouldCancelTask(ctx, task)
}

func (w *workflowServiceImpl) shouldCancelTask(ctx context.Context, task *model.WorkflowQueue) (bool, error) {
	return w.shouldCancelTaskWithStore(ctx, w.Store, task)
}

func (w *workflowServiceImpl) shouldCancelTaskWithStore(ctx context.Context, store datastore.DataStore, task *model.WorkflowQueue) (bool, error) {
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return false, nil
	}
	if task.Status == "" || isWorkflowActiveStatus(task.Status) {
		return true, nil
	}
	if task.Status != config.StatusCancelled {
		return false, nil
	}
	return taskHasActiveJobs(ctx, store, task.TaskID)
}

func (w *workflowServiceImpl) CancelDelayedVersionTaskForApp(ctx context.Context, appID, userName, taskID, reason string) error {
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		return err
	}
	if task.AppID == "" || task.AppID != appID {
		return bcode.ErrWorkflowNotExist
	}
	now := time.Now().Unix()
	if task.Status != config.StatusWaiting || task.ExecuteAt <= now {
		return bcode.ErrVersionUpdateTaskNotCancellable
	}
	return w.cancelWorkflowTaskIfStatus(ctx, task, userName, reason, config.StatusWaiting, bcode.ErrVersionUpdateTaskNotCancellable)
}

func (w *workflowServiceImpl) ApproveWorkflowTask(ctx context.Context, taskID, action, userName, reason string) (*apis.TaskApprovalResponse, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, bcode.ErrWorkflowTaskNotExist
	}
	normalizedAction := strings.ToLower(strings.TrimSpace(action))
	if normalizedAction != workflowApprovalActionContinue && normalizedAction != workflowApprovalActionCancel {
		return nil, bcode.ErrWorkflowApprovalActionInvalid
	}
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrWorkflowTaskNotExist
		}
		return nil, err
	}
	if !task.ApprovalPending {
		return nil, bcode.ErrWorkflowTaskNotAwaitingApproval
	}
	if task.Status != config.StatusWaitingApprove && task.Status != config.StatusWaiting {
		return nil, bcode.ErrWorkflowTaskNotAwaitingApproval
	}
	expectedCondition := repository.ApproveTaskCondition{
		ApprovalPending:     true,
		Status:              task.Status,
		CurrentStep:         task.CurrentStep,
		PendingApprovalStep: task.PendingApprovalStep,
	}

	if normalizedAction == workflowApprovalActionContinue {
		nextStep := task.CurrentStep
		if nextStep < 0 {
			nextStep = 0
		}
		nextStep++
		updates := map[string]interface{}{
			"approval_pending":      false,
			"pending_approval_step": "",
			"status":                config.StatusWaiting,
			"current_step":          nextStep,
		}
		swapped, err := repository.ApproveTaskCAS(ctx, w.Store, taskID, expectedCondition, updates)
		if err != nil {
			return nil, err
		}
		if !swapped {
			return nil, bcode.ErrWorkflowTaskNotAwaitingApproval
		}
		approvaltimeout.Cancel(taskID)
		return &apis.TaskApprovalResponse{
			TaskID: task.TaskID,
			Action: normalizedAction,
			Status: string(config.StatusWaiting),
		}, nil
	}

	user := strings.TrimSpace(userName)
	if user == "" {
		user = config.DefaultTaskRevoker
	}
	cancelReason := strings.TrimSpace(reason)
	if cancelReason == "" {
		cancelReason = fmt.Sprintf("cancelled by %s", user)
	}
	cancelUpdates := map[string]interface{}{
		"approval_pending":      false,
		"pending_approval_step": "",
		"status":                config.StatusCancelled,
		"task_revoker":          user,
		"cancel_source":         config.CancelSourceUser,
	}
	if err := runCancelledTaskStoreUpdate(ctx, w.Store, taskID, cancelReason, func(store datastore.DataStore) error {
		swapped, err := repository.ApproveTaskCAS(ctx, store, taskID, expectedCondition, cancelUpdates)
		if err != nil {
			return err
		}
		if !swapped {
			return bcode.ErrWorkflowTaskNotAwaitingApproval
		}
		return nil
	}); err != nil {
		return nil, err
	}
	approvaltimeout.Cancel(taskID)
	task.Status = config.StatusCancelled
	task.ApprovalPending = false
	task.PendingApprovalStep = ""
	task.TaskRevoker = user
	task.CancelSource = config.CancelSourceUser
	w.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, config.StatusCancelled, cancelReason)
	return &apis.TaskApprovalResponse{
		TaskID: task.TaskID,
		Action: normalizedAction,
		Status: string(config.StatusCancelled),
	}, nil
}

func (w *workflowServiceImpl) cancelWorkflowTask(ctx context.Context, task *model.WorkflowQueue, userName, reason string) error {
	return w.cancelWorkflowTaskIfStatus(ctx, task, userName, reason, "", nil)
}

func (w *workflowServiceImpl) cancelWorkflowTaskIfStatus(ctx context.Context, task *model.WorkflowQueue, userName, reason string, expectedStatus config.Status, conflictErr error) error {
	// Audit log: record cancel operation with full context
	klog.Infof("AUDIT: cancel workflow task taskID=%s workflowID=%s workflowName=%s user=%s reason=%s prevStatus=%s",
		task.TaskID, task.WorkflowID, task.WorkflowName, userName, reason, task.Status)
	cancellable, err := w.shouldCancelTask(ctx, task)
	if err != nil {
		return err
	}
	if !cancellable {
		klog.Warningf("AUDIT: cancel workflow task rejected due terminal status taskID=%s user=%s status=%s", task.TaskID, userName, task.Status)
		return bcode.ErrWorkflowTaskNotCancellable
	}
	approvalPausedForCallback := shouldTriggerTerminalCallbackOnCancel(task)
	redisClient, cancelSignalErr := cancelsignal.RedisClientForCancelSignal(ctx, w.Cache)
	if cancelSignalErr != nil && !approvalPausedForCallback {
		if expectedStatus != "" && conflictErr != nil {
			current, stateErr := repository.TaskByID(ctx, w.Store, task.TaskID)
			switch {
			case errors.Is(stateErr, datastore.ErrRecordNotExist):
				klog.Warningf("AUDIT: cancel workflow task rejected because task disappeared during cancel signal preflight taskID=%s user=%s", task.TaskID, userName)
				return conflictErr
			case stateErr == nil && (current == nil || current.Status != expectedStatus):
				klog.Warningf("AUDIT: cancel workflow task rejected due status drift during cancel signal preflight taskID=%s user=%s expectedStatus=%s", task.TaskID, userName, expectedStatus)
				return conflictErr
			case stateErr != nil:
				klog.Warningf("AUDIT: cancel workflow task state recheck failed after cancel signal backend error taskID=%s user=%s error=%v", task.TaskID, userName, stateErr)
			}
		}
		klog.Errorf("AUDIT: cancel workflow task missing cancel signal backend taskID=%s user=%s error=%v", task.TaskID, userName, cancelSignalErr)
		return cancelSignalErr
	}
	updates := map[string]interface{}{
		"task_revoker":          userName,
		"status":                config.StatusCancelled,
		"cancel_source":         config.CancelSourceUser,
		"approval_pending":      false,
		"pending_approval_step": "",
	}
	if reason == "" {
		reason = fmt.Sprintf("cancelled by %s", userName)
	}
	cancelledFromApprovalPause := approvalPausedForCallback
	if err := runCancelledTaskStoreUpdate(ctx, w.Store, task.TaskID, reason, func(store datastore.DataStore) error {
		if expectedStatus != "" {
			swapped, err := repository.UpdateTaskFieldsIfStatus(ctx, store, task.TaskID, expectedStatus, updates)
			if err != nil {
				return err
			}
			if !swapped {
				if conflictErr != nil {
					return conflictErr
				}
				return datastore.ErrRecordNotExist
			}
			return nil
		}
		var err error
		cancelledFromApprovalPause, err = w.cancelWorkflowTaskState(ctx, store, task, userName, updates, cancelSignalErr)
		return err
	}); err != nil {
		klog.Errorf("AUDIT: cancel workflow task failed taskID=%s user=%s error=%v", task.TaskID, userName, err)
		return err
	}
	approvalPausedForCallback = cancelledFromApprovalPause
	task.TaskRevoker = userName
	task.Status = config.StatusCancelled
	task.CancelSource = config.CancelSourceUser
	task.ApprovalPending = false
	task.PendingApprovalStep = ""
	approvaltimeout.Cancel(task.TaskID)
	if redisClient == nil {
		var err error
		redisClient, err = cancelsignal.RedisClientForCancelSignal(ctx, w.Cache)
		if err != nil {
			// Approval-paused tasks are already terminally cancelled in storage.
			// Best-effort signal publish failure should not fail API response or suppress callbacks.
			klog.Warningf("AUDIT: signal cancel skipped for approval-paused task taskID=%s user=%s error=%v", task.TaskID, userName, err)
			if approvalPausedForCallback {
				w.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, config.StatusCancelled, reason)
			}
			klog.Infof("AUDIT: cancel workflow task completed taskID=%s user=%s", task.TaskID, userName)
			return nil
		}
	}
	if err := cancelsignal.PublishWorkflowCancelSignal(ctx, task.TaskID, reason, redisClient); err != nil {
		if approvalPausedForCallback {
			// Approval-paused tasks are already terminally cancelled in storage.
			// Best-effort signal publish failure should not fail API response or suppress callbacks.
			klog.Warningf("AUDIT: signal cancel skipped for approval-paused task taskID=%s user=%s error=%v", task.TaskID, userName, err)
		} else {
			klog.Errorf("AUDIT: signal cancel failed taskID=%s user=%s error=%v", task.TaskID, userName, err)
			return err
		}
	}
	if approvalPausedForCallback {
		w.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, config.StatusCancelled, reason)
	}

	klog.Infof("AUDIT: cancel workflow task completed taskID=%s user=%s", task.TaskID, userName)
	return nil
}

func (w *workflowServiceImpl) cancelWorkflowTaskState(
	ctx context.Context,
	store datastore.DataStore,
	task *model.WorkflowQueue,
	userName string,
	updates map[string]interface{},
	cancelSignalErr error,
) (bool, error) {
	current := task
	for attempt := 0; attempt < cancelWorkflowTaskCASMaxAttempts; attempt++ {
		cancellable, err := w.shouldCancelTaskWithStore(ctx, store, current)
		if err != nil {
			return false, err
		}
		if !cancellable {
			return false, bcode.ErrWorkflowTaskNotCancellable
		}
		if workflowTaskCancellationMatches(current, userName) {
			*task = *current
			return false, nil
		}

		cancelledFromApprovalPause := current.ApprovalPending
		if !cancelledFromApprovalPause && cancelSignalErr != nil {
			return false, cancelSignalErr
		}

		var swapped bool
		if cancelledFromApprovalPause {
			expectedCondition := repository.ApproveTaskCondition{
				ApprovalPending:     true,
				Status:              current.Status,
				CurrentStep:         current.CurrentStep,
				PendingApprovalStep: current.PendingApprovalStep,
			}
			swapped, err = repository.ApproveTaskCAS(ctx, store, current.TaskID, expectedCondition, updates)
		} else {
			swapped, err = repository.UpdateTaskFieldsIfStatus(ctx, store, current.TaskID, current.Status, updates)
		}
		if err != nil {
			return false, err
		}
		if swapped {
			return cancelledFromApprovalPause, nil
		}

		latest, err := repository.TaskByID(ctx, store, current.TaskID)
		if err != nil {
			return false, err
		}
		*task = *latest
		current = task

		cancellable, err = w.shouldCancelTaskWithStore(ctx, store, current)
		if err != nil {
			return false, err
		}
		if !cancellable {
			return false, bcode.ErrWorkflowTaskNotCancellable
		}
		if workflowTaskCancellationMatches(current, userName) {
			return false, nil
		}
		if attempt == cancelWorkflowTaskCASMaxAttempts-1 {
			return false, bcode.ErrWorkflowTaskCancelConflict
		}
	}
	return false, bcode.ErrWorkflowTaskCancelConflict
}

func workflowTaskCancellationMatches(task *model.WorkflowQueue, userName string) bool {
	return task != nil &&
		task.Status == config.StatusCancelled &&
		task.TaskRevoker == userName &&
		task.CancelSource == config.CancelSourceUser &&
		!task.ApprovalPending &&
		task.PendingApprovalStep == ""
}

func runCancelledTaskStoreUpdate(ctx context.Context, store datastore.DataStore, taskID, reason string, updateTask func(datastore.DataStore) error) error {
	run := func(tx datastore.DataStore) error {
		if updateTask != nil {
			if err := updateTask(tx); err != nil {
				return err
			}
		}
		if err := TerminalizePrecreatedVersionUpdateCleanupJobs(ctx, tx, taskID, config.StatusCancelled, reason); err != nil {
			return fmt.Errorf("terminalize version update cleanup jobs: %w", err)
		}
		return nil
	}
	if txStore, ok := store.(datastore.Transactional); ok {
		return txStore.WithTransaction(ctx, run)
	}
	return run(store)
}

// TerminalizePrecreatedVersionUpdateCleanupJobs marks version-update cleanup jobs
// that were persisted before execution when their workflow exits before running them.
func TerminalizePrecreatedVersionUpdateCleanupJobs(ctx context.Context, store datastore.DataStore, taskID string, targetStatus config.Status, reason string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || store == nil {
		return nil
	}
	targetStatus = config.Status(strings.TrimSpace(string(targetStatus)))
	if !shouldTerminalizePrecreatedCleanupTargetStatus(targetStatus) {
		return nil
	}
	opts := &datastore.ListOptions{}
	opts.FilterOptions.In = append(opts.FilterOptions.In, datastore.InQueryOption{
		Key:    "type",
		Values: []string{string(config.JobCleanupResources)},
	})
	entities, err := store.List(ctx, &model.JobInfo{TaskID: taskID}, opts)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil
		}
		return err
	}
	now := time.Now().Unix()
	for _, entity := range entities {
		jobInfo, ok := entity.(*model.JobInfo)
		if !ok || jobInfo == nil {
			return datastore.ErrEntityInvalid
		}
		marked, err := isVersionUpdateRemoveCleanupJobInfo(jobInfo)
		if err != nil {
			return err
		}
		if !marked || !shouldTerminalizePrecreatedCleanupStatus(config.Status(strings.TrimSpace(jobInfo.Status))) {
			continue
		}
		updates := map[string]interface{}{
			"status":   string(targetStatus),
			"end_time": now,
			"error":    strings.TrimSpace(reason),
		}
		updated, err := store.CompareAndSwap(ctx, jobInfo, "status", jobInfo.Status, updates)
		if err != nil {
			return err
		}
		if !updated {
			continue
		}
		jobInfo.Status = string(targetStatus)
		jobInfo.EndTime = now
		jobInfo.Error = strings.TrimSpace(reason)
	}
	return nil
}

func isVersionUpdateRemoveCleanupJobInfo(jobInfo *model.JobInfo) (bool, error) {
	if jobInfo == nil || strings.TrimSpace(jobInfo.Type) != string(config.JobCleanupResources) {
		return false, nil
	}
	raw := strings.TrimSpace(jobInfo.InternalInfo)
	if raw == "" {
		return false, nil
	}
	var marker struct {
		Source string `json:"source"`
	}
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		return false, nil
	}
	return marker.Source == config.JobInfoSourceVersionUpdateRemove, nil
}

func shouldTerminalizePrecreatedCleanupStatus(status config.Status) bool {
	switch status {
	case "", config.StatusCreated, config.StatusQueued, config.StatusWaiting, config.QueueItemPending:
		return true
	default:
		return false
	}
}

func shouldTerminalizePrecreatedCleanupTargetStatus(status config.Status) bool {
	switch status {
	case config.StatusCancelled, config.StatusFailed, config.StatusTimeout, config.StatusReject:
		return true
	default:
		return false
	}
}

func shouldTriggerTerminalCallbackOnCancel(task *model.WorkflowQueue) bool {
	if task == nil || !task.ApprovalPending {
		return false
	}
	switch task.Status {
	case config.StatusWaitingApprove, config.StatusWaiting, config.StatusQueued:
		return true
	default:
		return false
	}
}

func (w *workflowServiceImpl) triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx context.Context, task *model.WorkflowQueue, status config.Status, reason string) {
	if w == nil || task == nil {
		return
	}
	taskSnapshot := *task
	callbackCtx := detachWorkflowCallbackParentContext(ctx)
	go w.triggerWorkflowTerminalCallbackOnApprovalAction(callbackCtx, &taskSnapshot, status, reason)
}

func TriggerWorkflowTerminalCallbackAsync(ctx context.Context, store datastore.DataStore, cfg *config.Config, provider *urlpolicy.Provider, task *model.WorkflowQueue, status config.Status, reason string) {
	svc := &workflowServiceImpl{
		Store:                     store,
		Cfg:                       cfg,
		URLSecurityPolicyProvider: provider,
	}
	svc.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, status, reason)
}

func (w *workflowServiceImpl) triggerWorkflowTerminalCallbackOnApprovalAction(ctx context.Context, task *model.WorkflowQueue, status config.Status, reason string) {
	if w == nil || w.Store == nil || task == nil {
		return
	}
	workflowID := strings.TrimSpace(task.WorkflowID)
	parentCtx := inheritWorkflowCallbackParentContext(ctx)
	callbackSource := model.WorkflowCallbackSource(task, nil, nil)
	if callbackSource == nil {
		if workflowID == "" {
			return
		}
		workflow := &model.Workflow{ID: workflowID}
		loadCtx, cancel := inheritWorkflowCallbackTimeout(parentCtx, 5*time.Second)
		defer cancel()
		if err := w.Store.Get(loadCtx, workflow); err != nil {
			klog.Errorf("load workflow %s for callback failed: %v", workflowID, err)
			return
		}
		callbackSource = model.WorkflowCallbackSource(nil, workflow, nil)
		if callbackSource == nil && strings.TrimSpace(task.AppID) != "" {
			app := &model.Applications{ID: task.AppID}
			if err := w.Store.Get(loadCtx, app); err != nil {
				klog.Errorf("load application %s for callback failed: %v", task.AppID, err)
				return
			}
			callbackSource = model.WorkflowCallbackSource(nil, workflow, app)
		}
	}
	if callbackSource == nil {
		return
	}
	var callback model.WorkflowCallback
	if err := decodeWorkflowCallbackForTerminal(callbackSource, &callback); err != nil {
		klog.Errorf("decode workflow %s callback failed: %v", workflowID, err)
		return
	}
	event, targetURL := callbackTargetForTerminalStatus(&callback, status)
	if targetURL == "" {
		return
	}
	method := callbackMethodForTerminalEvent(&callback, event)
	payload := workflowjob.CallbackPayload{
		Event:        event,
		Status:       string(status),
		AppID:        task.AppID,
		WorkflowID:   task.WorkflowID,
		WorkflowName: task.WorkflowName,
		TaskID:       task.TaskID,
		WorkflowType: task.Type,
		StartTime:    task.CreateTime.Unix(),
		EndTime:      time.Now().Unix(),
		Reason:       strings.TrimSpace(reason),
	}
	callbackTimeoutMax := config.ResolveWorkflowCallbackTimeoutMax(w.Cfg)
	callbackTimeoutSeconds := config.ClampWorkflowCallbackTimeoutSeconds(callback.TimeoutSeconds, callbackTimeoutMax)
	callbackJob := &model.JobTask{
		Name:       fmt.Sprintf("workflow-callback-%s", task.TaskID),
		WorkflowID: task.WorkflowID,
		ProjectID:  task.ProjectID,
		AppID:      task.AppID,
		TaskID:     task.TaskID,
		JobType:    string(config.JobDeployCallback),
		JobInfo: &workflowjob.CallbackJobInfo{
			Event:          event,
			URL:            targetURL,
			Method:         method,
			Headers:        callback.Headers,
			TimeoutSeconds: callbackTimeoutSeconds,
			TimeoutMaxSec:  config.ResolveWorkflowCallbackTimeoutMaxSeconds(callbackTimeoutMax),
			TimeoutMaxNS:   int64(callbackTimeoutMax),
			Payload:        payload,
		},
		Status: config.StatusRunning,
	}
	callbackTimeout := config.ResolveWorkflowCallbackTimeout(callback.TimeoutSeconds, callbackTimeoutMax)
	runCtx, runCancel := inheritWorkflowCallbackTimeout(parentCtx, callbackTimeout)
	defer runCancel()
	callbackJob.StartTime = time.Now().Unix()
	urlPolicy, err := urlpolicy.ResolvePolicy(parentCtx, w.URLSecurityPolicyProvider)
	if err != nil {
		callbackJob.Status = config.StatusFailed
		callbackJob.Error = fmt.Sprintf("load url security policy: %v", err)
		callbackJob.EndTime = time.Now().Unix()
		callbackCtl := workflowjob.NewCallbackJobCtl(callbackJob, w.Store, nil)
		if callbackCtl != nil {
			if saveErr := callbackCtl.SaveInfo(parentCtx); saveErr != nil {
				klog.Warningf("save callback job info failed for workflow %s task %s: %v", task.WorkflowID, task.TaskID, saveErr)
			}
		}
		return
	}
	callbackCtl := workflowjob.NewCallbackJobCtl(callbackJob, w.Store, urlPolicy)
	if callbackCtl == nil {
		klog.Warningf("init callback job controller failed for workflow %s task %s", task.WorkflowID, task.TaskID)
		return
	}
	if err := callbackCtl.Run(runCtx); err != nil {
		callbackJob.Error = err.Error()
		if callbackJob.Status == config.StatusRunning {
			callbackJob.Status = config.StatusFailed
		}
	} else if callbackJob.Status == config.StatusRunning {
		callbackJob.Status = config.StatusCompleted
	}
	callbackJob.EndTime = time.Now().Unix()
	saveCtx, saveCancel := inheritWorkflowCallbackTimeout(parentCtx, 5*time.Second)
	defer saveCancel()
	if err := callbackCtl.SaveInfo(saveCtx); err != nil {
		klog.Warningf("save callback job info failed for workflow %s task %s: %v", task.WorkflowID, task.TaskID, err)
	}
}

func inheritWorkflowCallbackParentContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func detachWorkflowCallbackParentContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func inheritWorkflowCallbackTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(inheritWorkflowCallbackParentContext(ctx), timeout)
}

func decodeWorkflowCallbackForTerminal(raw *model.JSONStruct, target interface{}) error {
	if raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func callbackTargetForTerminalStatus(callback *model.WorkflowCallback, status config.Status) (string, string) {
	if callback == nil {
		return "", ""
	}
	switch status {
	case config.StatusCompleted, config.StatusPassed:
		return "success", strings.TrimSpace(callback.Success)
	case config.StatusCancelled:
		if callback.Cancelled != "" {
			return "cancelled", strings.TrimSpace(callback.Cancelled)
		}
		return "failure", strings.TrimSpace(callback.Failure)
	case config.StatusTimeout:
		if callback.Timeout != "" {
			return "timeout", strings.TrimSpace(callback.Timeout)
		}
		return "failure", strings.TrimSpace(callback.Failure)
	case config.StatusReject:
		if callback.Reject != "" {
			return "reject", strings.TrimSpace(callback.Reject)
		}
		return "failure", strings.TrimSpace(callback.Failure)
	case config.StatusFailed:
		return "failure", strings.TrimSpace(callback.Failure)
	default:
		return "", ""
	}
}

func callbackMethodForTerminalEvent(callback *model.WorkflowCallback, event string) string {
	if callback == nil || len(callback.Methods) == 0 || event == "" {
		return ""
	}
	method, ok := callback.Methods[strings.ToLower(event)]
	if !ok {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(method))
}

func (w *workflowServiceImpl) enqueueWorkflowTask(ctx context.Context, workflow *model.Workflow, executeAt int64) (*apis.ExecWorkflowResponse, error) {
	return w.enqueueWorkflowTaskWithStore(ctx, w.Store, workflow, executeAt)
}

func (w *workflowServiceImpl) enqueueWorkflowTaskWithStore(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64) (*apis.ExecWorkflowResponse, error) {
	return w.enqueueWorkflowTaskWithStoreAndIdempotencyKey(ctx, store, workflow, executeAt, "")
}

func (w *workflowServiceImpl) enqueueWorkflowTaskWithStoreAndIdempotencyKey(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey string) (*apis.ExecWorkflowResponse, error) {
	if err := validateWorkflowTaskEnqueue(ctx, store, workflow, true); err != nil {
		return nil, err
	}

	workflowTask, err := createWorkflowQueueTask(ctx, store, workflow, executeAt, idempotencyKey)
	if err != nil {
		return nil, err
	}
	return &apis.ExecWorkflowResponse{TaskID: workflowTask.TaskID}, nil
}

func validateWorkflowTaskEnqueue(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, requireComponentInventory bool) error {
	if workflow == nil || workflow.Steps == nil {
		return bcode.ErrExecWorkflow
	}

	if workflow.Disabled {
		return bcode.ErrExecWorkflow
	}

	if workflowHasNoSteps(workflow) {
		return bcode.ErrWorkflowEmpty
	}

	if err := validateWorkflowTaskJobTypes(workflow); err != nil {
		return err
	}
	if workflow.AppID != "" {
		app, err := repository.ApplicationByID(ctx, store, workflow.AppID)
		if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
			return err
		}
		if app != nil {
			switch app.EffectiveManagementMode() {
			case config.ManagementModeObserve:
				return fmt.Errorf("%w: observe applications are read-only", bcode.ErrApplicationManagementMode)
			case config.ManagementModeAdopted:
				if workflowContainsJobType(workflow, config.JobDatabaseReset) ||
					workflowContainsJobType(workflow, config.JobCleanupResources) {
					return fmt.Errorf("%w: adopted workflows cannot reset databases or perform unfingerprinted cleanup", bcode.ErrApplicationManagementMode)
				}
			}
		}
	}

	if requireComponentInventory && workflow.AppID != "" && workflowRequiresComponents(workflow) {
		components, err := repository.FindComponentsByAppID(ctx, store, workflow.AppID)
		if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
			return err
		}
		if len(components) == 0 {
			return bcode.ErrApplicationNoComponents
		}
	}
	return nil
}

func workflowContainsJobType(workflow *model.Workflow, target config.JobType) bool {
	if workflow == nil || workflow.Steps == nil {
		return false
	}
	var steps model.WorkflowSteps
	if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
		return false
	}
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if step.WorkflowType == target {
			return true
		}
		for _, sub := range step.SubSteps {
			if sub != nil && sub.WorkflowType == target {
				return true
			}
		}
	}
	return false
}

func validateWorkflowTaskJobTypes(workflow *model.Workflow) error {
	if workflow == nil || workflow.Steps == nil {
		return nil
	}
	var steps model.WorkflowSteps
	if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
		return fmt.Errorf("%w: decode workflow steps: %v", bcode.ErrWorkflowConfig, err)
	}
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeApproval {
			continue
		}
		if !config.IsSupportedWorkflowJobType(step.WorkflowType) {
			return fmt.Errorf("%w: workflow step %q has unsupported jobType %q", bcode.ErrWorkflowConfig, step.Name, step.WorkflowType)
		}
		for _, sub := range step.SubSteps {
			if sub == nil {
				continue
			}
			if !config.IsSupportedWorkflowJobType(sub.WorkflowType) {
				return fmt.Errorf("%w: workflow step %q has unsupported jobType %q", bcode.ErrWorkflowConfig, sub.Name, sub.WorkflowType)
			}
		}
	}
	return nil
}

func ValidateWorkflowTaskEnqueue(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, requireComponentInventory bool) error {
	return validateWorkflowTaskEnqueue(ctx, store, workflow, requireComponentInventory)
}

func createWorkflowQueueTask(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey string) (*model.WorkflowQueue, error) {
	return createWorkflowQueueTaskWithCleanupInfo(ctx, store, workflow, executeAt, idempotencyKey, "")
}

func createWorkflowQueueTaskWithCleanupInfo(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey, cleanupInfo string) (*model.WorkflowQueue, error) {
	return createWorkflowQueueTaskWithCallback(ctx, store, workflow, executeAt, idempotencyKey, cleanupInfo, nil)
}

func createWorkflowQueueTaskWithCallback(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey, cleanupInfo string, callback *model.JSONStruct) (*model.WorkflowQueue, error) {
	return createWorkflowQueueTaskWithResourceActionInfoAndCallback(ctx, store, workflow, executeAt, idempotencyKey, cleanupInfo, "", callback)
}

func createWorkflowQueueTaskWithResourceActionInfoAndCallback(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey, cleanupInfo, resourceActionInfo string, callback *model.JSONStruct) (*model.WorkflowQueue, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	var idempotencyKeyPtr *string
	if idempotencyKey != "" {
		idempotencyKeyPtr = &idempotencyKey
	}
	workflowTask := newWorkflowQueueTask(workflow, executeAt)
	workflowTask.IdempotencyKey = idempotencyKeyPtr
	workflowTask.CleanupInfo = strings.TrimSpace(cleanupInfo)
	workflowTask.ResourceActionInfo = strings.TrimSpace(resourceActionInfo)
	workflowTask.Callback = callback

	if err := repository.CreateWorkflowQueue(ctx, store, workflowTask); err != nil {
		if idempotencyKey != "" && errors.Is(err, datastore.ErrRecordExist) {
			existingTask, lookupErr := repository.TaskByIdempotencyKey(ctx, store, idempotencyKey)
			if errors.Is(lookupErr, datastore.ErrRecordNotExist) {
				return nil, err
			}
			if lookupErr != nil {
				return nil, fmt.Errorf("resolve workflow queue idempotency conflict: %w", lookupErr)
			}
			if existingTask.AppID != workflow.AppID || existingTask.WorkflowID != workflow.ID {
				return nil, fmt.Errorf("workflow queue idempotency key collision: key=%s taskID=%s appID=%s workflowID=%s", idempotencyKey, existingTask.TaskID, existingTask.AppID, existingTask.WorkflowID)
			}
			return existingTask, nil
		}
		return nil, err
	}
	return workflowTask, nil
}

func CreateWorkflowQueueTaskWithCleanupInfo(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey, cleanupInfo string) (*model.WorkflowQueue, error) {
	return createWorkflowQueueTaskWithCleanupInfo(ctx, store, workflow, executeAt, idempotencyKey, cleanupInfo)
}

func CreateWorkflowQueueTaskWithCallback(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey, cleanupInfo string, callback *model.JSONStruct) (*model.WorkflowQueue, error) {
	return createWorkflowQueueTaskWithCallback(ctx, store, workflow, executeAt, idempotencyKey, cleanupInfo, callback)
}

func CreateWorkflowQueueTaskWithResourceActionInfoAndCallback(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey, cleanupInfo, resourceActionInfo string, callback *model.JSONStruct) (*model.WorkflowQueue, error) {
	return createWorkflowQueueTaskWithResourceActionInfoAndCallback(ctx, store, workflow, executeAt, idempotencyKey, cleanupInfo, resourceActionInfo, callback)
}

func newWorkflowQueueTask(workflow *model.Workflow, executeAt int64) *model.WorkflowQueue {
	return &model.WorkflowQueue{
		TaskID:              utils.RandStringByNumLowercase(24),
		AppID:               workflow.AppID,
		WorkflowID:          workflow.ID,
		ProjectID:           workflow.ProjectID,
		WorkflowName:        workflow.Name,
		WorkflowDisplayName: workflow.Alias,
		Type:                workflow.WorkflowType,
		Status:              config.StatusWaiting,
		ExecuteAt:           executeAt,
	}
}

func (w *workflowServiceImpl) rollbackWorkflowCreation(ctx context.Context, workflow *model.Workflow) {
	if workflow == nil {
		return
	}
	if err := repository.DelComponentsByAppID(ctx, w.Store, workflow.ID); err != nil {
		klog.Errorf("cleanup components for workflow %s failed: %v", workflow.ID, err)
	}
	if err := repository.DelWorkflow(ctx, w.Store, workflow); err != nil {
		klog.Errorf("cleanup workflow %s failed: %v", workflow.ID, err)
	}
}

func EnsureAppWorkflowIdle(ctx context.Context, store datastore.DataStore, appID string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" || store == nil {
		return nil
	}
	tasks, err := repository.FindWorkflowTasksByAppID(ctx, store, appID)
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	now := time.Now().Unix()
	for _, current := range tasks {
		if current == nil {
			continue
		}
		if current.Status == config.StatusWaiting && current.ExecuteAt > now {
			activeJobs, err := taskHasActiveJobs(ctx, store, current.TaskID)
			if err != nil {
				return err
			}
			if activeJobs {
				return bcode.ErrWorkflowTaskRunning
			}
			continue
		}
		if isWorkflowActiveStatus(current.Status) {
			return bcode.ErrWorkflowTaskRunning
		}
		if current.Status == config.StatusCancelled {
			cancelling, err := taskHasActiveJobs(ctx, store, current.TaskID)
			if err != nil {
				return err
			}
			if cancelling {
				return bcode.ErrWorkflowTaskCancelling
			}
		}
	}
	return nil
}

func normalizeExecuteAt(executeAt int64) (int64, error) {
	if executeAt < 0 {
		return 0, bcode.ErrWorkflowConfig
	}
	if executeAt == 0 {
		return 0, nil
	}
	now := time.Now().Unix()
	if executeAt <= now {
		return 0, nil
	}
	return executeAt, nil
}

func NormalizeExecuteAt(executeAt int64) (int64, error) {
	return normalizeExecuteAt(executeAt)
}

var activeWorkflowStatuses = map[config.Status]struct{}{
	config.StatusCreated:        {},
	config.StatusRunning:        {},
	config.StatusWaiting:        {},
	config.StatusQueued:         {},
	config.StatusBlocked:        {},
	config.QueueItemPending:     {},
	config.StatusPrepare:        {},
	config.StatusWaitingApprove: {},
	config.StatusDistributed:    {},
	config.StatusDebugBefore:    {},
	config.StatusDebugAfter:     {},
}

func taskHasActiveJobs(ctx context.Context, store datastore.DataStore, taskID string) (bool, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || store == nil {
		return false, nil
	}
	entities, err := store.List(ctx, &model.JobInfo{TaskID: taskID}, &datastore.ListOptions{})
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return false, err
	}
	for _, entity := range entities {
		job, ok := entity.(*model.JobInfo)
		if !ok || job == nil {
			continue
		}
		if !isJobTerminalStatus(config.Status(job.Status)) {
			return true, nil
		}
	}
	return false, nil
}

func TaskHasActiveJobs(ctx context.Context, store datastore.DataStore, taskID string) (bool, error) {
	return taskHasActiveJobs(ctx, store, taskID)
}

var terminalJobStatuses = map[config.Status]struct{}{
	config.StatusCompleted: {},
	config.StatusPassed:    {},
	config.StatusSkipped:   {},
	config.StatusFailed:    {},
	config.StatusTimeout:   {},
	config.StatusReject:    {},
	config.StatusCancelled: {},
}

func statusIn(status config.Status, set map[config.Status]struct{}) bool {
	_, ok := set[status]
	return ok
}

func isWorkflowActiveStatus(status config.Status) bool {
	return statusIn(status, activeWorkflowStatuses)
}

func IsWorkflowActiveStatus(status config.Status) bool {
	return isWorkflowActiveStatus(status)
}

func isJobTerminalStatus(status config.Status) bool {
	return statusIn(status, terminalJobStatuses)
}

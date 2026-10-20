package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type WorkflowService interface {
	ExecWorkflowTaskForApp(ctx context.Context, appID, workflowID string, executeAt int64, idempotencyKey string) (*apis.ExecWorkflowResponse, error)
	WaitingTasks(ctx context.Context) ([]*model.WorkflowQueue, error)
	ClaimTaskForDispatch(ctx context.Context, task *model.WorkflowQueue, leaseDuration time.Duration) (*model.WorkflowQueue, bool, error)
	CancelWorkflowTask(ctx context.Context, userName, taskID, reason string) error
	CancelWorkflowTaskForApp(ctx context.Context, appID, userName, taskID, reason string) error
	CancelAllWorkflowTasksForApp(ctx context.Context, appID, userName, reason string) ([]string, error)
	CancelDelayedVersionTaskForApp(ctx context.Context, appID, userName, taskID, reason string) error
	ApproveWorkflowTask(ctx context.Context, taskID, action, userName, reason string) (*apis.TaskApprovalResponse, error)
	GetTaskStatus(ctx context.Context, taskID string) (*apis.TaskStatusResponse, error)
	GetTaskStages(ctx context.Context, taskID string) (*apis.TaskStagesResponse, error)
	UpsertWorkflowSchedule(ctx context.Context, appID string, req apis.UpsertWorkflowScheduleRequest) (*apis.UpsertWorkflowScheduleResponse, error)
	ListWorkflowSchedules(ctx context.Context, appID string) ([]apis.WorkflowSchedule, error)
	DeleteWorkflowSchedule(ctx context.Context, appID, workflowID string) error
	DispatchWorkflowSchedules(ctx context.Context) (int, error)
}

type workflowServiceImpl struct {
	Store                     datastore.DataStore  `inject:"datastore"`
	KubeClient                kubernetes.Interface `inject:"kubeClient"`
	KubeConfig                *rest.Config         `inject:"kubeConfig"`
	Cache                     cache.ICache         `inject:"cache"`
	RedisClient               *redis.Client        `inject:"redisClient"`
	Cfg                       *config.Config       `inject:""`
	URLSecurityPolicyProvider *urlpolicy.Provider  `inject:""`
	ScheduleLocker            locker.Locker        `inject:"appScheduleLocker"`
	scheduleDispatchMu        sync.Mutex
	scheduleDispatchPage      int
}

// NewWorkflowService new workflow service
func NewWorkflowService() WorkflowService {
	return &workflowServiceImpl{}
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

func (w *workflowServiceImpl) ExecWorkflowTaskForApp(ctx context.Context, appID, workflowID string, executeAt int64, idempotencyKey string) (*apis.ExecWorkflowResponse, error) {
	return w.execWorkflowTaskForAppLocked(ctx, appID, workflowID, executeAt, idempotencyKey)
}

func (w *workflowServiceImpl) execWorkflowTaskForAppLocked(ctx context.Context, appID, workflowID string, executeAt int64, idempotencyKey string) (*apis.ExecWorkflowResponse, error) {
	requestedExecuteAt := executeAt
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
		return repository.WithApplicationSchedulingTransaction(lockCtx, w.Store, appID, func(tx datastore.DataStore) error {
			workflow, err := repository.WorkflowByID(lockCtx, tx, workflowID)
			if err != nil {
				return err
			}
			if workflow.AppID == "" || workflow.AppID != appID {
				return bcode.ErrWorkflowNotExist
			}
			scopedIdempotencyKey := workflowSubmissionIdempotencyKey(lockCtx, appID, idempotencyKey)
			if scopedIdempotencyKey != "" {
				existing, lookupErr := repository.TaskByIdempotencyKey(lockCtx, tx, scopedIdempotencyKey)
				switch {
				case lookupErr == nil:
					if existing.AppID != appID || existing.WorkflowID != workflowID || !equivalentWorkflowExecuteAt(existing.ExecuteAt, requestedExecuteAt, normalized) {
						return bcode.ErrWorkflowIdempotencyConflict
					}
					resp = &apis.ExecWorkflowResponse{
						TaskID:              existing.TaskID,
						Status:              string(existing.Status),
						PendingApprovalStep: existing.PendingApprovalStep,
					}
					return nil
				case !errors.Is(lookupErr, datastore.ErrRecordNotExist):
					return fmt.Errorf("lookup workflow submission idempotency key: %w", lookupErr)
				}
			}
			if err := EnsureAppWorkflowIdle(lockCtx, tx, appID); err != nil {
				return err
			}
			if err := EnsureNoPendingStatefulSetCleanup(lockCtx, tx, appID); err != nil {
				return err
			}
			resp, err = w.enqueueWorkflowTaskWithStoreAndIdempotencyKey(lockCtx, tx, workflow, normalized, scopedIdempotencyKey)
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func workflowSubmissionIdempotencyKey(ctx context.Context, appID, clientKey string) string {
	clientKey = strings.TrimSpace(clientKey)
	if clientKey == "" {
		return ""
	}
	workspaceID := ""
	if scope, ok := access.FromContext(ctx); ok {
		workspaceID = strings.TrimSpace(scope.WorkspaceID)
	}
	digest := sha256.Sum256([]byte(workspaceID + "\x00" + strings.TrimSpace(appID) + "\x00" + clientKey))
	return fmt.Sprintf("api:workflow:%x", digest[:])
}

func equivalentWorkflowExecuteAt(existing, requested, normalized int64) bool {
	return existing == normalized || (existing > 0 && existing == requested)
}

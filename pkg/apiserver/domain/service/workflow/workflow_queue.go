package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (w *workflowServiceImpl) WaitingTasks(ctx context.Context) ([]*model.WorkflowQueue, error) {
	list, err := repository.WaitingTasks(ctx, w.Store)
	if err != nil {
		return nil, err
	}
	return list, err
}

func (w *workflowServiceImpl) enqueueWorkflowTaskWithStoreAndIdempotencyKey(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey string) (*apis.ExecWorkflowResponse, error) {
	if err := ValidateWorkflowTaskEnqueue(ctx, store, workflow, true); err != nil {
		return nil, err
	}

	workflowTask, err := CreateWorkflowQueueTask(ctx, store, workflow, QueueTaskOptions{ExecuteAt: executeAt, IdempotencyKey: idempotencyKey})
	if err != nil {
		return nil, err
	}
	return &apis.ExecWorkflowResponse{TaskID: workflowTask.TaskID, Status: string(workflowTask.Status)}, nil
}

func ValidateWorkflowTaskEnqueue(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, requireComponentInventory bool) error {
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

// QueueTaskOptions contains the optional scheduling and execution snapshots for a task.
type QueueTaskOptions struct {
	ExecuteAt          int64
	IdempotencyKey     string
	CleanupInfo        string
	ResourceActionInfo string
	Callback           *model.JSONStruct
}

// CreateWorkflowQueueTask persists one task, resolving retries by their idempotency key.
// The caller holds the application row in a read-committed scheduling transaction.
func CreateWorkflowQueueTask(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, options QueueTaskOptions) (*model.WorkflowQueue, error) {
	idempotencyKey := strings.TrimSpace(options.IdempotencyKey)
	var idempotencyKeyPtr *string
	if idempotencyKey != "" {
		idempotencyKeyPtr = &idempotencyKey
	}
	workflowTask := newWorkflowQueueTask(workflow, options.ExecuteAt)
	workflowTask.IdempotencyKey = idempotencyKeyPtr
	workflowTask.CleanupInfo = strings.TrimSpace(options.CleanupInfo)
	workflowTask.ResourceActionInfo = strings.TrimSpace(options.ResourceActionInfo)
	workflowTask.Callback = options.Callback

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

func EnsureAppWorkflowIdle(ctx context.Context, store datastore.DataStore, appID string) error {
	appID = strings.TrimSpace(appID)
	if store == nil {
		return fmt.Errorf("check application workflow idle: datastore is nil")
	}
	if appID == "" {
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
			activeJobs, err := TaskHasActiveJobs(ctx, store, current.TaskID)
			if err != nil {
				return err
			}
			if activeJobs {
				return bcode.ErrWorkflowTaskRunning
			}
			continue
		}
		if model.IsWorkflowActiveStatus(current.Status) {
			return bcode.ErrWorkflowTaskRunning
		}
		if current.Status == config.StatusCancelled {
			cancelling, err := TaskHasActiveJobs(ctx, store, current.TaskID)
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

func NormalizeExecuteAt(executeAt int64) (int64, error) {
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

func TaskHasActiveJobs(ctx context.Context, store datastore.DataStore, taskID string) (bool, error) {
	taskID = strings.TrimSpace(taskID)
	if store == nil {
		return false, fmt.Errorf("check active jobs: datastore is nil")
	}
	if taskID == "" {
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

func isJobTerminalStatus(status config.Status) bool {
	return statusIn(status, terminalJobStatuses)
}

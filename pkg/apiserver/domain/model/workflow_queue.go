package model

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

const (
	VersionUpdateCleanupInfoVersionV1                     = 1
	VersionUpdateCleanupInfoVersionStatefulSetDeletion    = 2
	VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion = 3
)

// WorkflowQueue is a durable execution instance, including standalone workspace
// Jobs. Workflow holds the reusable definition; message queues only dispatch this
// record. Status writes still require the repository's transaction and ownership guards.
type WorkflowQueue struct {
	TaskID              string                  `json:"task_id" gorm:"primaryKey;type:varchar(255);column:task_id"`
	WorkspaceID         string                  `json:"workspaceId,omitempty" gorm:"type:varchar(36);column:workspace_id;index"`
	IdempotencyKey      *string                 `json:"idempotencyKey,omitempty" gorm:"type:varchar(255);column:idempotency_key;uniqueIndex:idx_workflow_queue_idempotency_key"`
	ProjectID           string                  `json:"projectId" gorm:"type:varchar(64);column:project_id"`
	WorkflowName        string                  `json:"workflow_name" gorm:"type:varchar(255);column:workflow_name"`
	AppID               string                  `json:"app_id" gorm:"type:varchar(64);column:app_id"`
	WorkflowID          string                  `json:"workflow_id" gorm:"type:varchar(64);column:workflow_id"`
	WorkflowDisplayName string                  `json:"workflow_display_name" gorm:"type:varchar(255);column:workflow_display_name"`
	Status              config.Status           `json:"status,omitempty" gorm:"type:varchar(32);column:status;index:idx_workflow_queue_reaper,priority:1;index:idx_workflow_queue_dispatch,priority:1"`
	TaskCreator         string                  `json:"task_creator,omitempty" gorm:"type:varchar(128);column:task_creator"`
	TaskRevoker         string                  `json:"task_revoker,omitempty" gorm:"type:varchar(128);column:task_revoker"`
	CancelSource        string                  `json:"cancel_source,omitempty" gorm:"type:varchar(32);column:cancel_source"`
	Type                config.WorkflowTaskType `json:"type,omitempty" gorm:"type:varchar(32);column:type"`
	ExecuteAt           int64                   `json:"executeAt,omitempty" gorm:"type:bigint;column:execute_at;index:idx_workflow_queue_dispatch,priority:2"`
	CurrentStep         int                     `json:"currentStep,omitempty" gorm:"type:int;column:current_step"`
	ApprovalPending     bool                    `json:"approvalPending,omitempty" gorm:"column:approval_pending"`
	PendingApprovalStep string                  `json:"pendingApprovalStep,omitempty" gorm:"type:varchar(255);column:pending_approval_step"`
	Callback            *JSONStruct             `json:"callback,omitempty" gorm:"serializer:json"`
	CleanupInfo         string                  `json:"-" gorm:"type:longtext;column:cleanup_info"`
	ResourceActionInfo  string                  `json:"-" gorm:"type:longtext;column:resource_action_info"`
	JobSpec             string                  `json:"-" gorm:"type:longtext;column:job_spec"`
	RunGeneration       uint64                  `json:"runGeneration,omitempty" gorm:"column:run_generation;not null;default:0;index:idx_workflow_queue_run_lease,priority:2"`
	RunToken            string                  `json:"-" gorm:"type:varchar(64);column:run_token;index:idx_workflow_queue_run_lease,priority:3"`
	WorkerID            string                  `json:"workerId,omitempty" gorm:"type:varchar(255);column:worker_id;index"`
	HeartbeatAt         *time.Time              `json:"heartbeatAt,omitempty" gorm:"column:heartbeat_at"`
	LeaseExpiresAt      *time.Time              `json:"leaseExpiresAt,omitempty" gorm:"column:lease_expires_at;index:idx_workflow_queue_run_lease,priority:1;index:idx_workflow_queue_reaper,priority:2"`
	DispatchAttempts    uint                    `json:"dispatchAttempts,omitempty" gorm:"column:dispatch_attempts;not null;default:0"`
	SchedulingReason    string                  `json:"schedulingReason,omitempty" gorm:"type:varchar(255);column:scheduling_reason"`
	BaseModel
}

// IsWorkflowActiveStatus classifies execution states for lifecycle decisions.
// Empty and unknown states are not active. Callers that accept an empty status
// for cancellation must handle it explicitly; cancelled tasks may still have
// active Jobs and require a separate persisted-state check.
func IsWorkflowActiveStatus(status config.Status) bool {
	switch status {
	case config.StatusCreated, config.StatusRunning, config.StatusWaiting,
		config.StatusQueued, config.StatusBlocked, config.QueueItemPending,
		config.StatusPrepare, config.StatusWaitingApprove, config.StatusDistributed,
		config.StatusDebugBefore, config.StatusDebugAfter:
		return true
	default:
		return false
	}
}

type VersionUpdateCleanupInfo struct {
	Source      string `json:"source"`
	Version     int    `json:"version"`
	CleanupOnly bool   `json:"cleanupOnly,omitempty"`
	// ResolvesTaskIDs records the exact unfinished migration attempts that this
	// task may resolve, but only after both its cleanup jobs and workflow succeed.
	ResolvesTaskIDs []string                        `json:"resolvesTaskIDs,omitempty"`
	Components      []VersionUpdateCleanupComponent `json:"components,omitempty"`
}

// NormalizeVersionUpdateCleanupResolutionTaskIDs validates and copies one task's
// persisted cleanup references without changing the stored values.
func NormalizeVersionUpdateCleanupResolutionTaskIDs(taskID string, values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	taskID = strings.TrimSpace(taskID)
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, rawValue := range values {
		value := strings.TrimSpace(rawValue)
		if value == "" {
			return nil, fmt.Errorf("task %s has an empty resolvesTaskIDs entry", taskID)
		}
		if value == taskID {
			return nil, fmt.Errorf("task %s cannot resolve itself", taskID)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("task %s resolves StatefulSet cleanup task %s more than once", taskID, value)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

// ValidateVersionUpdateCleanupResolutionGraph checks normalized references for
// unknown tasks and cycles. Callers retain cleanup success and coverage rules.
func ValidateVersionUpdateCleanupResolutionGraph(references map[string][]string) error {
	for taskID, resolvedTaskIDs := range references {
		for _, resolvedTaskID := range resolvedTaskIDs {
			if _, exists := references[resolvedTaskID]; !exists {
				return fmt.Errorf("task %s resolves unknown StatefulSet cleanup task %s", taskID, resolvedTaskID)
			}
		}
	}
	const (
		resolutionVisiting = iota + 1
		resolutionVisited
	)
	states := make(map[string]int, len(references))
	var visit func(string) error
	visit = func(taskID string) error {
		switch states[taskID] {
		case resolutionVisiting:
			return fmt.Errorf("StatefulSet cleanup resolution graph contains a cycle at task %s", taskID)
		case resolutionVisited:
			return nil
		}
		states[taskID] = resolutionVisiting
		for _, resolvedTaskID := range references[taskID] {
			if err := visit(resolvedTaskID); err != nil {
				return err
			}
		}
		states[taskID] = resolutionVisited
		return nil
	}
	for taskID := range references {
		if err := visit(taskID); err != nil {
			return err
		}
	}
	return nil
}

type VersionUpdateCleanupComponent struct {
	Component                       *ApplicationComponent `json:"component"`
	ResourceAppName                 string                `json:"resourceAppName,omitempty"`
	InsertBeforeStepIndex           int                   `json:"insertBeforeStepIndex"`
	RequireStatefulSetDeletion      bool                  `json:"requireStatefulSetDeletion,omitempty"`
	StatefulSetPVCTemplatesToDelete []string              `json:"statefulSetPVCTemplatesToDelete,omitempty"`
}

type VersionUpdateResourceActionInfo struct {
	Source                   string                                 `json:"source"`
	Version                  int                                    `json:"version"`
	RestartOnly              bool                                   `json:"restartOnly,omitempty"`
	RestartComponents        []string                               `json:"restartComponents,omitempty"`
	ImageReadyComponents     []string                               `json:"imageReadyComponents,omitempty"`
	ImageReadyTimeoutSeconds int64                                  `json:"imageReadyTimeoutSeconds,omitempty"`
	ExecutionScope           domainspec.VersionUpdateExecutionScope `json:"executionScope,omitempty"`
	ExecutionComponents      []string                               `json:"executionComponents,omitempty"`
}

func (wq *WorkflowQueue) PrimaryKey() string {
	return wq.TaskID
}

func (wq *WorkflowQueue) TableName() string {
	return tableNamePrefix + "workflow_queue"
}

func WorkflowCallbackSource(task *WorkflowQueue, workflow *Workflow, app *Applications) *JSONStruct {
	if task != nil && task.Callback != nil {
		return task.Callback
	}
	if workflow != nil && workflow.Callback != nil {
		return workflow.Callback
	}
	if app != nil {
		return app.Callback
	}
	return nil
}

func (wq *WorkflowQueue) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if wq.AppID != "" {
		index["app_id"] = wq.AppID
	}
	if wq.WorkspaceID != "" {
		index["workspace_id"] = wq.WorkspaceID
	}
	if wq.TaskID != "" {
		index["task_id"] = wq.TaskID
	}
	if wq.IdempotencyKey != nil && *wq.IdempotencyKey != "" {
		index["idempotency_key"] = *wq.IdempotencyKey
	}
	if wq.Status != "" {
		index["status"] = wq.Status
	}
	return index
}

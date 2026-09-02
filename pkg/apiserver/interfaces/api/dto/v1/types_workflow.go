package v1

import (
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

type CreateWorkflowRequest struct {
	Name        string                       `json:"name" validate:"checkname"`
	Project     string                       `json:"project" validate:"checkname"`
	Alias       string                       `json:"alias"`
	Description string                       `json:"description" optional:"true"`
	Labels      map[string]string            `json:"labels,omitempty"`
	Component   []CreateComponentRequest     `json:"component"`
	Workflows   []CreateWorkflowStepsRequest `json:"workflow"`
	TryRun      bool                         `json:"tryRun"`
}

type Properties = spec.Properties

type Traits = spec.Traits

type WorkflowProperties struct {
	Policies  []string `json:"policies"`
	Path      string   `json:"path,omitempty"`
	Container string   `json:"container,omitempty"`
}

type WorkflowTraits struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}

type CreateWorkflowStepsRequest struct {
	Name               string                         `json:"name"`
	ComponentType      config.JobType                 `json:"jobType,omitempty"`
	WorkflowProperties WorkflowPolicies               `json:"properties,omitempty"`
	Components         []string                       `json:"components,omitempty"`
	Mode               string                         `json:"mode,omitempty"`
	SubSteps           []CreateWorkflowSubStepRequest `json:"subSteps,omitempty"`
}

type CreateWorkflowSubStepRequest struct {
	Name         string             `json:"name"`
	WorkflowType config.JobType     `json:"jobType,omitempty"`
	Properties   WorkflowProperties `json:"properties,omitempty"`
	Components   []string           `json:"components,omitempty"`

	propertiesList      []WorkflowProperties
	propertiesFromArray bool
}

func (r CreateWorkflowStepRequest) WorkflowPropertiesList() []WorkflowProperties {
	if !r.propertiesFromArray {
		return nil
	}
	return r.propertiesList
}

func (r CreateWorkflowStepRequest) WorkflowPropertiesFromArray() bool {
	return r.propertiesFromArray
}

func (r CreateWorkflowSubStepRequest) WorkflowPropertiesList() []WorkflowProperties {
	if !r.propertiesFromArray {
		return nil
	}
	return r.propertiesList
}

func (r CreateWorkflowSubStepRequest) WorkflowPropertiesFromArray() bool {
	return r.propertiesFromArray
}

type CreateConfigMapFromMapRequest struct {
	Name        string            `json:"name" validate:"required"`
	Namespace   string            `json:"namespace"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Data        map[string]string `json:"data" validate:"required"`
}

type CreateConfigMapFromURLRequest struct {
	Name        string            `json:"name" validate:"required"`
	Namespace   string            `json:"namespace"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	URL         string            `json:"url" validate:"required,url"`
	FileName    string            `json:"fileName,omitempty"`
}

type ConfigMapResponse struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Data        map[string]string `json:"data"`
	CreateTime  time.Time         `json:"createTime"`
	UpdateTime  time.Time         `json:"updateTime"`
}

type WorkflowPolicies struct {
	Policies  []string `json:"policies"`
	Path      string   `json:"path,omitempty"`
	Container string   `json:"container,omitempty"`
}

type WorkflowCallback struct {
	Success        string            `json:"success,omitempty"`
	Failure        string            `json:"failure,omitempty"`
	Timeout        string            `json:"timeout,omitempty"`
	Reject         string            `json:"reject,omitempty"`
	Cancelled      string            `json:"cancelled,omitempty"`
	Methods        map[string]string `json:"methods,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitempty"`
}

type CreateWorkflowResponse struct {
	WorkflowID string `json:"workflowId"`
}

type UpdateApplicationWorkflowRequest struct {
	WorkflowID       string                       `json:"workflowId,omitempty"`
	Name             string                       `json:"name,omitempty"`
	Alias            string                       `json:"alias,omitempty"`
	Callback         *WorkflowCallback            `json:"callback,omitempty"`
	WorkflowType     config.WorkflowTaskType      `json:"workflowType,omitempty"`
	FailurePolicy    config.WorkflowFailurePolicy `json:"failurePolicy,omitempty"`
	FailurePolicySet bool                         `json:"-"`
	Workflow         []CreateWorkflowStepRequest  `json:"workflow" validate:"required,min=1,dive"`
}

type UpdateWorkflowResponse struct {
	WorkflowID string `json:"workflowId"`
}

type ExecWorkflowRequest struct {
	WorkflowID string `json:"workflowId" validate:"checkname"`
	ExecuteAt  int64  `json:"executeAt,omitempty"`
}

type ExecWorkflowResponse struct {
	TaskID string `json:"taskId"`
}

const (
	CreateAndExecStatusQueued = "queued"
	CreateAndExecStatusFailed = "failed"
)

type CreateAndExecApplicationResponse struct {
	Application *ApplicationBase `json:"application,omitempty"`
	WorkflowID  string           `json:"workflowId,omitempty"`
	TaskID      string           `json:"taskId,omitempty"`
	ExecStatus  string           `json:"execStatus"`
	ExecError   string           `json:"execError,omitempty"`
}

type UpsertWorkflowScheduleRequest struct {
	WorkflowID string `json:"workflowId" validate:"required,checkname"`
	Cron       string `json:"cron" validate:"required"`
	Enabled    *bool  `json:"enabled,omitempty"`
}

type WorkflowSchedule struct {
	ID            string    `json:"id"`
	AppID         string    `json:"appId"`
	WorkflowID    string    `json:"workflowId"`
	WorkflowName  string    `json:"workflowName,omitempty"`
	WorkflowAlias string    `json:"workflowAlias,omitempty"`
	Cron          string    `json:"cron"`
	Enabled       bool      `json:"enabled"`
	NextRun       int64     `json:"nextRun"`
	LastRun       int64     `json:"lastRun,omitempty"`
	CreateTime    time.Time `json:"createTime"`
	UpdateTime    time.Time `json:"updateTime"`
}

type UpsertWorkflowScheduleResponse struct {
	Schedule WorkflowSchedule `json:"schedule"`
}

type ListWorkflowSchedulesResponse struct {
	Schedules []WorkflowSchedule `json:"schedules"`
}

type DeleteWorkflowScheduleResponse struct {
	WorkflowID string `json:"workflowId"`
}

type CancelWorkflowRequest struct {
	TaskID string `json:"taskId" validate:"required"`
	User   string `json:"user,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type CancelWorkflowResponse struct {
	TaskID string `json:"taskId"`
	Status string `json:"status"`
}

type CancelAllApplicationWorkflowsResponse struct {
	AppID            string   `json:"appId"`
	CancelledTaskIDs []string `json:"cancelledTaskIds"`
}

type TaskStatusResponse struct {
	TaskID              string                  `json:"taskId"`
	Status              string                  `json:"status"`
	WorkflowID          string                  `json:"workflowId,omitempty"`
	WorkflowName        string                  `json:"workflowName,omitempty"`
	AppID               string                  `json:"appId,omitempty"`
	Type                config.WorkflowTaskType `json:"type,omitempty"`
	PendingApprovalStep string                  `json:"pendingApprovalStep,omitempty"`
	Components          []ComponentTaskStatus   `json:"components,omitempty"`
}

type TaskStagesResponse struct {
	TaskID              string                  `json:"taskId"`
	Status              string                  `json:"status,omitempty"`
	WorkflowID          string                  `json:"workflowId,omitempty"`
	WorkflowName        string                  `json:"workflowName,omitempty"`
	AppID               string                  `json:"appId,omitempty"`
	Type                config.WorkflowTaskType `json:"type,omitempty"`
	PendingApprovalStep string                  `json:"pendingApprovalStep,omitempty"`
	Stages              []TaskStageDetail       `json:"stages,omitempty"`
}

type TaskApprovalRequest struct {
	Action string `json:"action" validate:"required,oneof=continue cancel"`
	User   string `json:"user,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type TaskApprovalResponse struct {
	TaskID string `json:"taskId"`
	Action string `json:"action"`
	Status string `json:"status"`
}

type ComponentTaskStatus struct {
	Name      string `json:"name"`
	Type      string `json:"type,omitempty"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	StartTime int64  `json:"startTime,omitempty"`
	EndTime   int64  `json:"endTime,omitempty"`
}

type TaskStageDetail struct {
	ID        int                `json:"id,omitempty"`
	Name      string             `json:"name,omitempty"`
	Type      string             `json:"type,omitempty"`
	Status    string             `json:"status,omitempty"`
	Info      []TaskStageMessage `json:"info,omitempty"`
	Error     []TaskStageMessage `json:"error,omitempty"`
	StartTime int64              `json:"startTime,omitempty"`
	EndTime   int64              `json:"endTime,omitempty"`
}

type TaskStageMessage struct {
	Component string `json:"component,omitempty"`
	Type      string `json:"type,omitempty"`
	Message   string `json:"message,omitempty"`
}

type ApplicationTask struct {
	TaskID              string                  `json:"taskId"`
	AppID               string                  `json:"appId,omitempty"`
	WorkflowID          string                  `json:"workflowId,omitempty"`
	WorkflowName        string                  `json:"workflowName,omitempty"`
	WorkflowDisplayName string                  `json:"workflowDisplayName,omitempty"`
	Status              string                  `json:"status,omitempty"`
	Type                config.WorkflowTaskType `json:"type,omitempty"`
	TaskCreator         string                  `json:"taskCreator,omitempty"`
	TaskRevoker         string                  `json:"taskRevoker,omitempty"`
	CreateTime          time.Time               `json:"createTime"`
	UpdateTime          time.Time               `json:"updateTime"`
}

type ListApplicationTasksResponse struct {
	Tasks []*ApplicationTask `json:"tasks"`
}

type ListApplicationWorkflowsResponse struct {
	Workflows []*ApplicationWorkflow `json:"workflows"`
}

type ApplicationWorkflow struct {
	ID            string                       `json:"id"`
	Name          string                       `json:"name"`
	Alias         string                       `json:"alias"`
	Namespace     string                       `json:"namespace,omitempty"`
	ProjectID     string                       `json:"projectId,omitempty"`
	Description   string                       `json:"description,omitempty"`
	Status        string                       `json:"status"`
	Disabled      bool                         `json:"disabled"`
	FailurePolicy config.WorkflowFailurePolicy `json:"failurePolicy"`
	Steps         []WorkflowStepDetail         `json:"steps,omitempty"`
	Callback      *WorkflowCallback            `json:"callback,omitempty"`
	CreateTime    time.Time                    `json:"createTime"`
	UpdateTime    time.Time                    `json:"updateTime"`
	WorkflowType  config.WorkflowTaskType      `json:"workflowType"`
}

type WorkflowStepDetail struct {
	Name         string                  `json:"name"`
	StepType     config.WorkflowStepType `json:"stepType,omitempty"`
	WorkflowType config.JobType          `json:"workflowType,omitempty"`
	Mode         config.WorkflowMode     `json:"mode,omitempty"`
	Approval     *WorkflowStepApproval   `json:"approval,omitempty"`
	Components   []string                `json:"components,omitempty"`
	Properties   []WorkflowProperties    `json:"properties,omitempty"`
	SubSteps     []WorkflowSubStepDetail `json:"subSteps,omitempty"`
}

type WorkflowSubStepDetail struct {
	Name         string               `json:"name"`
	WorkflowType config.JobType       `json:"workflowType,omitempty"`
	Components   []string             `json:"components,omitempty"`
	Properties   []WorkflowProperties `json:"properties,omitempty"`
}

type CronJobInfo struct {
	Name                       string     `json:"name"`
	Namespace                  string     `json:"namespace"`
	Schedule                   string     `json:"schedule"`
	Suspend                    bool       `json:"suspend"`
	ConcurrencyPolicy          string     `json:"concurrencyPolicy,omitempty"`
	SuccessfulJobsHistoryLimit *int32     `json:"successfulJobsHistoryLimit,omitempty"`
	FailedJobsHistoryLimit     *int32     `json:"failedJobsHistoryLimit,omitempty"`
	LastScheduleTime           *time.Time `json:"lastScheduleTime,omitempty"`
	LastSuccessfulTime         *time.Time `json:"lastSuccessfulTime,omitempty"`
	CreateTime                 time.Time  `json:"createTime"`
}

type ScheduledJobInfo struct {
	AppID              string    `json:"appId"`
	AppName            string    `json:"appName"`
	AppNamespace       string    `json:"appNamespace"`
	ComponentName      string    `json:"componentName"`
	ComponentNamespace string    `json:"componentNamespace"`
	Image              string    `json:"image,omitempty"`
	Schedule           string    `json:"schedule,omitempty"`
	StartTime          int64     `json:"startTime,omitempty"`
	RunPolicy          string    `json:"runPolicy,omitempty"`
	CreateTime         time.Time `json:"createTime"`
	UpdateTime         time.Time `json:"updateTime"`
}

package v1

import (
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

// ApplicationBase application base model
type ApplicationBase struct {
	WorkspaceID     string               `json:"workspaceID"`
	ID              string               `json:"id"`
	Name            string               `json:"name"`
	Namespace       string               `json:"namespace"`
	Alias           string               `json:"alias"`
	Project         string               `json:"project"`
	Version         string               `json:"version"`
	Description     string               `json:"description"`
	CreateTime      time.Time            `json:"createTime"`
	UpdateTime      time.Time            `json:"updateTime"`
	Icon            string               `json:"icon"`
	WorkflowID      string               `json:"workflowId"`
	TemplateEnabled bool                 `json:"templateEnabled"`
	Resources       ApplicationResources `json:"resources"`
}

type ApplicationResources struct {
	CPUReq   string `json:"cpuReq"`
	MemReq   string `json:"memReq"`
	CPULimit string `json:"cpuLimit"`
	MemLimit string `json:"memLimit"`
	Replicas int32  `json:"replicas"`
}

type CreateApplicationsRequest struct {
	ID          string                      `json:"id,omitempty"`
	Name        string                      `json:"name" validate:"checkname"`
	Namespace   string                      `json:"namespace,omitempty"`
	Alias       string                      `json:"alias,omitempty"`
	Version     string                      `json:"version,omitempty"`
	Project     string                      `json:"project,omitempty"`
	Description string                      `json:"description,omitempty" optional:"true"`
	Icon        string                      `json:"icon,omitempty"`
	Components  []CreateComponentRequest    `json:"components"`
	Workflow    []CreateWorkflowStepRequest `json:"workflow,omitempty"`
	Callback    *WorkflowCallback           `json:"callback,omitempty"`
	// FailurePolicy applies to the Application's workflow. Workflow-specific
	// callback overrides remain available through the Workflow update API.
	FailurePolicy workflowconfig.WorkflowFailurePolicy `json:"failurePolicy,omitempty"`

	// TemplateEnabled 标记该应用是否允许作为模板被引用
	TemplateEnabled *bool `json:"templateEnabled,omitempty"`
}

// CreateAndExecApplicationRequest combines application creation with workflow execution.
type CreateAndExecApplicationRequest struct {
	CreateApplicationsRequest
	WorkflowID string `json:"workflowId,omitempty" validate:"omitempty,checkname"`
	ExecuteAt  int64  `json:"executeAt,omitempty"`
}

type ConvertApplicationsRequest struct {
	YAML     string `json:"yaml,omitempty"`
	FileURL  string `json:"fileUrl,omitempty"`
	Validate *bool  `json:"validate,omitempty"`
}

type ConvertApplicationsResponse struct {
	Components []CreateComponentRequest `json:"components"`
	Valid      bool                     `json:"valid"`
	Errors     []ValidationError        `json:"errors,omitempty"`
	Warnings   []string                 `json:"warnings,omitempty"`
}

type CreateComponentRequest = spec.Component

type TemplateRef = spec.TemplateRef

type CreateWorkflowStepRequest struct {
	SchedulingClass string                         `json:"schedulingClass,omitempty"`
	Name            string                         `json:"name"`
	StepType        config.WorkflowStepType        `json:"stepType,omitempty"`
	WorkflowType    config.JobType                 `json:"jobType,omitempty"`
	Approval        *WorkflowStepApproval          `json:"approval,omitempty"`
	Properties      WorkflowProperties             `json:"properties,omitempty"`
	Components      []string                       `json:"components,omitempty"`
	Mode            string                         `json:"mode,omitempty"`
	SubSteps        []CreateWorkflowSubStepRequest `json:"subSteps,omitempty"`

	propertiesList      []WorkflowProperties
	propertiesFromArray bool
}

type WorkflowStepApproval struct {
	NotifyURL      string            `json:"notifyUrl"`
	Message        string            `json:"message,omitempty"`
	Method         string            `json:"method,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitempty"`
}

// ListApplicationResponse list applications by query params
type ListApplicationResponse struct {
	Applications []*ApplicationBase `json:"applications"`
}

type BatchGetApplicationsRequest struct {
	AppIDs []string `json:"appIds"`
}

type ApplicationWithComponents struct {
	ApplicationBase
	Components []*BatchApplicationComponent `json:"components"`
}

type BatchApplicationComponent struct {
	ID            int                                 `json:"id"`
	AppID         string                              `json:"appId"`
	Name          string                              `json:"name"`
	Namespace     string                              `json:"namespace"`
	Replicas      int32                               `json:"replicas"`
	ComponentType config.JobType                      `json:"type"`
	Properties    BatchApplicationComponentProperties `json:"properties"`
}

type BatchApplicationComponentProperties struct {
	Ports []spec.Ports `json:"ports"`
}

type BatchGetApplicationsResponse struct {
	Applications []*ApplicationWithComponents `json:"applications"`
}

type ListApplicationsQuery struct {
	Page     int `form:"page"`
	PageSize int `form:"pageSize"`
}

type CleanupApplicationResourcesResponse struct {
	AppID            string   `json:"appId"`
	TaskID           string   `json:"taskId,omitempty"`
	DeletedResources []string `json:"deletedResources"`
	FailedResources  []string `json:"failedResources,omitempty"`
}

type DeleteApplicationRequest struct {
	WaitSeconds *int64 `json:"waitSeconds,omitempty"`
}

type DeleteApplicationResponse struct {
	AppID            string              `json:"appId"`
	CancelledTaskIDs []string            `json:"cancelledTaskIds,omitempty"`
	ActiveTaskIDs    []string            `json:"activeTaskIds,omitempty"`
	DeletedResources []string            `json:"deletedResources,omitempty"`
	FailedResources  []string            `json:"failedResources,omitempty"`
	Warnings         []string            `json:"warnings,omitempty"`
	DeletedCounts    DeleteResourceCount `json:"deletedCounts,omitempty"`
}

type DeleteResourceCount struct {
	Schedules  int64 `json:"schedules"`
	Workflows  int64 `json:"workflows"`
	Components int64 `json:"components"`
	Tasks      int64 `json:"tasks"`
	Jobs       int64 `json:"jobs"`
	Apps       int64 `json:"apps"`
}

type ApplicationLifecycleRequest struct {
	Callback *WorkflowCallback `json:"callback,omitempty"`
}

// RestartApplicationWorkloadsResponse restart workloads response
type RestartApplicationWorkloadsResponse struct {
	AppID              string   `json:"appId"`
	TaskID             string   `json:"taskId,omitempty"`
	RestartedAt        string   `json:"restartedAt"`
	RestartedResources []string `json:"restartedResources"`
	SkippedResources   []string `json:"skippedResources,omitempty"`
	FailedResources    []string `json:"failedResources,omitempty"`
}

// StopApplicationDeploymentsResponse reports deployment stop results.
type StopApplicationDeploymentsResponse struct {
	AppID            string   `json:"appId"`
	TaskID           string   `json:"taskId,omitempty"`
	StoppedAt        string   `json:"stoppedAt"`
	StoppedResources []string `json:"stoppedResources"`
	SkippedResources []string `json:"skippedResources,omitempty"`
	FailedResources  []string `json:"failedResources,omitempty"`
}

// StartApplicationDeploymentsResponse reports deployment start results.
type StartApplicationDeploymentsResponse struct {
	AppID            string   `json:"appId"`
	TaskID           string   `json:"taskId,omitempty"`
	StartedAt        string   `json:"startedAt"`
	StartedResources []string `json:"startedResources"`
	SkippedResources []string `json:"skippedResources,omitempty"`
	FailedResources  []string `json:"failedResources,omitempty"`
}

// DatabaseResetRequest requests a workflow-backed reset of selected database components.
type DatabaseResetRequest struct {
	Components         []string `json:"components" validate:"required,min=1,dive,required"`
	InitSQLURL         string   `json:"initSqlUrl,omitempty"`
	initSQLURLProvided bool
}

// DatabaseResetResponse reports the queued database reset workflow task.
type DatabaseResetResponse struct {
	AppID              string   `json:"appId"`
	WorkflowID         string   `json:"workflowId"`
	TaskID             string   `json:"taskId"`
	DatabaseComponents []string `json:"databaseComponents"`
	RestartComponents  []string `json:"restartComponents"`
}

// LogArchiveDownloadRequest downloads one component log archive as a file stream.
type LogArchiveDownloadRequest struct {
	Name       string              `json:"name,omitempty"`
	JobType    config.JobType      `json:"jobType,omitempty"`
	Mode       config.WorkflowMode `json:"mode,omitempty"`
	Components []string            `json:"components" validate:"required,min=1,dive,required"`
	Path       string              `json:"path" validate:"required"`
	Container  string              `json:"container,omitempty"`
}

type ExportComponentFilesRequest struct {
	Path      string `json:"path"`
	Container string `json:"container,omitempty"`
}

type ExecComponentShellScriptRequest struct {
	Script    string `json:"script"`
	Container string `json:"container,omitempty"`
}

type ExecComponentShellScriptResponse struct {
	Namespace     string `json:"namespace"`
	PodName       string `json:"podName"`
	ContainerName string `json:"containerName"`
	Stdout        string `json:"stdout,omitempty"`
	Stderr        string `json:"stderr,omitempty"`
	ExitCode      int    `json:"exitCode"`
	Succeeded     bool   `json:"succeeded"`
}

type ComponentContainersResponse struct {
	AppID         string                   `json:"appId"`
	ComponentName string                   `json:"componentName"`
	ComponentType config.JobType           `json:"componentType"`
	Pods          []ComponentPodContainers `json:"pods"`
}

type ComponentPodContainers struct {
	PodName    string                   `json:"podName"`
	Namespace  string                   `json:"namespace"`
	Phase      string                   `json:"phase,omitempty"`
	Containers []ComponentContainerInfo `json:"containers"`
}

type ComponentContainerInfo struct {
	Name         string `json:"name"`
	Image        string `json:"image,omitempty"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restartCount"`
	State        string `json:"state"`
	Reason       string `json:"reason,omitempty"`
}

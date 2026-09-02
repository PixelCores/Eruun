package v1

import (
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// ApplicationBase application base model
type ApplicationBase struct {
	ID              string                `json:"id"`
	Name            string                `json:"name"`
	Namespace       string                `json:"namespace"`
	Alias           string                `json:"alias"`
	Project         string                `json:"project"`
	Version         string                `json:"version"`
	Description     string                `json:"description"`
	CreateTime      time.Time             `json:"createTime"`
	UpdateTime      time.Time             `json:"updateTime"`
	Icon            string                `json:"icon"`
	WorkflowID      string                `json:"workflowId"`
	TemplateEnabled bool                  `json:"templateEnabled"`
	ManagementMode  config.ManagementMode `json:"managementMode"`
	Resources       ApplicationResources  `json:"resources"`
}

type ApplicationResources struct {
	CPUReq   string `json:"cpuReq"`
	MemReq   string `json:"memReq"`
	CPULimit string `json:"cpuLimit"`
	MemLimit string `json:"memLimit"`
	Replicas int32  `json:"replicas"`
}

// ProjectBase project base model
type ProjectBase struct {
	Name        string    `json:"name"`
	Alias       string    `json:"alias"`
	Description string    `json:"description"`
	CreateTime  time.Time `json:"createTime"`
	UpdateTime  time.Time `json:"updateTime"`
	Owner       NameAlias `json:"owner,omitempty"`
	Namespace   string    `json:"namespace"`
}

// NameAlias name and alias
type NameAlias struct {
	Name  string `json:"name"`
	Alias string `json:"alias"`
}

type CreateApplicationsRequest struct {
	ID            string                      `json:"id"`
	Name          string                      `json:"name" validate:"checkname"`
	Namespace     string                      `json:"namespace"`
	Alias         string                      `json:"alias"`
	Version       string                      `json:"version"`
	Project       string                      `json:"project"`
	Description   string                      `json:"description" optional:"true"`
	Icon          string                      `json:"icon"`
	Component     []CreateComponentRequest    `json:"component"`
	WorkflowSteps []CreateWorkflowStepRequest `json:"workflow"`
	Callback      *WorkflowCallback           `json:"callback,omitempty"`

	// WorkflowCallback is populated from the new workflow object request shape:
	// {"workflow":{"callback":{...},"steps":[...]}}.
	WorkflowCallback *WorkflowCallback `json:"-"`

	// WorkflowFailurePolicy is populated from the workflow object request shape:
	// {"workflow":{"failurePolicy":"cleanup_all","steps":[...]}}.
	WorkflowFailurePolicy config.WorkflowFailurePolicy `json:"-"`

	// TemplateEnabled 标记该应用是否允许作为模板被引用
	TemplateEnabled *bool `json:"templateEnabled,omitempty"`

	// ImportAsObserve is set only by the namespace import domain service. Public
	// JSON cannot use the generic application endpoint to change management mode.
	ImportAsObserve bool `json:"-"`
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

type ImportNamespaceApplicationsRequest struct {
	Namespace       string                              `json:"namespace"`
	Mode            string                              `json:"mode,omitempty"`
	ManagementMode  config.ManagementMode               `json:"managementMode,omitempty"`
	Applications    []ImportNamespaceApplicationMapping `json:"applications,omitempty"`
	PlanFingerprint string                              `json:"planFingerprint,omitempty"`
	IncludeKinds    []string                            `json:"includeKinds,omitempty"`
}

type ImportNamespaceApplicationMapping struct {
	Name        string                            `json:"name"`
	Alias       string                            `json:"alias,omitempty"`
	TargetAppID string                            `json:"targetAppId,omitempty"`
	Components  []ImportNamespaceComponentMapping `json:"components"`
}

type ImportNamespaceComponentMapping struct {
	Name     string                           `json:"name"`
	Workload ImportNamespaceWorkloadReference `json:"workload"`
}

type ImportNamespaceWorkloadReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

type ImportNamespaceSummary struct {
	AppsPlanned             int `json:"appsPlanned"`
	AppsApplied             int `json:"appsApplied"`
	ComponentsPlanned       int `json:"componentsPlanned"`
	ComponentsApplied       int `json:"componentsApplied"`
	ResourcesScanned        int `json:"resourcesScanned"`
	ResourcesLabeledSuccess int `json:"resourcesLabeledSuccess"`
	ResourcesLabeledFailed  int `json:"resourcesLabeledFailed"`
}

type ImportNamespaceAppResult struct {
	AppID            string   `json:"appId"`
	Name             string   `json:"name"`
	WorkflowDisabled bool     `json:"workflowDisabled"`
	Components       []string `json:"components,omitempty"`
	Error            string   `json:"error,omitempty"`
}

// ImportNamespaceResourceIdentity is shared by internal adoption planning
// contracts. Public observe imports leave it nil until explicit adoption is
// activated by the dedicated import API.
type ImportNamespaceResourceIdentity struct {
	APIVersion      string `json:"apiVersion"`
	Kind            string `json:"kind"`
	Namespace       string `json:"namespace,omitempty"`
	Name            string `json:"name"`
	UID             string `json:"uid,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
	SpecDigest      string `json:"specDigest,omitempty"`
}

type ImportNamespaceResourceResult struct {
	Kind           string                           `json:"kind"`
	Namespace      string                           `json:"namespace"`
	Name           string                           `json:"name"`
	Source         *ImportNamespaceResourceIdentity `json:"source,omitempty"`
	AppID          string                           `json:"appId,omitempty"`
	ComponentName  string                           `json:"componentName,omitempty"`
	DependencyRole string                           `json:"dependencyRole,omitempty"`
	Ownership      string                           `json:"ownership,omitempty"`
	Disposition    string                           `json:"disposition,omitempty"`
	Status         string                           `json:"status"`
	Error          string                           `json:"error,omitempty"`
}

type ImportNamespaceApplicationsResponse struct {
	Namespace       string                          `json:"namespace"`
	Mode            string                          `json:"mode"`
	ManagementMode  config.ManagementMode           `json:"managementMode"`
	PlanFingerprint string                          `json:"planFingerprint,omitempty"`
	Summary         ImportNamespaceSummary          `json:"summary"`
	Apps            []ImportNamespaceAppResult      `json:"apps,omitempty"`
	ResourceResults []ImportNamespaceResourceResult `json:"resourceResults,omitempty"`
	Warnings        []string                        `json:"warnings,omitempty"`
}

type TryImportNamespaceApplicationsRequest struct {
	Namespace    string   `json:"namespace"`
	IncludeKinds []string `json:"includeKinds,omitempty"`
}

type TryImportNamespaceSummary struct {
	ResourcesScanned int `json:"resourcesScanned"`
	AppsMatched      int `json:"appsMatched"`
	OrphansDetected  int `json:"orphansDetected"`
}

type TryImportNamespaceAppResult struct {
	AppID                string   `json:"appId,omitempty"`
	Name                 string   `json:"name"`
	ScannedComponents    []string `json:"scannedComponents,omitempty"`
	OrphanComponentNames []string `json:"orphanComponentNames,omitempty"`
	Error                string   `json:"error,omitempty"`
}

type TryImportNamespaceOrphanComponent struct {
	AppID               string         `json:"appId"`
	AppName             string         `json:"appName"`
	ComponentName       string         `json:"componentName"`
	ComponentType       config.JobType `json:"componentType"`
	Reason              string         `json:"reason"`
	MatchedIncludeKinds []string       `json:"matchedIncludeKinds,omitempty"`
}

type TryImportNamespaceApplicationsResponse struct {
	Namespace        string                              `json:"namespace"`
	Summary          TryImportNamespaceSummary           `json:"summary"`
	Apps             []TryImportNamespaceAppResult       `json:"apps,omitempty"`
	OrphanComponents []TryImportNamespaceOrphanComponent `json:"orphanComponents,omitempty"`
	Warnings         []string                            `json:"warnings,omitempty"`
}

type CreateComponentRequest struct {
	Name          string         `json:"name"`
	ComponentType config.JobType `json:"type"`
	Image         string         `json:"image,omitempty"`
	Namespace     string         `json:"namespace"`
	Replicas      int32          `json:"replicas"`
	Properties    Properties     `json:"properties"`
	Traits        Traits         `json:"traits"`
	Template      *TemplateRef   `json:"tmp,omitempty"`
}

type TemplateRef struct {
	ID                  string `json:"id"`
	Target              string `json:"target,omitempty"`              // 目标模板组件名，用于精确匹配
	DefaultStorageClass string `json:"defaultStorageClass,omitempty"` // 模板展开时写入空 persistent storageClass 的默认值
}

type CreateWorkflowStepRequest struct {
	Name         string                         `json:"name"`
	StepType     config.WorkflowStepType        `json:"stepType,omitempty"`
	WorkflowType config.JobType                 `json:"jobType,omitempty"`
	Approval     *WorkflowStepApproval          `json:"approval,omitempty"`
	Properties   WorkflowProperties             `json:"properties,omitempty"`
	Components   []string                       `json:"components,omitempty"`
	Mode         string                         `json:"mode,omitempty"`
	SubSteps     []CreateWorkflowSubStepRequest `json:"subSteps,omitempty"`

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

type ApplicationsDeployRequest struct {
	WorkflowName string `json:"workflowName"`
	Name         string `json:"appName"`
}

type CleanupApplicationResourcesResponse struct {
	AppID             string   `json:"appId"`
	TaskID            string   `json:"taskId,omitempty"`
	DeletedResources  []string `json:"deletedResources"`
	FailedResources   []string `json:"failedResources,omitempty"`
	RetainedResources []string `json:"retainedResources,omitempty"`
}

type CleanupApplicationResourcesRequest struct {
	PlanFingerprint string `json:"planFingerprint,omitempty"`
}

type CleanupApplicationResourcesPlanResponse struct {
	AppID           string                          `json:"appId"`
	PlanFingerprint string                          `json:"planFingerprint"`
	ResourceResults []ImportNamespaceResourceResult `json:"resourceResults"`
	Warnings        []string                        `json:"warnings,omitempty"`
}

type DeleteApplicationRequest struct {
	WaitSeconds *int64 `json:"waitSeconds,omitempty"`
}

type DeleteApplicationResponse struct {
	AppID             string              `json:"appId"`
	CancelledTaskIDs  []string            `json:"cancelledTaskIds,omitempty"`
	ActiveTaskIDs     []string            `json:"activeTaskIds,omitempty"`
	DeletedResources  []string            `json:"deletedResources,omitempty"`
	FailedResources   []string            `json:"failedResources,omitempty"`
	Warnings          []string            `json:"warnings,omitempty"`
	DeletedCounts     DeleteResourceCount `json:"deletedCounts,omitempty"`
	ResourcesRetained bool                `json:"resourcesRetained,omitempty"`
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

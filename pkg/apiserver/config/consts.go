package config

import (
	"strings"
	"time"
)

const (
	REDIS        = "redis"
	KAFKA        = "kafka"
	TIDB         = "tidb"
	MYSQL        = "mysql"
	DBNAME_ERUUN = "eruun"
	NAMESPACE    = "eruun-system"
)

const (
	LabelManagedBy     = "app.kubernetes.io/managed-by"
	ManagedByEruun     = "eruun"
	LabelAppID         = "eruun.io/app-id"
	LabelComponentID   = "eruun.io/component-id"
	LabelComponentName = "eruun.io/component-name"
	LabelImportAppKey  = "eruun.io/import-app-key"
	LabelStorageRole   = "eruun.io/pvc-role"
	LabelShareName     = "eruun.io/share-name"
	LabelShareStrategy = "eruun.io/share-strategy"
)

type JobType string
type JobErrorPolicy string
type WorkflowTaskType string
type WorkflowMode string
type WorkflowStepType string
type Status string
type JobResultOutboxState string
type JobDelayState string
type VersionUpdateExecutionScope string
type ManagementMode string
type RuntimeRole string

func (s Status) ToLower() Status {
	return Status(strings.ToLower(string(s)))
}

const (
	RuntimeRoleAPI        RuntimeRole = "api"
	RuntimeRoleController RuntimeRole = "controller"
	RuntimeRoleScheduler  RuntimeRole = "scheduler"
	RuntimeRoleWorker     RuntimeRole = "worker"
)

const (
	ManagementModeNative  ManagementMode = "native"
	ManagementModeObserve ManagementMode = "observe"
	ManagementModeAdopted ManagementMode = "adopted"
)

func NormalizeManagementMode(value string) (ManagementMode, bool) {
	switch ManagementMode(strings.ToLower(strings.TrimSpace(value))) {
	case ManagementModeNative:
		return ManagementModeNative, true
	case ManagementModeObserve:
		return ManagementModeObserve, true
	case ManagementModeAdopted:
		return ManagementModeAdopted, true
	default:
		return "", false
	}
}

const (
	DefaultStorageMode                    = 420
	DefaultTaskRevoker                    = "system"
	CancelSourceSystem                    = "system"
	CancelSourceUser                      = "user"
	DefaultNamespace                      = "default"
	DeployTimeout                         = 60 * 20 // 20 minutes
	DefaultVersionUpdateImageReadyTimeout = 60 * 5  // 5 minutes
	CloudJobTimeout                       = 60 * 20 // 20 minutes
	DelTimeOut                            = 30 * time.Second
	JobNameRegx                           = "^[a-z\u4e00-\u9fa5][a-z0-9\u4e00-\u9fa5-]{0,31}$"
	WorkflowRegx                          = "^[a-zA-Z0-9-]+$"

	// ServerJob JobType 的类型分为几种：1.无状态服务 2.存储服务 3.网络服务
	ServerJob    JobType = "webservice"
	StoreJob     JobType = "store"
	ConfJob      JobType = "config"
	SecretJob    JobType = "secret"
	CloudJob     JobType = "cloudjob"
	Service      JobType = "service"
	InstantJob   JobType = "job"
	ScheduledJob JobType = "scheduledjob"

	JobDeploy                    JobType = "deploy"
	JobDeployService             JobType = "service_deploy"
	JobDeployStore               JobType = "store_deploy"
	JobDeployPVC                 JobType = "store_pvc_deploy"
	JobDeployConfigMap           JobType = "configmap_deploy"
	JobDeploySecret              JobType = "secret_deploy"
	JobDeployIngress             JobType = "ingress_deploy"
	JobDeployServiceAccount      JobType = "service_account_deploy"
	JobDeployRole                JobType = "role_deploy"
	JobDeployRoleBinding         JobType = "role_binding_deploy"
	JobDeployClusterRole         JobType = "cluster_role_deploy"
	JobDeployClusterRoleBinding  JobType = "cluster_role_binding_deploy"
	JobDeployPodDisruptionBudget JobType = "pod_disruption_budget_deploy"
	JobDeployNetworkPolicy       JobType = "network_policy_deploy"
	JobDeployInstant             JobType = "instant_job"
	JobDeployScheduled           JobType = "scheduled_job"
	JobDeployCloud               JobType = "cloudjob_deploy"
	JobDeployCallback            JobType = "callback"
	JobCleanupResources          JobType = "cleanup_resources"
	JobDatabaseReset             JobType = "database_reset"
	JobLogArchiveUpload          JobType = "log_archive_upload"
	JobVersionRestart            JobType = "version_restart"
)

const (
	JobInfoSourceVersionUpdateRemove = "version_update_remove"
	JobInfoSourceVersionUpdateAction = "version_update_action"
)

const (
	WorkflowTaskTypeWorkflow         WorkflowTaskType = "workflow"
	WorkflowTaskTypeTesting          WorkflowTaskType = "test"
	WorkflowTaskTypeScanning         WorkflowTaskType = "scan"
	WorkflowTaskTypeDelivery         WorkflowTaskType = "delivery"
	WorkflowTaskTypeUpdate           WorkflowTaskType = "update"
	WorkflowTaskTypeCleanup          WorkflowTaskType = "cleanup"
	WorkflowTaskTypeRestart          WorkflowTaskType = "restart"
	WorkflowTaskTypeStop             WorkflowTaskType = "stop"
	WorkflowTaskTypeStart            WorkflowTaskType = "start"
	WorkflowTaskTypeDatabaseReset    WorkflowTaskType = "database_reset"
	WorkflowTaskTypeLogArchiveUpload WorkflowTaskType = "log_archive_upload"

	WorkflowModeStepByStep WorkflowMode = "StepByStep"
	WorkflowModeDAG        WorkflowMode = "DAG"

	WorkflowStepTypeComponent WorkflowStepType = "component"
	WorkflowStepTypeApproval  WorkflowStepType = "approval"

	AnnotationJobStartTime     = "eruun.job/startTime"
	AnnotationJobRunPolicy     = "eruun.job/runPolicy"
	AnnotationJobTaskID        = "eruun.job/taskId"
	AnnotationJobExecutionKey  = "eruun.io/job-execution-key"
	AnnotationJobRunGeneration = "eruun.io/job-run-generation"
	// AnnotationComponentName stores the raw component name for task aggregation.
	AnnotationComponentName = "eruun.io/component-name"
	// AnnotationNamespaceAutoCreated marks namespaces created automatically by Eruun.
	AnnotationNamespaceAutoCreated = "eruun.io/namespace-auto-created"
	// AnnotationNamespaceOwnerAppID stores the appID that auto-created the namespace.
	AnnotationNamespaceOwnerAppID = "eruun.io/namespace-owner-app-id"
	// AnnotationAdoptedStatefulSetRetentionRestore marks a safe recreation whose original PVC retention policy still needs restoration.
	AnnotationAdoptedStatefulSetRetentionRestore = "eruun.io/adopted-statefulset-retention-restore"
	// AnnotationAdoptedRecreationToken binds a recreated object to its persisted write-ahead claim.
	AnnotationAdoptedRecreationToken = "eruun.io/adopted-recreation-token"
	// AnnotationWorkloadRestartAt aligns with kubectl rollout restart.
	AnnotationWorkloadRestartAt = "kubectl.kubernetes.io/restartedAt"
	WorkflowWorkerQueueGroup    = "workflow-workers"
	DelayQueueGroup             = "job-delay-dispatcher"
	ResultQueueGroup            = "job-result-dispatcher"

	WaitingTasksQueryTimeout            = 5 * time.Second
	TaskStateTransitionTimeout          = 5 * time.Second
	QueueDispatchTimeout                = 5 * time.Second
	DefaultJobTaskTimeout               = 20 * time.Minute //超时时间设置为20分钟
	DefaultJobTTLSeconds          int32 = 3600             // 默认 1 小时后清理完成的 Job
	DefaultCronJobSuccessfulLimit int32 = 3                // 默认保留成功的 CronJob 历史数
	DefaultCronJobFailedLimit     int32 = 3                // 默认保留失败的 CronJob 历史数
	DefaultRequestBodyLimitBytes  int64 = 24 * 1024 * 1024 // 默认请求体上限 24MB（覆盖 10MB inline YAML 的 JSON 包装开销）

	DefaultApplicationCleanupTimeout                    = 10 * time.Second
	DefaultApplicationWorkloadRestartTimeout            = 10 * time.Second
	DefaultApplicationWorkloadScaleTimeout              = 10 * time.Second
	DefaultApplicationStatusTransientFailedWindow       = 3 * time.Minute
	DefaultDeleteApplicationWaitSeconds           int64 = 60
	DeleteApplicationTaskPollInterval                   = 1 * time.Second

	DefaultComponentLogTailLines int64 = 200         // 组件日志默认拉取行数
	DefaultJobLogTailLines       int64 = 2000        // 默认最多拉取 2000 行日志
	DefaultJobLogLimitBytes      int64 = 1024 * 1024 // 默认最多拉取 1MB 日志
	DefaultHTTPShutdownTimeout         = 30 * time.Second
)

const (
	JobResultOutboxStateResultPending         JobResultOutboxState = "result_pending"
	JobResultOutboxStateResultDispatching     JobResultOutboxState = "result_dispatching_queue"
	JobResultOutboxStateResultQueued          JobResultOutboxState = "result_queued"
	JobResultOutboxStateResultProcessingQueue JobResultOutboxState = "result_processing_queue"
	JobResultOutboxStateResultProcessingLocal JobResultOutboxState = "result_processing_local"
	JobResultOutboxStateFailed                JobResultOutboxState = "failed"
)

const (
	JobDelayStatePending    JobDelayState = "pending"
	JobDelayStateDispatched JobDelayState = "dispatched"
)

const (
	StatusCompleted      Status = "completed"                      //执行完毕
	StatusDisabled       Status = "disabled"                       //已关闭
	StatusCreated        Status = "created"                        //创建
	StatusRunning        Status = "running"                        //运行中
	StatusPassed         Status = "passed"                         //通过
	StatusSkipped        Status = "skipped"                        //跳过
	StatusFailed         Status = "failed"                         //错误
	StatusTimeout        Status = "timeout"                        //超时
	StatusCancelled      Status = "cancelled"                      //取消
	StatusPause          Status = "pause"                          //暂停
	StatusWaiting        Status = "waiting"                        //等待中
	StatusQueued         Status = "queued"                         //排队中
	StatusBlocked        Status = "blocked"                        //阻塞
	QueueItemPending     Status = "pending"                        //等待调度
	StatusChanged        Status = "changed"                        //改变
	StatusNotRun         Status = "notRun"                         //没有运行
	StatusPrepare        Status = "prepare"                        //准备
	StatusReject         Status = "reject"                         //拒绝
	StatusDistributed    Status = "distributed"                    //分布式
	StatusWaitingApprove Status = "wait_for_approval"              //等待批准
	StatusDebugBefore    Status = "debug_before"                   //调试开始
	StatusDebugAfter     Status = "debug_after"                    //调试之后
	StatusUnstable       Status = "unstable"                       //不稳定
	StatusManualApproval Status = "wait_for_manual_error_handling" //等待手动错误处理
)

// ComponentStatus 组件运行时状态（由 Informer 同步）
type ComponentStatus string

const (
	ComponentStatusRunning    ComponentStatus = "Running"    // 运行中（所有副本就绪）
	ComponentStatusPending    ComponentStatus = "Pending"    // 部分副本就绪或等待调度
	ComponentStatusFailed     ComponentStatus = "Failed"     // 失败
	ComponentStatusUnknown    ComponentStatus = "Unknown"    // 未知状态
	ComponentStatusNotDeploy  ComponentStatus = "Not Deploy" // 未部署
	ComponentStatusCleaning   ComponentStatus = "Cleaning"   // 清理中
	ComponentStatusDeploying  ComponentStatus = "Deploying"  // 部署中
	ComponentStatusUpdating   ComponentStatus = "Updating"   // 更新中
	ComponentStatusRestarting ComponentStatus = "Restarting" // 重启中
	ComponentStatusStarting   ComponentStatus = "Starting"   // 启动中
	ComponentStatusStopped    ComponentStatus = "Stopped"    // 已停止
)

const (
	JobPriorityMaxHigh = 0
	// JobPriorityHigh defines the high priority level, for resources like Service, PVC, ConfigMap, Secret.
	JobPriorityHigh = 1
	// JobPriorityNormal defines the normal priority level, for resources like Deployments, StatefulSets.
	JobPriorityNormal = 10
	// JobPriorityLow defines the low priority level, for cleanup or notification jobs.
	JobPriorityLow = 20
)

func NormalizeWorkflowTaskType(value WorkflowTaskType) WorkflowTaskType {
	normalized := strings.ToLower(strings.TrimSpace(string(value)))
	return WorkflowTaskType(normalized)
}

func IsSupportedWorkflowTaskType(value WorkflowTaskType) bool {
	switch NormalizeWorkflowTaskType(value) {
	case WorkflowTaskTypeWorkflow,
		WorkflowTaskTypeUpdate,
		WorkflowTaskTypeTesting,
		WorkflowTaskTypeScanning,
		WorkflowTaskTypeDelivery,
		WorkflowTaskTypeDatabaseReset,
		WorkflowTaskTypeLogArchiveUpload:
		return true
	default:
		return false
	}
}

// ParseWorkflowMode normalizes workflow mode values, defaulting to StepByStep when empty or unknown.
func ParseWorkflowMode(mode string) WorkflowMode {
	switch WorkflowMode(mode) {
	case WorkflowModeDAG:
		return WorkflowModeDAG
	case WorkflowModeStepByStep:
		return WorkflowModeStepByStep
	default:
		return WorkflowModeStepByStep
	}
}

// ParseWorkflowStepType normalizes workflow step type values, defaulting to component.
func ParseWorkflowStepType(stepType string) WorkflowStepType {
	switch WorkflowStepType(strings.ToLower(strings.TrimSpace(stepType))) {
	case WorkflowStepTypeApproval:
		return WorkflowStepTypeApproval
	default:
		return WorkflowStepTypeComponent
	}
}

func IsSupportedWorkflowJobType(jobType JobType) bool {
	switch JobType(strings.TrimSpace(string(jobType))) {
	case "", JobDeploy, JobCleanupResources, JobDatabaseReset, JobLogArchiveUpload:
		return true
	default:
		return false
	}
}

func ComponentTypeUsesPods(componentType JobType) bool {
	switch JobType(strings.TrimSpace(string(componentType))) {
	case ServerJob, StoreJob, InstantJob, ScheduledJob:
		return true
	default:
		return false
	}
}

// IsParallel reports whether the workflow mode permits parallel execution.
func (m WorkflowMode) IsParallel() bool {
	return m == WorkflowModeDAG
}

// 用户侧声明的存储类型（API 入参）
const (
	StorageTypePersistent  = "persistent"
	StorageTypeEphemeral   = "ephemeral"
	StorageTypeHostMounted = "host-mounted"
	StorageTypeConfig      = "config"
	StorageTypeSecret      = "secret"
)

// Kubernetes 中内部映射的 Volume 类型
const (
	VolumeTypePVC       = "pvc"
	VolumeTypeEmptyDir  = "emptyDir"
	VolumeTypeHostPath  = "hostPath"
	VolumeTypeConfigMap = "configMap"
	VolumeTypeSecret    = "secret"
)

var StorageTypeMapping = map[string]string{
	StorageTypePersistent:  VolumeTypePVC,
	StorageTypeEphemeral:   VolumeTypeEmptyDir,
	StorageTypeHostMounted: VolumeTypeHostPath,
	StorageTypeConfig:      VolumeTypeConfigMap,
	StorageTypeSecret:      VolumeTypeSecret,
}

// UpdateStrategy 版本更新策略类型
type UpdateStrategy string

const (
	// UpdateStrategyRolling 滚动更新（默认）- 逐步替换Pod，保证服务可用性
	UpdateStrategyRolling UpdateStrategy = "rolling"
	// UpdateStrategyRecreate 重建更新 - 先删除所有旧Pod，再创建新Pod
	UpdateStrategyRecreate UpdateStrategy = "recreate"
	// UpdateStrategyCanary 金丝雀更新 - 先更新部分Pod，验证后再全量更新
	UpdateStrategyCanary UpdateStrategy = "canary"
	// UpdateStrategyBlueGreen 蓝绿部署 - 创建新版本，切换流量后销毁旧版本
	UpdateStrategyBlueGreen UpdateStrategy = "blue-green"
)

// ParseUpdateStrategy 解析更新策略，默认返回滚动更新
func ParseUpdateStrategy(strategy string) UpdateStrategy {
	switch UpdateStrategy(strategy) {
	case UpdateStrategyRecreate:
		return UpdateStrategyRecreate
	case UpdateStrategyCanary:
		return UpdateStrategyCanary
	case UpdateStrategyBlueGreen:
		return UpdateStrategyBlueGreen
	case UpdateStrategyRolling:
		return UpdateStrategyRolling
	default:
		return UpdateStrategyRolling
	}
}

const (
	VersionUpdateExecutionScopeFullWorkflow      VersionUpdateExecutionScope = "full_workflow"
	VersionUpdateExecutionScopeChangedComponents VersionUpdateExecutionScope = "changed_components"
)

func NormalizeVersionUpdateExecutionScope(scope string) (VersionUpdateExecutionScope, bool) {
	switch VersionUpdateExecutionScope(strings.ToLower(strings.TrimSpace(scope))) {
	case "":
		return VersionUpdateExecutionScopeFullWorkflow, true
	case VersionUpdateExecutionScopeFullWorkflow:
		return VersionUpdateExecutionScopeFullWorkflow, true
	case VersionUpdateExecutionScopeChangedComponents:
		return VersionUpdateExecutionScopeChangedComponents, true
	default:
		return "", false
	}
}

// ComponentAction 组件操作类型
type ComponentAction string

const (
	// ComponentActionUpdate 更新组件（默认）
	ComponentActionUpdate ComponentAction = "update"

	// ComponentActionAdd 新增组件
	ComponentActionAdd ComponentAction = "add"

	// ComponentActionRemove 删除组件
	ComponentActionRemove ComponentAction = "remove"

	// ComponentActionRestart 重启组件工作负载
	ComponentActionRestart ComponentAction = "restart"
)

// ParseComponentAction 解析组件操作类型，默认返回更新
func ParseComponentAction(action string) ComponentAction {
	switch ComponentAction(action) {
	case ComponentActionAdd:
		return ComponentActionAdd
	case ComponentActionRemove:
		return ComponentActionRemove
	case ComponentActionRestart:
		return ComponentActionRestart
	case ComponentActionUpdate:
		return ComponentActionUpdate
	default:
		return ComponentActionUpdate
	}
}

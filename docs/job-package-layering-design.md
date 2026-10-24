# job 包分层拆分设计

> 状态：Implemented Reference。本文记录 `pkg/apiserver/event/workflow/job` 包按职责拆成子包的设计与实施结果。已于 2026-10-20 通过 #172–#178 实施，没有改变任何 HTTP/gRPC、DSL、数据库或部署契约。第 1–14 节保留实施前的设计，其中的数字是当时的静态分析估计；实际结果以“实施结果”一节为准。

## 实施结果

| PR | 内容 |
|---|---|
| #170 | 前置：4 个大文件在原包内按职责拆分 |
| #172 | 单包内预整理：拆开同时包含多个目标包声明的文件，`CleanCancelled` 移回各自类型所在文件 |
| #173 | 抽出 `jobinfo` |
| #174 | 抽出 `jobexec` |
| #175 | 抽出 `jobdeploy` |
| #176 | 抽出 `jobcleanup`、`jobresult` |
| #177 | 抽出 `jobinstant`、`jobdelay` |
| #178 | 抽出 `databasereset`、`logarchive`、`jobcallback` |

**最终结构**：`job` 包只剩 `job.go`、`job_pool.go`、`evaluation_recovery.go`，内容是 `RunJobs`/`runJob`、`initJobCtl` 工厂、`Pool` 和评测恢复。跨包共用的测试辅助代码放在 `job/jobtest`，这是一个非测试包，只被 `_test.go` import。

**与设计的差异**：

- **命名**：子包统一加 `job` 前缀，`databasereset`、`logarchive` 保持原名，原因见第 4.2 节的命名说明。
- **导出的声明**：生产代码共新导出 148 个声明：`jobinfo` 71、`jobexec` 63、`jobresult` 9、`jobdeploy` 2、`jobcleanup` 1、`jobinstant` 1、`logarchive` 1。比设计多出的 1 个是 `jobresult.BuildJobResultOutbox`，为测试导出。
- **导出的字段**：设计时没有统计字段，实际导出了 34 个：
  - 控制器基类 `DeployNamespacedResourceJobBase` 的 9 个字段，控制器里 `c.job` 等访问随之改为 `c.Job`，约 750 行；
  - `Runtime` 2 个、`JobBuildOptions` 2 个、`SharedResourceAccessOptions` 8 个、`TrackedResourceReconcileOptions` 7 个；
  - `jobresult` 的 6 个配置和计数字段，为综合测试导出。
- **测试**：只测某个子包内部的测试函数随代码迁移，保留白盒调用。同时覆盖多个子包的测试按包拆分，不改成调用公开入口。各步的测试函数数量都与基线一致，只有 #177 拆分综合测试时从 2679 个增加到 2684 个；断言数没有减少。
- **待决事项**（第 14 节）均已决定：接受导出代价，不使用 `internal`，子包统一加 `job` 前缀，在没有其他 `job` 改动的窗口内连续实施。
- **不变量**：fencing/CAS 原语保持未导出。每一步都做了逐声明 token 比对（只允许首字母大小写和包限定符不同），并跑了宿主机全量门禁。没有在真实集群上运行。

**遗留的 P3**：

- `jobresult.ResultDispatcher.EnsureFailures` 为测试导出后，包外代码也能修改；
- 部分测试辅助代码在两个包里各有一份副本；
- `jobtest.ShareLockerPrefix` 复制了生产常量的值。

## 1. 背景与目标

`pkg/apiserver/event/workflow/job` 现在是一个包：57 个源文件、71 个测试文件（测试约 2.9 万行）。部署、清理、延迟派发、结果回传、重试、数据库重置、日志归档等 Job 都在同一个包里，目录很难一眼看出边界。

在 Go 里，一个目录就是一个包，所以“按目录组织”就等于拆包。本文回答四个问题：

1. 拆成哪几个包，依赖方向是什么；
2. 基础层包含哪些符号；
3. `InstantJobCtl` 等跨文件的类型怎么收拢；
4. 测试怎么搬，分几个 PR 推进。

**目标**

- 包之间的依赖严格单向（分层无环），每个子包对应一类 Job 或一组共用原语。
- 拆分全过程不改变运行行为：错误语义、启动顺序、租约/fencing/CAS、恢复协议都保持原样。

**非目标**

- 不借拆包顺便重构逻辑、改名或删能力。
- 不引入注册表、接口注入等新的间接层来绕开依赖。依赖关系按现状搬，必要时把声明下沉到更低的层。
- 不提供兼容别名。Eruun 未发布，调用方直接改 import。

## 2. 分析方法与证据等级

- **基线**：PR #170（`refactor/split-large-files`，`44c0eff`）。这个 PR 只按职责拆分了 `job.go` 等大文件，没有改变包结构。
- **工具**：用 `go/types` 对 `job` 包及其包内测试做类型检查，建立“顶层声明 → 它引用的包内顶层声明”的依赖图（含方法、字段所属类型、接口方法）。共 57 个源文件、70 个包内测试文件；另有 1 个外部测试包 `job_test`，它只用导出 API。
- **验证**：给定“文件/声明 → 目标包”的映射和层次顺序，检查是否存在同层或向上的引用，并统计需要新导出的符号。
- **证据等级**：以上全部是静态分析。本文的数字（导出数、测试归属、文件规模）都来自这套分析，没有经过真实拆分编译验证。实施时每个 PR 以编译和门禁为准，数字可能有小幅偏差。

## 3. 现状：为什么不能直接按现有文件分组

把 57 个文件按文件名分成 6 组（core、resource、cleanup、delay、result、special），6 组两两互相依赖：

- `job.go` 的 `initJobCtl` 要构造所有控制器，所以 core 依赖其他每一组；反过来，每一组都用 core 的 `Runtime`、`saveJobInfo`、`deployNamespacedResourceJobBase` 等。
- 共享原语分散在上层文件里。`job_batch.go` 实际上是“K8s Job 原语”（所有权、等待、删除超时），被 cleanup、delay、result、logarchive 共用；databasereset、retry 的检查点编解码被 `job_info.go` 的 `findExistingJobInfo` 调用。
- 一个类型的方法分散在不同组：`InstantJobCtl` 的方法分布在 4 个文件；`cancel_recovery.go` 里还放着 `InstantJobCtl` 和 `ScheduledJobCtl` 的 `CleanCancelled`。
- 跨组引用 192 个名字，其中 137 个是未导出的。

所以不能按现有文件整体搬。要先把“被多方共用的声明”下沉到基础层，再按 Job 类型分包。

## 4. 目标结构

### 4.1 层次

```text
L5  job                 RunJobs / runJob / initJobCtl 工厂 / Pool / 评测恢复
L4  jobinstant  jobdelay  databasereset  logarchive  jobcallback
L3  jobcleanup  jobresult
L2  jobdeploy
L1  jobexec             执行环境与 K8s 执行原语
L0  jobinfo             JobInfo 记录、载荷、检查点、状态转换
```

只允许高层引用低层，同层之间不引用。分析得到的包间依赖（不含传递依赖）：

| 包 | 直接依赖 |
|---|---|
| `job` | jobcallback、jobcleanup、databasereset、jobdelay、jobdeploy、jobexec、jobinstant、logarchive、jobinfo |
| `jobinstant` | jobresult、jobexec、jobinfo |
| `jobdelay` | jobresult、jobexec、jobinfo |
| `databasereset` | jobcleanup、jobdeploy、jobexec、jobinfo |
| `logarchive` | jobexec、jobinfo |
| `jobcallback` | jobinfo |
| `jobcleanup` | jobdeploy、jobexec、jobinfo |
| `jobresult` | jobexec、jobinfo |
| `jobdeploy` | jobexec、jobinfo |
| `jobexec` | jobinfo |
| `jobinfo` | 无（只依赖 `job` 目录以外的包） |

### 4.2 包与职责

规模是该包里顶层声明的行数，不含 import 和测试。

| 包（目录 `job/<name>`） | 规模 | 职责 | 主要来源 |
|---|---:|---|---|
| `jobinfo` | ~1480 | JobInfo 的持久化与所有权写入、各类 Job 载荷与检查点编解码、Job 状态转换、`StatusError`、延迟队列载荷 | `job_info`、`job_payload`、`job_status_transition`、`delay_queue`、`configmap_input`，以及从 databasereset/retry/callback/result 下沉的检查点与载荷声明 |
| `jobexec` | ~2180 | `Runtime`、控制器基类、K8s Job 构建/等待/所有权校验、资源 reconcile 与共享资源保护、清理跟踪与执行器、Pod 日志收集、取消恢复、组件状态与元数据、`PrepareTask` | `job_batch`、`job_controller_base`、`resource_reconcile`、`shared_resource_*`、`cleanup_tracker`、`cleanup_resource_executor`、`job_delete`、`shared_cleanup_policy`、`job_pod_logs`、`cancel_recovery`、`job_component_*`、`workspace_task` |
| `jobdeploy` | ~3560 | 各类 K8s 资源控制器（Deployment、StatefulSet、Service、入口路由、ConfigMap、Secret、PVC、版本重启）和全部 `Generate*` 生成器 | `job_deploy`、`job_statefulset`、`job_service`、`route`、`job_configmap`、`job_secret`、`job_pvc`、`job_version_restart`、`workload_*`、`podspec_compare`、`metadata_preserve`、`resource_generation`、`resource_wait` |
| `jobcleanup` | ~3640 | `CleanupResourcesJobCtl` 及 StatefulSet Pod/PVC 清理、检查点与保留策略 | `job_cleanup_resources*` |
| `jobresult` | ~1250 | 结果派发与 outbox | `job_result`、`job_result_outbox` |
| `jobinstant` | ~1120 | `InstantJobCtl`：一次性 K8s Job、重试、准入预算 | `job_instant`、`job_retry`、`job_retry_resources`、`job_scheduling`、`job_scheduling_resources` |
| `jobdelay` | ~1150 | 延迟派发器与 `ScheduledJobCtl` | `delay_dispatcher`、`job_scheduled` |
| `databasereset` | ~550 | `DatabaseResetJobCtl` | `job_database_reset` |
| `logarchive` | ~190 | 日志归档上传 Job 与上传器配置 | `job_log_archive_upload` |
| `jobcallback` | ~380 | 回调 Job | `job_callback` |
| `job` | ~840 | 对外入口：`RunJobs`、`runJob`、`runAdmittedJob`、`initJobCtl` 工厂、`Pool`、`WithEvaluationRecovery` | `job.go`、`job_pool`、`evaluation_recovery` |

命名说明：

- 不用 `resource`：`k8s.io/apimachinery/pkg/api/resource` 在这个包里就被 import 为 `resource`，同名会造成混淆。
- 不用 `batch`：它和 `batchv1` 语义重叠，而且这个包的核心类型是 `InstantJobCtl`。
- 子包统一加 `job` 前缀（`databasereset`、`logarchive` 无冲突，保持原名）。原因是 `record`、`result`、`callback`、`deploy`、`execution`、`cleanup`、`delay` 在这些代码里被大量用作局部变量名，例如 `record` 出现在 49 个文件、164 行；`result` 出现在 70 个文件、172 行。包名会被同名变量遮蔽，加前缀后零冲突，也不需要 import 别名。共享测试辅助包叫 `jobtest`。

## 5. 基础层

基础层分两层：`jobinfo` 只管“数据和持久化”，`jobexec` 管“在 K8s 上执行”。分析表明这样拆是无环的：`jobexec` 依赖 `jobinfo`，`jobinfo` 不依赖 `jobexec`。前提是把 `StatusError` 系列和 `configmap_input` 放进 `jobinfo`，因为 `job_info.go` 和载荷代码直接用到它们。

### 5.1 `jobinfo`（L0）

按来源文件列出全部声明。带 `*` 的来自其他职责文件，是因为 `findExistingJobInfo` 等持久化代码要用到才下沉：

| 来源 | 声明 |
|---|---|
| `job_info.go` | `addJobInfoIfWorkflowOwned`、`buildJobInfoRecord`、`compareAndSwapJobInfoIfWorkflowOwned`、`componentNameFromJobInfo`、`copyJobInfoRecord`、`findExistingJobInfo`、`findExistingVersionUpdateCleanupJobInfo`、`isVersionUpdateRemoveCleanupInternalInfo`、`jobInfoAnnotations`、`jobInfoExecutionKey`、`jobInfoLabels`、`jobInfoSaveMaxAttempts`、`loadJobInfos`、`parseVersionUpdateCleanupInternalInfo`、`preserveEvaluationRunnerCheckpoint`、`putJobInfoIfWorkflowOwned`、`rawComponentNameFromJobInfo`、`resolveJobServiceName`、`saveExecutionJobInfo`、`saveJobInfo`、`saveOrUpdateJobInfo`、`saveOrUpdateVersionUpdateCleanupJobInfo`、`shareStrategyFromJobInfo`、`validateVersionUpdateCleanupInternalInfo`、`versionUpdateCleanupJobInfo*`（6 个）、`versionUpdateCleanupRequiresStatefulSetDeletion`、`versionUpdateCleanupStatefulSetPVCTemplatesToDelete`、`withJobInfoOwnership` |
| `job_payload.go` | `ApplyExecutionIdentity`、`ApplyTaskIDAnnotation`、`isNilJobInfo`、`jobInfoTypeName`、`optionalJobInfo`、`requiredJobInfo`、`scheduledJobInfo`，以及各类 `xxxFromJobInfo` 载荷解码函数 |
| `job_status_transition.go` | `applyJobError`、`completeJobTransition`、`finishJobTransitionWithError`、`jobErrorMessage`、`runJobFunc`、`runJobWithStatus`、`runJobWithWait`、`startJobTransition`、`statusFromError`、`waitJobFunc` |
| `delay_queue.go` | `DelayJobPayload`、`EnqueueDelayJob`、`ErrDelayQueueUnavailable`、`delayJobNotification`、`delayJobNotificationVersion`、`notificationForDelayJob`、`persistDelayJobCheckpoint`、`validateDelayJobIdentity`、`validateDelayJobPayload` |
| `configmap_input.go` | `ConfigMapInput`、`(ConfigMapInput).GenerateConf`、`SecretInput`、`configMapMaxSize`、`extractFileNameFromURLForSecret`、`extractFileNameFromURLSimple` |
| `job.go`* | `StatusError`、`NewStatusError`、`ExtractStatusError` |
| `job_retry.go`*、`job_retry_resources.go`* | `EvaluationRunnerCheckpoint`、`SetEvaluationRunnerCheckpoint`、`HasInstantJobRetryCheckpoint`、`ValidateInstantJobRetryExecution`、`instantJobRetryCheckpoint`、`decodeInstantJobRetryCheckpoint`、`retryJobMatchesTask`、`retryPolicyFromJob`、`validateRetryJobResources` |
| `job_database_reset.go`* | `DatabaseResetJobInfo`、`databaseResetExecutionKey`、`databaseResetExecutionKeyFromJob`、`databaseResetReplicaCheckpoint`、`databaseResetReplicaCheckpointVersion`、`decodeDatabaseResetReplicaCheckpoint`、`preservePreparedDatabaseResetCheckpointForSave`、`selectDatabaseResetCheckpointJobInfo` |
| `job_callback.go`* | `CallbackJobInfo`、`CallbackPayload` |
| `job_result.go`* | `isSuccessfulTerminalJobStatus`、`shouldKeepExistingJobInfoStatus`、`stampWorkspaceJobPodIdentity` |
| 其他* | `isSettledDelayedExecutionStatus`（`delay_dispatcher.go`）、`errJobExecutionIdentityChanged`（`job_batch.go`） |

**正确性约束**：JobInfo 的 fencing/CAS 核心 `compareAndSwapJobInfoIfWorkflowOwned`、`putJobInfoIfWorkflowOwned`、`addJobInfoIfWorkflowOwned` 只在 `jobinfo` 内部使用，拆分后保持未导出。对外只导出 `saveJobInfo`、`withJobInfoOwnership` 等原有的包装函数，写入路径不变。

### 5.2 `jobexec`（L1）

| 来源 | 声明 |
|---|---|
| `job.go`* | `Runtime` 及其方法 `close`、`GenerateServiceResult`、`WithTaskMetadata`、`TaskIDFromContext`、`taskIDKey`、`infrastructureStopCause`、`isSharedDefaultSkippedConfigJob` |
| `job_controller_base.go` | `deployNamespacedResourceJobBase` 及其方法 `SaveInfo`、`cleanCreated`、`cleanCreatedBatchJobs`、`runWithStatus`、`runWithWait`，`newDeployNamespacedResourceJobBase` |
| `job_batch.go` | `buildJob`、`buildJobContainer`、`buildCronJob`、`jobBuildOptions`、`(deployNamespacedResourceJobBase).createBatchJob`/`waitBatchJob`、`applyJobRunPolicy`、`runPolicyAction*`、`runPolicyFromJob`、Job 所有权（`ensureJobWorkflowOwnership`、`ensureCurrentJobWorkflowOwnership`、`currentJobWorkflowOwnershipStatus`、`errWorkflowJobOwnershipChanged`、`jobOwnerGeneration`、`validateExistingJobExecutionIdentity`、`adoptReusableJobExecution`）、等待与查询（`waitForJobCompletion`、`waitForJobCompletionMatching`、`jobExists`、`jobTerminalStatus`、`jobCompletedInStore`、`jobSucceededOwnedPodsCompleted`、`jobRequiredCompletions`、`completedJobFilters`、`podOwnedByJob`、`startTimeFromJob`、`isTerminalWorkflowJobTaskStatus`）、常量（`jobDeleteTimeout`、`jobPollInterval`、`jobTTLSeconds`、`cronHistoryLimit`）、`jobTypeForTask` |
| `resource_reconcile.go` | `createOrUpdateResource`、`createOrUpdateTrackedResource`、`reconcileTrackedResource`、`resolveSharedResourceAccess`、`trackResourcePresence`、`updateResourceWithRetry` 及相关函数类型与选项 |
| `shared_resource_locking.go`、`shared_resource_guard.go`、`shared_cleanup_policy.go` | `acquireShareLock`、`newShareLocker`、`shareInfoFromLabels`、`resolveSharedResource`、`hasSharedResources`、`shareListOptions`、`shareLockKey`、`shareLockerPrefix`、`handleSharedResource`、`resourceListFn`、`shouldForceCleanupSharedWorkload` |
| `cleanup_tracker.go`、`cleanup_resource_executor.go`、`job_delete.go` | `WithCleanupTracker`、`MarkResourceCreated`、`markResourceObserved`、`resourcesForCleanup`、`cleanupTracker`、`resourceRef`、`cleanupNamespacedResources`、`cleanupClusterResources`、`deleteNamespacedFunc`、`deleteClusterFunc`、`deleteJobForeground`、`foregroundJobDeleteTimeout`、`waitForJobUIDDeletion`、`jobUIDAbsent` |
| `job_pod_logs.go` | `collectJobPodLogs`、`collectJobPodLogsForJob`、`finalizeCompletedJob`、`finalizeCompletedJobIfNeeded`、`deleteCompletedJobAndPods`、`readPodContainerLogs`、`readPodContainerLogsWithMode`、`podLogContainerNames` 等 14 个 |
| `cancel_recovery.go` | `CleanupRecoveredCancelledJobs`、`CleanupRecoveredCancelledJobsPage`、`IsCancelledJobCleanupPending`、`cancelledJobCleaner` 接口、`beginCancelledJobCleanup`、`finishOnlineCancelledJobCleanup`、`deleteCancelledBatchJob`、`deleteCancelledKubernetesJob`/`CronJob`、`recoverableKubernetesJobType` 等（不含两个 `CleanCancelled` 方法） |
| `job_component_status.go`、`job_component_metadata.go`、`workspace_task.go` | 组件状态同步 8 个函数；`ParseProperties`、`BuildLabels`、`BuildAnnotations`、`ApplyComponentManagedLabels`、`ApplyComponentAnnotationsToObject`；`PrepareTask` |
| 其他* | `ArchiveUploader`/`ArchiveUploadInput`/`ArchiveUploadResult`（`job_log_archive_upload.go`）、`EvaluationDeadlineAnnotation`（`evaluation_recovery.go`）、`databaseResetNamespaceOrDefault`、`isStatefulSetTemplatePVCName`（`job_database_reset.go`） |

`deployNamespacedResourceJobBase` 是所有控制器嵌入的基类。跨包后它要导出（例如改名为 `ControllerBase`），`runWithStatus`、`runWithWait`、`cleanCreated`、`createBatchJob`、`waitBatchJob` 这些通过嵌入调用的方法也要导出。

## 6. 各子包边界

| 包 | 对 `job` 目录外暴露 | 不放进来的东西 |
|---|---|---|
| `jobdeploy` | `Generate*` 生成器 8 个、`VersionRestartJobInfo`、各 `NewDeploy*JobCtl` | `GenerateInstantJob`、`GenerateScheduledCronJob` 放这里（`jobcleanup` 要用它们重算待删对象），控制器本身留在 `jobinstant`/`jobdelay` |
| `jobcleanup` | `NewCleanupResourcesJobCtl` | 共享的删除、等待原语（在 `jobexec`） |
| `jobresult` | `NewResultDispatcher`、`NewResultOutboxDispatcher` | 执行身份戳以外的 JobInfo 写入（在 `jobinfo`） |
| `jobinstant` | `NewInstantJobCtl`、`GenerateOneTimeJob`、`RestoreInstantJobRetryCheckpoint` | 重试检查点编解码（在 `jobinfo`） |
| `jobdelay` | `NewDelayDispatcher`、`NewScheduledJobCtl` | 延迟队列载荷与入队（在 `jobinfo`） |
| `databasereset` | `NewDatabaseResetJobCtl` | 检查点类型（在 `jobinfo`）、命名空间与 PVC 名判断（在 `jobexec`） |
| `logarchive` | `LogArchiveUploadJobInfo`、`SetArchiveUploader`、`NewLogArchiveUploadJobCtl` | 上传器接口类型（在 `jobexec`） |
| `jobcallback` | `NewCallbackJobCtl`、`TerminalCallbackExecutionKey`、`TerminalCallbackKeyEvent` | `CallbackJobInfo`、`CallbackPayload`（在 `jobinfo`） |
| `job` | `RunJobs`、`WithEvaluationRecovery`、`NewPool` | 其余全部下沉 |

## 7. `InstantJobCtl` 的收拢

现状：`InstantJobCtl` 嵌入 `deployNamespacedResourceJobBase`，它的 22 个方法分散在 4 个文件里：

| 文件 | 方法 |
|---|---|
| `job_instant.go` | `Clean`、`SaveInfo`、`Run`、`run`、`allowEvaluationTerminalCleanup`、`evaluationSourceReady` |
| `job_retry.go` | `ensureRetryWorkflowOwnership`、`retainRetryCheckpoint`、`runWithRetryPolicy`、`newRetryCheckpoint`、`persistRetryCheckpoint`、`ensureRetryAttempt`、`markLostRetryExecution`、`markAuthoritativeLostRetryExecution`、`retainLiveRetryAttempt`、`persistRetryAttemptUID`、`waitRetryAttempt`、`deleteRetryJob`、`cleanRetryAttempt`、`waitPreviousRetryAttempt` |
| `job_scheduling.go` | `waitRetryCreationBudget` |
| `cancel_recovery.go` | `CleanCancelled` |

收拢方式：

1. 22 个方法全部进入 `jobinstant` 包，文件名保持职责划分（`instant.go`、`retry.go`、`scheduling.go`、`cancel.go`）。
2. 不依赖 `InstantJobCtl` 的部分下沉：
   - 重试检查点编解码和评测 Runner 检查点进入 `jobinfo`。`job_info.go` 的 `preserveEvaluationRunnerCheckpoint` 和取消恢复都要用。
   - `GenerateInstantJob` 进入 `jobdeploy`，因为 `jobcleanup` 要用它重算待删对象。
3. 同样处理 `ScheduledJobCtl.CleanCancelled`：从 `cancel_recovery.go` 移到 `jobdelay`，`cancel_recovery.go` 的其余部分进入 `jobexec`。

收拢只是搬移，方法体不改。`waitForJobAdmission` 由 `job` 包的 `runJob` 调用，需要从 `jobinstant` 导出。

## 8. 必须拆散的现有文件

下面 15 个文件里的声明会落到不同的包，要先在单包内拆好（见第 10 节的 PR 1）：

| 文件 | 去向 |
|---|---|
| `job.go` | `job` 748 行 / `jobexec` 51 行 / `jobinfo` 20 行 |
| `job_database_reset.go` | `databasereset` 550 / `jobinfo` 106 / `jobexec` 27 / `jobdeploy` 14 |
| `job_retry.go` | `jobinstant` 551 / `jobinfo` 123 |
| `cancel_recovery.go` | `jobexec` 320 / `jobdelay` 30 / `jobinstant` 14 |
| `job_result.go` | `jobresult` 785 / `jobinfo` 26 |
| `job_callback.go` | `jobcallback` 381 / `jobinfo` 25 |
| `job_retry_resources.go` | `jobinstant` 73 / `jobinfo` 27 |
| `job_log_archive_upload.go` | `logarchive` 194 / `jobexec` 21 |
| `job_instant.go` | `jobinstant` 238 / `jobdeploy` 14 |
| `job_scheduled.go` | `jobdelay` 151 / `jobdeploy` 14 |
| `delay_dispatcher.go` | `jobdelay` 965 / `jobinfo` 8 |
| `evaluation_recovery.go` | `job` 30 / `jobexec` 1 |
| `job_batch.go` | `jobexec` 620 / `jobinfo` 1 |

## 9. 导出面

- **生产代码需要新导出 147 个符号**：`jobinfo` 71 个、`jobexec` 63 个、`jobresult` 8 个、`jobdeploy` 2 个、`jobcleanup`/`logarchive`/`jobinstant` 各 1 个，完整清单见附录。
- **测试另需 54 个**（见第 10 节），合计约 200 个。
- **还有字段**：依赖图把字段访问记成“依赖所属类型”，所以上面的数字不含结构体字段。实施时发现，控制器基类 `deployNamespacedResourceJobBase` 的 9 个字段（`job`、`client`、`store`、`namespace`、`ack` 等）被嵌入它的控制器大量直接访问，例如 `c.job` 约 450 处、`c.client` 约 180 处。抽出 `jobexec` 时这些字段也要导出，涉及 25 个控制器文件、约 750 行机械替换。
- **命名规则**：只把首字母改成大写；与已有导出名冲突时，按新包语境去掉冗余前缀，例如 `record.saveJobInfo` → `record.Save`。改名清单在各 PR 里逐个列出，不在本文预先确定。
- **关于 `internal`**：可以把 `jobinfo`、`jobexec` 放进 `job/internal/`，让新导出的符号只对 `job/...` 可见。但 `job` 目录外现在要用其中 23 个符号（`jobinfo` 12 个，如 `CallbackJobInfo`、`ApplyExecutionIdentity`；`jobexec` 11 个，如 `Runtime`、`BuildLabels`、`PrepareTask`）。要用 `internal`，就得把这部分对外类型再拆到非 internal 包，或者由 `job` 转发。本文建议不用 `internal`，代价是导出面变大，靠评审约束写入路径（见第 12 节）。

## 10. 测试搬迁

71 个测试文件按“引用了哪些包的未导出声明”归类：

- 35 个只引用单个目标包，跟着代码搬。
- 34 个跨包引用。规则是放进它引用到的最高层包，因为那个包能 import 所有低层包。两个同层包都被引用时（例如同时引用 `jobinstant` 和 `jobdelay`），放进 `job`。
- 按这个规则，最终分布如下：

| 包 | 测试文件 | 行数 |
|---|---:|---:|
| `job` | 15 | ~6430 |
| `jobcleanup` | 12 | ~6020 |
| `jobdeploy` | 14 | ~5120 |
| `jobdelay` | 7 | ~3780 |
| `jobresult` | 4 | ~1980 |
| `databasereset` | 1 | ~1300 |
| `jobinstant` | 5 | ~940 |
| `jobexec` | 4 | ~660 |
| `jobinfo` | 4 | ~640 |
| `jobcallback` | 1 | ~460 |
| `logarchive` | 1 | ~180 |

- 测试会引用 93 个别的包里的未导出声明，其中 54 个不在生产代码的新导出清单里。处理方式二选一：
  1. 改成通过已导出 API 断言；
  2. 确实需要白盒检查的，就留在被测包里，必要时拆成两个测试：低层逻辑的断言留在低层包，组合行为的断言放进上层包。

  不为测试单独新增导出。
- 有 66 个测试辅助声明（约 820 行，主要在 `job_batch_test.go`、`job_result_outbox_test.go`、`job_cleanup_resources_test.go`、`dispatcher_ack_test.go`、`job_database_reset_test.go`）被落在不同包的测试共用。把它们放进新包 `job/jobtest`（只被 `_test.go` import，不进生产二进制），里面是 fake store、fake kube client、JobInfo 构造器等。
- 外部测试包 `job_test` 只用导出 API，改 import 即可。

## 11. 包外调用方

`job` 目录外有 19 个源文件、17 个测试文件 import `job`。拆分后按新包改 import（数字是“符号数 / 涉及文件数”）：

| 新包 | 被外部使用的符号 |
|---|---|
| `jobinfo` | 拆分时的外部调用符号（已移除的能力除外）：`ApplyExecutionIdentity`、`ApplyTaskIDAnnotation`、`CallbackJobInfo`、`CallbackPayload`、`ConfigMapInput`、`SecretInput`、`DatabaseResetJobInfo`、`EvaluationRunnerCheckpoint`、`SetEvaluationRunnerCheckpoint`、`HasInstantJobRetryCheckpoint`、`ValidateInstantJobRetryExecution` |
| `jobexec` | 11 / 15：`Runtime`、`GenerateServiceResult`、`WithTaskMetadata`、`PrepareTask`、`ParseProperties`、`BuildLabels`、`ApplyComponentManagedLabels`、`ApplyComponentAnnotationsToObject`、`EvaluationDeadlineAnnotation`、`IsCancelledJobCleanupPending`、`CleanupRecoveredCancelledJobsPage` |
| `jobdeploy` | 9 / 5：`Generate*` 8 个、`VersionRestartJobInfo` |
| `jobcallback` | 3 / 7 |
| `jobinstant` | 3 / 5 |
| `job` | 2 / 4：`RunJobs`、`WithEvaluationRecovery` |
| `jobresult` | 2 / 1 |
| `jobcleanup`、`jobdelay`、`logarchive` | 各 1 |

调用方集中在 `event/workflow`（Job 构建器、控制器）、`domain/service/workflow`、`domain/service/application`、`jobs`。

## 12. 风险与约束

| 风险 | 说明 | 处理 |
|---|---|---|
| fencing/CAS 被绕过 | 导出 `saveJobInfo`、`withJobInfoOwnership` 后，`job` 目录外的代码也能直接写 JobInfo | CAS 原语保持未导出；PR 评审检查新增的 `jobinfo` 调用方；必要时再评估 `internal` |
| 可变全局状态 | `logarchive` 的 `defaultArchiveUploader`（带锁）、`callbackHTTPClient` | 跟着所属包搬，不复制、不新增访问路径 |
| 方法跨包失效 | 嵌入基类的未导出方法在其他包里无法提升调用 | 第 5.2 节列出的方法一律导出，由编译器检查 |
| 大量冲突 | 每个 PR 都会改到几十个文件 | 每个 PR 只做一层，合并前 rebase 并重跑门禁；拆分期间暂停 `job` 包的其他改动 |
| 核心路径 | `jobinfo` 和 `jobexec` 涉及执行身份、所有权、结果 outbox、取消恢复 | 按 `core` 类处理：自己实现，不派发给执行者；每个 PR 都做独立只读复核 |

## 13. PR 拆分

每个 PR 都是纯搬移加改名、改 import，不改逻辑。

| PR | 内容 | 预计规模 |
|---|---|---|
| 0 | 前置：合并 #170 | — |
| 1 | **单包内预整理**：把第 8 节 15 个文件中要去往不同包的声明拆到按目标包命名的文件里；收拢 `InstantJobCtl` 的 22 个方法；把 `ScheduledJobCtl.CleanCancelled` 移回 `job_scheduled.go`。仍是一个包，用声明指纹校验 | 约 20 个文件，无导出变化 |
| 2 | 抽出 `jobinfo` 和 `job/jobtest`：整文件移动，导出 71 个符号；改 `job` 包和 20 个外部文件的 import；搬 4 个测试文件 | 中 |
| 3 | 抽出 `jobexec`：导出 63 个符号，控制器基类改名导出；改 15 个外部文件 | 大 |
| 4 | 抽出 `jobdeploy` | 中 |
| 5 | 抽出 `jobcleanup`、`jobresult` | 中 |
| 6 | 抽出 `jobinstant`、`jobdelay` | 中 |
| 7 | 抽出 `databasereset`、`logarchive`、`jobcallback` | 小 |

每个 PR 的验证：

- 拆分前后，全部源文件的声明集合一致：只允许改名和导出，用改名映射表对照。
- 门禁：`gofmt`、`go vet`、`go build`、`go test ./... -race`、敏感内容检查。
- 跨包依赖方向与第 4.1 节一致：用 `go list -deps` 检查。
- Codex 只读复核。

PR 2 和 PR 3 涉及核心路径，由主会话实现，不派发给执行者。

## 14. 待决事项

1. ~~**是否接受约 200 个新导出符号的代价**~~（已决定：接受），换取按目录组织。如果不接受，就维持现状：单包加文件名前缀。
2. ~~子包命名~~：已决定统一加 `job` 前缀（见第 4.2 节命名说明）。
3. ~~**是否把 `jobinfo`/`jobexec` 放进 `internal`**~~（已决定：不放）。
4. ~~**实施时机**~~：已于 2026-10-20 连续实施完成。

## 附录：生产代码需要新导出的符号

- **jobinfo**：`applyJobError`、`batchJobFromJobInfo`、`buildJobInfoRecord`、`callbackInfoFromJobInfo`、`cleanupComponentFromJobInfo`、`clusterRoleBindingFromJobInfo`、`clusterRoleFromJobInfo`、`componentNameFromJobInfo`、`configMapFromJobInfo`、`databaseResetExecutionKey`、`databaseResetExecutionKeyFromJob`、`databaseResetReplicaCheckpoint`、`databaseResetReplicaCheckpointVersion`、`decodeDatabaseResetReplicaCheckpoint`、`decodeInstantJobRetryCheckpoint`、`delayJobNotificationVersion`、`deploymentFromJobInfo`、`errJobExecutionIdentityChanged`、`findExistingJobInfo`、`findExistingVersionUpdateCleanupJobInfo`、`instantJobRetryCheckpoint`、`isSettledDelayedExecutionStatus`、`isVersionUpdateRemoveCleanupInternalInfo`、`jobErrorMessage`、`jobInfoExecutionKey`、`jobInfoSaveMaxAttempts`、`loadJobInfos`、`notificationForDelayJob`、`optionalJobInfo`、`persistDelayJobCheckpoint`、`pvcFromJobInfo`、`requiredJobInfo`、`resolveJobServiceName`、`retryJobMatchesTask`、`retryPolicyFromJob`、`roleBindingFromJobInfo`、`roleFromJobInfo`、`runJobFunc`、`runJobWithStatus`、`runJobWithWait`、`saveJobInfo`、`saveOrUpdateJobInfo`、`saveOrUpdateVersionUpdateCleanupJobInfo`、`scheduledJobInfo`、`secretFromJobInfo`、`selectDatabaseResetCheckpointJobInfo`、`serviceAccountFromJobInfo`、`serviceFromJobInfo`、`shareStrategyFromJobInfo`、`stampWorkspaceJobPodIdentity`、`statefulSetFromJobInfo`、`statusFromError`、`validateDelayJobIdentity`、`validateDelayJobPayload`、`validateRetryJobResources`、`validateVersionUpdateCleanupInternalInfo`、`versionUpdateCleanupJobInfoConditions`、`versionUpdateCleanupJobInfoOwnershipMatches`、`versionUpdateCleanupJobInfoSameExecution`、`versionUpdateCleanupJobInfoUpdates`、`versionUpdateCleanupJobInfoWriteIsStale`、`versionUpdateCleanupRequiresStatefulSetDeletion`、`versionUpdateCleanupStatefulSetPVCTemplatesToDelete`、`waitJobFunc`、`withJobInfoOwnership`
- **jobexec（63）**：`Runtime` 的方法 `close`；`deployNamespacedResourceJobBase` 及其方法 `cleanCreated`、`cleanCreatedBatchJobs`、`createBatchJob`、`runWithStatus`、`runWithWait`、`waitBatchJob`，`newDeployNamespacedResourceJobBase`、`applyJobRunPolicy`、`beginCancelledJobCleanup`、`buildCronJob`、`buildJob`、`cancelledCronJobMatches`、`cancelledJobCleaner`、`cancelledJobCleanupComplete`、`cancelledJobCleanupPreserved`、`cleanupNamespacedResources`、`collectJobPodLogs`、`collectJobPodLogsForJob`、`createOrUpdateResource`、`createOrUpdateTrackedResource`、`currentJobWorkflowOwnershipStatus`、`databaseResetNamespaceOrDefault`、`deleteCancelledBatchJob`、`deleteCancelledKubernetesCronJob`、`deleteCompletedJobAndPods`、`ensureCurrentJobWorkflowOwnership`、`ensureJobWorkflowOwnership`、`errWorkflowJobOwnershipChanged`、`finalizeCompletedJobIfNeeded`、`finishOnlineCancelledJobCleanup`、`infrastructureStopCause`、`invalidateComponentsCache`、`isStatefulSetTemplatePVCName`、`jobBuildOptions`、`jobDeleteTimeout`、`jobExists`、`jobOwnerGeneration`、`jobPollInterval`、`jobTerminalStatus`、`jobTypeForTask`、`markResourceObserved`、`newShareLocker`、`podLogContainerNames`、`readPodContainerLogsWithMode`、`reconcileTrackedResource`、`recoverableKubernetesJobType`、`resolveSharedResourceAccess`、`resourceRef`、`resourcesForCleanup`、`runPolicyActionSkip`、`runPolicyFromJob`、`shareInfoFromLabels`、`sharedResourceAccessOptions`、`shouldForceCleanupSharedWorkload`、`startTimeFromJob`、`syncComponentStatusOnJobStart`、`syncConfigComponentStatusIfWorkflowOwned`、`trackResourcePresence`、`trackedResourceReconcileOptions`、`updateResourceWithRetry`、`waitForJobCompletionMatching`
- **jobresult（8）**：`createJobResultOutbox`、`errResultDispatchNoRetry`、`getJobResultOutboxByPayload`、`jobResultMatchesExecutionIdentity`、`newJobResultPayloadFromDelay`、`stampCronJobExecutionIdentity`、`stampJobExecutionIdentity`、`updateJobInfoStatus`
- **jobdeploy（2）**：`cleanObjectMeta`、`desiredReplicasFromStatefulSet`
- **jobcleanup（1）**：`cleanupPollInterval`
- **logarchive（1）**：`currentArchiveUploader`
- **jobinstant（1）**：`waitForJobAdmission`

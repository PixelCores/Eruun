package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobcleanup"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobdelay"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobdeploy"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinstant"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type JobCtl interface {
	Run(ctx context.Context) error
	Clean(ctx context.Context)
	SaveInfo(ctx context.Context) error
}

const (
	cancelledJobCleanupTimeout       = 4 * config.DelTimeOut
	cancelledJobCleanupLeaseDuration = config.DelTimeOut
	cancelledJobCleanupRenewInterval = config.DelTimeOut / 3
)

func newJobRuntime(config *jobexec.Runtime) *jobexec.Runtime {
	if config == nil {
		config = &jobexec.Runtime{}
	}
	return &jobexec.Runtime{
		Concurrency:       config.Concurrency,
		Client:            config.Client,
		Store:             config.Store,
		Ack:               config.Ack,
		StopOnFailure:     config.StopOnFailure,
		RedisClient:       config.RedisClient,
		Cache:             config.Cache,
		URLSecurityPolicy: config.URLSecurityPolicy,
		DelayQueue:        config.DelayQueue,
		ResourceWaiter:    config.ResourceWaiter,
		KubeConfig:        config.KubeConfig,
		ShareLocker:       jobexec.NewShareLocker(config.RedisClient),
		ArchiveUploader:   currentArchiveUploader(),
	}
}

func initJobCtl(job *model.JobTask, runtime *jobexec.Runtime) JobCtl {
	if !validJobControllerDependencies(job, runtime) {
		return nil
	}
	switch job.JobType {
	case string(config.JobDeploy):
		return jobdeploy.NewDeployJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployService):
		return jobdeploy.NewDeployServiceJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployStore):
		return jobdeploy.NewDeployStatefulSetJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployPVC):
		return jobdeploy.NewDeployPVCJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployConfigMap):
		return jobdeploy.NewDeployConfigMapJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeploySecret):
		return jobdeploy.NewDeploySecretJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployIngress):
		return jobdeploy.NewDeployIngressJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployServiceAccount):
		return jobdeploy.NewDeployServiceAccountJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployRole):
		return jobdeploy.NewDeployRoleJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployRoleBinding):
		return jobdeploy.NewDeployRoleBindingJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployClusterRole):
		return jobdeploy.NewDeployClusterRoleJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployClusterRoleBinding):
		return jobdeploy.NewDeployClusterRoleBindingJobCtl(job, runtime, runtime.ShareLocker)
	case string(config.JobDeployInstant), string(config.JobCommand), string(config.JobEval):
		return jobinstant.NewInstantJobCtl(job, runtime)
	case string(config.JobDeployScheduled):
		return jobdelay.NewScheduledJobCtl(job, runtime)
	case string(config.JobDeployCloud):
		return NewCloudJobCtl(job, runtime.Store)
	case string(config.JobDeployCallback):
		return NewCallbackJobCtl(job, runtime.Store, runtime.URLSecurityPolicy)
	case string(config.JobCleanupResources):
		cleanupCtl := jobcleanup.NewCleanupResourcesJobCtl(job, runtime)
		if cleanupCtl == nil {
			return nil
		}
		return cleanupCtl
	case string(config.JobDatabaseReset):
		return NewDatabaseResetJobCtl(job, runtime)
	case string(config.JobLogArchiveUpload):
		return NewLogArchiveUploadJobCtl(job, runtime)
	case string(config.JobVersionRestart):
		return jobdeploy.NewVersionRestartJobCtl(job, runtime)
	default:
		klog.ErrorS(fmt.Errorf("unknown job type"), "init job controller failed", "jobName", job.Name, "jobType", job.JobType)
		return nil
	}
}

func validJobControllerDependencies(job *model.JobTask, runtime *jobexec.Runtime) bool {
	if runtime == nil || runtime.Store == nil {
		klog.ErrorS(fmt.Errorf("store is nil"), "init job controller failed")
		return false
	}
	if job == nil {
		klog.ErrorS(fmt.Errorf("job is nil"), "init job controller failed")
		return false
	}
	if runtime.Client == nil && job.JobType != string(config.JobDeployCallback) {
		klog.ErrorS(fmt.Errorf("client is nil"), "init job controller failed", "jobName", job.Name, "jobType", job.JobType)
		return false
	}

	return true
}

func RunJobs(ctx context.Context, jobs []*model.JobTask, run *jobexec.Runtime) error {
	logger := klog.FromContext(ctx)
	if len(jobs) == 0 {
		logger.Info("no jobs to run")
		return nil
	}
	if run == nil {
		run = &jobexec.Runtime{}
	}
	store := run.Store
	concurrency, stopOnFailure := run.Concurrency, run.StopOnFailure

	if scope, ok := access.FromContext(ctx); ok {
		for _, task := range jobs {
			if task == nil || task.Namespace != scope.Namespace {
				return fmt.Errorf("job namespace does not match workspace")
			}
			if config.IsWorkspaceJobType(config.JobType(task.JobType)) || task.AppID == "" && task.JobType == string(config.JobDeployPVC) {
				if task.TaskID == "" || task.WorkspaceID != scope.WorkspaceID || (task.AppID != "" && task.JobType != string(config.JobEval)) {
					return fmt.Errorf("job workspace does not match execution scope")
				}
				if task.AppID == "" {
					if err := access.NewStore(store).Check(ctx, &model.JobInfo{
						TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Type: task.JobType, ServiceName: jobinfo.ResolveJobServiceName(task),
					}); err != nil {
						return fmt.Errorf("authorize workspace job: %w", err)
					}
					continue
				}
				// Application evaluations use the same application ownership check
				// as other component executions below.
			}
			if task.AppID == "" {
				return fmt.Errorf("job application is missing")
			}
			if err := access.NewStore(store).Get(ctx, &model.Applications{ID: task.AppID}); err != nil {
				return err
			}
			switch config.JobType(task.JobType) {
			case config.JobDeployCloud, config.JobDeployServiceAccount, config.JobDeployRole, config.JobDeployRoleBinding, config.JobDeployClusterRole, config.JobDeployClusterRoleBinding:
				return fmt.Errorf("job type %s is forbidden in workspaces", task.JobType)
			}
		}
	}
	runtime := newJobRuntime(run)
	defer runtime.Close()

	if concurrency == 1 {
		for _, job := range jobs {
			if ctx.Err() != nil {
				return jobexec.InfrastructureStopCause(ctx)
			}
			logger.Info("Job started", "jobName", job.Name, "jobType", job.JobType)
			if err := runJobWithEvaluationRecovery(ctx, job, runtime); err != nil {
				return err
			}
			if ctx.Err() != nil {
				return jobexec.InfrastructureStopCause(ctx)
			}
			// DEBUG: Log job completion status before checking for failure.
			logger.Info("DEBUG: Job finished running", "jobName", job.Name, "status", job.Status)
			if jobStatusFailed(job.Status) {
				if stopOnFailure {
					logger.Error(nil, "Job failed, stopping workflow execution.", "jobName", job.Name, "status", job.Status)
					return nil
				}
				logger.Error(nil, "Job failed, continuing workflow execution.", "jobName", job.Name, "status", job.Status)
			}
		}
		return nil
	}
	jobPool := NewPool(ctx, jobs, runtime)
	if err := jobPool.Run(); err != nil {
		return err
	}
	return jobexec.InfrastructureStopCause(ctx)
}

func runJob(ctx context.Context, job *model.JobTask, runtime *jobexec.Runtime) (resultErr error) {
	if runtime == nil {
		return fmt.Errorf("run job: runtime is nil")
	}
	client, store, ack := runtime.Client, runtime.Store, runtime.Ack
	tracer := otel.Tracer("job-runner")
	ctx, span := tracer.Start(ctx, job.Name, trace.WithAttributes(
		attribute.String("job.name", job.Name),
		attribute.String("job.type", job.JobType),
	))
	defer span.End()

	logger := klog.FromContext(ctx).WithValues(
		"spanID", span.SpanContext().SpanID().String(),
		"jobName", job.Name,
	)
	ctx = klog.NewContext(ctx, logger)
	ctx = jobexec.WithCleanupTracker(ctx)

	var (
		watcher  *signal.CancelWatcher
		cancelFn context.CancelFunc = func() {}
		jobCtx                      = ctx
	)

	defer func() {
		cancelFn()
	}()
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if watcher != nil {
			watcher.Stop(releaseCtx)
		}
	}()

	if jobExecutionAlreadySettled(job) {
		logger.Info("Job execution already settled", "status", job.Status, "runGeneration", job.RunGeneration, "executionKey", job.ExecutionKey)
		return
	}
	if job.Status == config.StatusSkipped {
		logger.Info("Job skipped", "status", job.Status)
		job.Error = ""
		now := time.Now().Unix()
		if job.StartTime == 0 {
			job.StartTime = now
		}
		if job.EndTime == 0 {
			job.EndTime = now
		}
		if ack != nil {
			ack()
		}
		if store == nil {
			klog.Error("start job store is nil")
			return
		}
		jobCtl := initJobCtl(job, runtime)
		if jobCtl == nil {
			logger.Error(nil, "Failed to initialize job controller for skipped job")
			return
		}
		return terminalJobPersistenceFailure(job, "persist skipped job state", persistTerminalJobState(ctx, jobCtl, job, store, runtime))
	}
	if store == nil {
		job.Status = config.StatusFailed
		job.Error = "job datastore is unavailable"
		job.StartTime = time.Now().Unix()
		job.EndTime = job.StartTime
		if ack != nil {
			ack()
		}
		logger.Error(nil, "Refusing job without datastore")
		return
	}
	jobCtl := initJobCtl(job, runtime)

	job.Status = config.StatusPrepare
	job.Error = ""
	job.StartTime = time.Now().Unix()
	if ack != nil {
		ack()
	}
	if ctx.Err() != nil {
		if signal.IsInfrastructureStop(ctx) {
			logger.Info("Skip terminal job state after infrastructure stop")
			return
		}
		job.Status = config.StatusCancelled
		job.Error = ctx.Err().Error()
		job.EndTime = time.Now().Unix()
		return terminalJobPersistenceFailure(job, "persist cancelled job state", persistTerminalJobState(ctx, jobCtl, job, store, runtime))
	}

	logger.Info("Starting job", "jobType", job.JobType, "status", job.Status)
	if jobCtl == nil {
		errMsg := fmt.Sprintf("failed to initialize job controller for job: %s", job.Name)
		logger.Error(nil, errMsg)
		job.Status = config.StatusFailed
		job.Error = errMsg
		job.EndTime = time.Now().Unix()
		span.SetStatus(codes.Error, "Failed to initialize job controller")
		span.RecordError(errors.New(errMsg))
		ack()
		return
	}

	if taskID := jobexec.TaskIDFromContext(ctx); taskID != "" {
		var err error
		var watcherCtx context.Context
		var watcherCancel context.CancelFunc
		var redisClient *redis.Client
		if runtime != nil {
			redisClient = runtime.RedisClient
		}
		watcher, watcherCtx, watcherCancel, err = signal.WatchWithClient(ctx, taskID, redisClient)
		if err != nil {
			if signal.IsInfrastructureStop(ctx) {
				logger.Info("Skip cancellation watcher failure after infrastructure stop", "taskID", taskID, "cause", context.Cause(ctx))
				return
			}
			errMsg := fmt.Sprintf("activate cancellation watcher: %v", err)
			logger.Error(err, "Failed to activate cancellation watcher", "taskID", taskID)
			job.Status = config.StatusFailed
			job.Error = errMsg
			job.EndTime = time.Now().Unix()
			span.SetStatus(codes.Error, "Failed to activate cancellation watcher")
			span.RecordError(err)
			ack()
			return terminalJobPersistenceFailure(job, "persist cancellation watcher failure", persistTerminalJobState(ctx, jobCtl, job, store, runtime))
		}
		jobCtx = watcherCtx
		cancelFn = watcherCancel
	}

	if signal.IsInfrastructureStop(jobCtx) {
		logger.Info("Skip job execution after infrastructure stop", "cause", context.Cause(jobCtx))
		return context.Cause(jobCtx)
	}
	releaseAdmission, admissionErr := jobinstant.WaitForJobAdmission(jobCtx, store, job, client)
	defer func() {
		if errors.Is(resultErr, signal.ErrInfrastructureStop) && job.Status == config.StatusCancelled && jobexec.RecoverableKubernetesJobType(job.JobType) {
			return
		}
		if err := releaseAdmission(); err != nil {
			resultErr = errors.Join(resultErr, signal.ErrInfrastructureStop, err)
		}
	}()
	if admissionErr != nil {
		if !suppressTerminalJobPersistence(jobCtx) {
			// The API commits cancellation before publishing its signal. A queue
			// poll can see that state first; only the same lease may finish this
			// unstarted Job under the cancelled parent instead of running it.
			ownershipCtx, cancel := context.WithTimeout(context.WithoutCancel(jobCtx), 5*time.Second)
			status, ownershipErr := jobexec.CurrentJobWorkflowOwnershipStatus(ownershipCtx, store, job)
			cancel()
			if ownershipErr != nil {
				return errors.Join(signal.ErrInfrastructureStop, admissionErr, ownershipErr)
			}
			cancelled := status == config.StatusCancelled
			if status == config.StatusCancelled && (job.OwnerStatus == "" || job.OwnerStatus == config.StatusRunning) {
				job.OwnerStatus = config.StatusCancelled
			}
			if cancelled {
				job.Status = config.StatusCancelled
				job.Error = context.Canceled.Error()
				if jobCtx.Err() != nil {
					job.Error = jobCtx.Err().Error()
				}
				job.EndTime = time.Now().Unix()
				if config.IsInstantJobType(config.JobType(job.JobType)) && job.InternalInfo != "" {
					// Recovery can wait for admission while its checkpoint still
					// owns a live attempt. Cancellation must stop that attempt too.
					if err := cleanCancelledJob(jobCtx, jobCtl, store, job); err != nil {
						return errors.Join(signal.ErrInfrastructureStop, err)
					}
				}
				return persistTerminalJobState(jobCtx, jobCtl, job, store, runtime)
			}
			if statusErr, ok := jobinfo.ExtractStatusError(admissionErr); ok && statusErr.Status == config.StatusTimeout && status == config.StatusRunning {
				// Expired checkpoints consume no new admission. Fence cleanup by
				// the same DB ownership check as the admitted execution path.
				ownershipCtx, cancel := persistenceContext(jobCtx)
				ownershipErr := jobinfo.WithJobInfoOwnership(ownershipCtx, store, job, func(datastore.DataStore) error { return nil })
				cancel()
				if ownershipErr != nil {
					return errors.Join(signal.ErrInfrastructureStop, admissionErr, ownershipErr)
				}
				if signal.IsInfrastructureStop(jobCtx) {
					return context.Cause(jobCtx)
				}
				jobinfo.ApplyJobError(job, statusErr, "")
				job.EndTime = time.Now().Unix()
				jobCtl.Clean(jobCtx)
				if ack != nil {
					ack()
				}
				return terminalJobPersistenceFailure(job, "persist expired job state", persistTerminalJobState(jobCtx, jobCtl, job, store, runtime))
			}
		}
		return errors.Join(signal.ErrInfrastructureStop, admissionErr)
	}
	return runAdmittedJob(jobCtx, jobCtl, job, client, store, ack, runtime, span)
}

func runAdmittedJob(jobCtx context.Context, jobCtl JobCtl, job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), runtime *jobexec.Runtime, span trace.Span) (resultErr error) {
	logger := klog.FromContext(jobCtx)
	// Admission may wait while the application is deleted. Check it still exists
	// after that wait, immediately before execution side effects.
	if jobRequiresApplicationWritePermission(job.JobType) {
		if err := requireApplicationForWrite(jobCtx, store, job.AppID); err != nil {
			if signal.IsInfrastructureStop(jobCtx) {
				logger.Info("Skip missing application failure after infrastructure stop", "cause", context.Cause(jobCtx))
				return
			}
			job.Status = config.StatusFailed
			job.Error = err.Error()
			job.StartTime = time.Now().Unix()
			job.EndTime = job.StartTime
			if ack != nil {
				ack()
			}
			span.SetStatus(codes.Error, "Application check rejected job")
			span.RecordError(err)
			logger.Error(err, "Refusing job for missing application")
			return terminalJobPersistenceFailure(job, "persist rejected job state", persistTerminalJobState(jobCtx, jobCtl, job, store, runtime))
		}
	}
	persistCtx, persistCancel := persistenceContext(jobCtx)
	var startStatusAppID string
	ownershipErr := jobinfo.WithJobInfoOwnership(persistCtx, store, job, func(writeStore datastore.DataStore) error {
		var err error
		startStatusAppID, err = jobexec.SyncComponentStatusOnJobStart(persistCtx, job, writeStore)
		return err
	})
	persistCancel()
	if ownershipErr != nil {
		logger.Error(ownershipErr, "Failed to verify workflow ownership before job execution", "runGeneration", job.OwnerRunGeneration)
		return errors.Join(
			signal.ErrInfrastructureStop,
			fmt.Errorf("verify workflow ownership before job execution: %w", ownershipErr),
		)
	}
	jobexec.InvalidateComponentsCache(jobCtx, runtime, startStatusAppID, "job start status sync")

	cleaned := false

	defer func() {
		recovered := recover()
		if signal.IsInfrastructureStop(jobCtx) || errors.Is(resultErr, signal.ErrInfrastructureStop) {
			logger.Info("Leave job attempt non-terminal for infrastructure recovery", "cause", context.Cause(jobCtx))
			return
		}
		if recovered != nil {
			if !cleaned {
				jobCtl.Clean(jobCtx)
				cleaned = true
			}
			errMsg := fmt.Sprintf("job panic: %v", recovered)
			logger.Error(errors.New(errMsg), "Panic recovered in job execution")
			job.Status = config.StatusFailed
			job.Error = errMsg
			span.SetStatus(codes.Error, "Panic in job execution")
			span.RecordError(errors.New(errMsg))
		}
		job.EndTime = time.Now().Unix()
		if job.Error != "" {
			logger.Error(errors.New(job.Error), "Finished job with error", "status", job.Status, "detail", job.Error)
		} else {
			logger.Info("Finished job successfully", "status", job.Status)
		}
		ack()
		logger.Info("Updating job info in db...")
		resultErr = errors.Join(resultErr, terminalJobPersistenceFailure(
			job,
			"persist terminal job state",
			persistTerminalJobState(jobCtx, jobCtl, job, store, runtime),
		))
	}()

	statusBeforeRun := job.Status
	errorBeforeRun := job.Error
	endTimeBeforeRun := job.EndTime
	runErr := jobCtl.Run(jobCtx)
	if signal.IsInfrastructureStop(jobCtx) || errors.Is(runErr, signal.ErrInfrastructureStop) || errors.Is(runErr, repository.ErrWorkflowOwnershipLost) {
		ownershipCtx, cancel := context.WithTimeout(context.WithoutCancel(jobCtx), 5*time.Second)
		status, ownershipErr := jobexec.CurrentJobWorkflowOwnershipStatus(ownershipCtx, store, job)
		cancel()
		if ownershipErr == nil && status == config.StatusCancelled {
			job.OwnerStatus = config.StatusCancelled
			runErr = context.Canceled
		} else {
			job.Status = statusBeforeRun
			job.Error = errorBeforeRun
			job.EndTime = endTimeBeforeRun
			if signal.IsInfrastructureStop(jobCtx) {
				return context.Cause(jobCtx)
			}
			return errors.Join(signal.ErrInfrastructureStop, runErr, ownershipErr)
		}
	}
	cancelled := errors.Is(runErr, context.Canceled) || errors.Is(jobCtx.Err(), context.Canceled)
	if cancelled {
		ownershipCtx, cancel := context.WithTimeout(context.WithoutCancel(jobCtx), 5*time.Second)
		cancelledOwner, ownershipErr := adoptCancelledWorkflowOwner(ownershipCtx, store, job)
		cancel()
		if ownershipErr != nil {
			return errors.Join(signal.ErrInfrastructureStop, ownershipErr)
		}
		if !cancelledOwner {
			return errors.Join(signal.ErrInfrastructureStop, context.Canceled)
		}
		cancelErr := runErr
		if cancelErr == nil {
			cancelErr = jobCtx.Err()
		}
		reason := signal.ReasonFromContext(jobCtx)
		jobinfo.ApplyJobError(job, cancelErr, reason)
		job.Status = config.StatusCancelled
		job.EndTime = time.Now().Unix()
		span.SetStatus(codes.Error, "Job execution cancelled")
		span.RecordError(cancelErr)
		if !cleaned {
			if cleanupErr := cleanCancelledJob(jobCtx, jobCtl, store, job); cleanupErr != nil {
				return errors.Join(signal.ErrInfrastructureStop, cleanupErr)
			}
			cleaned = true
		}
	} else if runErr != nil {
		if !cleaned {
			jobCtl.Clean(jobCtx)
			cleaned = true
		}
		span.SetStatus(codes.Error, "Job execution failed")
		span.RecordError(runErr)
		jobinfo.ApplyJobError(job, runErr, job.Error)
		if job.Status != config.StatusFailed && job.Status != config.StatusCancelled && job.Status != config.StatusTimeout {
			job.Status = config.StatusFailed
		}
	} else if job.Status == config.StatusPrepare || job.Status == config.StatusRunning {
		job.Status = config.StatusCompleted
	}

	if job.Status == config.StatusCompleted {
		jobexec.FinalizeCompletedJobIfNeeded(jobCtx, client, job)
	}

	if !cleaned && jobStatusFailed(job.Status) {
		jobCtl.Clean(jobCtx)
	}
	return nil
}

func adoptCancelledWorkflowOwner(ctx context.Context, store datastore.DataStore, job *model.JobTask) (bool, error) {
	status, err := jobexec.CurrentJobWorkflowOwnershipStatus(ctx, store, job)
	if err != nil {
		return false, err
	}
	switch status {
	case config.StatusRunning:
		return false, nil
	case config.StatusCancelled:
		job.OwnerStatus = config.StatusCancelled
		return true, nil
	default:
		return false, fmt.Errorf("%w: workflow is %s", jobexec.ErrWorkflowJobOwnershipChanged, status)
	}
}

func cleanCancelledJob(ctx context.Context, jobCtl JobCtl, store datastore.DataStore, job *model.JobTask) error {
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), cancelledJobCleanupTimeout)
	defer cleanupCancel()
	if err := renewCancelledJobCleanupLease(cleanupCtx, store, job); err != nil {
		return err
	}
	if err := jobexec.BeginCancelledJobCleanup(cleanupCtx, store, job); err != nil {
		return fmt.Errorf("persist cancelled job cleanup intent: %w", err)
	}
	renewCtx, stopRenew := context.WithCancel(cleanupCtx)
	renewDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(cancelledJobCleanupRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				renewDone <- nil
				return
			case <-ticker.C:
				renewCallCtx, cancel := context.WithTimeout(renewCtx, 5*time.Second)
				err := renewCancelledJobCleanupLease(renewCallCtx, store, job)
				cancel()
				if err != nil {
					cleanupCancel()
					renewDone <- err
					return
				}
			}
		}
	}()
	outcome := jobexec.CancelledJobCleanupComplete
	if cleaner, ok := jobCtl.(jobexec.CancelledJobCleaner); ok {
		var err error
		outcome, err = cleaner.CleanCancelled(cleanupCtx)
		if err != nil {
			stopRenew()
			<-renewDone
			return err
		}
	} else {
		jobCtl.Clean(cleanupCtx)
	}
	if err := jobexec.FinishOnlineCancelledJobCleanup(cleanupCtx, store, job, outcome); err != nil {
		stopRenew()
		<-renewDone
		return fmt.Errorf("complete cancelled job cleanup intent: %w", err)
	}
	stopRenew()
	if err := <-renewDone; err != nil {
		return err
	}
	if err := cleanupCtx.Err(); err != nil {
		return fmt.Errorf("clean cancelled job: %w", err)
	}
	return nil
}

func renewCancelledJobCleanupLease(ctx context.Context, store datastore.DataStore, job *model.JobTask) error {
	renewed, err := repository.RenewCancelledWorkflowTaskLease(
		ctx, store, job.TaskID, jobexec.JobOwnerGeneration(job), job.RunToken, job.WorkerID, cancelledJobCleanupLeaseDuration,
	)
	if err != nil {
		return fmt.Errorf("renew cancelled workflow cleanup lease: %w", err)
	}
	if !renewed {
		return fmt.Errorf("%w: cancelled workflow cleanup lease changed", jobexec.ErrWorkflowJobOwnershipChanged)
	}
	return nil
}

func jobExecutionAlreadySettled(job *model.JobTask) bool {
	if job == nil {
		return false
	}
	switch job.Status {
	case config.StatusCompleted,
		config.StatusPassed,
		config.StatusFailed,
		config.StatusTimeout,
		config.StatusCancelled,
		config.StatusReject:
		return true
	case config.StatusDistributed:
		jobType := config.JobType(job.JobType)
		return config.IsInstantJobType(jobType) || jobType == config.JobDeployScheduled
	default:
		return false
	}
}

// requireApplicationForWrite rechecks persisted state at the point of
// execution: a write job must not run after its application was deleted.
func requireApplicationForWrite(ctx context.Context, store datastore.DataStore, appID string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil
	}
	if store == nil {
		return fmt.Errorf("load application: datastore is nil")
	}
	if err := store.Get(ctx, &model.Applications{ID: appID}); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return fmt.Errorf("application %q no longer exists", appID)
		}
		return fmt.Errorf("load application %q: %w", appID, err)
	}
	return nil
}

func jobRequiresApplicationWritePermission(jobType string) bool {
	switch config.JobType(strings.TrimSpace(jobType)) {
	case config.JobDeploy,
		config.JobDeployService,
		config.JobDeployStore,
		config.JobDeployPVC,
		config.JobDeployConfigMap,
		config.JobDeploySecret,
		config.JobDeployIngress,
		config.JobDeployServiceAccount,
		config.JobDeployRole,
		config.JobDeployRoleBinding,
		config.JobDeployClusterRole,
		config.JobDeployClusterRoleBinding,
		config.JobDeployInstant,
		config.JobDeployScheduled,
		config.JobDeployCloud,
		config.JobCleanupResources,
		config.JobDatabaseReset,
		config.JobVersionRestart:
		return true
	default:
		return false
	}
}

func jobStatusFailed(status config.Status) bool {
	if status == config.StatusCancelled || status == config.StatusFailed || status == config.StatusTimeout || status == config.StatusReject {
		return true
	}
	return false
}

func persistTerminalJobState(ctx context.Context, jobCtl JobCtl, job *model.JobTask, store datastore.DataStore, runtime *jobexec.Runtime) error {
	if jobCtl == nil || job == nil || store == nil {
		return nil
	}
	if suppressTerminalJobPersistence(ctx) {
		klog.FromContext(ctx).Info("Skip terminal job persistence after infrastructure stop", "jobName", job.Name, "jobType", job.JobType)
		return nil
	}
	baseCtx := context.Background()
	if ctx != nil {
		baseCtx = context.WithoutCancel(ctx)
	}
	persistCtx, persistCancel := context.WithTimeout(baseCtx, 5*time.Second)
	defer persistCancel()
	stopInfrastructureWatch := func() bool { return false }
	if ctx != nil {
		stopInfrastructureWatch = context.AfterFunc(ctx, func() {
			if signal.IsInfrastructureStop(ctx) {
				persistCancel()
			}
		})
	}
	defer stopInfrastructureWatch()
	saveErr := jobCtl.SaveInfo(persistCtx)
	if saveErr != nil {
		klog.FromContext(persistCtx).Error(saveErr, "Failed to update job info in db")
		return saveErr
	}
	if err := jobexec.SyncConfigComponentStatusIfWorkflowOwned(persistCtx, job, store, runtime); err != nil {
		if errors.Is(err, repository.ErrWorkflowOwnershipLost) {
			klog.FromContext(persistCtx).V(2).Info("Skip terminal component status projection after workflow ownership transfer")
			return nil
		}
		klog.FromContext(persistCtx).Error(err, "Failed to project terminal component status")
		return err
	}
	return nil
}

func terminalJobPersistenceFailure(job *model.JobTask, operation string, err error) error {
	if err == nil || job == nil {
		return nil
	}
	if job.Status != config.StatusDistributed && job.RunToken == "" && job.ExecutionKey == "" {
		return nil
	}
	return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("%s: %w", operation, err))
}

func suppressTerminalJobPersistence(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	cause := context.Cause(ctx)
	return signal.IsInfrastructureStop(ctx) ||
		errors.Is(cause, repository.ErrWorkflowLeaseRenewalFailed) ||
		errors.Is(cause, repository.ErrWorkflowOwnershipLost)
}

func persistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx != nil && ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.Background(), 5*time.Second)
}

package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/importsecret"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type JobCtl interface {
	Run(ctx context.Context) error
	Clean(ctx context.Context)
	SaveInfo(ctx context.Context) error
}

type GenerateServiceResult struct {
	Service           interface{}
	AdditionalObjects []client.Object
}

type taskIDKey struct{}

type jobRuntime struct {
	redisClient             *redis.Client
	shareLocker             locker.Locker
	cache                   cache.ICache
	urlSecurityPolicy       *spec.URLSecurityPolicySpec
	delayQueue              msg.Queue
	resourceWaiter          informer.ComponentReadyObserver
	kubeConfig              *rest.Config
	archiveUploader         ArchiveUploader
	importSecretKeyring     *importsecret.Keyring
	adoptionPersistenceOnce sync.Once
	adoptionPersistenceGate chan struct{}
}

func newJobRuntime(cache cache.ICache, kubeConfig *rest.Config, urlSecurityPolicy *spec.URLSecurityPolicySpec, delayQueue msg.Queue, resourceWaiter informer.ComponentReadyObserver, keyrings ...*importsecret.Keyring) *jobRuntime {
	var redisClient *redis.Client
	if cache != nil {
		redisClient = cache.GetRedisClient()
	}
	var importSecretKeyring *importsecret.Keyring
	if len(keyrings) > 0 {
		importSecretKeyring = keyrings[0]
	}
	return &jobRuntime{
		redisClient:         redisClient,
		shareLocker:         newShareLocker(redisClient),
		cache:               cache,
		urlSecurityPolicy:   urlSecurityPolicy,
		delayQueue:          delayQueue,
		resourceWaiter:      resourceWaiter,
		kubeConfig:          kubeConfig,
		archiveUploader:     currentArchiveUploader(),
		importSecretKeyring: importSecretKeyring,
	}
}

func (r *jobRuntime) close() {
	if r == nil || r.shareLocker == nil {
		return
	}
	if err := r.shareLocker.Close(); err != nil {
		klog.ErrorS(err, "close share locker failed")
	}
}

func (r *jobRuntime) withAdoptionPersistenceContext(ctx context.Context, fn func() error) error {
	if r == nil {
		return fn()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.adoptionPersistenceOnce.Do(func() {
		r.adoptionPersistenceGate = make(chan struct{}, 1)
	})
	select {
	case r.adoptionPersistenceGate <- struct{}{}:
		defer func() { <-r.adoptionPersistenceGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	return fn()
}

// StatusError wraps an error with an explicit job status for persistence.
type StatusError struct {
	Status config.Status
	Err    error
}

func (s *StatusError) Error() string { return s.Err.Error() }

func (s *StatusError) Unwrap() error { return s.Err }

// NewStatusError constructs a StatusError with the provided status and error.
func NewStatusError(status config.Status, err error) error {
	if err == nil {
		return nil
	}
	return &StatusError{Status: status, Err: err}
}

// ExtractStatusError attempts to retrieve a StatusError from err.
func ExtractStatusError(err error) (*StatusError, bool) {
	if err == nil {
		return nil, false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se, true
	}
	return nil, false
}

// WithTaskMetadata injects workflow identifiers into context so job controllers
// can derive cancellation signals when needed.
func WithTaskMetadata(ctx context.Context, taskID string) context.Context {
	return context.WithValue(ctx, taskIDKey{}, taskID)
}

// TaskIDFromContext extracts the workflow task identifier from context.
func TaskIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(taskIDKey{}).(string); ok {
		return v
	}
	return ""
}

func initJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), runtime *jobRuntime) JobCtl {
	if store == nil {
		klog.ErrorS(fmt.Errorf("store is nil"), "init job controller failed")
		return nil
	}
	if job == nil {
		klog.ErrorS(fmt.Errorf("job is nil"), "init job controller failed")
		return nil
	}
	if client == nil {
		klog.ErrorS(fmt.Errorf("client is nil"), "init job controller failed", "jobName", job.Name, "jobType", job.JobType)
		return nil
	}

	var shareLocker locker.Locker
	var urlSecurityPolicy *spec.URLSecurityPolicySpec
	if runtime != nil {
		shareLocker = runtime.shareLocker
		urlSecurityPolicy = runtime.urlSecurityPolicy
	}

	var jobCtl JobCtl
	switch job.JobType {
	case string(config.JobDeploy):
		jobCtl = NewDeployJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployService):
		jobCtl = NewDeployServiceJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployStore):
		jobCtl = NewDeployStatefulSetJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployPVC):
		jobCtl = NewDeployPVCJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployConfigMap):
		jobCtl = NewDeployConfigMapJobCtl(job, client, store, ack, shareLocker, urlSecurityPolicy)
	case string(config.JobDeploySecret):
		jobCtl = NewDeploySecretJobCtl(job, client, store, ack, shareLocker, urlSecurityPolicy)
	case string(config.JobDeployIngress):
		jobCtl = NewDeployIngressJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployServiceAccount):
		jobCtl = NewDeployServiceAccountJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployRole):
		jobCtl = NewDeployRoleJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployRoleBinding):
		jobCtl = NewDeployRoleBindingJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployClusterRole):
		jobCtl = NewDeployClusterRoleJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployClusterRoleBinding):
		jobCtl = NewDeployClusterRoleBindingJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployPodDisruptionBudget):
		jobCtl = NewDeployAdoptedPodDisruptionBudgetJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployNetworkPolicy):
		jobCtl = NewDeployAdoptedNetworkPolicyJobCtl(job, client, store, ack, shareLocker)
	case string(config.JobDeployInstant):
		jobCtl = NewInstantJobCtl(job, client, store, ack)
	case string(config.JobDeployScheduled):
		jobCtl = NewScheduledJobCtl(job, client, store, ack)
	case string(config.JobDeployCloud):
		jobCtl = NewCloudJobCtl(job, store)
	case string(config.JobDeployCallback):
		jobCtl = NewCallbackJobCtl(job, store, urlSecurityPolicy)
	case string(config.JobCleanupResources):
		cleanupCtl := NewCleanupResourcesJobCtl(job, client, store, ack)
		if cleanupCtl != nil {
			cleanupCtl.runtime = runtime
		}
		jobCtl = cleanupCtl
	case string(config.JobDatabaseReset):
		jobCtl = NewDatabaseResetJobCtl(job, client, store, ack)
	case string(config.JobLogArchiveUpload):
		jobCtl = NewLogArchiveUploadJobCtl(job, client, store, ack)
	case string(config.JobVersionRestart):
		jobCtl = NewVersionRestartJobCtl(job, client, store, ack)
	default:
		klog.ErrorS(fmt.Errorf("unknown job type"), "init job controller failed", "jobName", job.Name, "jobType", job.JobType)
		return nil
	}
	if aware, ok := jobCtl.(interface{ setRuntime(*jobRuntime) }); ok {
		aware.setRuntime(runtime)
	}
	return jobCtl
}

func RunJobs(ctx context.Context, jobs []*model.JobTask, concurrency int, client kubernetes.Interface, kubeConfig *rest.Config, store datastore.DataStore, ack func(), stopOnFailure bool, cache cache.ICache, urlSecurityPolicy *spec.URLSecurityPolicySpec, delayQueue msg.Queue, resourceWaiter informer.ComponentReadyObserver, keyrings ...*importsecret.Keyring) error {
	logger := klog.FromContext(ctx)
	if len(jobs) == 0 {
		logger.Info("no jobs to run")
		return nil
	}

	runtime := newJobRuntime(cache, kubeConfig, urlSecurityPolicy, delayQueue, resourceWaiter, keyrings...)
	defer runtime.close()

	if concurrency == 1 {
		for _, job := range jobs {
			if ctx.Err() != nil {
				return infrastructureStopCause(ctx)
			}
			logger.Info("Job started", "jobName", job.Name, "jobType", job.JobType)
			if err := runJob(ctx, job, client, store, ack, runtime); err != nil {
				return err
			}
			if ctx.Err() != nil {
				return infrastructureStopCause(ctx)
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
	jobPool := NewPool(ctx, jobs, concurrency, client, store, ack, stopOnFailure, runtime)
	if err := jobPool.Run(); err != nil {
		return err
	}
	return infrastructureStopCause(ctx)
}

func infrastructureStopCause(ctx context.Context) error {
	if !signal.IsInfrastructureStop(ctx) {
		return nil
	}
	return context.Cause(ctx)
}

func runJob(ctx context.Context, job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), runtime *jobRuntime) (resultErr error) {
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
	ctx = WithCleanupTracker(ctx)

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
		jobCtl := initJobCtl(job, client, store, ack, runtime)
		if jobCtl == nil {
			logger.Error(nil, "Failed to initialize job controller for skipped job")
			return
		}
		persistTerminalJobState(ctx, jobCtl, job, store, runtime)
		return
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
	jobCtl := initJobCtl(job, client, store, ack, runtime)
	if jobRequiresApplicationWritePermission(job.JobType) {
		if err := validateApplicationManagementModeForWrite(ctx, store, job.AppID, true); err != nil {
			if signal.IsInfrastructureStop(ctx) {
				logger.Info("Skip management mode failure after infrastructure stop", "cause", context.Cause(ctx))
				return
			}
			job.Status = config.StatusFailed
			job.Error = err.Error()
			job.StartTime = time.Now().Unix()
			job.EndTime = job.StartTime
			if ack != nil {
				ack()
			}
			span.SetStatus(codes.Error, "Application management mode rejected job")
			span.RecordError(err)
			logger.Error(err, "Refusing job for application management mode")
			persistTerminalJobState(ctx, jobCtl, job, store, runtime)
			return
		}
	}
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
		persistTerminalJobState(ctx, jobCtl, job, store, runtime)
		return
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

	if taskID := TaskIDFromContext(ctx); taskID != "" {
		var err error
		var watcherCtx context.Context
		var watcherCancel context.CancelFunc
		var redisClient *redis.Client
		if runtime != nil {
			redisClient = runtime.redisClient
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
			persistTerminalJobState(ctx, jobCtl, job, store, runtime)
			return
		}
		jobCtx = watcherCtx
		cancelFn = watcherCancel
	}

	if signal.IsInfrastructureStop(jobCtx) {
		logger.Info("Skip job execution after infrastructure stop", "cause", context.Cause(jobCtx))
		return context.Cause(jobCtx)
	}
	persistCtx, persistCancel := persistenceContext(jobCtx)
	var startStatusAppID string
	ownershipErr := withJobInfoOwnership(persistCtx, store, job, func(writeStore datastore.DataStore) error {
		var err error
		startStatusAppID, err = syncComponentStatusOnJobStart(persistCtx, job, writeStore)
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
	invalidateComponentsCache(runtime, startStatusAppID, "job start status sync")

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
		persistErr := persistTerminalJobState(jobCtx, jobCtl, job, store, runtime)
		if persistErr != nil && (job.Status == config.StatusDistributed || job.RunToken != "") {
			resultErr = errors.Join(
				resultErr,
				signal.ErrInfrastructureStop,
				fmt.Errorf("persist terminal job state: %w", persistErr),
			)
		}
	}()

	statusBeforeRun := job.Status
	errorBeforeRun := job.Error
	endTimeBeforeRun := job.EndTime
	runErr := jobCtl.Run(jobCtx)
	if signal.IsInfrastructureStop(jobCtx) || errors.Is(runErr, signal.ErrInfrastructureStop) || errors.Is(runErr, repository.ErrWorkflowOwnershipLost) {
		job.Status = statusBeforeRun
		job.Error = errorBeforeRun
		job.EndTime = endTimeBeforeRun
		if signal.IsInfrastructureStop(jobCtx) {
			return context.Cause(jobCtx)
		}
		return errors.Join(signal.ErrInfrastructureStop, runErr)
	}
	if runErr != nil {
		if !cleaned {
			jobCtl.Clean(jobCtx)
			cleaned = true
		}
		span.SetStatus(codes.Error, "Job execution failed")
		span.RecordError(runErr)
		if errors.Is(runErr, context.Canceled) {
			reason := signal.ReasonFromContext(jobCtx)
			applyJobError(job, runErr, reason)
			job.Status = config.StatusCancelled
		} else {
			applyJobError(job, runErr, job.Error)
			if job.Status != config.StatusFailed && job.Status != config.StatusCancelled && job.Status != config.StatusTimeout {
				job.Status = config.StatusFailed
			}
		}
	} else if job.Status == config.StatusPrepare || job.Status == config.StatusRunning {
		job.Status = config.StatusCompleted
	}

	if job.Status == config.StatusCompleted {
		finalizeCompletedJobIfNeeded(jobCtx, client, job)
	}

	if !cleaned && jobStatusFailed(job.Status) {
		jobCtl.Clean(jobCtx)
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
		return jobType == config.JobDeployInstant || jobType == config.JobDeployScheduled
	default:
		return false
	}
}

// ValidateApplicationManagementModeForJob rechecks persisted state at the
// point of execution. Queue-time validation is insufficient because an
// application can be migrated to observe mode while an older task is waiting.
func ValidateApplicationManagementModeForJob(
	ctx context.Context,
	store datastore.DataStore,
	appID string,
) error {
	return validateApplicationManagementModeForWrite(ctx, store, appID, false)
}

func validateApplicationManagementModeForWrite(
	ctx context.Context,
	store datastore.DataStore,
	appID string,
	requireApplication bool,
) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil
	}
	if store == nil {
		return fmt.Errorf("load application management mode: datastore is nil")
	}
	app := &model.Applications{ID: appID}
	if err := store.Get(ctx, app); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			if !requireApplication {
				// Callback and recovery workflows may outlive application metadata.
				// Runtime write jobs perform the stricter check above.
				return nil
			}
			return fmt.Errorf("application %q no longer exists", appID)
		}
		return fmt.Errorf("load application %q management mode: %w", appID, err)
	}
	if app.EffectiveManagementMode() == config.ManagementModeObserve {
		return fmt.Errorf("application %q is in read-only observe mode", appID)
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
		config.JobDeployPodDisruptionBudget,
		config.JobDeployNetworkPolicy,
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

func syncConfigComponentStatus(ctx context.Context, job *model.JobTask, store datastore.DataStore) (string, error) {
	target := findComponentForJob(ctx, job, store)
	if target == nil {
		return "", nil
	}
	if target.ComponentType != config.ConfJob && target.ComponentType != config.SecretJob {
		return "", nil
	}
	if target.Status == string(config.ComponentStatusCleaning) {
		klog.V(4).InfoS("skip updating component status from cleaning", "component", target.Name, "status", target.Status)
		return "", nil
	}
	if !isConfigComponentJobType(job.JobType) {
		return "", nil
	}
	status, ok := componentStatusForJob(job.Status)
	if !ok {
		if !isSharedDefaultSkippedConfigJob(job) {
			return "", nil
		}
		status = config.ComponentStatusRunning
	}

	lastAbnormal := ""
	target.Status = string(status)
	if status == config.ComponentStatusFailed {
		lastAbnormal = strings.TrimSpace(job.Error)
	}
	target.LastAbnormal = lastAbnormal

	if err := repository.UpdateComponentRuntimeFields(ctx, store, target, map[string]interface{}{
		"status":        string(status),
		"last_abnormal": lastAbnormal,
	}); err != nil {
		return "", fmt.Errorf("update component %s status to %s: %w", target.Name, status, err)
	}
	return target.AppID, nil
}

func syncConfigComponentStatusIfWorkflowOwned(
	ctx context.Context,
	job *model.JobTask,
	store datastore.DataStore,
	runtime *jobRuntime,
) error {
	if job == nil || store == nil || !isConfigComponentJobType(job.JobType) {
		return nil
	}
	var appID string
	err := withJobInfoOwnership(ctx, store, job, func(tx datastore.DataStore) error {
		var syncErr error
		appID, syncErr = syncConfigComponentStatus(ctx, job, tx)
		return syncErr
	})
	if err != nil {
		return err
	}
	invalidateComponentsCache(runtime, appID, "config status sync")
	return nil
}

func syncComponentStatusOnJobStart(ctx context.Context, job *model.JobTask, store datastore.DataStore) (string, error) {
	target := findComponentForJob(ctx, job, store)
	if target == nil {
		return "", nil
	}
	if !shouldMarkComponentPendingOnJobStart(job.JobType) {
		return "", nil
	}
	status := config.ComponentStatusPending
	readyReplicas := int32(0)
	lastAbnormal := ""
	target.Status = string(status)
	target.ReadyReplicas = readyReplicas
	target.LastAbnormal = lastAbnormal
	if err := repository.UpdateComponentRuntimeFields(ctx, store, target, map[string]interface{}{
		"status":         string(status),
		"ready_replicas": readyReplicas,
		"last_abnormal":  lastAbnormal,
	}); err != nil {
		return "", fmt.Errorf("update component %s status to pending: %w", target.Name, err)
	}
	return target.AppID, nil
}

func invalidateComponentsCache(runtime *jobRuntime, appID string, reason string) {
	if runtime == nil || runtime.cache == nil || runtime.cache.IsCacheDisabled() {
		return
	}
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return
	}
	cacheKey := cache.ApplicationComponentsKey(appID)
	if err := runtime.cache.Delete(cacheKey); err != nil {
		klog.V(4).InfoS("invalidate component cache failed", "reason", reason, "appID", appID, "err", err)
	}
}

func shouldMarkComponentPendingOnJobStart(jobType string) bool {
	switch config.JobType(jobType) {
	case config.JobDeploy, config.JobDeployStore, config.JobDeployInstant, config.JobDeployScheduled:
		return true
	default:
		return false
	}
}

func findComponentForJob(ctx context.Context, job *model.JobTask, store datastore.DataStore) *model.ApplicationComponent {
	if job == nil || store == nil {
		return nil
	}
	appID := strings.TrimSpace(job.AppID)
	if appID == "" {
		appID = strings.TrimSpace(job.WorkflowID)
	}
	name := strings.TrimSpace(job.Name)
	if appID == "" || name == "" {
		return nil
	}
	entities, err := store.List(ctx, &model.ApplicationComponent{AppID: appID}, &datastore.ListOptions{})
	if err != nil {
		if !errors.Is(err, datastore.ErrRecordNotExist) {
			klog.ErrorS(err, "list components for app failed", "appID", appID)
		}
		return nil
	}
	for _, entity := range entities {
		component, ok := entity.(*model.ApplicationComponent)
		if !ok || component == nil {
			continue
		}
		if component.Name == name {
			return component
		}
	}
	return nil
}

func isConfigComponentJobType(jobType string) bool {
	switch config.JobType(jobType) {
	case config.JobDeployConfigMap, config.JobDeploySecret:
		return true
	default:
		return false
	}
}

func componentStatusForJob(status config.Status) (config.ComponentStatus, bool) {
	switch status {
	case config.StatusCompleted, config.StatusPassed:
		return config.ComponentStatusRunning, true
	case config.StatusFailed, config.StatusTimeout, config.StatusReject, config.StatusCancelled:
		return config.ComponentStatusFailed, true
	default:
		return "", false
	}
}

func persistTerminalJobState(ctx context.Context, jobCtl JobCtl, job *model.JobTask, store datastore.DataStore, runtime *jobRuntime) error {
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
	if err := syncConfigComponentStatusIfWorkflowOwned(persistCtx, job, store, runtime); err != nil {
		if errors.Is(err, repository.ErrWorkflowOwnershipLost) {
			klog.FromContext(persistCtx).V(2).Info("Skip terminal component status projection after workflow ownership transfer")
			return nil
		}
		klog.FromContext(persistCtx).Error(err, "Failed to project terminal component status")
		return err
	}
	return nil
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

func isSharedDefaultSkippedConfigJob(job *model.JobTask) bool {
	if job == nil {
		return false
	}
	if job.Status != config.StatusSkipped {
		return false
	}
	if !isConfigComponentJobType(job.JobType) {
		return false
	}
	strategy, shared := shareStrategyFromJobInfo(job.JobInfo)
	if !shared {
		return false
	}
	return strategy == spec.ShareStrategyDefault
}

func persistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx != nil && ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.Background(), 5*time.Second)
}

type Pool struct {
	Jobs          []*model.JobTask
	concurrency   int
	client        kubernetes.Interface
	store         datastore.DataStore
	jobsChan      chan *model.JobTask
	ack           func()
	ctx           context.Context
	cancel        context.CancelCauseFunc
	stopOnFailure bool
	wg            sync.WaitGroup
	failureOnce   sync.Once
	runErrOnce    sync.Once
	runErr        error
	runtime       *jobRuntime
}

func (p *Pool) Run() error {
	defer p.cancel(nil)
	for i := 0; i < p.concurrency; i++ {
		go p.work()
	}
enqueue:
	for _, task := range p.Jobs {
		if p.ctx.Err() != nil {
			break
		}
		p.wg.Add(1)
		select {
		case p.jobsChan <- task:
		case <-p.ctx.Done():
			p.wg.Done()
			break enqueue
		}
	}
	// all workers return
	close(p.jobsChan)
	p.wg.Wait()
	return p.runErr
}

// The work loop for any single goroutine.
func (p *Pool) work() {
	for job := range p.jobsChan {
		if p.ctx.Err() != nil {
			p.wg.Done()
			continue
		}
		if err := runJob(p.ctx, job, p.client, p.store, p.ack, p.runtime); err != nil {
			p.runErrOnce.Do(func() {
				p.runErr = err
			})
			p.cancel(err)
		}
		if p.stopOnFailure && jobStatusFailed(job.Status) {
			p.failureOnce.Do(func() {
				p.cancel(nil)
			})
		}
		p.wg.Done()
	}
}

// NewPool initializes a new pool with the given tasks and
// at the given concurrency.
func NewPool(ctx context.Context, jobs []*model.JobTask, concurrency int, client kubernetes.Interface, store datastore.DataStore, ack func(), stopOnFailure bool, runtime *jobRuntime) *Pool {
	ctxForPool, cancel := context.WithCancelCause(ctx)
	return &Pool{
		Jobs:          jobs,
		client:        client,
		store:         store,
		concurrency:   concurrency,
		jobsChan:      make(chan *model.JobTask),
		ack:           ack,
		ctx:           ctxForPool,
		cancel:        cancel,
		stopOnFailure: stopOnFailure,
		runtime:       runtime,
	}
}

func ParseProperties(properties *model.JSONStruct) model.Properties {
	cProperties, err := json.Marshal(properties)
	if err != nil {
		klog.ErrorS(err, "component properties serialization failed")
		return model.Properties{}
	}

	var propertied model.Properties
	err = json.Unmarshal(cProperties, &propertied)
	if err != nil {
		klog.ErrorS(err, "component properties deserialization failed")
		return model.Properties{}
	}
	return propertied
}

func BuildLabels(c *model.ApplicationComponent, p *model.Properties) map[string]string {
	labels := make(map[string]string)
	if p != nil {
		for k, v := range naming.NormalizeLabelValues(p.Labels) {
			labels[k] = v
		}
	}
	return ApplyComponentManagedLabels(labels, c)
}

func ApplyComponentManagedLabels(labels map[string]string, c *model.ApplicationComponent) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}
	if c == nil {
		return labels
	}
	labels[config.LabelManagedBy] = config.ManagedByEruun
	labels[config.LabelComponentID] = fmt.Sprintf("%d", c.ID)
	labels[config.LabelAppID] = c.AppID
	labels[config.LabelComponentName] = naming.BoundedLabelValue(c.Name)
	return labels
}

func BuildAnnotations(c *model.ApplicationComponent) map[string]string {
	if c == nil {
		return nil
	}
	name := strings.TrimSpace(c.Name)
	if name == "" {
		return nil
	}
	return map[string]string{
		config.AnnotationComponentName: name,
	}
}

func ApplyComponentAnnotationsToObject(obj metav1.Object, component *model.ApplicationComponent) {
	annotations := BuildAnnotations(component)
	if len(annotations) == 0 || obj == nil {
		return
	}
	current := obj.GetAnnotations()
	if current == nil {
		current = make(map[string]string, len(annotations))
	}
	for k, v := range annotations {
		current[k] = v
	}
	obj.SetAnnotations(current)
}

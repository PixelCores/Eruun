package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type CleanupResourcesJobCtl struct {
	deployNamespacedResourceJobBase
	runtime                      *jobRuntime
	skipSaveInfo                 bool
	requiredStatefulSetPVCTarget *requiredStatefulSetPVCDeletionTarget
	statefulSetRetentionTargets  map[string]statefulSetRetentionTarget
	requiredStatefulSetPodTarget *requiredStatefulSetPodDeletionTarget
}

func NewCleanupResourcesJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func()) *CleanupResourcesJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("NewCleanupResourcesJobCtl", job, client, store, ack, nil)
	if !ok {
		return nil
	}
	return &CleanupResourcesJobCtl{deployNamespacedResourceJobBase: base}
}

func (c *CleanupResourcesJobCtl) Clean(context.Context) {}

func (c *CleanupResourcesJobCtl) SaveInfo(ctx context.Context) error {
	if c.skipSaveInfo {
		return nil
	}
	if isVersionUpdateRemoveCleanupInternalInfo(c.job.InternalInfo) {
		if versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo) {
			return c.saveRequiredStatefulSetCleanupJobInfo(ctx)
		}
		return saveOrUpdateVersionUpdateCleanupJobInfo(ctx, c.store, c.job)
	}
	return saveJobInfo(ctx, c.store, c.job)
}

func (c *CleanupResourcesJobCtl) saveRequiredStatefulSetCleanupJobInfo(ctx context.Context) error {
	if c == nil || c.job == nil {
		return fmt.Errorf("save required StatefulSet cleanup job info: job is nil")
	}
	if c.store == nil {
		return fmt.Errorf("save required StatefulSet cleanup job info: store is nil")
	}
	conditionalStore, ok := c.store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return fmt.Errorf("save required StatefulSet cleanup job info: datastore does not support conditional compare-and-swap")
	}
	desired := buildJobInfoRecord(c.job)
	checkpointAdvanceFailure := false
	for attempt := 1; attempt <= jobInfoSaveMaxAttempts; attempt++ {
		existing, err := findExistingVersionUpdateCleanupJobInfo(ctx, c.store, c.job)
		if err != nil {
			return err
		}
		if existing == nil {
			desired.InternalInfo = c.job.InternalInfo
			return c.store.Add(ctx, &desired)
		}
		if versionUpdateCleanupJobInfoWriteIsStale(*existing, desired) {
			klog.V(2).InfoS("ignore stale required StatefulSet cleanup job info write",
				"taskID", desired.TaskID,
				"serviceName", desired.ServiceName,
				"storedRunGeneration", existing.RunGeneration,
				"writeRunGeneration", desired.RunGeneration,
				"storedAttempt", existing.Attempt,
				"writeAttempt", desired.Attempt)
			return nil
		}
		existingStatus := config.Status(strings.TrimSpace(existing.Status))
		if versionUpdateCleanupJobInfoSameExecution(*existing, desired) && isTerminalVersionUpdateCleanupStatus(existingStatus) {
			// A terminal database record for the same execution is authoritative,
			// including when a duplicate worker carries an older checkpoint.
			// Never downgrade an already completed cleanup to the recoverable
			// checkpoint-advance failure below.
			c.skipSaveInfo = true
			c.job.Status = existingStatus
			c.job.StartTime = existing.StartTime
			c.job.EndTime = existing.EndTime
			c.job.Info = existing.Info
			c.job.Error = existing.Error
			c.job.InternalInfo = existing.InternalInfo
			return nil
		}
		if isSuccessfulVersionUpdateCleanupStatus(c.job.Status) && existing.InternalInfo != desired.InternalInfo {
			// Completion is the linearization point for destructive cleanup. Do
			// not accept a checkpoint another worker added after this worker's
			// final reconciliation; otherwise that new Pod/Job/PVC identity could
			// be stranded behind a falsely completed cleanup record. Persist a
			// recoverable failure while preserving the newer checkpoint instead.
			message := "required StatefulSet cleanup checkpoint advanced before completion; retry the version update cleanup"
			c.job.Status = config.StatusFailed
			c.job.Error = message
			checkpointAdvanceFailure = true
			desired = buildJobInfoRecord(c.job)
			updated, err := conditionalStore.CompareAndSwapWithConditions(
				ctx,
				existing,
				versionUpdateCleanupJobInfoConditions(existing),
				versionUpdateCleanupJobInfoUpdates(desired, false),
			)
			if err != nil {
				return fmt.Errorf("save required StatefulSet cleanup checkpoint-advance failure: %w", err)
			}
			if !updated {
				continue
			}
			c.job.InternalInfo = existing.InternalInfo
			if c.ack != nil {
				c.ack()
			}
			return nil
		}
		c.job.InternalInfo = existing.InternalInfo
		updated, err := conditionalStore.CompareAndSwapWithConditions(
			ctx,
			existing,
			versionUpdateCleanupJobInfoConditions(existing),
			versionUpdateCleanupJobInfoUpdates(desired, false),
		)
		if err != nil {
			return fmt.Errorf("save required StatefulSet cleanup job info: %w", err)
		}
		if updated {
			if checkpointAdvanceFailure && c.ack != nil {
				c.ack()
			}
			return nil
		}
	}
	latest, err := findExistingVersionUpdateCleanupJobInfo(ctx, c.store, c.job)
	if err != nil {
		return fmt.Errorf("save required StatefulSet cleanup job info: reload after concurrent changes: %w", err)
	}
	if latest != nil {
		c.job.InternalInfo = latest.InternalInfo
	}
	return fmt.Errorf("save required StatefulSet cleanup job info: concurrent identity, status, or checkpoint changes did not converge after %d attempts", jobInfoSaveMaxAttempts)
}

func (c *CleanupResourcesJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "cleanup resources job run error")
}

func (c *CleanupResourcesJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	if c.store == nil {
		return fmt.Errorf("store is nil")
	}
	component, err := cleanupComponentFromJobInfo(c.job)
	if err != nil {
		return err
	}
	componentCopy := *component
	if strings.TrimSpace(componentCopy.Namespace) == "" {
		componentCopy.Namespace = config.DefaultNamespace
	}
	component = &componentCopy
	if component.HasSourceWorkload() {
		return fmt.Errorf("adopted source resource cleanup requires an explicit cleanup plan fingerprint")
	}
	if err := c.ensureVersionUpdateCleanupTargetNotReused(ctx, component); err != nil {
		return err
	}
	skipCleanup, err := c.markVersionUpdateCleanupRunning(ctx)
	if err != nil {
		return err
	}
	if skipCleanup {
		return nil
	}
	if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, "starting required StatefulSet cleanup"); err != nil {
		return err
	}
	if err := c.ensureRequiredStatefulSetDeletionAllowed(ctx, component); err != nil {
		return err
	}
	if err := c.prepareRequiredStatefulSetDeletion(ctx, component); err != nil {
		return err
	}
	if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, "starting resource deletion after required StatefulSet retention"); err != nil {
		return err
	}

	deleted := c.deleteComponentResources(ctx, component)
	if err := errors.Join(deleted.errs...); err != nil {
		return err
	}
	if err := c.waitForCleanup(ctx, component, deleted.refs); err != nil {
		return err
	}
	return c.markComponentNotDeploy(ctx, component)
}

func (c *CleanupResourcesJobCtl) ensureRequiredStatefulSetDeletionAllowed(ctx context.Context, component *model.ApplicationComponent) error {
	if c == nil || c.job == nil {
		return nil
	}
	if err := validateVersionUpdateCleanupInternalInfo(c.job.InternalInfo, component); err != nil {
		return err
	}
	if !versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo) {
		return nil
	}
	if component == nil || component.ComponentType != config.StoreJob {
		return fmt.Errorf("required StatefulSet deletion is only valid for store components")
	}
	ref, err := requiredStatefulSetCleanupRef(component)
	if err != nil {
		return err
	}
	protected, _, err := c.resourceDeleteProtected(ctx, ref)
	if err != nil {
		return fmt.Errorf("inspect required StatefulSet deletion target %s: %w", cleanupResourceDisplayName(ref), err)
	}
	if protected {
		return c.requiredStatefulSetDeletionProtectedError(ref)
	}
	if err := c.ensureRequiredStatefulSetDeletionTargetsAllowed(ctx, component, ref); err != nil {
		return err
	}
	if err := c.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component); err != nil {
		return err
	}
	return c.ensureRequiredStatefulSetPVCDeletionAllowed(ctx, component)
}

func (c *CleanupResourcesJobCtl) ensureRequiredStatefulSetDeletionTargetsAllowed(
	ctx context.Context,
	component *model.ApplicationComponent,
	requiredRef cleanupResourceRef,
) error {
	selector := cleanupLabelSelector(component)
	if selector == "" {
		return fmt.Errorf("required StatefulSet deletion selector is empty for component %s", component.Name)
	}
	list, err := c.client.AppsV1().StatefulSets(requiredRef.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list required StatefulSet deletion targets for component %s: %w", component.Name, err)
	}
	for i := range list.Items {
		item := &list.Items[i]
		ref, ok := newCleanupResourceRef(domainspec.ResourceStatefulSet, item.Namespace, item.Name, false)
		if !ok {
			continue
		}
		_, protected := cleanupResourceShareProtected(item.Labels)
		if ref.namespace != requiredRef.namespace || ref.name != requiredRef.name {
			if protected {
				return c.requiredStatefulSetDeletionProtectedError(ref)
			}
			return k8serrors.NewConflict(
				schema.GroupResource{Group: "apps", Resource: "statefulsets"},
				ref.name,
				fmt.Errorf("label-matched StatefulSet %s is not the required deletion target %s", cleanupResourceDisplayName(ref), cleanupResourceDisplayName(requiredRef)),
			)
		}
		if !protected {
			continue
		}
		return c.requiredStatefulSetDeletionProtectedError(ref)
	}
	return nil
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetDeletionProtectedError(ref cleanupResourceRef) error {
	if c == nil || c.job == nil || ref.kind != domainspec.ResourceStatefulSet || !versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo) {
		return nil
	}
	componentName := strings.TrimSpace(resolveJobServiceName(c.job))
	if componentName == "" {
		componentName = "<unknown>"
	}
	return fmt.Errorf("required StatefulSet deletion blocked for component %s: %s is protected by live share labels", componentName, cleanupResourceDisplayName(ref))
}

func (c *CleanupResourcesJobCtl) markVersionUpdateCleanupRunning(ctx context.Context) (bool, error) {
	if !isVersionUpdateRemoveCleanupInternalInfo(c.job.InternalInfo) {
		return false, nil
	}
	c.job.Status = config.StatusRunning
	if c.job.StartTime == 0 {
		c.job.StartTime = time.Now().Unix()
	}
	c.job.EndTime = 0
	c.job.Error = ""
	desired := buildJobInfoRecord(c.job)
	conditionalStore, ok := c.store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return false, fmt.Errorf("start version update cleanup: datastore does not support conditional compare-and-swap")
	}
	for attempt := 1; attempt <= jobInfoSaveMaxAttempts; attempt++ {
		existing, err := findExistingVersionUpdateCleanupJobInfo(ctx, c.store, c.job)
		if err != nil {
			return false, err
		}
		if existing == nil {
			return false, fmt.Errorf("precreated version update cleanup job info not found for task %s component %s", c.job.TaskID, resolveJobServiceName(c.job))
		}
		status := config.Status(strings.TrimSpace(existing.Status))
		if isSuccessfulVersionUpdateCleanupStatus(status) {
			c.skipSaveInfo = true
			c.job.Status = config.StatusCompleted
			c.job.Error = ""
			c.job.StartTime = existing.StartTime
			c.job.EndTime = existing.EndTime
			return true, nil
		}
		if isTerminalVersionUpdateCleanupStatus(status) {
			return false, c.abortTerminalizedVersionUpdateCleanup(status, existing.Status)
		}
		if versionUpdateCleanupJobInfoWriteIsStale(*existing, desired) {
			return false, c.versionUpdateCleanupOwnershipError(existing, "starting cleanup")
		}
		if versionUpdateCleanupJobInfoOwnershipMatches(*existing, desired) && status == config.StatusRunning {
			c.job.InternalInfo = existing.InternalInfo
			if existing.StartTime > 0 {
				c.job.StartTime = existing.StartTime
			}
			return false, nil
		}
		if status != config.StatusRunning && !isStartableVersionUpdateCleanupStatus(status) {
			return false, fmt.Errorf("version update cleanup job info for task %s component %s is %s, expected queued or running", c.job.TaskID, resolveJobServiceName(c.job), existing.Status)
		}
		c.job.InternalInfo = existing.InternalInfo
		desired.InternalInfo = existing.InternalInfo
		updated, err := conditionalStore.CompareAndSwapWithConditions(
			ctx,
			existing,
			versionUpdateCleanupJobInfoConditions(existing),
			versionUpdateCleanupJobInfoUpdates(desired, false),
		)
		if err != nil {
			return false, err
		}
		if updated {
			return false, nil
		}
	}
	return false, fmt.Errorf("start version update cleanup: concurrent identity or status changes did not converge after %d attempts", jobInfoSaveMaxAttempts)
}

func (c *CleanupResourcesJobCtl) ensureVersionUpdateCleanupJobInfoOwned(existing *model.JobInfo, operation string) error {
	if c == nil || c.job == nil || existing == nil {
		return nil
	}
	desired := buildJobInfoRecord(c.job)
	if versionUpdateCleanupJobInfoOwnershipMatches(*existing, desired) {
		return nil
	}
	return c.versionUpdateCleanupOwnershipError(existing, operation)
}

func (c *CleanupResourcesJobCtl) versionUpdateCleanupOwnershipError(existing *model.JobInfo, operation string) error {
	c.skipSaveInfo = true
	storedGeneration := uint64(0)
	storedExecutionKey := ""
	if existing != nil {
		storedGeneration = existing.RunGeneration
		storedExecutionKey = jobInfoExecutionKey(*existing)
	}
	return NewStatusError(config.StatusCancelled, fmt.Errorf(
		"version update cleanup ownership changed before %s: task %s execution %q generation %d, stored execution %q generation %d",
		operation,
		c.job.TaskID,
		c.job.ExecutionKey,
		c.job.RunGeneration,
		storedExecutionKey,
		storedGeneration,
	))
}

func (c *CleanupResourcesJobCtl) abortTerminalizedVersionUpdateCleanup(status config.Status, rawStatus string) error {
	c.skipSaveInfo = true
	return NewStatusError(versionUpdateCleanupAbortStatus(status), fmt.Errorf("version update cleanup job info for task %s component %s is already %s", c.job.TaskID, resolveJobServiceName(c.job), rawStatus))
}

func versionUpdateCleanupAbortStatus(status config.Status) config.Status {
	switch status {
	case config.StatusFailed, config.StatusTimeout, config.StatusCancelled, config.StatusReject:
		return status
	default:
		return config.StatusCancelled
	}
}

func (c *CleanupResourcesJobCtl) ensureRequiredStatefulSetWorkflowTaskActive(ctx context.Context, operation string) error {
	if c == nil || c.job == nil || !versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo) {
		return nil
	}
	if c.job.Status != config.StatusRunning {
		// Production enters the required StatefulSet path through Run, which marks
		// the in-memory job running first. Keep direct package-level helper tests
		// independent from workflow persistence.
		return nil
	}
	taskID := strings.TrimSpace(c.job.TaskID)
	if err := ctx.Err(); err != nil {
		return requiredStatefulSetWorkflowTaskContextError(taskID, operation, err)
	}
	if taskID == "" {
		return NewStatusError(config.StatusFailed, fmt.Errorf("required StatefulSet cleanup workflow task id is empty before %s", operation))
	}
	if c.store == nil {
		return NewStatusError(config.StatusFailed, fmt.Errorf("load workflow task %s before %s: store is nil", taskID, operation))
	}

	for attempt := 1; ; attempt++ {
		task := &model.WorkflowQueue{TaskID: taskID}
		if err := c.store.Get(ctx, task); err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return requiredStatefulSetWorkflowTaskContextError(taskID, operation, contextErr)
			}
			return NewStatusError(config.StatusFailed, fmt.Errorf("load workflow task %s before %s: %w", taskID, operation, err))
		}
		if err := ctx.Err(); err != nil {
			return requiredStatefulSetWorkflowTaskContextError(taskID, operation, err)
		}
		status := config.Status(strings.TrimSpace(string(task.Status)))
		if status != config.StatusRunning {
			rawStatus := strings.TrimSpace(string(task.Status))
			if rawStatus == "" {
				rawStatus = "<empty>"
			}
			err := fmt.Errorf("workflow task %s is %s before %s; required StatefulSet cleanup requires running", taskID, rawStatus, operation)
			if isTerminalVersionUpdateCleanupStatus(status) {
				// Unlike a terminal JobInfo, a terminal workflow task must not suppress
				// SaveInfo: cancellation can commit while signalling the running worker
				// fails, and that JobInfo still needs to be terminalized by this worker.
				return NewStatusError(versionUpdateCleanupAbortStatus(status), err)
			}
			return NewStatusError(config.StatusFailed, err)
		}
		if c.job.RunGeneration > 0 && task.RunGeneration != c.job.RunGeneration {
			c.skipSaveInfo = true
			return NewStatusError(config.StatusCancelled, fmt.Errorf(
				"workflow task %s generation changed from %d to %d before %s",
				taskID,
				c.job.RunGeneration,
				task.RunGeneration,
				operation,
			))
		}

		if err := c.ensureRequiredStatefulSetSafetyCurrent(ctx); err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return requiredStatefulSetWorkflowTaskContextError(taskID, operation, contextErr)
			}
			refreshErr := newRequiredStatefulSetSafetyRefreshError(err)
			if !refreshErr.retryable || attempt == requiredStatefulSetSafetyRefreshMaxAttempts {
				return fmt.Errorf("revalidate required StatefulSet safety before %s: %w", operation, refreshErr)
			}
			select {
			case <-ctx.Done():
				return requiredStatefulSetWorkflowTaskContextError(taskID, operation, ctx.Err())
			case <-time.After(cleanupPollInterval):
			}
			continue
		}
		return nil
	}
}

func requiredStatefulSetWorkflowTaskContextError(taskID, operation string, err error) error {
	status := config.StatusCancelled
	if errors.Is(err, context.DeadlineExceeded) {
		status = config.StatusTimeout
	}
	return NewStatusError(status, fmt.Errorf("workflow task %s context ended before %s: %w", taskID, operation, err))
}

func isStartableVersionUpdateCleanupStatus(status config.Status) bool {
	switch status {
	case "", config.StatusCreated, config.StatusQueued, config.StatusWaiting, config.QueueItemPending, config.StatusPrepare:
		return true
	default:
		return false
	}
}

func isSuccessfulVersionUpdateCleanupStatus(status config.Status) bool {
	return status == config.StatusCompleted
}

func isTerminalVersionUpdateCleanupStatus(status config.Status) bool {
	switch status {
	case config.StatusCompleted, config.StatusPassed, config.StatusSkipped, config.StatusFailed, config.StatusTimeout, config.StatusCancelled, config.StatusReject:
		return true
	default:
		return false
	}
}

func (c *CleanupResourcesJobCtl) deleteComponentResources(ctx context.Context, component *model.ApplicationComponent) cleanupResourceSet {
	props := ParseProperties(component.Properties)
	deleted := cleanupResourceSet{seen: make(map[string]struct{})}
	c.deleteGeneratedResources(ctx, component, &props, &deleted)
	c.deleteLabeledResources(ctx, component, &deleted)
	return deleted
}

func (c *CleanupResourcesJobCtl) ensureVersionUpdateCleanupTargetNotReused(ctx context.Context, component *model.ApplicationComponent) error {
	if c == nil || c.job == nil || !isVersionUpdateRemoveCleanupInternalInfo(c.job.InternalInfo) {
		return nil
	}
	current, err := c.currentComponentByName(ctx, component)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	if component.ID == 0 || current.ID != component.ID {
		return fmt.Errorf("cleanup removed component %s blocked: current component id=%d does not match removed component id=%d", component.Name, current.ID, component.ID)
	}
	return nil
}

func (c *CleanupResourcesJobCtl) markComponentNotDeploy(ctx context.Context, component *model.ApplicationComponent) error {
	target, err := c.currentComponentByName(ctx, component)
	if err != nil {
		return err
	}
	if target == nil {
		return nil
	}
	if component.ID != 0 && target.ID != component.ID {
		return nil
	}
	status := config.ComponentStatusNotDeploy
	readyReplicas := int32(0)
	lastAbnormal := ""
	target.Status = string(status)
	target.ReadyReplicas = readyReplicas
	target.LastAbnormal = lastAbnormal
	if err := repository.UpdateComponentRuntimeFields(ctx, c.store, target, map[string]interface{}{
		"status":         string(status),
		"ready_replicas": readyReplicas,
		"last_abnormal":  lastAbnormal,
	}); err != nil {
		return fmt.Errorf("mark component %s not deploy: %w", component.Name, err)
	}
	invalidateComponentsCache(c.runtime, target.AppID, "cleanup resources status sync")
	return nil
}

func (c *CleanupResourcesJobCtl) currentComponentByName(ctx context.Context, component *model.ApplicationComponent) (*model.ApplicationComponent, error) {
	if c == nil || c.store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	if component == nil {
		return nil, fmt.Errorf("component is nil")
	}
	componentName := strings.TrimSpace(component.Name)
	if componentName == "" {
		return nil, nil
	}
	entities, err := c.store.List(ctx, &model.ApplicationComponent{AppID: component.AppID}, &datastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, entity := range entities {
		current, ok := entity.(*model.ApplicationComponent)
		if ok && current != nil && strings.EqualFold(strings.TrimSpace(current.Name), componentName) {
			return current, nil
		}
	}
	return nil, nil
}

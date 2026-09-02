package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

var (
	ErrWorkflowFencingUnsupported = errors.New("workflow fencing requires multi-condition compare-and-swap")
	ErrWorkflowClockUnsupported   = errors.New("workflow leases require an authoritative database clock")
	ErrWorkflowOwnershipRequired  = errors.New("workflow ownership is incomplete")
	ErrWorkflowLeaseRenewalFailed = errors.New("workflow execution lease renewal failed")
	ErrWorkflowOwnershipLost      = errors.New("workflow execution ownership changed")
)

const workflowLeaseRecoveryBatchSize = 100

// ClaimWorkflowTaskForDispatch atomically creates a new execution generation.
func ClaimWorkflowTaskForDispatch(ctx context.Context, store datastore.DataStore, task *model.WorkflowQueue, leaseDuration time.Duration) (*model.WorkflowQueue, bool, error) {
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return nil, false, datastore.ErrPrimaryEmpty
	}
	if leaseDuration <= 0 {
		return nil, false, fmt.Errorf("workflow dispatch lease duration must be positive")
	}
	now, err := currentWorkflowDatabaseTime(ctx, store)
	if err != nil {
		return nil, false, err
	}
	token := uuid.NewString()
	expiresAt := now.Add(leaseDuration)
	updates := map[string]interface{}{
		"status":            config.StatusQueued,
		"run_generation":    task.RunGeneration + 1,
		"run_token":         token,
		"worker_id":         "",
		"heartbeat_at":      nil,
		"lease_expires_at":  expiresAt,
		"dispatch_attempts": task.DispatchAttempts + 1,
		"scheduling_reason": "waiting task selected",
	}
	conditions := map[string]interface{}{
		"status":         config.StatusWaiting,
		"run_generation": task.RunGeneration,
	}
	updated, err := compareWorkflowTaskWithConditions(ctx, store, task.TaskID, conditions, updates)
	if err != nil || !updated {
		return nil, updated, err
	}
	claimed := *task
	claimed.Status = config.StatusQueued
	claimed.RunGeneration++
	claimed.RunToken = token
	claimed.WorkerID = ""
	claimed.HeartbeatAt = nil
	claimed.LeaseExpiresAt = &expiresAt
	claimed.DispatchAttempts++
	claimed.SchedulingReason = "waiting task selected"
	return &claimed, true, nil
}

// ClaimWorkflowTaskForExecution transfers a queued dispatch lease to one worker.
func ClaimWorkflowTaskForExecution(ctx context.Context, store datastore.DataStore, taskID string, generation uint64, token, workerID string, leaseDuration time.Duration) (*model.WorkflowQueue, bool, error) {
	if err := validateWorkflowExecutionIdentity(taskID, generation, token); err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(workerID) == "" {
		return nil, false, fmt.Errorf("%w: workflow execution claim requires worker identity", ErrWorkflowOwnershipRequired)
	}
	if leaseDuration <= 0 {
		return nil, false, fmt.Errorf("workflow execution lease duration must be positive")
	}
	now, err := currentWorkflowDatabaseTime(ctx, store)
	if err != nil {
		return nil, false, err
	}
	expiresAt := now.Add(leaseDuration)
	conditions := map[string]interface{}{
		"status":         config.StatusQueued,
		"run_generation": generation,
		"run_token":      token,
	}
	updates := map[string]interface{}{
		"status":            config.StatusRunning,
		"worker_id":         workerID,
		"heartbeat_at":      now,
		"lease_expires_at":  expiresAt,
		"scheduling_reason": "worker claimed dispatch",
	}
	updated, err := compareWorkflowTaskWithConditions(ctx, store, taskID, conditions, updates)
	if err != nil || !updated {
		return nil, updated, err
	}
	task, err := TaskByID(ctx, store, taskID)
	if err != nil {
		return nil, false, err
	}
	if task.Status != config.StatusRunning ||
		task.RunGeneration != generation ||
		task.RunToken != token ||
		task.WorkerID != workerID {
		return task, false, nil
	}
	return task, true, nil
}

func RenewWorkflowTaskLease(ctx context.Context, store datastore.DataStore, taskID string, generation uint64, token, workerID string, leaseDuration time.Duration) (bool, error) {
	if err := validateWorkflowExecutionIdentity(taskID, generation, token); err != nil {
		return false, err
	}
	if strings.TrimSpace(workerID) == "" {
		return false, fmt.Errorf("%w: workflow lease renewal requires worker identity", ErrWorkflowOwnershipRequired)
	}
	if leaseDuration <= 0 {
		return false, fmt.Errorf("workflow execution lease duration must be positive")
	}
	now, err := currentWorkflowDatabaseTime(ctx, store)
	if err != nil {
		return false, err
	}
	return compareWorkflowTaskWithConditions(ctx, store, taskID, map[string]interface{}{
		"status":         config.StatusRunning,
		"run_generation": generation,
		"run_token":      token,
		"worker_id":      workerID,
	}, map[string]interface{}{
		"heartbeat_at":     now,
		"lease_expires_at": now.Add(leaseDuration),
	})
}

func ExpireWorkflowTaskLease(ctx context.Context, store datastore.DataStore, taskID string, generation uint64, token, workerID string) (bool, error) {
	if err := validateWorkflowExecutionIdentity(taskID, generation, token); err != nil {
		return false, err
	}
	if strings.TrimSpace(workerID) == "" {
		return false, fmt.Errorf("%w: workflow lease expiration requires worker identity", ErrWorkflowOwnershipRequired)
	}
	now, err := currentWorkflowDatabaseTime(ctx, store)
	if err != nil {
		return false, err
	}
	return compareWorkflowTaskWithConditions(ctx, store, taskID, map[string]interface{}{
		"status":         config.StatusRunning,
		"run_generation": generation,
		"run_token":      token,
		"worker_id":      workerID,
	}, map[string]interface{}{
		"lease_expires_at":  now,
		"scheduling_reason": "worker execution stopped",
	})
}

func UpdateTaskFieldsIfOwned(ctx context.Context, store datastore.DataStore, task *model.WorkflowQueue, expectedStatus config.Status, updates map[string]interface{}) (bool, error) {
	if task == nil || task.TaskID == "" {
		return false, datastore.ErrPrimaryEmpty
	}
	if err := validateWorkflowExecutionIdentity(task.TaskID, task.RunGeneration, task.RunToken); err != nil {
		return false, err
	}
	if strings.TrimSpace(task.WorkerID) == "" {
		return false, fmt.Errorf("%w: owned update requires worker identity", ErrWorkflowOwnershipRequired)
	}
	conditions := map[string]interface{}{
		"status":         expectedStatus,
		"run_generation": task.RunGeneration,
		"run_token":      task.RunToken,
		"worker_id":      task.WorkerID,
	}
	return compareWorkflowTaskWithConditions(ctx, store, task.TaskID, conditions, updates)
}

// WithWorkflowTaskOwnership serializes a fenced side-effect write with ownership
// transfer by touching the WorkflowQueue row and running persist in one transaction.
func WithWorkflowTaskOwnership(
	ctx context.Context,
	store datastore.DataStore,
	task *model.WorkflowQueue,
	persist func(datastore.DataStore) error,
) error {
	if store == nil || task == nil {
		return datastore.ErrPrimaryEmpty
	}
	if persist == nil {
		return fmt.Errorf("workflow ownership persistence callback is nil")
	}
	if task.RunToken == "" {
		return persist(store)
	}
	if task.TaskID == "" {
		return datastore.ErrPrimaryEmpty
	}
	if task.RunGeneration == 0 || task.WorkerID == "" {
		return fmt.Errorf("workflow ownership requires generation, token, and worker identity")
	}
	transactional, ok := store.(datastore.Transactional)
	if !ok {
		return ErrWorkflowFencingUnsupported
	}
	return transactional.WithTransaction(ctx, func(tx datastore.DataStore) error {
		expectedStatus := task.Status
		if expectedStatus == "" {
			expectedStatus = config.StatusRunning
		}
		owned, err := compareWorkflowTaskWithConditions(ctx, tx, task.TaskID, map[string]interface{}{
			"status":         expectedStatus,
			"run_generation": task.RunGeneration,
			"run_token":      task.RunToken,
			"worker_id":      task.WorkerID,
		}, map[string]interface{}{})
		if err != nil {
			return fmt.Errorf("verify workflow execution ownership: %w", err)
		}
		if !owned {
			return fmt.Errorf("%w: task %s generation %d worker %s", ErrWorkflowOwnershipLost, task.TaskID, task.RunGeneration, task.WorkerID)
		}
		return persist(tx)
	})
}

func ReleaseWorkflowDispatchClaim(ctx context.Context, store datastore.DataStore, task *model.WorkflowQueue, reason string) (bool, error) {
	if task == nil || task.TaskID == "" {
		return false, datastore.ErrNilEntity
	}
	if err := validateWorkflowExecutionIdentity(task.TaskID, task.RunGeneration, task.RunToken); err != nil {
		return false, err
	}
	return compareWorkflowTaskWithConditions(ctx, store, task.TaskID, map[string]interface{}{
		"status":         config.StatusQueued,
		"run_generation": task.RunGeneration,
		"run_token":      task.RunToken,
	}, map[string]interface{}{
		"status":            config.StatusWaiting,
		"run_token":         "",
		"worker_id":         "",
		"heartbeat_at":      nil,
		"lease_expires_at":  nil,
		"scheduling_reason": reason,
	})
}

func RecoverExpiredWorkflowTasks(ctx context.Context, store datastore.DataStore) (int, error) {
	now, err := currentWorkflowDatabaseTime(ctx, store)
	if err != nil {
		return 0, err
	}
	entities, err := store.List(ctx, &model.WorkflowQueue{}, &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{
			In: []datastore.InQueryOption{{
				Key: "status", Values: []string{string(config.StatusQueued), string(config.StatusRunning)},
			}},
			NotEqual: []datastore.ComparisonQueryOption{{Key: "run_token", Value: ""}},
			LessThan: []datastore.ComparisonQueryOption{{Key: "lease_expires_at", Value: now}},
		},
		Page:     1,
		PageSize: workflowLeaseRecoveryBatchSize,
		SortBy: []datastore.SortOption{{
			Key: "lease_expires_at", Order: datastore.SortOrderAscending,
		}},
	})
	if err != nil {
		return 0, err
	}
	if len(entities) > workflowLeaseRecoveryBatchSize {
		entities = entities[:workflowLeaseRecoveryBatchSize]
	}
	recovered := 0
	for _, entity := range entities {
		task, ok := entity.(*model.WorkflowQueue)
		if !ok {
			continue
		}
		if err := validateWorkflowExecutionIdentity(task.TaskID, task.RunGeneration, task.RunToken); err != nil {
			return recovered, fmt.Errorf("active workflow task has invalid execution identity: %w", err)
		}
		if task.LeaseExpiresAt == nil {
			return recovered, fmt.Errorf("active workflow task %s has no lease expiration", task.TaskID)
		}
		if task.LeaseExpiresAt.After(now) {
			continue
		}
		updated, updateErr := compareWorkflowTaskWithConditions(ctx, store, task.TaskID, map[string]interface{}{
			"status":           task.Status,
			"run_generation":   task.RunGeneration,
			"run_token":        task.RunToken,
			"lease_expires_at": *task.LeaseExpiresAt,
		}, map[string]interface{}{
			"status": config.StatusWaiting, "run_token": "", "worker_id": "", "heartbeat_at": nil,
			"lease_expires_at": nil, "scheduling_reason": "execution lease expired",
		})
		if updateErr != nil {
			return recovered, updateErr
		}
		if updated {
			recovered++
		}
	}
	return recovered, nil
}

func currentWorkflowDatabaseTime(ctx context.Context, store datastore.DataStore) (time.Time, error) {
	clock, ok := store.(datastore.DatabaseClock)
	if !ok {
		return time.Time{}, ErrWorkflowClockUnsupported
	}
	now, err := clock.CurrentDatabaseTime(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("query workflow database clock: %w", err)
	}
	if now.IsZero() {
		return time.Time{}, fmt.Errorf("query workflow database clock: zero timestamp")
	}
	return now.UTC(), nil
}

func validateWorkflowExecutionIdentity(taskID string, generation uint64, token string) error {
	if strings.TrimSpace(taskID) == "" || generation == 0 || strings.TrimSpace(token) == "" {
		return fmt.Errorf("%w: workflow execution identity requires task, generation, and token", ErrWorkflowOwnershipRequired)
	}
	return nil
}

func compareWorkflowTaskWithConditions(ctx context.Context, store datastore.DataStore, taskID string, conditions, updates map[string]interface{}) (bool, error) {
	if taskID == "" {
		return false, datastore.ErrPrimaryEmpty
	}
	conditional, ok := store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return false, ErrWorkflowFencingUnsupported
	}
	return conditional.CompareAndSwapWithConditions(ctx, &model.WorkflowQueue{TaskID: taskID}, conditions, updates)
}

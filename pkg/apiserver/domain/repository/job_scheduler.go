package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

var (
	ErrJobAdmissionExpired  = errors.New("job admission expired")
	ErrJobAdmissionReleased = errors.New("job admission released")
)

const jobSchedulerBatchSize = 100

func EnsureJobSchedulerPolicy(ctx context.Context, store datastore.DataStore) error {
	_, err := LoadJobSchedulerPolicy(ctx, store)
	if err == nil || !errors.Is(err, datastore.ErrRecordNotExist) {
		return err
	}
	value, err := json.Marshal(workflowconfig.DefaultJobSchedulerPolicy())
	if err != nil {
		return err
	}
	err = store.Add(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler, Value: value})
	if errors.Is(err, datastore.ErrRecordExist) {
		_, err = LoadJobSchedulerPolicy(ctx, store)
	}
	return err
}

func LoadJobSchedulerPolicy(ctx context.Context, store datastore.DataStore) (workflowconfig.JobSchedulerPolicy, error) {
	setting := &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler}
	if err := store.Get(ctx, setting); err != nil {
		return workflowconfig.JobSchedulerPolicy{}, fmt.Errorf("load job scheduler policy: %w", err)
	}
	return workflowconfig.ParseJobSchedulerPolicy(setting.Value)
}

// EnqueueJobForScheduling registers only dependency-ready work. Repeated polls
// retain the first queue time, so retrying a notification cannot defeat aging.
// A nil owner is reserved for a committed, due delayed Job checkpoint.
func EnqueueJobForScheduling(ctx context.Context, store datastore.DataStore, owner *model.WorkflowQueue, job *model.JobInfo, deadline *time.Time) error {
	if job == nil || job.ExecutionKey == nil || *job.ExecutionKey == "" {
		return datastore.ErrPrimaryEmpty
	}
	if err := validateJobSchedulingOwner(owner); err != nil {
		return err
	}
	return withJobSchedulingOwner(ctx, store, owner, func(tx datastore.DataStore) error {
		current, err := scheduledJobByExecutionKey(ctx, tx, owner, *job.ExecutionKey)
		if errors.Is(err, datastore.ErrRecordNotExist) && owner != nil {
			current = new(model.JobInfo)
			*current = *job
			if err = tx.Add(ctx, current); err != nil {
				return fmt.Errorf("create queued job: %w", err)
			}
		}
		if err != nil {
			return fmt.Errorf("load queued job: %w", err)
		}
		if owner == nil {
			// Detached dispatchers have no parent row to serialize their polls.
			// Lock the existing checkpoint, then re-read under READ COMMITTED.
			locked, err := tx.CompareAndSwap(ctx, current, "execution_key", *job.ExecutionKey, nil)
			if err != nil {
				return fmt.Errorf("lock delayed job checkpoint: %w", err)
			}
			if !locked {
				return ErrWorkflowOwnershipLost
			}
			if err := tx.Get(ctx, current); err != nil {
				return err
			}
		}
		if current.ExecutionKey == nil || *current.ExecutionKey != *job.ExecutionKey || current.RunGeneration != job.RunGeneration {
			return ErrWorkflowOwnershipLost
		}
		now, err := currentWorkflowDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		generation, status := current.RunGeneration, config.Status("")
		if owner != nil {
			generation, status = owner.RunGeneration, owner.Status
			if status == "" {
				status = config.StatusRunning
			}
			if current.TaskID != owner.TaskID {
				return ErrWorkflowOwnershipLost
			}
		}
		if err := validateSchedulableJob(current, owner, now, deadline); err != nil {
			return err
		}
		class := job.SchedulingClass
		if class == "" {
			class = "normal"
		}
		priority, err := workflowconfig.ResolveJobSchedulingPriority(class)
		if err != nil {
			return err
		}
		queuedAt := &now
		if current.SchedulingGeneration == generation && current.SchedulingOwnerStatus == status && current.SchedulingQueuedAt != nil {
			// Callback callers enqueue once and poll through IsJobAdmitted.
			// A second API/timer caller must not share the first caller's slot,
			// even when both inherit the same parent deadline.
			if owner != nil && status != config.StatusRunning && current.Type == string(config.JobDeployCallback) &&
				(current.SchedulingState == workflowconfig.JobSchedulingQueued || current.SchedulingState == workflowconfig.JobSchedulingAdmitted) && !jobAdmissionExpired(current, now) {
				return fmt.Errorf("%w: callback already has an active admission", ErrWorkflowOwnershipLost)
			}
			queuedAt = current.SchedulingQueuedAt
			if current.SchedulingState == workflowconfig.JobSchedulingAdmitted && !jobAdmissionExpired(current, now) {
				*job = *current
				return nil
			}
		}
		conditions := jobSchedulingConditions(current)
		updates := map[string]interface{}{
			"scheduling_state": workflowconfig.JobSchedulingQueued,
			"scheduling_class": class, "scheduling_priority": priority,
			"scheduling_queued_at": queuedAt, "scheduling_generation": generation,
			"scheduling_owner_status": status, "scheduling_expires_at": deadline,
			"scheduling_reason": "waiting for global job admission",
		}
		if err := updateJobScheduling(ctx, tx, current, conditions, updates); err != nil {
			return err
		}
		if err := tx.Get(ctx, current); err != nil {
			return err
		}
		*job = *current
		return nil
	})
}

func IsJobAdmitted(ctx context.Context, store datastore.DataStore, owner *model.WorkflowQueue, executionKey string, expectedDeadline ...*time.Time) (bool, error) {
	if err := validateJobSchedulingOwner(owner); err != nil {
		return false, err
	}
	var admitted bool
	err := withJobSchedulingOwner(ctx, store, owner, func(tx datastore.DataStore) error {
		job, err := scheduledJobByExecutionKey(ctx, tx, owner, executionKey)
		if err != nil {
			return err
		}
		if err := matchJobSchedulingOwner(job, owner); err != nil {
			return err
		}
		if err := matchJobSchedulingDeadline(job, owner, expectedDeadline); err != nil {
			return err
		}
		now, err := currentWorkflowDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if jobAdmissionExpired(job, now) {
			return ErrJobAdmissionExpired
		}
		valid, err := jobSchedulingCurrent(ctx, tx, job, now, nil, false)
		if err != nil {
			return err
		}
		if !valid {
			return ErrWorkflowOwnershipLost
		}
		switch job.SchedulingState {
		case workflowconfig.JobSchedulingQueued:
			return nil
		case workflowconfig.JobSchedulingAdmitted:
			admitted = true
			return nil
		default:
			return ErrJobAdmissionReleased
		}
	})
	return admitted, err
}

func ReleaseJobAdmission(ctx context.Context, store datastore.DataStore, owner *model.WorkflowQueue, executionKey, reason string, expectedDeadline ...*time.Time) error {
	if err := validateJobSchedulingOwner(owner); err != nil {
		return err
	}
	releaseOwner := owner
	if owner != nil && (owner.Status == "" || owner.Status == config.StatusRunning) {
		current := &model.WorkflowQueue{TaskID: owner.TaskID}
		if err := store.Get(ctx, current); err != nil {
			return fmt.Errorf("load job admission release owner: %w", err)
		}
		if current.Status == config.StatusCancelled && current.RunGeneration == owner.RunGeneration &&
			current.RunToken == owner.RunToken && current.WorkerID == owner.WorkerID {
			// Cancellation ends execution permission, but the same owner must
			// still be able to release its original running admission after cleanup.
			releaseOwner = current
		}
	}
	return withJobSchedulingOwner(ctx, store, releaseOwner, func(tx datastore.DataStore) error {
		job, err := scheduledJobByExecutionKey(ctx, tx, owner, executionKey)
		if err != nil {
			return err
		}
		if err := matchJobSchedulingOwner(job, owner); err != nil {
			return err
		}
		if err := matchJobSchedulingDeadline(job, owner, expectedDeadline); err != nil {
			return err
		}
		if job.SchedulingState == workflowconfig.JobSchedulingReleased {
			return nil
		}
		conditions := jobSchedulingConditions(job)
		err = updateJobScheduling(ctx, tx, job, conditions, map[string]interface{}{
			"scheduling_state": workflowconfig.JobSchedulingReleased, "scheduling_reason": reason,
		})
		if !errors.Is(err, ErrWorkflowOwnershipLost) {
			return err
		}
		// A scheduler may have released the completed Job after our read. Only
		// that same admission is idempotent; a new deadline/generation is fenced.
		latest, loadErr := scheduledJobByExecutionKey(ctx, tx, owner, executionKey)
		if loadErr != nil {
			return loadErr
		}
		if matchJobSchedulingOwner(latest, owner) == nil && matchJobSchedulingDeadline(latest, owner, expectedDeadline) == nil && latest.SchedulingState == workflowconfig.JobSchedulingReleased {
			return nil
		}
		return err
	})
}

// AdmitQueuedJobs serializes every admission decision on the existing policy
// row. READ COMMITTED ensures a second scheduler counts the first one's writes
// after waiting for this lock, including across scheduler leader changes.
func AdmitQueuedJobs(ctx context.Context, store datastore.DataStore) (int, error) {
	transactional, ok := store.(datastore.ReadCommittedTransactional)
	if !ok {
		return 0, fmt.Errorf("job scheduling requires read-committed transactions")
	}
	admitted := 0
	err := transactional.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		locked, err := tx.CompareAndSwap(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler}, "type", model.SystemSettingTypeWorkflowScheduler, map[string]interface{}{})
		if err != nil {
			return fmt.Errorf("lock job scheduler policy: %w", err)
		}
		if !locked {
			return fmt.Errorf("lock job scheduler policy: %w", datastore.ErrRecordNotExist)
		}
		policy, err := LoadJobSchedulerPolicy(ctx, tx)
		if err != nil {
			return err
		}
		now, err := currentWorkflowDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		active, perWorkspace := 0, map[string]int{}
		parents := map[string]*model.WorkflowQueue{}
		var queued []*model.JobInfo
		err = scanScheduledJobs(ctx, tx, []string{workflowconfig.JobSchedulingQueued, workflowconfig.JobSchedulingAdmitted}, func(job *model.JobInfo) error {
			valid, err := jobSchedulingCurrent(ctx, tx, job, now, parents, true)
			if err != nil {
				return err
			}
			if !valid {
				err := updateJobScheduling(ctx, tx, job, jobSchedulingConditions(job), map[string]interface{}{
					"scheduling_state": workflowconfig.JobSchedulingReleased, "scheduling_reason": "execution completed or scheduling ownership expired",
				})
				if errors.Is(err, ErrWorkflowOwnershipLost) {
					return nil
				}
				return err
			}
			if job.SchedulingState == workflowconfig.JobSchedulingAdmitted {
				active++
				perWorkspace[job.WorkspaceID]++
			} else {
				// The queue snapshot retains only admission metadata. Large workload
				// and delayed payloads do not survive their database page.
				queued = append(queued, &model.JobInfo{
					ID: job.ID, WorkspaceID: job.WorkspaceID, ExecutionKey: job.ExecutionKey, RunGeneration: job.RunGeneration,
					SchedulingState: job.SchedulingState, SchedulingGeneration: job.SchedulingGeneration,
					SchedulingOwnerStatus: job.SchedulingOwnerStatus, SchedulingQueuedAt: job.SchedulingQueuedAt,
					SchedulingPriority:  job.SchedulingPriority,
					SchedulingExpiresAt: job.SchedulingExpiresAt,
				})
			}
			return nil
		})
		if err != nil {
			return err
		}
		// Re-evaluate fairness in memory after each admission. Each Job and
		// parent workflow is read once per tick, regardless of available slots.
		for active < policy.MaxConcurrentJobs && admitted < jobSchedulerBatchSize {
			var best *model.JobInfo
			for _, job := range queued {
				if job.SchedulingState != workflowconfig.JobSchedulingQueued || perWorkspace[job.WorkspaceID] >= policy.MaxConcurrentJobsPerWorkspace {
					continue
				}
				if best == nil || preferScheduledJob(job, best, policy, now, perWorkspace) {
					best = job
				}
			}
			if best == nil {
				break
			}
			if err := updateJobScheduling(ctx, tx, best, jobSchedulingConditions(best), map[string]interface{}{
				"scheduling_state": workflowconfig.JobSchedulingAdmitted, "scheduling_reason": "admitted by global job scheduler",
			}); err != nil {
				if errors.Is(err, ErrWorkflowOwnershipLost) {
					// A refreshed/released candidate is reconsidered next tick.
					best.SchedulingState = workflowconfig.JobSchedulingReleased
					continue
				}
				return err
			}
			active++
			best.SchedulingState = workflowconfig.JobSchedulingAdmitted
			perWorkspace[best.WorkspaceID]++
			admitted++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("admit queued jobs: %w", err)
	}
	return admitted, nil
}

func preferScheduledJob(a, b *model.JobInfo, p workflowconfig.JobSchedulerPolicy, now time.Time, active map[string]int) bool {
	if p.Strategy == workflowconfig.JobSchedulerPriority {
		score := func(job *model.JobInfo) int64 {
			wait := max(int64(0), now.Unix()-job.SchedulingQueuedAt.Unix())
			return int64(job.SchedulingPriority) + wait/int64(p.AgingSeconds)
		}
		if score(a) != score(b) {
			return score(a) > score(b)
		}
		if active[a.WorkspaceID] != active[b.WorkspaceID] {
			return active[a.WorkspaceID] < active[b.WorkspaceID]
		}
	}
	if !a.SchedulingQueuedAt.Equal(*b.SchedulingQueuedAt) {
		return a.SchedulingQueuedAt.Before(*b.SchedulingQueuedAt)
	}
	return a.ID < b.ID
}

func scanScheduledJobs(ctx context.Context, store datastore.DataStore, states []string, visit func(*model.JobInfo) error) error {
	lastID := 0
	for {
		opts := &datastore.ListOptions{
			Page: 1, PageSize: jobSchedulerBatchSize,
			FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{{Key: "scheduling_state", Values: states}}},
			SortBy:        []datastore.SortOption{{Key: "id", Order: datastore.SortOrderDescending}},
		}
		if lastID != 0 {
			opts.LessThan = []datastore.ComparisonQueryOption{{Key: "id", Value: lastID}}
		}
		entities, err := store.List(ctx, &model.JobInfo{}, opts)
		if err != nil {
			return fmt.Errorf("list scheduled jobs: %w", err)
		}
		for _, entity := range entities {
			job, ok := entity.(*model.JobInfo)
			if !ok || job.ID <= 0 || (lastID != 0 && job.ID >= lastID) {
				return fmt.Errorf("invalid job scheduler page")
			}
			lastID = job.ID
			if err := visit(job); err != nil {
				return err
			}
		}
		if len(entities) < jobSchedulerBatchSize {
			return nil
		}
	}
}

func validateJobSchedulingOwner(owner *model.WorkflowQueue) error {
	if owner == nil {
		return nil
	}
	if owner.TaskID == "" {
		return datastore.ErrPrimaryEmpty
	}
	// API cancellation can terminate a Workflow before any worker claims it.
	// Its callback is fenced by the terminal parent snapshot and deadline.
	if jobSchedulingTerminal(string(owner.Status)) {
		return nil
	}
	if err := validateWorkflowExecutionIdentity(owner.TaskID, owner.RunGeneration, owner.RunToken); err != nil {
		return err
	}
	if owner.WorkerID == "" {
		return ErrWorkflowOwnershipRequired
	}
	return nil
}

func withJobSchedulingOwner(ctx context.Context, store datastore.DataStore, owner *model.WorkflowQueue, fn func(datastore.DataStore) error) error {
	if owner != nil {
		return WithWorkflowTaskOwnership(ctx, store, owner, fn)
	}
	transactional, ok := store.(datastore.ReadCommittedTransactional)
	if !ok {
		return ErrWorkflowFencingUnsupported
	}
	return transactional.WithReadCommittedTransaction(ctx, fn)
}

func validateSchedulableJob(job *model.JobInfo, owner *model.WorkflowQueue, now time.Time, deadline *time.Time) error {
	if job.WorkspaceID == "" {
		return fmt.Errorf("job scheduling requires a workspace")
	}
	if jobSchedulingTerminal(job.Status) {
		return ErrJobAdmissionReleased
	}
	if deadline != nil && !deadline.After(now) {
		return ErrJobAdmissionExpired
	}
	if owner == nil {
		if job.DelayState != config.JobDelayStatePending || job.DelayPayload == "" || job.DelayExecuteAt > now.Unix() || job.Status != string(config.StatusDistributed) || deadline == nil {
			return fmt.Errorf("detached job admission requires a due pending checkpoint and deadline")
		}
	} else if owner.Status != "" && owner.Status != config.StatusRunning {
		if job.Type != string(config.JobDeployCallback) || deadline == nil {
			return fmt.Errorf("non-running workflow admission requires a bounded callback")
		}
	}
	return nil
}

func matchJobSchedulingOwner(job *model.JobInfo, owner *model.WorkflowQueue) error {
	if owner == nil {
		if job.SchedulingOwnerStatus != "" || job.DelayState == "" || job.SchedulingGeneration != job.RunGeneration {
			return ErrWorkflowOwnershipLost
		}
		return nil
	}
	status := owner.Status
	if status == "" {
		status = config.StatusRunning
	}
	if job.TaskID != owner.TaskID || job.SchedulingGeneration != owner.RunGeneration || job.SchedulingOwnerStatus != status {
		return ErrWorkflowOwnershipLost
	}
	return nil
}

// A deadline is also the admission identity for bounded callbacks and detached
// dispatch. An expired caller cannot release a later admission of the same Job.
func matchJobSchedulingDeadline(job *model.JobInfo, owner *model.WorkflowQueue, expected []*time.Time) error {
	if len(expected) == 0 {
		if owner != nil && job.SchedulingExpiresAt == nil {
			return nil
		}
		return ErrWorkflowOwnershipRequired
	}
	if len(expected) != 1 {
		return fmt.Errorf("job admission requires one expected deadline")
	}
	if expected[0] == nil {
		if owner != nil && job.SchedulingExpiresAt == nil {
			return nil
		}
		return ErrWorkflowOwnershipLost
	}
	if job.SchedulingExpiresAt == nil || !job.SchedulingExpiresAt.Equal(*expected[0]) {
		return ErrWorkflowOwnershipLost
	}
	return nil
}

func jobSchedulingCurrent(ctx context.Context, store datastore.DataStore, job *model.JobInfo, now time.Time, parents map[string]*model.WorkflowQueue, retainRunningAdmission bool) (bool, error) {
	if job.WorkspaceID == "" || job.SchedulingQueuedAt == nil || jobAdmissionExpired(job, now) || jobSchedulingTerminal(job.Status) {
		return false, nil
	}
	if job.SchedulingOwnerStatus == "" {
		return job.DelayState == config.JobDelayStatePending && job.DelayPayload != "" && job.Status == string(config.StatusDistributed) && job.DelayExecuteAt <= now.Unix() && job.SchedulingGeneration == job.RunGeneration && job.SchedulingExpiresAt != nil, nil
	}
	parent, cached := parents[job.TaskID]
	if !cached {
		parent = &model.WorkflowQueue{TaskID: job.TaskID}
		if err := store.Get(ctx, parent); err != nil {
			if !errors.Is(err, datastore.ErrRecordNotExist) {
				return false, fmt.Errorf("load scheduling workflow: %w", err)
			}
			parent = nil
		}
		if parents != nil {
			parents[job.TaskID] = parent
		}
	}
	if parent == nil {
		return false, nil
	}
	if parent.RunGeneration != job.SchedulingGeneration || parent.Status != job.SchedulingOwnerStatus {
		return false, nil
	}
	if !jobSchedulingTerminal(string(parent.Status)) && (parent.RunToken == "" || parent.WorkerID == "") {
		return false, nil
	}
	if parent.Status == config.StatusRunning {
		// A late heartbeat may still renew an expired lease until the workflow
		// reaper revokes its generation/token. Do not hand this running Job's
		// slot to another Job before that authoritative ownership transition.
		if retainRunningAdmission && job.SchedulingState == workflowconfig.JobSchedulingAdmitted {
			return true, nil
		}
		return parent.LeaseExpiresAt != nil && parent.LeaseExpiresAt.After(now), nil
	}
	return job.Type == string(config.JobDeployCallback) && job.SchedulingExpiresAt != nil, nil
}

func jobAdmissionExpired(job *model.JobInfo, now time.Time) bool {
	return job.SchedulingExpiresAt != nil && !job.SchedulingExpiresAt.After(now)
}

func jobSchedulingTerminal(status string) bool {
	switch config.Status(status) {
	case config.StatusCompleted, config.StatusPassed, config.StatusSkipped, config.StatusFailed, config.StatusTimeout, config.StatusCancelled, config.StatusReject, config.StatusNotRun:
		return true
	}
	return false
}

func scheduledJobByExecutionKey(ctx context.Context, store datastore.DataStore, owner *model.WorkflowQueue, key string) (*model.JobInfo, error) {
	if key == "" {
		return nil, datastore.ErrPrimaryEmpty
	}
	query := &model.JobInfo{}
	if owner != nil && owner.AppID == "" {
		// Resource import Jobs have no application. Their task and workspace
		// let the access store validate the parent before querying these rows.
		query.TaskID, query.WorkspaceID = owner.TaskID, owner.WorkspaceID
	}
	entities, err := store.List(ctx, query, &datastore.ListOptions{Page: 1, PageSize: 2, FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{{Key: "execution_key", Values: []string{key}}}}})
	if err != nil {
		return nil, fmt.Errorf("load scheduled job: %w", err)
	}
	if len(entities) != 1 {
		return nil, datastore.ErrRecordNotExist
	}
	job, ok := entities[0].(*model.JobInfo)
	if !ok {
		return nil, datastore.ErrEntityInvalid
	}
	return job, nil
}

func jobSchedulingConditions(job *model.JobInfo) map[string]interface{} {
	conditions := map[string]interface{}{
		"execution_key": job.ExecutionKey, "run_generation": job.RunGeneration,
		"scheduling_generation": job.SchedulingGeneration,
		"scheduling_expires_at": nil,
	}
	if job.SchedulingExpiresAt != nil {
		conditions["scheduling_expires_at"] = *job.SchedulingExpiresAt
	}
	// Columns introduced on existing jobs can be NULL until first enqueue.
	if job.SchedulingState != "" {
		conditions["scheduling_state"] = job.SchedulingState
	}
	if job.SchedulingOwnerStatus != "" {
		conditions["scheduling_owner_status"] = job.SchedulingOwnerStatus
	}
	return conditions
}

func updateJobScheduling(ctx context.Context, store datastore.DataStore, job *model.JobInfo, conditions, updates map[string]interface{}) error {
	conditional, ok := store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return ErrWorkflowFencingUnsupported
	}
	updated, err := conditional.CompareAndSwapWithConditions(ctx, job, conditions, updates)
	if err != nil {
		return fmt.Errorf("update job scheduling: %w", err)
	}
	if !updated {
		return ErrWorkflowOwnershipLost
	}
	return nil
}

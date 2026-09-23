package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type jobAdmissionRecoveryIdentity struct {
	UID           string
	Namespace     string
	Name          string
	TaskID        string
	ExecutionKey  string
	RunGeneration uint64
	Deadline      int64
	Attempt       uint
	Kind          string
}

// Only an immutable, already-created execution whose admission is still active
// may retain capacity through ownership recovery. Once released, the execution
// must re-enter the serialized scheduler before it can run again.
func recoverableJobAdmission(job *model.JobInfo, now time.Time) *jobAdmissionRecoveryIdentity {
	if job == nil || job.SchedulingQueuedAt == nil || job.SchedulingGeneration == 0 || jobSchedulingTerminal(job.Status) || jobAdmissionExpired(job, now) {
		return nil
	}
	if job.SchedulingState != workflowconfig.JobSchedulingAdmitted {
		return nil
	}
	return jobAdmissionCheckpointIdentity(job, now)
}

// Runner heartbeat/progress and retention metadata may change independently;
// compare only the immutable execution identity when transferring ownership.
func jobAdmissionCheckpointIdentity(job *model.JobInfo, now time.Time) *jobAdmissionRecoveryIdentity {
	if job == nil || (job.Type != string(config.JobCommand) && job.Type != string(config.JobEval)) || job.ExecutionKey == nil || *job.ExecutionKey == "" || job.RunGeneration == 0 {
		return nil
	}
	var checkpoint struct {
		Kind       string `json:"kind"`
		Version    int    `json:"version"`
		Attempt    uint   `json:"attempt"`
		CurrentUID string `json:"currentUID"`
		Deadline   int64  `json:"deadline"`
		Job        *struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
		} `json:"job"`
	}
	if json.Unmarshal([]byte(job.InternalInfo), &checkpoint) != nil || checkpoint.Kind != "instant_job_retry" || checkpoint.Version != 1 ||
		checkpoint.CurrentUID == "" || checkpoint.Attempt != 1 || checkpoint.Attempt != job.Attempt ||
		!time.Unix(0, checkpoint.Deadline).After(now) || checkpoint.Job == nil {
		return nil
	}
	metadata := checkpoint.Job.Metadata
	if metadata.Name == "" || metadata.Namespace == "" || metadata.Annotations[config.AnnotationJobTaskID] != job.TaskID ||
		metadata.Annotations[config.AnnotationJobExecutionKey] != *job.ExecutionKey ||
		metadata.Annotations[config.AnnotationJobRunGeneration] != strconv.FormatUint(job.RunGeneration, 10) ||
		metadata.Annotations[workflowconfig.AnnotationJobAttempt] != strconv.FormatUint(uint64(checkpoint.Attempt), 10) {
		return nil
	}
	var policy workflowconfig.JobRetryPolicy
	if json.Unmarshal([]byte(metadata.Annotations[workflowconfig.AnnotationJobRetryPolicy]), &policy) != nil || policy.OnOOM != "stop" || policy.Validate() != nil {
		return nil
	}
	return &jobAdmissionRecoveryIdentity{UID: checkpoint.CurrentUID, Namespace: metadata.Namespace, Name: metadata.Name,
		TaskID: job.TaskID, ExecutionKey: *job.ExecutionKey, RunGeneration: job.RunGeneration, Deadline: checkpoint.Deadline, Attempt: checkpoint.Attempt, Kind: job.Type}
}

// ReleaseLostJobAdmission releases capacity only when the current workflow
// owner proves that the exact immutable Kubernetes execution recorded by an
// active admission is authoritatively absent or replaced. The owner-row lock
// serializes this decision with lease transfer and checkpoint persistence.
func ReleaseLostJobAdmission(ctx context.Context, store datastore.DataStore, owner *model.WorkflowQueue, supplied *model.JobInfo, reason string) error {
	if owner == nil {
		return ErrWorkflowOwnershipRequired
	}
	if err := validateJobSchedulingOwner(owner); err != nil {
		return err
	}
	if supplied == nil || supplied.ExecutionKey == nil || *supplied.ExecutionKey == "" {
		return datastore.ErrPrimaryEmpty
	}
	return withJobSchedulingOwner(ctx, store, owner, func(tx datastore.DataStore) error {
		current, err := scheduledJobByExecutionKey(ctx, tx, owner, *supplied.ExecutionKey)
		if err != nil {
			return err
		}
		if current.TaskID != owner.TaskID {
			return ErrWorkflowOwnershipLost
		}
		now, err := currentWorkflowDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		persistedIdentity := jobAdmissionCheckpointIdentity(current, now)
		suppliedIdentity := jobAdmissionCheckpointIdentity(supplied, now)
		if persistedIdentity == nil || suppliedIdentity == nil || *persistedIdentity != *suppliedIdentity {
			return fmt.Errorf("%w: lost Job admission checkpoint changed", ErrWorkflowOwnershipLost)
		}
		switch current.SchedulingState {
		case workflowconfig.JobSchedulingQueued, workflowconfig.JobSchedulingReleased:
			return nil
		case workflowconfig.JobSchedulingAdmitted:
		default:
			return ErrWorkflowOwnershipLost
		}
		conditions := jobSchedulingConditions(current)
		conditions["task_id"] = current.TaskID
		conditions["type"] = current.Type
		conditions["attempt"] = current.Attempt
		conditions["internal_info"] = current.InternalInfo
		err = updateJobScheduling(ctx, tx, current, conditions, map[string]interface{}{
			"scheduling_state":  workflowconfig.JobSchedulingReleased,
			"scheduling_reason": reason,
		})
		if !errors.Is(err, ErrWorkflowOwnershipLost) {
			return err
		}
		// A concurrent scheduler may already have released or requeued this
		// exact execution. Treat only those non-active states as idempotent.
		latest, loadErr := scheduledJobByExecutionKey(ctx, tx, owner, *supplied.ExecutionKey)
		if loadErr != nil {
			return loadErr
		}
		latestIdentity := jobAdmissionCheckpointIdentity(latest, now)
		if latest.TaskID == owner.TaskID && latestIdentity != nil && *latestIdentity == *suppliedIdentity &&
			(latest.SchedulingState == workflowconfig.JobSchedulingQueued || latest.SchedulingState == workflowconfig.JobSchedulingReleased) {
			return nil
		}
		return err
	})
}

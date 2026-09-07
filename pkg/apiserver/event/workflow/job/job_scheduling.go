package job

import (
	"context"
	"fmt"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// Only ready jobs reach this point: the workflow controller has already enforced
// step dependencies and its local concurrency limit. Queueing does not consume
// the job's execution timeout, but remains bounded by the caller's cancellation.
func waitForJobAdmission(ctx context.Context, store datastore.DataStore, task *model.JobTask) (func() error, error) {
	noop := func() error { return nil }
	// Direct, non-distributed controller calls have no durable execution identity.
	// Explicit non-running owners still require admission: API-side terminal
	// callbacks can belong to workflows that never obtained an execution token.
	if task.RunToken == "" && task.RunGeneration == 0 && (task.OwnerStatus == "" || task.OwnerStatus == config.StatusRunning) {
		return noop, nil
	}
	owner := &model.WorkflowQueue{
		TaskID: task.TaskID, RunGeneration: jobOwnerGeneration(task),
		RunToken: task.RunToken, WorkerID: task.WorkerID, Status: task.OwnerStatus,
	}
	if owner.Status == "" {
		owner.Status = config.StatusRunning
	}
	record := buildJobInfoRecord(task)
	record.SchedulingClass = task.SchedulingClass
	var deadline *time.Time
	if d, ok := ctx.Deadline(); ok {
		deadline = &d
	}
	if err := repository.EnqueueJobForScheduling(ctx, store, owner, &record, deadline); err != nil {
		return noop, fmt.Errorf("queue ready job: %w", err)
	}
	release := func() error {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return repository.ReleaseJobAdmission(releaseCtx, store, owner, task.ExecutionKey, "job execution returned", record.SchedulingExpiresAt)
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		admitted, err := repository.IsJobAdmitted(ctx, store, owner, task.ExecutionKey, record.SchedulingExpiresAt)
		if err != nil {
			return release, fmt.Errorf("wait for job admission: %w", err)
		}
		if admitted {
			return release, nil
		}
		select {
		case <-ctx.Done():
			return release, ctx.Err()
		case <-ticker.C:
		}
	}
}

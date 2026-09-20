package job

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

// Only ready jobs reach this point: the workflow controller has already enforced
// step dependencies and its local concurrency limit. Queueing does not consume
// the job's execution timeout, but remains bounded by the caller's cancellation.
func waitForJobAdmission(ctx context.Context, store datastore.DataStore, task *model.JobTask, client kubernetes.Interface) (func() error, error) {
	noop := func() error { return nil }
	// Direct, non-distributed controller calls have no durable execution identity.
	// Explicit non-running owners still require admission: API-side terminal
	// callbacks can belong to workflows that never obtained an execution token.
	if task.RunToken == "" && task.RunGeneration == 0 && (task.OwnerStatus == "" || task.OwnerStatus == config.StatusRunning) {
		return noop, nil
	}
	owner := &model.WorkflowQueue{
		TaskID: task.TaskID, RunGeneration: jobOwnerGeneration(task),
		AppID: task.AppID, WorkspaceID: task.WorkspaceID,
		RunToken: task.RunToken, WorkerID: task.WorkerID, Status: task.OwnerStatus,
	}
	if owner.Status == "" {
		owner.Status = config.StatusRunning
	}
	record := buildJobInfoRecord(task)
	record.SchedulingClass = task.SchedulingClass
	resources, err := jobSchedulingResourceSnapshot(task)
	if err != nil {
		return noop, fmt.Errorf("snapshot job admission resources: %w", err)
	}
	record.SchedulingResources = resources
	var deadline *time.Time
	if d, ok := ctx.Deadline(); ok {
		deadline = &d
	}
	confirmedUID, err := confirmJobAdmissionRecovery(ctx, client, task)
	if err != nil {
		return noop, errors.Join(signal.ErrInfrastructureStop, err)
	}
	if err := repository.EnqueueJobForScheduling(ctx, store, owner, &record, deadline, confirmedUID); err != nil {
		return noop, fmt.Errorf("queue ready job: %w", err)
	}
	release := func() error {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return repository.ReleaseJobAdmission(releaseCtx, store, owner, task.ExecutionKey, "job execution returned", record.SchedulingExpiresAt)
	}
	// Admission changes at the scheduler's cadence. Keep the first checks fast,
	// then stop issuing five ownership transactions per second for each queued
	// execution. Cancellation remains immediate while the timer is waiting.
	delay := 200 * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
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
		case <-timer.C:
		}
		delay = min(delay*2, workflowconfig.DefaultDispatchPollInterval)
		timer.Reset(delay)
	}
}

// Healthy recovered resources take no creation permit. New standalone command
// and all evaluation Runner attempts share the same persisted budget as trial
// Sandbox creation, including across API and Worker replicas.
func (c *InstantJobCtl) waitRetryCreationBudget(ctx context.Context) error {
	if c.job.JobType != string(config.JobEval) && c.job.JobType != string(config.JobCommand) {
		return nil
	}
	for {
		if err := c.ensureRetryWorkflowOwnership(ctx); err != nil {
			return err
		}
		delay, err := repository.ReserveResourceCreation(ctx, c.store)
		if err != nil {
			return errors.Join(signal.ErrInfrastructureStop, err)
		}
		if delay == 0 {
			return c.ensureRetryWorkflowOwnership(ctx)
		}
		// A rejected reservation retains no permit. Spread waiting callers so a
		// tick does not force every Worker to contend for the policy row at once.
		delay += time.Duration(rand.Int64N(int64(time.Second)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// A persisted UID makes this a reattachment, never a new resource admission.
// Check it once outside the database transaction; ensureRetryAttempt repeats
// the identity check and refuses a Create if the object disappears meanwhile.
func confirmJobAdmissionRecovery(ctx context.Context, client kubernetes.Interface, task *model.JobTask) (string, error) {
	if (task.JobType != string(config.JobCommand) && task.JobType != string(config.JobEval)) || task.InternalInfo == "" {
		return "", nil
	}
	cp, err := decodeInstantJobRetryCheckpoint(task)
	if err != nil {
		return "", err
	}
	if cp.CurrentUID == "" {
		return "", nil
	}
	policy, err := retryPolicyFromJob(cp.Job)
	if err != nil {
		return "", err
	}
	if policy.OnOOM != "stop" {
		return "", nil
	}
	if !time.Unix(0, cp.Deadline).After(time.Now()) {
		return "", context.DeadlineExceeded
	}
	if client == nil {
		return "", fmt.Errorf("confirm existing Job admission: Kubernetes client is required")
	}
	live, err := client.BatchV1().Jobs(cp.Job.Namespace).Get(ctx, cp.Job.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("confirm existing Job admission: %w", err)
	}
	record := buildJobInfoRecord(task)
	if err := ValidateInstantJobRetryExecution(&record, live); err != nil {
		return "", err
	}
	return string(cp.CurrentUID), nil
}

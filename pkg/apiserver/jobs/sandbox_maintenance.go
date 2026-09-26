package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) reconcileSandboxes(ctx context.Context, limit int) error {
	if s.SandboxClient == nil {
		return nil
	}
	now, err := s.Store.CurrentDatabaseTime(ctx)
	if err != nil {
		return err
	}
	rows, err := s.Store.List(ctx, &model.JobSandbox{}, &datastore.ListOptions{Page: 1, PageSize: limit,
		FilterOptions: datastore.FilterOptions{NotEqual: []datastore.ComparisonQueryOption{{Key: "state", Value: sandboxReleased}}, LessThan: []datastore.ComparisonQueryOption{{Key: "reconcile_at", Value: now}}},
		SortBy:        []datastore.SortOption{{Key: "reconcile_at", Order: datastore.SortOrderAscending}}})
	if err != nil {
		return err
	}
	var group errgroup.Group
	group.SetLimit(8)
	for _, entity := range rows {
		row := entity.(*model.JobSandbox)
		group.Go(func() error {
			operation, cancel := context.WithTimeout(ctx, sandboxOperationTimeout)
			defer cancel()
			if err := s.maintainSandbox(operation, row); err != nil {
				if ctx.Err() != nil {
					return err
				}
				if retryErr := s.delayFailedSandbox(ctx, row); retryErr != nil {
					return errors.Join(err, retryErr)
				}
				return err
			}
			return nil
		})
	}
	return group.Wait()
}

// Move a failed row behind the current due backlog without changing its
// reservation. Only the observed lifecycle version may receive this delay.
func (s *Service) delayFailedSandbox(ctx context.Context, row *model.JobSandbox) error {
	now, err := s.Store.CurrentDatabaseTime(ctx)
	if err != nil {
		return fmt.Errorf("read failed Sandbox maintenance retry time: %w", err)
	}
	next := now.Add(15 * time.Second)
	if !row.ReconcileAt.Before(next) {
		return nil
	}

	// A concurrent lifecycle change owns its own next reconcile time.
	_, err = s.Store.CompareAndSwapWithConditions(ctx, &model.JobSandbox{ID: row.ID},
		map[string]interface{}{"reconcile_at": row.ReconcileAt, "lease_token": row.LeaseToken, "state": row.State},
		map[string]interface{}{"reconcile_at": next})
	if err != nil {
		return fmt.Errorf("delay failed Sandbox maintenance: %w", err)
	}
	return nil
}

func (s *Service) maintainSandbox(ctx context.Context, candidate *model.JobSandbox) error {
	ctx = account.WithScope(ctx, account.Scope{WorkspaceID: candidate.WorkspaceID, Namespace: candidate.Namespace, Role: "member"})
	stopped := ""
	if !candidate.ReleaseRequested && candidate.SlotReserved {
		pod, err := s.Kube.CoreV1().Pods(candidate.Namespace).Get(ctx, candidate.RunnerPodName, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) || (err == nil && string(pod.UID) != candidate.RunnerUID) {
			stopped = "runner_lost"
		} else if err != nil {
			return err
		}
	}
	advance := false
	err := artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		task, job, orphaned, err := lockSandboxMaintenanceOwners(ctx, tx, candidate)
		if err != nil {
			return err
		}
		row := &model.JobSandbox{ID: candidate.ID}
		if err := tx.GetForUpdate(ctx, row); err != nil {
			return err
		}
		if row.WorkspaceID != candidate.WorkspaceID || row.Namespace != candidate.Namespace || row.RunnerUID != candidate.RunnerUID || row.JobID != candidate.JobID || row.TaskID != candidate.TaskID || row.ExecutionKey != candidate.ExecutionKey {
			return ErrRunnerConflict
		}
		now, err := tx.CurrentDatabaseTime(ctx)
		if err != nil {
			return err
		}
		if row.LeaseUntil != nil && now.Before(*row.LeaseUntil) {
			return nil
		}
		// Recovery owns the shutdown-to-terminal proof. Ordinary retention must
		// never extend shutdownTime while that proof is in progress.
		if row.Reason == "recovery_isolation" && !orphaned && task.Status == config.StatusRunning && now.Before(row.Deadline) {
			row.ReconcileAt = now.Add(15 * time.Second)
			return putSandbox(ctx, tx, row)
		}
		if orphaned {
			stopped = "execution_finished"
		} else {
			claim, _, claimErr := decodeRunnerState(job)
			if claimErr != nil || claim == nil || claim.OwnerPodUID != row.RunnerUID || claim.OwnerPodName != row.RunnerPodName {
				stopped = "runner_lost"
			}
			if task.Status == config.StatusCancelled {
				stopped = "cancelled"
			} else if !now.Before(row.Deadline) {
				stopped = "execution_deadline"
			} else if terminal(task.Status) || terminal(config.Status(job.Status)) {
				stopped = "execution_finished"
			}
		}
		if stopped != "" {
			stopSandbox(row, now, stopped)
		}
		row.ReconcileAt = now.Add(15 * time.Second)
		if row.State == sandboxFailed && !row.SlotReserved {
			row.ReconcileAt = now.Add(sandboxRetention)
		}
		if row.ReleaseRequested && row.State != sandboxReleased {
			until := now.Add(sandboxLeaseDuration)
			row.LeaseToken, row.LeaseUntil, advance = uuid.NewString(), &until, true
		}
		if err := putSandbox(ctx, tx, row); err != nil {
			return err
		}
		*candidate = *row
		return nil
	})
	if err != nil || !advance {
		return err
	}
	return s.cleanupSandbox(ctx, nil, candidate)
}

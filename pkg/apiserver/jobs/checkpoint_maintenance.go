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
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (s *Service) reconcileCheckpoints(ctx context.Context, limit int) error {
	if s.SandboxClient == nil {
		return nil
	}
	now, err := s.Store.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
	if err != nil {
		return err
	}
	rows, err := s.Store.List(ctx, &model.JobCheckpoint{}, &datastore.ListOptions{Page: 1, PageSize: limit, FilterOptions: datastore.FilterOptions{LessThan: []datastore.ComparisonQueryOption{{Key: "reconcile_at", Value: now}}}, SortBy: []datastore.SortOption{{Key: "reconcile_at", Order: datastore.SortOrderAscending}}})
	if err != nil {
		return err
	}
	var group errgroup.Group
	group.SetLimit(8)
	for _, entity := range rows {
		row := entity.(*model.JobCheckpoint)
		group.Go(func() error {
			operation, cancel := context.WithTimeout(ctx, sandboxOperationTimeout)
			defer cancel()
			if err := s.maintainCheckpoint(operation, row); err != nil {
				if ctx.Err() != nil {
					return err
				}
				now, clockErr := s.Store.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
				if clockErr != nil {
					return errors.Join(err, clockErr)
				}
				// Failed cloud reads must not pin the same rows at the head of
				// every bounded maintenance page. Never delay a newer lifecycle.
				_, delayErr := s.Store.(datastore.ConditionalCompareAndSwap).CompareAndSwapWithConditions(ctx, &model.JobCheckpoint{ID: row.ID}, map[string]interface{}{"reconcile_at": row.ReconcileAt, "lease_token": row.LeaseToken, "state": row.State}, map[string]interface{}{"reconcile_at": now.Add(15 * time.Second)})
				return fmt.Errorf("maintain checkpoint %s: %w", row.ID, errors.Join(err, delayErr))
			}
			return nil
		})
	}
	return group.Wait()
}

func (s *Service) maintainCheckpoint(ctx context.Context, candidate *model.JobCheckpoint) error {
	ctx = account.WithScope(ctx, account.Scope{WorkspaceID: candidate.WorkspaceID, Namespace: candidate.Namespace, Role: "member"})
	runnerLost := false
	if candidate.State == sandboxPending {
		pod, err := s.Kube.CoreV1().Pods(candidate.Namespace).Get(ctx, candidate.RunnerPodName, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) || (err == nil && (string(pod.UID) != candidate.RunnerUID || pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded)) {
			runnerLost = true
		} else if err != nil {
			return err
		}
	}
	cleanup := false
	err := s.Store.(datastore.Transactional).WithTransaction(ctx, func(tx datastore.DataStore) error {
		task, job, orphaned, err := lockSandboxMaintenanceOwners(ctx, tx, &model.JobSandbox{WorkspaceID: candidate.WorkspaceID, TaskID: candidate.TaskID, JobID: candidate.JobID, ExecutionKey: candidate.ExecutionKey})
		if err != nil {
			return err
		}
		row := &model.JobCheckpoint{ID: candidate.ID}
		if err := tx.(datastore.RowLocker).GetForUpdate(ctx, row); err != nil {
			return err
		}
		now, err := tx.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
		if err != nil {
			return err
		}
		if row.Cleaned {
			if !now.Before(row.SourceDeadline.Add(sandboxRetention)) {
				return tx.Delete(ctx, row)
			}
			row.ReconcileAt = row.SourceDeadline.Add(sandboxRetention)
			return putCheckpoint(ctx, tx, row)
		}
		if row.LeaseUntil != nil && now.Before(*row.LeaseUntil) {
			return nil
		}
		row.ReconcileAt = now.Add(15 * time.Second)
		if orphaned || terminal(task.Status) || !now.Before(row.SourceDeadline) {
			// A distributed successor may never reach a terminal JobInfo if its
			// parent is cancelled before dispatch. Parent authority and the
			// immutable source deadline also terminate that reference.
			row.ReferencedByExecutionKey, row.ExpiresAt = "", now
		} else if row.ReferencedByExecutionKey != "" {
			refs, err := tx.List(ctx, &model.JobInfo{TaskID: row.TaskID, WorkspaceID: row.WorkspaceID}, &datastore.ListOptions{Page: 1, PageSize: 2, FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{{Key: "execution_key", Values: []string{row.ReferencedByExecutionKey}}}}})
			if err != nil {
				return err
			}
			if len(refs) == 1 {
				reference := refs[0].(*model.JobInfo)
				if reference.ExecutionKey == nil || *reference.ExecutionKey != row.ReferencedByExecutionKey {
					return ErrRunnerConflict
				}
				if terminal(config.Status(reference.Status)) {
					row.ReferencedByExecutionKey = ""
				}
			} else if len(refs) == 0 {
				row.ReferencedByExecutionKey = ""
			}
		}
		if row.State == sandboxPending {
			claim, _, claimErr := decodeRunnerState(job)
			stopped := orphaned || runnerLost || claimErr != nil || claim == nil || claim.OwnerPodUID != row.RunnerUID || claim.OwnerPodName != row.RunnerPodName || terminal(task.Status) || terminal(config.Status(job.Status)) || !now.Before(row.SourceDeadline)
			if stopped || now.Sub(row.CreateTime) >= checkpointTimeout {
				row.State, row.Reason, row.ExpiresAt = sandboxFailed, "checkpoint_timeout", now
				if stopped {
					row.Reason = "execution_stopped"
				}
			}
		}
		if row.ReferencedByExecutionKey == "" && !now.Before(row.ExpiresAt) {
			until := now.Add(sandboxLeaseDuration)
			row.LeaseToken, row.LeaseUntil, cleanup = uuid.NewString(), &until, true
		}
		if err := putCheckpoint(ctx, tx, row); err != nil {
			return err
		}
		*candidate = *row
		return nil
	})
	if err != nil || !cleanup {
		return err
	}
	members, err := checkpointMembers(candidate)
	if err != nil {
		return err
	}
	allGone := true
	client := s.SandboxClient.Resource(CheckpointGVR).Namespace(candidate.Namespace)
	for _, member := range members {
		object, err := client.Get(ctx, member.SnapshotName, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			// A timed-out request can still create its deterministic CR. Its
			// deletion cannot establish that it never existed. Keep this discovery
			// intent through the source's absolute retention bound.
			if member.CreateRequested && member.SnapshotUID == "" {
				now, err := s.Store.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
				if err != nil {
					return err
				}
				if now.Before(candidate.SourceDeadline.Add(sandboxRetention)) {
					allGone = false
				}
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("get expired snapshot: %w", err)
		}
		if !ownedCheckpoint(candidate, member, object) {
			return fmt.Errorf("expired checkpoint resource identity changed: %w", ErrRunnerConflict)
		}
		phase, _ := checkpointPhase(object)
		if phase != "Succeeded" && phase != "Failed" {
			allGone = false
			continue
		}
		uid := types.UID(object.GetUID())
		if err := client.Delete(ctx, member.SnapshotName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !k8serrors.IsNotFound(err) {
			return fmt.Errorf("delete expired snapshot: %w", err)
		}
		if _, err := client.Get(ctx, member.SnapshotName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
			if err != nil {
				return fmt.Errorf("confirm snapshot deletion: %w", err)
			}
			allGone = false
		}
	}
	return s.Store.(datastore.Transactional).WithTransaction(ctx, func(tx datastore.DataStore) error {
		if _, _, _, err := lockSandboxMaintenanceOwners(ctx, tx, &model.JobSandbox{WorkspaceID: candidate.WorkspaceID, TaskID: candidate.TaskID, JobID: candidate.JobID, ExecutionKey: candidate.ExecutionKey}); err != nil {
			return err
		}
		row := &model.JobCheckpoint{ID: candidate.ID}
		if err := tx.(datastore.RowLocker).GetForUpdate(ctx, row); err != nil {
			return err
		}
		now, err := tx.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
		if err != nil {
			return err
		}
		if row.LeaseToken != candidate.LeaseToken || row.LeaseUntil == nil || !now.Before(*row.LeaseUntil) || row.ReferencedByExecutionKey != "" {
			return ErrRunnerConflict
		}
		row.LeaseToken, row.LeaseUntil, row.ReconcileAt = "", nil, now.Add(15*time.Second)
		if allGone {
			if err := artifacts.DeleteCheckpoint(ctx, tx, row.WorkspaceID, row.MaterialID); err != nil {
				return err
			}
			row.Cleaned, row.State = true, sandboxFailed
			row.ReconcileAt = row.SourceDeadline.Add(sandboxRetention)
			if row.Reason == "" {
				row.Reason = "expired"
			}
		}
		return putCheckpoint(ctx, tx, row)
	})
}

func (s *Service) sandboxCheckpointActive(ctx context.Context, row *model.JobSandbox) (bool, error) {
	points, err := s.Store.List(ctx, &model.JobCheckpoint{WorkspaceID: row.WorkspaceID, ExecutionKey: row.ExecutionKey}, &datastore.ListOptions{Page: 1, PageSize: 5, FilterOptions: datastore.FilterOptions{NotEqual: []datastore.ComparisonQueryOption{{Key: "cleaned", Value: true}, {Key: "state", Value: sandboxReady}}}})
	if err != nil {
		return false, err
	}
	for _, entity := range points {
		point := entity.(*model.JobCheckpoint)
		members, err := checkpointMembers(point)
		if err != nil {
			return false, err
		}
		for _, member := range members {
			if member.SandboxID != row.ID || !member.CreateRequested {
				continue
			}
			object, err := s.SandboxClient.Resource(CheckpointGVR).Namespace(point.Namespace).Get(ctx, member.SnapshotName, metav1.GetOptions{})
			if k8serrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return false, err
			}
			if !ownedCheckpoint(point, member, object) {
				return false, ErrRunnerConflict
			}
			phase, _ := checkpointPhase(object)
			if phase != "Succeeded" && phase != "Failed" {
				return true, nil
			}
		}
	}
	return false, nil
}

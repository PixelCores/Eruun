package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// DispatchSchedules rotates bounded pages just like DispatchWorkflowSchedules.
// Failed rows remain due, but cannot starve later pages of definitions.
func (s *Service) DispatchSchedules(ctx context.Context) (int, error) {
	s.scheduleDispatchMu.Lock()
	defer s.scheduleDispatchMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.scheduleDispatchPage == 0 {
		s.scheduleDispatchPage = 1
	}
	now := time.Now().UTC()
	rows, err := s.Store.List(ctx, &model.JobDefinition{}, &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{
			In:       []datastore.InQueryOption{{Key: "schedule_enabled", Values: []string{"1"}}},
			LessThan: []datastore.ComparisonQueryOption{{Key: "next_run", Value: now.Unix() + 1}},
		},
		Page: s.scheduleDispatchPage, PageSize: 100,
		SortBy: []datastore.SortOption{{Key: "next_run", Order: datastore.SortOrderAscending}, {Key: "id", Order: datastore.SortOrderAscending}},
	})
	if err != nil {
		return 0, fmt.Errorf("list due Job definitions: %w", err)
	}
	processed := 0
	var dispatchErrs []error
	for _, row := range rows {
		definition := row.(*model.JobDefinition)
		queued, err := s.dispatchSchedule(ctx, definition, now)
		if err != nil {
			dispatchErr := fmt.Errorf("dispatch Job definition %s: %w", definition.ID, err)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return processed, dispatchErr
			}
			dispatchErrs = append(dispatchErrs, dispatchErr)
			continue
		}
		if queued {
			processed++
		}
	}
	// Successful claims shrink the set. Wrapping revisits rows shifted to earlier
	// pages; advancing even after failures lets later definitions make progress.
	if len(rows) == 100 {
		s.scheduleDispatchPage++
	} else {
		s.scheduleDispatchPage = 1
	}
	return processed, errors.Join(dispatchErrs...)
}

func (s *Service) dispatchSchedule(ctx context.Context, definition *model.JobDefinition, now time.Time) (bool, error) {
	if definition == nil || !definition.ScheduleEnabled || definition.Cron == "" || definition.NextRun > now.Unix() {
		return false, nil
	}
	nextRun, err := workflow.NextScheduleRun(definition.Cron, now)
	if err != nil {
		return false, fmt.Errorf("calculate next Job schedule occurrence: %w", err)
	}
	space := &model.Workspace{ID: definition.WorkspaceID}
	if err := s.Store.Get(ctx, space); err != nil {
		return false, fmt.Errorf("read Job schedule workspace: %w", err)
	}
	scope := account.ForWorkspace(space)
	scope.UserID = definition.Creator
	ctx = account.WithScope(ctx, scope)
	// Avoid cancelling from an already stale page after deletion, an update or
	// another dispatch. The transaction's CAS rechecks this after preparation.
	current, err := scopedDefinition(ctx, s.Store, definition.ID, false)
	if errors.Is(err, datastore.ErrRecordNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current.NextRun != definition.NextRun || current.Revision != definition.Revision || !current.ScheduleEnabled {
		return false, nil
	}
	var declaration spec.JobSpec
	var task *model.WorkflowQueue
	err = spec.DecodeJobJSON([]byte(definition.Spec), &declaration)
	if err == nil {
		task, err = s.prepareTask(ctx, scope, declaration)
	}
	skipReason := ""
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		detail := bcode.SafeClientMessage(err)
		if detail == "" {
			detail = err.Error()
		}
		skipReason = "validate scheduled Job: " + detail
	} else {
		policy := definition.ConcurrencyPolicy
		if policy != concurrencyForbid && policy != concurrencyAllow && policy != concurrencyReplace {
			return false, fmt.Errorf("invalid stored Job concurrencyPolicy %q", policy)
		}
		if definition.LastRunTaskID != "" && policy != concurrencyAllow {
			previous := &model.WorkflowQueue{TaskID: definition.LastRunTaskID}
			err := s.Store.Get(ctx, previous)
			if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
				return false, fmt.Errorf("read previous scheduled Job: %w", err)
			}
			if err == nil {
				if previous.WorkspaceID != scope.WorkspaceID || previous.JobDefinitionID != definition.ID || previous.Type != config.WorkflowTaskTypeJob || previous.AppID != "" {
					return false, fmt.Errorf("previous scheduled Job %s does not belong to definition %s", previous.TaskID, definition.ID)
				}
				if model.IsWorkflowActiveStatus(previous.Status) {
					if policy == concurrencyForbid {
						skipReason = fmt.Sprintf("Forbid: previous scheduled Job %s is active (%s)", previous.TaskID, previous.Status)
					} else {
						if s.Workflow == nil {
							return false, fmt.Errorf("replace scheduled Job: WorkflowService is unavailable")
						}
						// Cancellation must finish before claiming/enqueuing. If it
						// fails (or the process stops), this occurrence remains due.
						if err := s.Workflow.CancelWorkflowTask(ctx, definition.Creator, previous.TaskID, "replaced by schedule"); err != nil {
							return false, fmt.Errorf("replace scheduled Job %s: %w", previous.TaskID, err)
						}
					}
				}
			}
		}
	}
	if len(skipReason) > 255 {
		skipReason = skipReason[:255]
		for !utf8.ValidString(skipReason) {
			skipReason = skipReason[:len(skipReason)-1]
		}
	}
	queued := false
	err = artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		// Match submission's workspace-first lock order, including space deletion.
		locked := &model.Workspace{ID: scope.WorkspaceID}
		if err := tx.GetForUpdate(ctx, locked); err != nil {
			return err
		}
		if locked.Namespace != scope.Namespace {
			return bcode.ErrForbidden
		}
		claimed, err := tx.CompareAndSwapWithConditions(ctx, &model.JobDefinition{ID: definition.ID, WorkspaceID: scope.WorkspaceID},
			map[string]interface{}{"next_run": definition.NextRun, "schedule_enabled": true, "revision": definition.Revision},
			map[string]interface{}{"next_run": nextRun, "last_skip_reason": skipReason})
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil // Definition deleted between validation and claim.
		}
		if err != nil || !claimed {
			return err
		}
		if skipReason != "" {
			return nil
		}
		task.JobSpec = definition.Spec
		task.JobDefinitionID, task.JobDefinitionRevision = definition.ID, definition.Revision
		key := fmt.Sprintf("jobdef:%s:%d", definition.ID, definition.NextRun)
		task.IdempotencyKey = &key
		accepted, err := s.enqueue(ctx, tx, scope, task, 0)
		if err != nil {
			return err
		}
		updated, err := tx.CompareAndSwap(ctx, &model.JobDefinition{ID: definition.ID, WorkspaceID: scope.WorkspaceID}, "next_run", nextRun,
			map[string]interface{}{"last_run_task_id": accepted.TaskID})
		if err != nil {
			return err
		}
		if !updated {
			return fmt.Errorf("claimed Job definition disappeared before recording its run")
		}
		queued = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("claim Job schedule occurrence: %w", err)
	}
	return queued, nil
}

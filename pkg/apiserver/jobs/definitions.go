package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type ScheduleRequest struct {
	Cron              string `json:"cron"`
	ConcurrencyPolicy string `json:"concurrencyPolicy"`
	Enabled           bool   `json:"enabled"`
}

type DefinitionRequest struct {
	spec.JobSpec
	Schedule *ScheduleRequest `json:"schedule,omitempty"`
}

type Schedule struct {
	ScheduleRequest
	NextRun        int64  `json:"nextRun"`
	LastRunTaskID  string `json:"lastRunTaskId"`
	LastSkipReason string `json:"lastSkipReason"`
}

type Definition struct {
	ID         string       `json:"id"`
	Name       string       `json:"name"`
	Revision   uint64       `json:"revision"`
	Spec       spec.JobSpec `json:"spec"`
	Schedule   *Schedule    `json:"schedule,omitempty"`
	Creator    string       `json:"creator"`
	CreateTime time.Time    `json:"createTime"`
	UpdateTime time.Time    `json:"updateTime"`
}

type DefinitionPage struct {
	Items    []*Definition `json:"items"`
	Total    int64         `json:"total"`
	Page     int           `json:"page"`
	PageSize int           `json:"pageSize"`
}

type RunSummary struct {
	Accepted
	Name                  string    `json:"name"`
	JobDefinitionID       string    `json:"jobDefinitionId,omitempty"`
	JobDefinitionRevision uint64    `json:"jobDefinitionRevision,omitempty"`
	ExecuteAt             int64     `json:"executeAt"`
	Creator               string    `json:"creator"`
	CreateTime            time.Time `json:"createTime"`
	UpdateTime            time.Time `json:"updateTime"`
}

type RunPage struct {
	Items    []*RunSummary `json:"items"`
	Total    int64         `json:"total"`
	Page     int           `json:"page"`
	PageSize int           `json:"pageSize"`
}

func definitionResponse(record *model.JobDefinition) (*Definition, error) {
	out := &Definition{ID: record.ID, Name: record.Name, Revision: record.Revision, Creator: record.Creator, CreateTime: record.CreateTime, UpdateTime: record.UpdateTime}
	if err := spec.DecodeJobJSON([]byte(record.Spec), &out.Spec); err != nil {
		return nil, fmt.Errorf("read Job definition specification: %w", err)
	}
	if record.ConcurrencyPolicy != "" {
		out.Schedule = &Schedule{
			ScheduleRequest: ScheduleRequest{Cron: record.Cron, ConcurrencyPolicy: record.ConcurrencyPolicy, Enabled: record.ScheduleEnabled},
			NextRun:         record.NextRun, LastRunTaskID: record.LastRunTaskID, LastSkipReason: record.LastSkipReason,
		}
	}
	return out, nil
}

func prepareDefinitionSchedule(request *ScheduleRequest, now time.Time) (ScheduleRequest, int64, error) {
	if request == nil {
		return ScheduleRequest{}, 0, nil
	}
	schedule := *request
	if schedule.ConcurrencyPolicy == "" {
		schedule.ConcurrencyPolicy = "Forbid"
	}
	switch schedule.ConcurrencyPolicy {
	case "Forbid", "Allow", "Replace":
	default:
		return ScheduleRequest{}, 0, invalid(fmt.Errorf("schedule concurrencyPolicy must be Forbid, Allow, or Replace"))
	}
	schedule.Cron = strings.TrimSpace(schedule.Cron)
	if schedule.Cron == "" {
		if schedule.Enabled {
			return ScheduleRequest{}, 0, invalid(fmt.Errorf("schedule cron is required when enabled"))
		}
		return schedule, 0, nil
	}
	if len(strings.Fields(schedule.Cron)) != 5 {
		return ScheduleRequest{}, 0, invalid(fmt.Errorf("schedule cron must have 5 fields"))
	}
	cron, err := utils.NormalizeCronSchedule(schedule.Cron)
	if err != nil {
		return ScheduleRequest{}, 0, invalid(fmt.Errorf("schedule cron: %w", err))
	}
	if len(cron) > 64 {
		return ScheduleRequest{}, 0, invalid(fmt.Errorf("schedule cron exceeds 64 bytes"))
	}
	nextRun, err := workflow.NextScheduleRun(cron, now)
	if err != nil {
		return ScheduleRequest{}, 0, invalid(fmt.Errorf("schedule cron: %w", err))
	}
	schedule.Cron = cron
	if !schedule.Enabled {
		nextRun = 0
	}
	return schedule, nextRun, nil
}

// scopedDefinition also checks raw backends used without account.Store. Locked
// reads use the transaction handle so authorization and mutation share the row.
func scopedDefinition(ctx context.Context, store artifacts.Backend, id string, lock bool) (*model.JobDefinition, error) {
	scope, err := Scope(ctx, false)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, bcode.ErrNotFound
	}
	record := &model.JobDefinition{ID: id}
	if lock {
		err = store.GetForUpdate(ctx, record)
	} else {
		err = store.Get(ctx, record)
	}
	if err != nil {
		return nil, err
	}
	if record.WorkspaceID != scope.WorkspaceID {
		return nil, bcode.ErrNotFound
	}
	return record, nil
}

func (s *Service) CreateDefinition(ctx context.Context, declaration DefinitionRequest) (*Definition, error) {
	scope, err := Scope(ctx, true)
	if err != nil {
		return nil, err
	}
	schedule, nextRun, err := prepareDefinitionSchedule(declaration.Schedule, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	task, err := s.prepareTask(ctx, scope, declaration.JobSpec)
	if err != nil {
		return nil, err
	}
	record := &model.JobDefinition{ID: task.TaskID, WorkspaceID: scope.WorkspaceID, Name: task.WorkflowName, Revision: 1, Spec: task.JobSpec, Creator: scope.UserID}
	record.Cron, record.ConcurrencyPolicy, record.ScheduleEnabled, record.NextRun = schedule.Cron, schedule.ConcurrencyPolicy, schedule.Enabled, nextRun
	err = artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		space := &model.Workspace{ID: scope.WorkspaceID}
		if err := tx.GetForUpdate(ctx, space); err != nil {
			return err
		}
		if space.Namespace != scope.Namespace {
			return bcode.ErrForbidden
		}
		return tx.Add(ctx, record)
	})
	if errors.Is(err, datastore.ErrRecordExist) {
		return nil, bcode.ErrJobDefinitionConflict
	}
	if err != nil {
		return nil, fmt.Errorf("create Job definition: %w", err)
	}
	return definitionResponse(record)
}

func (s *Service) GetDefinition(ctx context.Context, id string) (*Definition, error) {
	record, err := scopedDefinition(ctx, s.Store, id, false)
	if err != nil {
		return nil, err
	}
	return definitionResponse(record)
}

func (s *Service) ListDefinitions(ctx context.Context, page, pageSize int) (*DefinitionPage, error) {
	scope, err := Scope(ctx, false)
	if err != nil {
		return nil, err
	}
	if page < 1 || page > 10000000 || pageSize < 1 || pageSize > 100 {
		return nil, invalid(fmt.Errorf("page must be 1..10000000 and pageSize must be 1..100"))
	}
	query := &model.JobDefinition{WorkspaceID: scope.WorkspaceID}
	total, err := s.Store.Count(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("count Job definitions: %w", err)
	}
	rows, err := s.Store.List(ctx, query, &datastore.ListOptions{Page: page, PageSize: pageSize, SortBy: []datastore.SortOption{{Key: "create_time", Order: datastore.SortOrderDescending}, {Key: "id", Order: datastore.SortOrderDescending}}})
	if err != nil {
		return nil, fmt.Errorf("list Job definitions: %w", err)
	}
	out := &DefinitionPage{Items: make([]*Definition, 0, len(rows)), Total: total, Page: page, PageSize: pageSize}
	for _, row := range rows {
		item, err := definitionResponse(row.(*model.JobDefinition))
		if err != nil {
			return nil, err
		}
		out.Items = append(out.Items, item)
	}
	return out, nil
}

func (s *Service) UpdateDefinition(ctx context.Context, id string, declaration DefinitionRequest) (*Definition, error) {
	scope, err := Scope(ctx, true)
	if err != nil {
		return nil, err
	}
	if _, err := scopedDefinition(ctx, s.Store, id, false); err != nil {
		return nil, err
	}
	// Validation can read workspace policy and artifacts. Complete it before
	// opening the write transaction, then lock the current revision for update.
	schedule, nextRun, err := prepareDefinitionSchedule(declaration.Schedule, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	task, err := s.prepareTask(ctx, scope, declaration.JobSpec)
	if err != nil {
		return nil, err
	}
	var record *model.JobDefinition
	err = artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		var err error
		record, err = scopedDefinition(ctx, tx, id, true)
		if err != nil {
			return err
		}
		if record.Revision == ^uint64(0) {
			return fmt.Errorf("Job definition revision is exhausted")
		}
		record.Name, record.Spec = task.WorkflowName, task.JobSpec
		if record.Cron != schedule.Cron || record.ScheduleEnabled != schedule.Enabled {
			record.NextRun = nextRun
		}
		record.Cron, record.ConcurrencyPolicy, record.ScheduleEnabled = schedule.Cron, schedule.ConcurrencyPolicy, schedule.Enabled
		if declaration.Schedule == nil {
			// Keep LastRunTaskID so a later Forbid or Replace still sees an active run.
			record.NextRun, record.LastSkipReason = 0, ""
		}
		record.Revision++
		// Put uses struct updates, which omit false, zero, and empty strings.
		// Persist the full replacement so disabling or removing a schedule is durable.
		updated, err := tx.CompareAndSwap(ctx, record, "revision", record.Revision-1, map[string]interface{}{
			"name": record.Name, "spec": record.Spec, "revision": record.Revision,
			"cron": record.Cron, "concurrency_policy": record.ConcurrencyPolicy,
			"schedule_enabled": record.ScheduleEnabled, "next_run": record.NextRun,
			"last_run_task_id": record.LastRunTaskID, "last_skip_reason": record.LastSkipReason,
		})
		if err != nil {
			return err
		}
		if !updated {
			return fmt.Errorf("Job definition revision changed while locked")
		}
		return nil
	})
	if errors.Is(err, datastore.ErrRecordExist) {
		return nil, bcode.ErrJobDefinitionConflict
	}
	if err != nil {
		return nil, fmt.Errorf("update Job definition: %w", err)
	}
	return definitionResponse(record)
}

func (s *Service) DeleteDefinition(ctx context.Context, id string) error {
	if _, err := Scope(ctx, true); err != nil {
		return err
	}
	err := artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		record, err := scopedDefinition(ctx, tx, id, true)
		if err != nil {
			return err
		}
		// Runs own their immutable snapshot and keep this ID for history and
		// filtering. Deleting a definition never updates or deletes its runs.
		return tx.Delete(ctx, record)
	})
	if err != nil {
		return fmt.Errorf("delete Job definition: %w", err)
	}
	return nil
}

func (s *Service) Run(ctx context.Context, id string, executeAt int64) (*Accepted, error) {
	scope, err := Scope(ctx, true)
	if err != nil {
		return nil, err
	}
	record, err := scopedDefinition(ctx, s.Store, id, false)
	if err != nil {
		return nil, err
	}
	declaration, err := definitionResponse(record)
	if err != nil {
		return nil, err
	}
	task, err := s.prepareTask(ctx, scope, declaration.Spec)
	if err != nil {
		return nil, err
	}
	task.JobSpec = record.Spec
	task.JobDefinitionID, task.JobDefinitionRevision = record.ID, record.Revision
	return s.enqueue(ctx, nil, scope, task, executeAt)
}

func (s *Service) List(ctx context.Context, definitionID string, page, pageSize int) (*RunPage, error) {
	scope, err := Scope(ctx, false)
	if err != nil {
		return nil, err
	}
	if page < 1 || page > 10000000 || pageSize < 1 || pageSize > 100 {
		return nil, invalid(fmt.Errorf("page must be 1..10000000 and pageSize must be 1..100"))
	}
	query := &model.WorkflowQueue{WorkspaceID: scope.WorkspaceID, Type: config.WorkflowTaskTypeJob}
	options := &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{
			{Key: "type", Values: []string{string(config.WorkflowTaskTypeJob)}},
			{Key: "app_id", Values: []string{""}},
		}},
		Page: page, PageSize: pageSize,
		SortBy: []datastore.SortOption{{Key: "create_time", Order: datastore.SortOrderDescending}, {Key: "task_id", Order: datastore.SortOrderDescending}},
	}
	if definitionID != "" {
		options.In = append(options.In, datastore.InQueryOption{Key: "job_definition_id", Values: []string{definitionID}})
	}
	total, err := s.Store.Count(ctx, query, &options.FilterOptions)
	if err != nil {
		return nil, fmt.Errorf("count Job runs: %w", err)
	}
	rows, err := s.Store.List(ctx, query, options)
	if err != nil {
		return nil, fmt.Errorf("list Job runs: %w", err)
	}
	out := &RunPage{Items: make([]*RunSummary, 0, len(rows)), Total: total, Page: page, PageSize: pageSize}
	for _, row := range rows {
		task := row.(*model.WorkflowQueue)
		out.Items = append(out.Items, &RunSummary{
			Accepted: Accepted{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Type: string(task.Type), Status: task.Status},
			Name:     task.WorkflowName, JobDefinitionID: task.JobDefinitionID, JobDefinitionRevision: task.JobDefinitionRevision,
			ExecuteAt: task.ExecuteAt, Creator: task.TaskCreator, CreateTime: task.CreateTime, UpdateTime: task.UpdateTime,
		})
	}
	return out, nil
}

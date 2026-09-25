package account

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// Store applies workspace predicates before pagination and validates writes at
// the shared persistence boundary. Trusted runtime contexts still explicitly
// obtain their scope from the persisted application or workspace task before executing jobs.
type Store struct{ raw datastore.DataStore }

func NewStore(raw datastore.DataStore) *Store { return &Store{raw: raw} }

// CountStartingSandboxes is a narrow internal aggregate for the shared start
// budget. It exposes no cross-workspace identities or records. The reservation
// caller must first authorize its own Sandbox within the same transaction.
func (s *Store) CountStartingSandboxes(ctx context.Context) (int64, error) {
	return s.raw.Count(ctx, &model.JobSandbox{StartReserved: true}, nil)
}

func artifactWorkspace(e datastore.Entity) (string, bool) {
	switch value := e.(type) {
	case *model.JobArtifact:
		return value.WorkspaceID, true
	case *model.ArtifactChunk:
		return value.WorkspaceID, true
	case *model.JobDelivery:
		return value.WorkspaceID, true
	case *model.JobSandbox:
		return value.WorkspaceID, true
	case *model.JobCheckpoint:
		return value.WorkspaceID, true
	default:
		return "", false
	}
}

func runtimeEntity(e datastore.Entity) (appID string, application bool, scoped bool) {
	switch v := e.(type) {
	case *model.Applications:
		return v.ID, true, true
	case *model.ApplicationComponent:
		return v.AppID, false, true
	case *model.Workflow:
		return v.AppID, false, true
	case *model.WorkflowQueue:
		return v.AppID, false, true
	case *model.WorkflowSchedule:
		return v.AppID, false, true
	case *model.JobInfo:
		return v.AppID, false, true
	}
	return "", false, false
}

func (s *Store) Check(ctx context.Context, e datastore.Entity) error {
	scope, ok := FromContext(ctx)
	if !ok {
		return nil
	}
	if workspaceID, scoped := artifactWorkspace(e); scoped {
		if workspaceID == "" || workspaceID != scope.WorkspaceID {
			return bcode.ErrForbidden
		}
		return nil
	}
	id, isApp, scoped := runtimeEntity(e)
	if !scoped {
		return nil
	}
	if isApp {
		app := e.(*model.Applications)
		if app.WorkspaceID != scope.WorkspaceID || app.Namespace != scope.Namespace {
			return bcode.ErrForbidden
		}
		return nil
	}
	switch v := e.(type) {
	case *model.ApplicationComponent:
		if v.Namespace != "" && v.Namespace != scope.Namespace {
			return bcode.ErrForbidden
		}
	case *model.Workflow:
		if v.Namespace != "" && v.Namespace != scope.Namespace {
			return bcode.ErrForbidden
		}
	case *model.WorkflowQueue:
		if v.AppID == "" {
			if v.WorkspaceID != scope.WorkspaceID || !workspaceOwnedTask(v) {
				return bcode.ErrForbidden
			}
			return nil
		}
	case *model.JobInfo:
		if v.AppID == "" && v.TaskID != "" && v.WorkspaceID == scope.WorkspaceID {
			return s.checkWorkspaceJob(ctx, v, scope)
		}
	}
	if id == "" {
		return bcode.ErrForbidden
	}
	app := &model.Applications{ID: id}
	if err := s.raw.Get(ctx, app); err != nil {
		return err
	}
	return s.Check(ctx, app)
}

// workspaceTaskJobType binds an app-less execution record to the type accepted
// in its durable parent snapshot. Runtime or request fields cannot widen this.
func workspaceTaskJobType(task *model.WorkflowQueue) config.JobType {
	if task == nil || task.AppID != "" {
		return ""
	}
	switch task.Type {
	case config.WorkflowTaskTypeResourceImportScan:
		return config.JobResourceImportScan
	case config.WorkflowTaskTypeResourceImportManage:
		return config.JobResourceImportManage
	case config.WorkflowTaskTypeJob:
		var snapshot struct {
			Type   config.JobType `json:"type"`
			Traits spec.JobTraits `json:"traits"`
		}
		if json.Unmarshal([]byte(task.JobSpec), &snapshot) == nil {
			if snapshot.Type == config.JobCommand && snapshot.Traits.Evaluation == nil {
				return config.JobCommand
			}
			if snapshot.Type == config.InstantJob && snapshot.Traits.Evaluation != nil && snapshot.Traits.Evaluation.Normalize() == nil {
				return config.JobEval
			}
		}
	}
	return ""
}

func workspaceOwnedTask(task *model.WorkflowQueue) bool {
	return workspaceTaskJobType(task) != ""
}

func (s *Store) checkWorkspaceJob(ctx context.Context, v *model.JobInfo, scope Scope) error {
	task := &model.WorkflowQueue{TaskID: v.TaskID}
	if err := s.raw.Get(ctx, task); err != nil {
		return err
	}
	if task.AppID != "" || task.WorkspaceID != scope.WorkspaceID || !workspaceOwnedTask(task) {
		return bcode.ErrForbidden
	}
	expectedJobType := workspaceTaskJobType(task)
	if v.Type != "" && v.Type != string(expectedJobType) {
		return bcode.ErrForbidden
	}
	return nil
}

func (s *Store) options(ctx context.Context, e datastore.Entity, input *datastore.ListOptions) (*datastore.ListOptions, error) {
	opts := datastore.ListOptions{}
	if input != nil {
		opts = *input
		opts.In = append([]datastore.InQueryOption(nil), input.In...)
	}
	scope, ok := FromContext(ctx)
	if !ok {
		return &opts, nil
	}
	if workspaceID, scoped := artifactWorkspace(e); scoped {
		if workspaceID != "" && workspaceID != scope.WorkspaceID {
			return nil, bcode.ErrForbidden
		}
		opts.In = append(opts.In, datastore.InQueryOption{Key: "workspace_id", Values: []string{scope.WorkspaceID}})
		return &opts, nil
	}
	appID, isApp, scoped := runtimeEntity(e)
	if !scoped {
		return &opts, nil
	}
	if isApp {
		opts.In = append(opts.In, datastore.InQueryOption{Key: "workspaceid", Values: []string{scope.WorkspaceID}})
		return &opts, nil
	}
	if job, ok := e.(*model.JobInfo); ok && job.AppID == "" && job.TaskID != "" {
		query := *job
		if query.WorkspaceID == "" {
			parent := &model.WorkflowQueue{TaskID: query.TaskID}
			if s.raw.Get(ctx, parent) == nil && workspaceOwnedTask(parent) {
				query.WorkspaceID = parent.WorkspaceID
			}
		}
		if query.WorkspaceID != "" {
			if err := s.Check(ctx, &query); err != nil {
				return nil, err
			}
			opts.In = append(opts.In, datastore.InQueryOption{Key: "workspace_id", Values: []string{scope.WorkspaceID}})
			return &opts, nil
		}
	}
	if appID != "" {
		if err := s.Check(ctx, e); err != nil {
			return nil, err
		}
		return &opts, nil
	}
	apps, err := s.raw.List(ctx, &model.Applications{WorkspaceID: scope.WorkspaceID}, &datastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(apps))
	for _, a := range apps {
		ids = append(ids, a.(*model.Applications).ID)
	}
	// A nonempty impossible ID preserves fail-closed behavior in datastores which
	// otherwise skip an empty IN predicate.
	if len(ids) == 0 {
		ids = []string{"!no-workspace-applications"}
	}
	opts.In = append(opts.In, datastore.InQueryOption{Key: "app_id", Values: ids})
	return &opts, nil
}

func (s *Store) Get(ctx context.Context, e datastore.Entity) error {
	copy, err := datastore.NewEntity(e)
	if err != nil {
		return err
	}
	reflect.ValueOf(copy).Elem().Set(reflect.ValueOf(e).Elem())
	if err = s.raw.Get(ctx, copy); err != nil {
		return err
	}
	if err = s.Check(ctx, copy); err != nil {
		return err
	}
	reflect.ValueOf(e).Elem().Set(reflect.ValueOf(copy).Elem())
	return nil
}
func (s *Store) List(ctx context.Context, e datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	o, err := s.options(ctx, e, opts)
	if err != nil {
		return nil, err
	}
	return s.raw.List(ctx, e, o)
}
func (s *Store) Count(ctx context.Context, e datastore.Entity, opts *datastore.FilterOptions) (int64, error) {
	in := &datastore.ListOptions{}
	if opts != nil {
		in.FilterOptions = *opts
	}
	o, err := s.options(ctx, e, in)
	if err != nil {
		return 0, err
	}
	return s.raw.Count(ctx, e, &o.FilterOptions)
}
func (s *Store) IsExist(ctx context.Context, e datastore.Entity) (bool, error) {
	n, err := s.Count(ctx, e, nil)
	return n > 0, err
}
func (s *Store) IsExistByCondition(ctx context.Context, table string, cond map[string]interface{}, dest interface{}) (bool, error) {
	e, ok := dest.(datastore.Entity)
	if !ok {
		if _, scoped := FromContext(ctx); scoped {
			return false, fmt.Errorf("scoped condition query requires an entity")
		}
		return s.raw.IsExistByCondition(ctx, table, cond, dest)
	}
	opts, err := s.options(ctx, e, nil)
	if err != nil {
		return false, err
	}
	conditions := map[string]interface{}{}
	for k, v := range cond {
		conditions[k] = v
	}
	for _, v := range opts.In {
		if prior, exists := conditions[v.Key]; exists {
			found := false
			for _, allowed := range v.Values {
				if fmt.Sprint(prior) == allowed {
					found = true
				}
			}
			if !found {
				return false, nil
			}
		} else {
			conditions[v.Key] = v.Values
		}
	}
	return s.raw.IsExistByCondition(ctx, table, conditions, dest)
}

func (s *Store) Add(ctx context.Context, e datastore.Entity) error {
	if err := s.Check(ctx, e); err != nil {
		return err
	}
	if a, ok := e.(*model.Applications); ok {
		if _, scoped := FromContext(ctx); scoped {
			// Serialize with team deletion and membership changes. All application
			// creation paths already execute within the datastore transaction.
			locker, ok := s.raw.(datastore.RowLocker)
			if !ok {
				return fmt.Errorf("workspace application creation requires row locking")
			}
			if err := locker.GetForUpdate(ctx, &model.Workspace{ID: a.WorkspaceID}); err != nil {
				return err
			}
		}
	}
	return s.raw.Add(ctx, e)
}

func (s *Store) GetForUpdate(ctx context.Context, e datastore.Entity) error {
	locker, ok := s.raw.(datastore.RowLocker)
	if !ok {
		return fmt.Errorf("datastore does not support row locking")
	}
	copy, err := datastore.NewEntity(e)
	if err != nil {
		return err
	}
	reflect.ValueOf(copy).Elem().Set(reflect.ValueOf(e).Elem())
	if err := locker.GetForUpdate(ctx, copy); err != nil {
		return err
	}
	if err := s.Check(ctx, copy); err != nil {
		return err
	}
	reflect.ValueOf(e).Elem().Set(reflect.ValueOf(copy).Elem())
	return nil
}
func (s *Store) BatchAdd(ctx context.Context, entities []datastore.Entity) error {
	return s.WithTransaction(ctx, func(tx datastore.DataStore) error {
		for _, e := range entities {
			if err := tx.Add(ctx, e); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) checkExisting(ctx context.Context, e datastore.Entity) error {
	if _, ok := FromContext(ctx); !ok {
		return nil
	}
	_, _, scoped := runtimeEntity(e)
	_, artifactScoped := artifactWorkspace(e)
	if !scoped && !artifactScoped {
		return nil
	}
	copy, err := datastore.NewEntity(e)
	if err != nil {
		return err
	}
	reflect.ValueOf(copy).Elem().Set(reflect.ValueOf(e).Elem())
	return s.Get(ctx, copy)
}
func (s *Store) Put(ctx context.Context, e datastore.Entity) error {
	if err := s.checkExisting(ctx, e); err != nil {
		return err
	}
	if err := s.Check(ctx, e); err != nil {
		return err
	}
	return s.raw.Put(ctx, e)
}
func (s *Store) Delete(ctx context.Context, e datastore.Entity) error {
	if err := s.checkExisting(ctx, e); err != nil {
		return err
	}
	return s.raw.Delete(ctx, e)
}
func (s *Store) DeleteByFilter(ctx context.Context, e datastore.Entity, opts *datastore.FilterOptions) error {
	in := &datastore.ListOptions{}
	if opts != nil {
		in.FilterOptions = *opts
	}
	o, err := s.options(ctx, e, in)
	if err != nil {
		return err
	}
	return s.raw.DeleteByFilter(ctx, e, &o.FilterOptions)
}
func (s *Store) CompareAndSwap(ctx context.Context, e datastore.Entity, k string, v interface{}, updates map[string]interface{}) (bool, error) {
	return s.CompareAndSwapWithConditions(ctx, e, map[string]interface{}{k: v}, updates)
}
func (s *Store) CompareAndSwapWithConditions(ctx context.Context, e datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if err := s.checkExisting(ctx, e); err != nil {
		return false, err
	}
	if scope, ok := FromContext(ctx); ok {
		_, artifactScoped := artifactWorkspace(e)
		if _, _, scoped := runtimeEntity(e); scoped || artifactScoped {
			for _, key := range []string{"workspaceid", "workspace_id", "namespace", "app_id"} {
				if value, exists := updates[key]; exists {
					switch key {
					case "workspaceid":
						if value != scope.WorkspaceID {
							return false, bcode.ErrForbidden
						}
					case "workspace_id":
						if value != scope.WorkspaceID {
							return false, bcode.ErrForbidden
						}
					case "namespace":
						if value != scope.Namespace {
							return false, bcode.ErrForbidden
						}
					case "app_id":
						if value == "" {
							switch entity := e.(type) {
							case *model.JobInfo:
								if entity.AppID == "" && s.Check(ctx, entity) == nil {
									if changed, ok := updates["task_id"]; ok && changed != entity.TaskID {
										return false, bcode.ErrForbidden
									}
									if changed, ok := updates["type"]; ok && changed != entity.Type {
										return false, bcode.ErrForbidden
									}
									continue
								}
							case *model.WorkflowQueue:
								if workspaceOwnedTask(entity) && s.Check(ctx, entity) == nil {
									continue
								}
							}
						}
						if err := s.Check(ctx, &model.Workflow{AppID: fmt.Sprint(value)}); err != nil {
							return false, err
						}
					}
				}
			}
		}
	}
	cas, ok := s.raw.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return false, fmt.Errorf("scoped datastore requires conditional updates")
	}
	return cas.CompareAndSwapWithConditions(ctx, e, conditions, updates)
}
func (s *Store) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	tx, ok := s.raw.(datastore.Transactional)
	if !ok {
		return fmt.Errorf("scoped datastore requires transactions")
	}
	return tx.WithTransaction(ctx, func(raw datastore.DataStore) error { return fn(NewStore(raw)) })
}
func (s *Store) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	tx, ok := s.raw.(datastore.ReadCommittedTransactional)
	if !ok {
		return fmt.Errorf("scoped datastore requires read-committed transactions")
	}
	return tx.WithReadCommittedTransaction(ctx, func(raw datastore.DataStore) error { return fn(NewStore(raw)) })
}
func (s *Store) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	clock, ok := s.raw.(datastore.DatabaseClock)
	if !ok {
		return time.Time{}, fmt.Errorf("datastore clock unavailable")
	}
	return clock.CurrentDatabaseTime(ctx)
}

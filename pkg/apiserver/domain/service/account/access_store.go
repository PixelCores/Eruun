package account

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// Store applies workspace predicates before pagination and validates writes at
// the shared persistence boundary. Trusted runtime contexts still explicitly
// obtain their scope from the persisted application before executing jobs.
type Store struct{ raw datastore.DataStore }

func NewStore(raw datastore.DataStore) *Store { return &Store{raw: raw} }

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
			if v.WorkspaceID != scope.WorkspaceID ||
				(v.Type != config.WorkflowTaskTypeResourceImportScan && v.Type != config.WorkflowTaskTypeResourceImportManage) {
				return bcode.ErrForbidden
			}
			return nil
		}
	case *model.JobInfo:
		if v.AppID == "" && v.TaskID != "" && v.WorkspaceID == scope.WorkspaceID {
			task := &model.WorkflowQueue{TaskID: v.TaskID}
			if err := s.raw.Get(ctx, task); err != nil {
				return err
			}
			if task.AppID != "" || task.WorkspaceID != scope.WorkspaceID ||
				(task.Type != config.WorkflowTaskTypeResourceImportScan && task.Type != config.WorkflowTaskTypeResourceImportManage) {
				return bcode.ErrForbidden
			}
			expectedJobType := config.JobResourceImportScan
			if task.Type == config.WorkflowTaskTypeResourceImportManage {
				expectedJobType = config.JobResourceImportManage
			}
			if v.Type != "" && v.Type != string(expectedJobType) {
				return bcode.ErrForbidden
			}
			return nil
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
	appID, isApp, scoped := runtimeEntity(e)
	if !scoped {
		return &opts, nil
	}
	if isApp {
		opts.In = append(opts.In, datastore.InQueryOption{Key: "workspaceid", Values: []string{scope.WorkspaceID}})
		return &opts, nil
	}
	if job, ok := e.(*model.JobInfo); ok && job.AppID == "" && job.TaskID != "" && job.WorkspaceID == scope.WorkspaceID {
		if err := s.Check(ctx, job); err != nil {
			return nil, err
		}
		return &opts, nil
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
	if !scoped {
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
		if _, _, scoped := runtimeEntity(e); scoped {
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
func (s *Store) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	clock, ok := s.raw.(datastore.DatabaseClock)
	if !ok {
		return time.Time{}, fmt.Errorf("datastore clock unavailable")
	}
	return clock.CurrentDatabaseTime(ctx)
}

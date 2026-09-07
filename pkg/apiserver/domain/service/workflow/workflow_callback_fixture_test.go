package workflow

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

// Callback contract tests admit immediately. The separate SQL/HTTP integration
// tests verify the real global scheduler and its waiting/ownership boundaries.
// Keep this adapter local to callback fixtures so cancellation rollback tests
// retain the original datastore capabilities and fault injection.
type callbackAdmissionTestStore struct {
	*statusDataStore
	mu   sync.Mutex
	jobs *sqlstore.Driver
}

func withImmediateCallbackAdmission(t testing.TB, svc *workflowServiceImpl) *workflowServiceImpl {
	withAllowPrivateURLPolicy(t, svc)
	wrapCallbackAdmissionFixture(t, svc)
	return svc
}

func wrapCallbackAdmissionFixture(t testing.TB, svc *workflowServiceImpl) {
	raw, ok := svc.Store.(*statusDataStore)
	if !ok {
		return
	}
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "callbacks.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(&model.JobInfo{}))
	if raw.task != nil {
		setCallbackFixtureApp(raw, raw.task.AppID)
	}
	wrapper := &callbackAdmissionTestStore{statusDataStore: raw, jobs: &sqlstore.Driver{Client: *db}}
	svc.Store = wrapper
	t.Cleanup(func() {
		require.Eventually(t, func() bool {
			wrapper.mu.Lock()
			defer wrapper.mu.Unlock()
			var active int64
			return db.Model(&model.JobInfo{}).Where("scheduling_state IN ?", []string{"queued", "admitted"}).Count(&active).Error == nil && active == 0
		}, 2*time.Second, time.Millisecond, "wait for callback persistence and admission release before closing its store")
	})
}

func setCallbackFixtureApp(store *statusDataStore, appID string) {
	if store.app == nil {
		store.app = &model.Applications{ID: appID}
	}
	store.app.WorkspaceID = "callback-space"
	store.app.Namespace = "callback-space"
}

func prepareTerminalCallbackFixture(t testing.TB, svc *workflowServiceImpl, store *statusDataStore, task *model.WorkflowQueue, status config.Status) {
	cp := *task
	cp.Status = status
	store.task = &cp
	setCallbackFixtureApp(store, task.AppID)
	wrapCallbackAdmissionFixture(t, svc)
}

func (s *callbackAdmissionTestStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}
func (s *callbackAdmissionTestStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}
func (s *callbackAdmissionTestStore) CurrentDatabaseTime(context.Context) (time.Time, error) {
	return time.Now().UTC(), nil
}
func (s *callbackAdmissionTestStore) Add(ctx context.Context, e datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := e.(*model.JobInfo); ok {
		if err := s.jobs.Add(ctx, e); err != nil {
			return err
		}
	}
	return s.statusDataStore.Add(ctx, e)
}
func (s *callbackAdmissionTestStore) Get(ctx context.Context, e datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := e.(*model.JobInfo); ok {
		return s.jobs.Get(ctx, e)
	}
	return s.statusDataStore.Get(ctx, e)
}
func (s *callbackAdmissionTestStore) List(ctx context.Context, e datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := e.(*model.JobInfo); ok {
		return s.jobs.List(ctx, e, opts)
	}
	return s.statusDataStore.List(ctx, e, opts)
}
func (s *callbackAdmissionTestStore) CompareAndSwap(ctx context.Context, e datastore.Entity, key string, value interface{}, updates map[string]interface{}) (bool, error) {
	return s.CompareAndSwapWithConditions(ctx, e, map[string]interface{}{key: value}, updates)
}
func (s *callbackAdmissionTestStore) CompareAndSwapWithConditions(ctx context.Context, e datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := e.(*model.JobInfo); ok {
		updated, err := s.jobs.CompareAndSwapWithConditions(ctx, e, conditions, updates)
		if err != nil || !updated {
			return updated, err
		}
		latest := &model.JobInfo{ID: job.ID}
		if err := s.jobs.Get(ctx, latest); err != nil {
			return false, err
		}
		if latest.SchedulingState == "queued" {
			if _, err := s.jobs.CompareAndSwap(ctx, latest, "scheduling_state", "queued", map[string]interface{}{"scheduling_state": "admitted"}); err != nil {
				return false, err
			}
			latest.SchedulingState = "admitted"
		}
		if existing := s.findJobInfo(latest); existing != nil {
			*existing = *latest
		}
		return true, nil
	}
	copyConditions := make(map[string]interface{}, len(conditions))
	for key, value := range conditions {
		switch key {
		case "run_generation":
			if s.task == nil || s.task.RunGeneration != value {
				return false, nil
			}
		case "run_token":
			if s.task == nil || s.task.RunToken != value {
				return false, nil
			}
		case "worker_id":
			if s.task == nil || s.task.WorkerID != value {
				return false, nil
			}
		default:
			copyConditions[key] = value
		}
	}
	return s.statusDataStore.CompareAndSwapWithConditions(ctx, e, copyConditions, updates)
}

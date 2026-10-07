//go:build integration

package application

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	mysqldsn "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestDirectVersionUpdateMySQLApplicationLock(t *testing.T) {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN is required for isolated application scheduling tests")
	}
	parsed, err := mysqldsn.ParseDSN(dsn)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(parsed.DBName, "eruun_scheduler_test"), "requires an isolated test schema")
	parsed.ClientFoundRows = true
	db, err := gorm.Open(mysqlgorm.Open(parsed.FormatDSN()), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	entities := []any{&model.Applications{}, &model.ApplicationComponent{}, &model.Workflow{}, &model.WorkflowQueue{}, &model.JobInfo{}}
	for _, entity := range entities {
		require.False(t, db.Migrator().HasTable(entity), "test schema must be empty")
	}
	require.NoError(t, db.AutoMigrate(entities...))
	t.Cleanup(func() { require.NoError(t, db.Migrator().DropTable(entities...)) })
	store := &sqlstore.Driver{Client: *db}

	t.Run("direct and auto use the same lock order", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		app, components, workflow := seedTransactionApp(t, store)
		// The Redis critical sections overlap after lease loss. Exercise both
		// production commit paths against independent database connections.
		ctx = context.WithValue(ctx, applicationMutationLockContextKey{}, app.ID)
		autoApp := *app
		autoComponents := make(map[string]*model.ApplicationComponent, len(components))
		for name, component := range components {
			copy := *component
			autoComponents[name] = &copy
		}
		beforeApp, resume, autoComponentWrite := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
		defer func() {
			select {
			case <-resume:
			default:
				close(resume)
			}
		}()
		directStore := &transactionFaultStore{DataStore: store, beforeWrite: func(entity datastore.Entity) error {
			if _, ok := entity.(*model.Applications); ok {
				close(beforeApp)
				select {
				case <-resume:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}}
		autoStore := &transactionFaultStore{DataStore: store, beforeWrite: func(entity datastore.Entity) error {
			if _, ok := entity.(*model.ApplicationComponent); ok {
				autoComponentWrite <- struct{}{}
			}
			return nil
		}}
		direct := &applicationsServiceImpl{Store: directStore}
		auto := &applicationsServiceImpl{Store: autoStore}
		req := apis.UpdateVersionRequest{Version: "2.0.0", Components: []apis.ComponentUpdateSpec{{Name: "api", Image: "api:direct"}}}
		directDone, autoDone := make(chan error, 1), make(chan error, 1)
		go func() {
			directDone <- direct.commitDirectVersionUpdate(ctx, &versionUpdateRun{app: app, normalReq: req, componentMap: components, newVersion: req.Version})
		}()
		select {
		case <-beforeApp:
		case err := <-directDone:
			t.Fatalf("direct mutation did not reach application write: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		go func() {
			_, _, _, _, _, err := auto.commitAutoExecVersionUpdate(ctx, &autoApp, autoComponents,
				apis.UpdateVersionRequest{Components: []apis.ComponentUpdateSpec{{Name: "api", Image: "api:auto"}}}, "3.0.0", workflow, 0, nil, versionUpdateResourceActions{}, nil, spec.VersionUpdateExecutionScope(""))
			autoDone <- err
		}()
		earlyComponentWrite := false
		select {
		case <-autoComponentWrite:
			earlyComponentWrite = true
		case <-time.After(150 * time.Millisecond):
		}
		close(resume)
		directErr, autoErr := <-directDone, <-autoDone
		require.False(t, earlyComponentWrite, "auto submission must wait on the application before touching the component held by direct update")
		require.NoError(t, directErr)
		require.ErrorIs(t, autoErr, bcode.ErrVersionUpdateConflict)
		persisted := &model.Applications{ID: app.ID}
		require.NoError(t, store.Get(ctx, persisted))
		require.Equal(t, "2.0.0", persisted.Version)
	})
	t.Run("stale preflight is rejected", func(t *testing.T) {
		testVersionUpdateRejectsStalePreflight(t, func(*testing.T) *sqlstore.Driver { return store })
	})
	t.Run("runtime observations remain current", func(t *testing.T) {
		testVersionUpdateAcceptsRuntimeSnapshotChanges(t, func(*testing.T) *sqlstore.Driver { return store })
	})
	t.Run("idle policy is rechecked", func(t *testing.T) {
		testDirectVersionUpdateRechecksIdleAtCommit(t, func(*testing.T) *sqlstore.Driver { return store })
	})
}

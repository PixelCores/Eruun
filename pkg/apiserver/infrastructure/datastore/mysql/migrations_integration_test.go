//go:build integration

package mysql

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysqldsn "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

func TestSchemaMigrationIntegrationInvalidatesFailedAttemptAndCompletesRetry(t *testing.T) {
	db := integrationMigrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, db.Migrator().DropTable(&model.SystemSetting{}))
	t.Cleanup(func() { require.NoError(t, db.Migrator().DropTable(&model.SystemSetting{})) })
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}))
	require.NoError(t, writeSchemaMigrationMarker(ctx, db))
	require.NoError(t, validateSchemaMigrationMarker(ctx, db))

	migrationErr := errors.New("injected migration failure")
	err := runSchemaMigration(ctx, db, func() error { return migrationErr })
	require.ErrorIs(t, err, migrationErr)
	require.ErrorContains(t, validateSchemaMigrationMarker(ctx, db), "is incomplete")

	require.NoError(t, runSchemaMigration(ctx, db, func() error {
		return db.WithContext(ctx).Exec("CREATE TABLE migration_probe (id BIGINT PRIMARY KEY, legacy_value TEXT)").Error
	}))
	t.Cleanup(func() { require.NoError(t, db.Exec("DROP TABLE IF EXISTS migration_probe").Error) })
	require.NoError(t, validateSchemaMigrationMarker(ctx, db))

	hasTable, err := schemaTableExists(ctx, db, "migration_probe")
	require.NoError(t, err)
	require.True(t, hasTable)
	hasColumn, err := schemaColumnExists(ctx, db, "migration_probe", "legacy_value")
	require.NoError(t, err)
	require.True(t, hasColumn)
}

func TestSchemaMigrationIntegrationLockSerializesConcurrentMigrators(t *testing.T) {
	db := integrationMigrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var active atomic.Int32
	var maximum atomic.Int32
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- withSchemaMigrationLock(ctx, db, func(*gorm.DB) error {
				current := active.Add(1)
				for {
					previous := maximum.Load()
					if current <= previous || maximum.CompareAndSwap(previous, current) {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
				active.Add(-1)
				return nil
			})
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, maximum.Load())
}

func integrationMigrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	rawDSN := strings.TrimSpace(os.Getenv("MYSQL_TEST_DSN"))
	if rawDSN == "" {
		t.Skip("MYSQL_TEST_DSN not set; requires an isolated MySQL test database")
	}
	parsed, err := mysqldsn.ParseDSN(rawDSN)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(parsed.DBName, "eruun_scheduler_test"), "integration database must start with eruun_scheduler_test")
	parsed.ClientFoundRows = true
	db, err := gorm.Open(mysqlgorm.Open(parsed.FormatDSN()), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{},
		TranslateError: true,
		Logger:         logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	return db
}

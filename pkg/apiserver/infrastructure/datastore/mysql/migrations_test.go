package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/stretchr/testify/require"
)

type legacyResourceCreationBudgetRow struct {
	ID          string    `gorm:"primaryKey;type:varchar(32);column:id"`
	AvailableAt time.Time `gorm:"precision:6;column:available_at;not null"`
	model.BaseModel
}

func TestRunSchemaMigrationsOrdersLockAndMigration(t *testing.T) {
	var events []string
	err := runSchemaMigrations(
		context.Background(),
		func(context.Context) error {
			events = append(events, "acquire")
			return nil
		},
		func(context.Context) error {
			events = append(events, "release")
			return nil
		},
		func() error {
			events = append(events, "migrate")
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, []string{"acquire", "migrate", "release"}, events)
}

func TestInitializeSchemaRejectsUnsupportedMode(t *testing.T) {
	err := initializeSchema(context.Background(), nil, nil, SchemaMode("unsafe"))
	require.ErrorContains(t, err, "unsupported MySQL schema mode")
}

func TestValidateSchemaRequiresDatabase(t *testing.T) {
	require.ErrorContains(t, validateSchema(context.Background(), nil, nil), "gorm db is nil")
}

func TestMigrateSchemaAddsRequiredCheckpointTable(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true,
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	models, err := model.BuiltinModels()
	require.NoError(t, err)
	legacy := make([]model.Interface, 0, len(models)-1)
	for _, entity := range models {
		if _, checkpoint := entity.(*model.JobCheckpoint); !checkpoint {
			legacy = append(legacy, entity)
		}
	}
	ctx := context.Background()
	require.NoError(t, migrateSchema(ctx, db, legacy))
	require.ErrorContains(t, validateSchema(ctx, db, models), "JobCheckpoint is missing")
	require.NoError(t, migrateSchema(ctx, db, models))
	require.NoError(t, validateSchema(ctx, db, models))
	require.True(t, db.Migrator().HasIndex(&model.JobCheckpoint{}, "idx_checkpoint_execution"))
	require.NoError(t, db.Migrator().DropColumn(&model.JobCheckpoint{}, "source_deadline"))
	require.ErrorContains(t, validateSchema(ctx, db, models), "job_checkpoint.source_deadline is missing")
}

func TestWriteSchemaMigrationMarkerIsIdempotent(t *testing.T) {
	db := newDryRunMySQL(t)
	var statement string
	require.NoError(t, db.Callback().Create().After("gorm:create").Register(
		"test:capture_schema_migration_marker",
		func(tx *gorm.DB) { statement = tx.Statement.SQL.String() },
	))

	require.NoError(t, writeSchemaMigrationMarker(context.Background(), db))
	require.Contains(t, statement, "ON DUPLICATE KEY UPDATE")
	require.Contains(t, statement, "`value`=VALUES(`value`)")
}

func TestRunSchemaMigrationInvalidatesPreviousSuccessBeforeFailure(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{},
		TranslateError: true,
		Logger:         logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}))
	require.NoError(t, writeSchemaMigrationMarker(context.Background(), db))
	require.NoError(t, validateSchemaMigrationMarker(context.Background(), db))
	migrationErr := errors.New("later migration failed")

	err = runSchemaMigration(context.Background(), db, func() error { return migrationErr })

	require.ErrorIs(t, err, migrationErr)
	require.ErrorContains(t, validateSchemaMigrationMarker(context.Background(), db), "is incomplete")
}

func TestRunSchemaMigrationMarksSuccessfulRetryComplete(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{},
		TranslateError: true,
		Logger:         logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}))
	require.NoError(t, writeSchemaMigrationState(context.Background(), db, incompleteSchemaMigrationJSON))

	require.NoError(t, runSchemaMigration(context.Background(), db, func() error { return nil }))
	require.NoError(t, validateSchemaMigrationMarker(context.Background(), db))
}

func TestMigrateSchemaInitializesLegacyResourceCreationBudgetInterval(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true,
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	ctx := context.Background()
	budgetTable := (&model.ResourceCreationBudget{}).TableName()
	require.NoError(t, db.Table(budgetTable).AutoMigrate(&legacyResourceCreationBudgetRow{}))
	require.False(t, db.Migrator().HasColumn(budgetTable, "interval_micros"))
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}))
	policy := workflowconfig.DefaultJobSchedulerPolicy()
	policy.ResourceCreationQPS, policy.ResourceCreationBurst = 0.1, 3
	encoded, err := json.Marshal(policy)
	require.NoError(t, err)
	require.NoError(t, db.Create(&model.SystemSetting{
		Type: model.SystemSettingTypeWorkflowScheduler, Value: encoded,
	}).Error)
	clock := &sqlstore.Driver{Client: *db}
	before, err := clock.CurrentDatabaseTime(ctx)
	require.NoError(t, err)
	require.NoError(t, db.Table(budgetTable).Create(&legacyResourceCreationBudgetRow{
		ID: "jobs-and-sandboxes", AvailableAt: before.Add(3 * time.Second),
	}).Error)
	models, err := model.BuiltinModels()
	require.NoError(t, err)

	require.NoError(t, migrateSchema(ctx, db, models))
	require.True(t, db.Migrator().HasColumn(budgetTable, "interval_micros"))
	var budget model.ResourceCreationBudget
	require.NoError(t, db.Where("id = ?", "jobs-and-sandboxes").Take(&budget).Error)
	require.Equal(t, int64((10*time.Second)/time.Microsecond), budget.IntervalMicros)
	require.True(t, budget.AvailableAt.After(before.Add(29*time.Second)), "migration must conservatively retain unknown live debt")
	require.NoError(t, validateSchemaMigrationMarker(ctx, db))
}

func TestRunSchemaMigrationStopsWhenMarkerTableProbeFails(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true,
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	migrated := false

	err = runSchemaMigration(ctx, db, func() error {
		migrated = true
		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, migrated)
}

func TestRunSchemaMigrationKeepsMarkerIncompleteWhenMigrationProbeFails(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true,
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}))
	require.NoError(t, writeSchemaMigrationMarker(context.Background(), db))

	ctx, cancel := context.WithCancel(context.Background())
	err = runSchemaMigration(ctx, db, func() error {
		cancel()
		_, err := schemaTableExists(ctx, db, (&model.SystemSetting{}).TableName())
		return err
	})

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, validateSchemaMigrationMarker(context.Background(), db), "is incomplete")
}

func TestSchemaColumnExistsPropagatesProbeFailure(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true,
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, db.Exec("CREATE TABLE probe_columns (id TEXT, legacy_value TEXT)").Error)

	exists, err := schemaColumnExists(context.Background(), db, "probe_columns", "legacy_value")
	require.NoError(t, err)
	require.True(t, exists)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = schemaColumnExists(ctx, db, "probe_columns", "legacy_value")
	require.ErrorIs(t, err, context.Canceled)
}

func TestMigrationSettingsHaveTimestamps(t *testing.T) {
	for _, tc := range []struct {
		name, settingType string
		migrate           func(context.Context, *gorm.DB) error
	}{
		{"schema marker", schemaMigrationMarker, writeSchemaMigrationMarker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
			createdAt := now
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
				NamingStrategy: sqlnamer.SQLNamer{},
				TranslateError: true,
				NowFunc:        func() time.Time { return now },
				Logger:         logger.Default.LogMode(logger.Silent),
			})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			sqlDB.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
			require.NoError(t, db.AutoMigrate(&model.SystemSetting{}, &model.Applications{}))

			require.NoError(t, tc.migrate(context.Background(), db))
			var setting model.SystemSetting
			require.NoError(t, db.Where("type = ?", tc.settingType).First(&setting).Error)
			require.Equal(t, createdAt, setting.CreateTime)
			require.Equal(t, createdAt, setting.UpdateTime)

			// Re-running a completed migration must not replace the existing record.
			now = now.Add(time.Hour)
			require.NoError(t, tc.migrate(context.Background(), db))
			var repeated model.SystemSetting
			require.NoError(t, db.Where("type = ?", tc.settingType).First(&repeated).Error)
			require.Equal(t, setting, repeated)
		})
	}
}

func TestValidateSchemaMigrationMarker(t *testing.T) {
	tests := []struct {
		name      string
		value     json.RawMessage
		rows      int64
		wantError string
	}{
		{name: "complete", value: json.RawMessage(completedSchemaMigrationJSON), rows: 1},
		{name: "missing", rows: 0, wantError: "is missing"},
		{name: "incomplete", value: json.RawMessage(`{"completed":false}`), rows: 1, wantError: "is incomplete"},
		{name: "invalid", value: json.RawMessage(`{"completed":`), rows: 1, wantError: "is incomplete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newDryRunMySQL(t)
			require.NoError(t, db.Callback().Query().After("gorm:query").Register(
				"test:load_schema_migration_marker",
				func(tx *gorm.DB) {
					marker, ok := tx.Statement.Dest.(*model.SystemSetting)
					if !ok {
						return
					}
					marker.Type = schemaMigrationMarker
					marker.Value = tt.value
					tx.RowsAffected = tt.rows
				},
			))

			err := validateSchemaMigrationMarker(context.Background(), db)
			if tt.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantError)
		})
	}
}

func TestRunSchemaMigrationsDoesNotMigrateWithoutLock(t *testing.T) {
	acquireErr := errors.New("lock unavailable")
	migrated := false
	released := false
	err := runSchemaMigrations(
		context.Background(),
		func(context.Context) error { return acquireErr },
		func(context.Context) error {
			released = true
			return nil
		},
		func() error {
			migrated = true
			return nil
		},
	)

	require.ErrorIs(t, err, acquireErr)
	require.False(t, migrated)
	require.False(t, released)
}

func TestRunSchemaMigrationsPassesCallerCancellationToLockAcquisition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	migrated := false
	released := false

	err := runSchemaMigrations(
		ctx,
		func(lockCtx context.Context) error {
			require.Same(t, ctx, lockCtx)
			return lockCtx.Err()
		},
		func(context.Context) error {
			released = true
			return nil
		},
		func() error {
			migrated = true
			return nil
		},
	)

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, migrated)
	require.False(t, released)
}

func TestRunSchemaMigrationsReportsMigrationAndReleaseFailures(t *testing.T) {
	migrationErr := errors.New("migration failed")
	releaseErr := errors.New("release failed")
	err := runSchemaMigrations(
		context.Background(),
		func(context.Context) error { return nil },
		func(context.Context) error { return releaseErr },
		func() error { return migrationErr },
	)

	require.ErrorIs(t, err, migrationErr)
	require.ErrorIs(t, err, releaseErr)
}

func TestApplicationComponentRuntimeNullBackfills(t *testing.T) {
	db := newDryRunMySQL(t)
	table := (&model.ApplicationComponent{}).TableName()
	tests := []struct {
		name      string
		field     applicationComponentRuntimeNullBackfill
		wantSQL   string
		wantValue interface{}
	}{
		{
			name:      "status",
			field:     applicationComponentRuntimeNullBackfill{column: "status", value: ""},
			wantSQL:   "UPDATE `eruun_app_components` SET `status`=? WHERE `eruun_app_components`.`status` IS NULL",
			wantValue: "",
		},
		{
			name:      "ready replicas",
			field:     applicationComponentRuntimeNullBackfill{column: "ready_replicas", value: int32(0)},
			wantSQL:   "UPDATE `eruun_app_components` SET `ready_replicas`=? WHERE `eruun_app_components`.`ready_replicas` IS NULL",
			wantValue: int32(0),
		},
		{
			name:      "last abnormal",
			field:     applicationComponentRuntimeNullBackfill{column: "last_abnormal", value: ""},
			wantSQL:   "UPDATE `eruun_app_components` SET `last_abnormal`=? WHERE `eruun_app_components`.`last_abnormal` IS NULL",
			wantValue: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := backfillApplicationComponentRuntimeNull(db.Session(&gorm.Session{DryRun: true}), table, tt.field)

			require.NoError(t, result.Error)
			require.Equal(t, tt.wantSQL, result.Statement.SQL.String())
			require.Equal(t, []interface{}{tt.wantValue}, result.Statement.Vars)
		})
	}
}

func TestApplicationComponentCreateWritesRuntimeZeroValues(t *testing.T) {
	db := newDryRunMySQL(t)
	result := db.Create(&model.ApplicationComponent{AppID: "app-1", Name: "web"})
	require.NoError(t, result.Error)

	values := insertValuesByColumn(t, result.Statement.SQL.String(), result.Statement.Vars)
	require.Equal(t, "", values["status"])
	require.EqualValues(t, 0, values["ready_replicas"])
	require.Equal(t, "", values["last_abnormal"])
}

func TestApplicationComponentRuntimeNullBackfillContract(t *testing.T) {
	require.Equal(t, []applicationComponentRuntimeNullBackfill{
		{column: "status", value: ""},
		{column: "ready_replicas", value: int32(0)},
		{column: "last_abnormal", value: ""},
	}, applicationComponentRuntimeNullBackfills)
}

func TestMigrateApplicationComponentRuntimeStatusRunsAllBackfills(t *testing.T) {
	db := newDryRunMySQL(t)
	var updates int
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register(
		"test:count_application_component_runtime_backfills",
		func(*gorm.DB) {
			updates++
		},
	))

	require.NoError(t, migrateApplicationComponentRuntimeStatus(context.Background(), db))
	require.Equal(t, len(applicationComponentRuntimeNullBackfills), updates)
}

func TestMigrateApplicationComponentRuntimeStatusStopsAfterBackfillError(t *testing.T) {
	db := newDryRunMySQL(t)
	sentinel := errors.New("injected update failure")
	var updates int
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register(
		"test:fail_second_application_component_runtime_backfill",
		func(tx *gorm.DB) {
			updates++
			if updates == 2 {
				tx.AddError(sentinel)
			}
		},
	))

	err := migrateApplicationComponentRuntimeStatus(context.Background(), db)
	require.ErrorIs(t, err, sentinel)
	require.ErrorContains(t, err, "backfill application component ready_replicas")
	require.Equal(t, 2, updates)
}

func newDryRunMySQL(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(mysqlgorm.New(mysqlgorm.Config{
		DSN:                       "gorm:gorm@tcp(127.0.0.1:9910)/gorm?charset=utf8mb4&parseTime=True&loc=Local",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DryRun:                 true,
		DisableAutomaticPing:   true,
		SkipDefaultTransaction: true,
		NamingStrategy:         sqlnamer.SQLNamer{},
	})
	require.NoError(t, err)
	return db
}

func insertValuesByColumn(t *testing.T, statement string, vars []interface{}) map[string]interface{} {
	t.Helper()
	columnsStart := strings.Index(statement, "(")
	valuesStart := strings.Index(statement, ") VALUES")
	require.GreaterOrEqual(t, columnsStart, 0, statement)
	require.Greater(t, valuesStart, columnsStart, statement)

	columns := strings.Split(statement[columnsStart+1:valuesStart], ",")
	require.Len(t, vars, len(columns), statement)
	values := make(map[string]interface{}, len(columns))
	for index, column := range columns {
		values[strings.Trim(strings.TrimSpace(column), "`")] = vars[index]
	}
	return values
}

func TestMigrateSchemaPreservesSchedulingProfilesAndSettings(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	models, err := model.BuiltinModels()
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, migrateSchema(ctx, db, models))
	profile := model.RBACProfile{Name: "reader", Rules: []spec.RBACRuleSpec{{Resources: []string{"pods"}, Verbs: []string{"get"}}}}
	pool := model.NodePool{Name: "cpu", NodeSelector: map[string]string{"pool": "cpu"}}
	require.NoError(t, db.Create(&profile).Error)
	require.NoError(t, db.Create(&pool).Error)
	for _, kind := range []string{model.SystemSettingTypeNodeSelector, model.SystemSettingTypeRBACPolicies} {
		require.NoError(t, db.Create(&model.SystemSetting{Type: kind, Value: json.RawMessage(`[]`)}).Error)
	}
	// Exercise the complete startup migration twice, including an existing marker.
	for range 2 {
		require.NoError(t, migrateSchema(ctx, db, models))
		var got model.RBACProfile
		require.NoError(t, db.First(&got, "name = ?", profile.Name).Error)
		require.Equal(t, profile, got)
		var gotPool model.NodePool
		require.NoError(t, db.First(&gotPool, "name = ?", pool.Name).Error)
		require.Equal(t, pool, gotPool)
		for _, kind := range []string{model.SystemSettingTypeNodeSelector, model.SystemSettingTypeRBACPolicies} {
			var setting model.SystemSetting
			require.NoError(t, db.First(&setting, "type = ?", kind).Error)
			require.JSONEq(t, `[]`, string(setting.Value))
		}
	}
}

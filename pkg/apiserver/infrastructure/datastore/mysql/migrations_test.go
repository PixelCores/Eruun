package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

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

func TestMigrateApplicationManagementModeRunsNativeAndObserveBackfills(t *testing.T) {
	db := newDryRunMySQL(t)
	var statements []string
	require.NoError(t, db.Callback().Update().After("gorm:update").Register(
		"test:capture_application_management_mode_backfills",
		func(tx *gorm.DB) {
			statements = append(statements, tx.Statement.SQL.String())
		},
	))

	require.NoError(t, migrateApplicationManagementModeTx(db.WithContext(context.Background())))
	require.Len(t, statements, 2)
	require.Contains(t, statements[0], "management_mode")
	require.Contains(t, statements[1], "LOWER(project) = ? AND LOWER(version) = ?")
}

func TestMigrateApplicationManagementModeDoesNotRepeatAfterMarkerExists(t *testing.T) {
	db := newDryRunMySQL(t)
	var updates int
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register(
		"test:existing_application_management_mode_marker",
		func(tx *gorm.DB) {
			if setting, ok := tx.Statement.Dest.(*model.SystemSetting); ok &&
				setting.Type == applicationManagementModeMigrationMarker {
				tx.AddError(gorm.ErrDuplicatedKey)
			}
		},
	))
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register(
		"test:no_repeated_application_management_mode_backfill",
		func(*gorm.DB) { updates++ },
	))

	require.NoError(t, migrateApplicationManagementModeTx(db.WithContext(context.Background())))
	require.Zero(t, updates)
}

func TestApplicationManagementModeSchemaKeepsLegacyWritesNullable(t *testing.T) {
	db := newDryRunMySQL(t)
	statement := &gorm.Statement{DB: db}
	require.NoError(t, statement.Parse(&model.Applications{}))

	field := statement.Schema.LookUpField("ManagementMode")
	require.NotNil(t, field)
	require.False(t, field.NotNull)
	require.False(t, field.HasDefaultValue)
	require.Empty(t, field.DefaultValue)

	legacyImportedAfterMarker := &model.Applications{Project: "imported", Version: "imported"}
	require.Equal(t, config.ManagementModeObserve, legacyImportedAfterMarker.EffectiveManagementMode())
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

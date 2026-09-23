package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

const (
	applicationManagementModeMigrationMarker = "migration.application-management-mode.v1"
	schemaMigrationMarker                    = "migration.schema.v1"
	completedSchemaMigrationJSON             = `{"completed":true}`
	incompleteSchemaMigrationJSON            = `{"completed":false}`
)

type schemaMigrationState struct {
	Completed bool `json:"completed"`
}

type nodeSelectorProfileRow struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Selection   json.RawMessage `json:"selection" gorm:"column:selection"`
}

type rbacProfileRow struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Policies    json.RawMessage `json:"policies" gorm:"column:policies"`
}

type applicationComponentRuntimeNullBackfill struct {
	column string
	value  interface{}
}

var applicationComponentRuntimeNullBackfills = []applicationComponentRuntimeNullBackfill{
	{column: "status", value: ""},
	{column: "ready_replicas", value: int32(0)},
	{column: "last_abnormal", value: ""},
}

// migrateApplicationComponentRuntimeStatus runs after AutoMigrate so legacy
// tables gain any missing runtime columns before nullable values are backfilled.
func migrateApplicationComponentRuntimeStatus(ctx context.Context, db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("gorm db is nil")
	}
	componentsTable := (&model.ApplicationComponent{}).TableName()

	var fieldUpdates int64
	for _, field := range applicationComponentRuntimeNullBackfills {
		result := backfillApplicationComponentRuntimeNull(db.WithContext(ctx), componentsTable, field)
		if result.Error != nil {
			return fmt.Errorf("backfill application component %s: %w", field.column, result.Error)
		}
		fieldUpdates += result.RowsAffected
	}
	if fieldUpdates > 0 {
		klog.InfoS("backfilled nullable application component runtime fields", "fieldUpdates", fieldUpdates)
	}
	return nil
}

// migrateApplicationManagementMode makes management intent explicit once.
// Historical namespace imports become read-only until a later explicit
// adoption flow takes ownership of them.
func migrateApplicationManagementMode(ctx context.Context, db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("gorm db is nil")
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return migrateApplicationManagementModeTx(tx)
	})
}

func migrateApplicationManagementModeTx(tx *gorm.DB) error {
	now := tx.NowFunc()
	marker := &model.SystemSetting{
		Type:      applicationManagementModeMigrationMarker,
		Value:     json.RawMessage(`{"completed":true}`),
		BaseModel: model.BaseModel{CreateTime: now, UpdateTime: now},
	}
	if err := tx.Create(marker).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return nil
		}
		return fmt.Errorf("claim application management mode migration: %w", err)
	}

	table := (&model.Applications{}).TableName()
	native := tx.Table(table).
		Where("management_mode IS NULL OR management_mode = ?", "").
		UpdateColumn("management_mode", config.ManagementModeNative)
	if native.Error != nil {
		return fmt.Errorf("backfill native application management mode: %w", native.Error)
	}

	observed := tx.Table(table).
		Where("LOWER(project) = ? AND LOWER(version) = ? AND management_mode = ?",
			"imported", "imported", config.ManagementModeNative).
		UpdateColumn("management_mode", config.ManagementModeObserve)
	if observed.Error != nil {
		return fmt.Errorf("migrate historical imported applications to observe: %w", observed.Error)
	}
	if native.RowsAffected+observed.RowsAffected > 0 {
		klog.InfoS("backfilled application management modes",
			"nativeRows", native.RowsAffected,
			"observeRows", observed.RowsAffected)
	}
	return nil
}

// migrateResourceCreationBudgetIntervals upgrades budgets created before their
// rate interval was persisted. The old interval cannot be reconstructed, so a
// live debt is saturated at the current burst instead of granting new permits.
func migrateResourceCreationBudgetIntervals(ctx context.Context, db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("gorm db is nil")
	}
	var pending int64
	if err := db.WithContext(ctx).Model(&model.ResourceCreationBudget{}).Where("interval_micros = ?", 0).Count(&pending).Error; err != nil {
		return fmt.Errorf("count resource creation budgets without intervals: %w", err)
	}
	if pending == 0 {
		return nil
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var setting model.SystemSetting
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("type = ?", model.SystemSettingTypeWorkflowScheduler).
			Take(&setting).Error; err != nil {
			return fmt.Errorf("lock workflow scheduler policy for resource creation budget migration: %w", err)
		}
		policy, err := workflowconfig.ParseJobSchedulerPolicy(setting.Value)
		if err != nil {
			return fmt.Errorf("parse workflow scheduler policy for resource creation budget migration: %w", err)
		}
		interval := time.Duration(math.Ceil(1e6/policy.ResourceCreationQPS)) * time.Microsecond
		clock := &sqlstore.Driver{Client: *tx}
		now, err := clock.CurrentDatabaseTime(ctx)
		if err != nil {
			return fmt.Errorf("query resource creation budget migration clock: %w", err)
		}

		var budgets []model.ResourceCreationBudget
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("interval_micros = ?", 0).
			Find(&budgets).Error; err != nil {
			return fmt.Errorf("lock resource creation budgets without intervals: %w", err)
		}
		for i := range budgets {
			budget := &budgets[i]
			budget.InitializeUnknownInterval(now, interval, policy.ResourceCreationBurst)
			result := tx.Model(&model.ResourceCreationBudget{}).
				Where("id = ? AND interval_micros = ?", budget.ID, 0).
				Updates(map[string]interface{}{
					"available_at": budget.AvailableAt, "interval_micros": budget.IntervalMicros,
				})
			if result.Error != nil {
				return fmt.Errorf("initialize resource creation budget %s interval: %w", budget.ID, result.Error)
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("initialize resource creation budget %s interval: row changed concurrently", budget.ID)
			}
		}
		klog.InfoS("initialized legacy resource creation budget intervals", "budgets", len(budgets))
		return nil
	})
}

func backfillApplicationComponentRuntimeNull(db *gorm.DB, table string, field applicationComponentRuntimeNullBackfill) *gorm.DB {
	return db.Table(table).
		Where(map[string]interface{}{field.column: nil}).
		UpdateColumn(field.column, field.value)
}

func migrateSystemSettings(ctx context.Context, db *gorm.DB) error {
	migrator := db.Migrator()
	settingsTable := (&model.SystemSetting{}).TableName()
	nodeTable := (&model.NodeSelectorProfile{}).TableName()
	rbacTable := (&model.RBACProfile{}).TableName()

	hasSettingsTable, err := schemaTableExists(ctx, db, settingsTable)
	if err != nil {
		return fmt.Errorf("inspect system settings table: %w", err)
	}
	if !hasSettingsTable {
		if err := db.WithContext(ctx).AutoMigrate(&model.SystemSetting{}); err != nil {
			return err
		}
	}

	nodeErr := migrateSettingFromTable(ctx, db, nodeTable, model.SystemSettingTypeNodeSelector, func(rows []nodeSelectorProfileRow) (json.RawMessage, error) {
		if len(rows) == 0 {
			return nil, nil
		}
		payload, err := json.Marshal(rows)
		if err != nil {
			return nil, err
		}
		return json.RawMessage(payload), nil
	})
	if nodeErr != nil {
		return nodeErr
	}

	rbacErr := migrateSettingFromTable(ctx, db, rbacTable, model.SystemSettingTypeRBACPolicies, func(rows []rbacProfileRow) (json.RawMessage, error) {
		if len(rows) == 0 {
			return nil, nil
		}
		payload, err := json.Marshal(rows)
		if err != nil {
			return nil, err
		}
		return json.RawMessage(payload), nil
	})
	if rbacErr != nil {
		return rbacErr
	}

	hasNodeTable, err := schemaTableExists(ctx, db, nodeTable)
	if err != nil {
		return fmt.Errorf("inspect legacy node selector table: %w", err)
	}
	if hasNodeTable {
		if err := migrator.DropTable(nodeTable); err != nil {
			return err
		}
	}
	hasRBACTable, err := schemaTableExists(ctx, db, rbacTable)
	if err != nil {
		return fmt.Errorf("inspect legacy RBAC table: %w", err)
	}
	if hasRBACTable {
		if err := migrator.DropTable(rbacTable); err != nil {
			return err
		}
	}

	klog.Info("system setting migration completed")
	return nil
}

func migrateTextOnlySecretSchema(ctx context.Context, db *gorm.DB) error {
	migrator := db.Migrator()
	componentsTable := (&model.ApplicationComponent{}).TableName()
	const legacySecretEncodingColumn = "secret_values_base64_encoded"

	hasComponentsTable, err := schemaTableExists(ctx, db, componentsTable)
	if err != nil {
		return fmt.Errorf("inspect application components table: %w", err)
	}
	if !hasComponentsTable {
		return nil
	}
	hasLegacyColumn, err := schemaColumnExists(ctx, db, componentsTable, legacySecretEncodingColumn)
	if err != nil {
		return fmt.Errorf("inspect legacy secret encoding column: %w", err)
	}
	if !hasLegacyColumn {
		return nil
	}
	if err := migrator.DropColumn(componentsTable, legacySecretEncodingColumn); err != nil {
		return err
	}
	klog.Info("dropped legacy secret encoding column from application components")
	return nil
}

func runSchemaMigration(ctx context.Context, db *gorm.DB, migrate func() error) error {
	if db == nil {
		return fmt.Errorf("gorm db is nil")
	}
	if migrate == nil {
		return fmt.Errorf("schema migration function is nil")
	}
	hasMarkerTable, err := schemaMigrationMarkerTableExists(ctx, db)
	if err != nil {
		return fmt.Errorf("inspect schema migration marker table: %w", err)
	}
	if hasMarkerTable {
		if err := writeSchemaMigrationState(ctx, db, incompleteSchemaMigrationJSON); err != nil {
			return fmt.Errorf("invalidate schema migration marker: %w", err)
		}
	}
	if err := migrate(); err != nil {
		return err
	}
	return writeSchemaMigrationMarker(ctx, db)
}

func schemaMigrationMarkerTableExists(ctx context.Context, db *gorm.DB) (bool, error) {
	return schemaTableExists(ctx, db, (&model.SystemSetting{}).TableName())
}

func schemaTableExists(ctx context.Context, db *gorm.DB, table string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("gorm db is nil")
	}
	var count int64
	switch db.Dialector.Name() {
	case "mysql":
		var database string
		if err := db.WithContext(ctx).Raw("SELECT DATABASE()").Scan(&database).Error; err != nil {
			return false, err
		}
		if err := db.WithContext(ctx).Raw(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			database, table,
		).Scan(&count).Error; err != nil {
			return false, err
		}
	case "sqlite":
		if err := db.WithContext(ctx).Raw(
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table,
		).Scan(&count).Error; err != nil {
			return false, err
		}
	default:
		tables, err := db.WithContext(ctx).Migrator().GetTables()
		if err != nil {
			return false, err
		}
		for _, candidate := range tables {
			if candidate == table {
				return true, nil
			}
		}
		return false, nil
	}
	return count > 0, nil
}

func schemaColumnExists(ctx context.Context, db *gorm.DB, table, column string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("gorm db is nil")
	}
	var count int64
	switch db.Dialector.Name() {
	case "mysql":
		var database string
		if err := db.WithContext(ctx).Raw("SELECT DATABASE()").Scan(&database).Error; err != nil {
			return false, err
		}
		if err := db.WithContext(ctx).Raw(
			"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?",
			database, table, column,
		).Scan(&count).Error; err != nil {
			return false, err
		}
	case "sqlite":
		if err := db.WithContext(ctx).Raw(
			"SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?", table, column,
		).Scan(&count).Error; err != nil {
			return false, err
		}
	default:
		columns, err := db.WithContext(ctx).Migrator().ColumnTypes(table)
		if err != nil {
			return false, err
		}
		for _, candidate := range columns {
			if candidate.Name() == column {
				return true, nil
			}
		}
		return false, nil
	}
	return count > 0, nil
}

func writeSchemaMigrationMarker(ctx context.Context, db *gorm.DB) error {
	return writeSchemaMigrationState(ctx, db, completedSchemaMigrationJSON)
}

func writeSchemaMigrationState(ctx context.Context, db *gorm.DB, value string) error {
	now := db.NowFunc()
	marker := &model.SystemSetting{
		Type:      schemaMigrationMarker,
		Value:     json.RawMessage(value),
		BaseModel: model.BaseModel{CreateTime: now, UpdateTime: now},
	}
	if err := db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "type"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(marker).Error; err != nil {
		return fmt.Errorf("write schema migration marker: %w", err)
	}
	return nil
}

func validateSchemaMigrationMarker(ctx context.Context, db *gorm.DB) error {
	var marker model.SystemSetting
	result := db.WithContext(ctx).
		Where("type = ?", schemaMigrationMarker).
		Limit(1).
		Find(&marker)
	if result.Error != nil {
		return fmt.Errorf("read schema migration marker: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("schema validation failed: migration marker %q is missing", schemaMigrationMarker)
	}
	var state schemaMigrationState
	if err := json.Unmarshal(marker.Value, &state); err != nil || !state.Completed {
		return fmt.Errorf("schema validation failed: migration marker %q is incomplete", schemaMigrationMarker)
	}
	return nil
}

func migrateSettingFromTable[T any](ctx context.Context, db *gorm.DB, tableName, settingType string, buildValue func([]T) (json.RawMessage, error)) error {
	hasTable, err := schemaTableExists(ctx, db, tableName)
	if err != nil {
		return fmt.Errorf("inspect legacy setting table %s: %w", tableName, err)
	}
	if !hasTable {
		return nil
	}

	var existing model.SystemSetting
	lookup := db.WithContext(ctx).Table((&model.SystemSetting{}).TableName()).Where("type = ?", settingType).Limit(1).Find(&existing)
	if lookup.Error != nil {
		return lookup.Error
	}
	if lookup.RowsAffected > 0 {
		return nil
	}

	var rows []T
	if err := db.WithContext(ctx).Table(tableName).Find(&rows).Error; err != nil {
		return err
	}

	value, err := buildValue(rows)
	if err != nil {
		return err
	}
	if len(value) == 0 {
		return nil
	}

	now := db.NowFunc()
	setting := &model.SystemSetting{
		Type:      settingType,
		Value:     value,
		BaseModel: model.BaseModel{CreateTime: now, UpdateTime: now},
	}
	if err := db.WithContext(ctx).Create(setting).Error; err != nil {
		return err
	}

	klog.Infof("migrated system setting type=%s from table=%s", settingType, tableName)
	return nil
}

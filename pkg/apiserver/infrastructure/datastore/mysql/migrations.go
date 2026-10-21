package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"k8s.io/klog/v2"
)

const (
	schemaMigrationMarker         = "migration.schema.v1"
	completedSchemaMigrationJSON  = `{"completed":true}`
	incompleteSchemaMigrationJSON = `{"completed":false}`
)

// 这些设置类型已经从代码中删除；它们的存量值没有读取方，而且 aliyunCloud 含有凭据。
var removedSystemSettingTypes = []string{"aliyunCloud", "nodeSelector", "rbacPolicies"}

func deleteRemovedSystemSettings(ctx context.Context, db *gorm.DB) error {
	hasTable, err := schemaTableExists(ctx, db, (&model.SystemSetting{}).TableName())
	if err != nil {
		return fmt.Errorf("inspect system settings table: %w", err)
	}
	if !hasTable {
		return nil
	}
	result := db.WithContext(ctx).Where("type IN ?", removedSystemSettingTypes).Delete(&model.SystemSetting{})
	if result.Error != nil {
		return fmt.Errorf("delete removed system settings: %w", result.Error)
	}
	if result.RowsAffected > 0 {
		klog.InfoS("deleted removed system settings", "rows", result.RowsAffected)
	}
	return nil
}

type schemaMigrationState struct {
	Completed bool `json:"completed"`
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

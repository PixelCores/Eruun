package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

const applicationManagementModeMigrationMarker = "migration.application-management-mode.v1"

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
	marker := &model.SystemSetting{
		Type:  applicationManagementModeMigrationMarker,
		Value: json.RawMessage(`{"completed":true}`),
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

	if !migrator.HasTable(settingsTable) {
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

	if migrator.HasTable(nodeTable) {
		if err := migrator.DropTable(nodeTable); err != nil {
			return err
		}
	}
	if migrator.HasTable(rbacTable) {
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

	if !migrator.HasTable(componentsTable) {
		return nil
	}
	if !migrator.HasColumn(componentsTable, legacySecretEncodingColumn) {
		return nil
	}
	if err := migrator.DropColumn(componentsTable, legacySecretEncodingColumn); err != nil {
		return err
	}
	klog.Info("dropped legacy secret encoding column from application components")
	return nil
}

func migrateSettingFromTable[T any](ctx context.Context, db *gorm.DB, tableName, settingType string, buildValue func([]T) (json.RawMessage, error)) error {
	migrator := db.Migrator()
	if !migrator.HasTable(tableName) {
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

	setting := &model.SystemSetting{Type: settingType, Value: value}
	if err := db.WithContext(ctx).Create(setting).Error; err != nil {
		return err
	}

	klog.Infof("migrated system setting type=%s from table=%s", settingType, tableName)
	return nil
}

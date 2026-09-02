package mysql

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"time"

	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

const (
	schemaMigrationLockName           = "eruun-schema-migration"
	schemaMigrationLockTimeoutSeconds = 120
	schemaMigrationLockReleaseTimeout = 5 * time.Second
)

type mysql struct {
	sqlstore.Driver
}

type SchemaMode string

const (
	SchemaModeMigrate  SchemaMode = "migrate"
	SchemaModeValidate SchemaMode = "validate"
)

// New mysql datastore instance
func New(ctx context.Context, cfg datastore.Config, models []model.Interface) (datastore.DataStore, error) {
	return NewWithSchemaMode(ctx, cfg, models, SchemaModeMigrate)
}

func NewWithSchemaMode(ctx context.Context, cfg datastore.Config, models []model.Interface, schemaMode SchemaMode) (datastore.DataStore, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, sqlDB, err := openDatabase(cfg)
	if err != nil {
		return nil, err
	}
	if err := initializeSchema(ctx, db, models, schemaMode); err != nil {
		return nil, errors.Join(err, sqlDB.Close())
	}

	m := &mysql{
		Driver: sqlstore.Driver{
			Client: *db.WithContext(ctx),
		},
	}
	return m, nil
}

// MigrateSchema applies all schema and data migrations and closes its dedicated
// database connection pool before returning.
func MigrateSchema(ctx context.Context, cfg datastore.Config, models []model.Interface) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, sqlDB, err := openDatabase(cfg)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, sqlDB.Close())
	}()
	return initializeSchema(ctx, db, models, SchemaModeMigrate)
}

func openDatabase(cfg datastore.Config) (*gorm.DB, *stdsql.DB, error) {
	db, err := gorm.Open(mysqlgorm.Open(cfg.URL), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{},
		Logger:         logger.Default.LogMode(logger.Silent),
		TranslateError: true,
	})
	if err != nil {
		return nil, nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	if cfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}

	return db, sqlDB, nil
}

func initializeSchema(ctx context.Context, db *gorm.DB, models []model.Interface, schemaMode SchemaMode) error {
	switch schemaMode {
	case SchemaModeMigrate:
		return withSchemaMigrationLock(ctx, db, func(migrationDB *gorm.DB) error {
			return migrateSchema(ctx, migrationDB, models)
		})
	case SchemaModeValidate:
		return validateSchema(ctx, db, models)
	default:
		return fmt.Errorf("unsupported MySQL schema mode %q", schemaMode)
	}
}

func withSchemaMigrationLock(ctx context.Context, db *gorm.DB, migrate func(*gorm.DB) error) error {
	if db == nil {
		return fmt.Errorf("gorm db is nil")
	}
	if migrate == nil {
		return fmt.Errorf("schema migration function is nil")
	}

	return db.WithContext(ctx).Connection(func(connection *gorm.DB) error {
		return runSchemaMigrations(
			ctx,
			func(lockCtx context.Context) error {
				return acquireSchemaMigrationLock(lockCtx, connection)
			},
			func(lockCtx context.Context) error {
				return releaseSchemaMigrationLock(lockCtx, connection)
			},
			func() error {
				return migrate(connection)
			},
		)
	})
}

func runSchemaMigrations(
	ctx context.Context,
	acquire func(context.Context) error,
	release func(context.Context) error,
	migrate func() error,
) (err error) {
	if acquire == nil || release == nil || migrate == nil {
		return fmt.Errorf("run MySQL schema migrations: lock callbacks are incomplete")
	}
	if err := acquire(ctx); err != nil {
		return fmt.Errorf("acquire MySQL schema migration lock: %w", err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), schemaMigrationLockReleaseTimeout)
		defer cancel()
		if releaseErr := release(releaseCtx); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("release MySQL schema migration lock: %w", releaseErr))
		}
	}()
	return migrate()
}

func acquireSchemaMigrationLock(ctx context.Context, db *gorm.DB) error {
	var acquired stdsql.NullInt64
	if err := db.WithContext(ctx).
		Raw("SELECT GET_LOCK(?, ?)", schemaMigrationLockName, schemaMigrationLockTimeoutSeconds).
		Scan(&acquired).Error; err != nil {
		return err
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return fmt.Errorf("timed out after %d seconds", schemaMigrationLockTimeoutSeconds)
	}
	return nil
}

func releaseSchemaMigrationLock(ctx context.Context, db *gorm.DB) error {
	var released stdsql.NullInt64
	if err := db.WithContext(ctx).
		Raw("SELECT RELEASE_LOCK(?)", schemaMigrationLockName).
		Scan(&released).Error; err != nil {
		return err
	}
	if !released.Valid || released.Int64 != 1 {
		return fmt.Errorf("lock was not held by the migration connection")
	}
	return nil
}

func migrateSchema(ctx context.Context, db *gorm.DB, models []model.Interface) error {
	for _, v := range models {
		if err := db.WithContext(ctx).AutoMigrate(v); err != nil {
			return fmt.Errorf("auto-migrate %T: %w", v, err)
		}
	}
	if err := migrateApplicationComponentRuntimeStatus(ctx, db.WithContext(ctx)); err != nil {
		return err
	}
	if err := migrateApplicationManagementMode(ctx, db.WithContext(ctx)); err != nil {
		return err
	}
	if err := migrateSystemSettings(ctx, db.WithContext(ctx)); err != nil {
		return err
	}
	if err := migrateTextOnlySecretSchema(ctx, db.WithContext(ctx)); err != nil {
		return err
	}
	return writeSchemaMigrationMarker(ctx, db)
}

func validateSchema(ctx context.Context, db *gorm.DB, models []model.Interface) error {
	if db == nil {
		return fmt.Errorf("gorm db is nil")
	}
	migrationDB := db.WithContext(ctx)
	migrator := migrationDB.Migrator()
	for _, entity := range models {
		if entity == nil {
			return fmt.Errorf("schema model is nil")
		}
		if !migrator.HasTable(entity) {
			return fmt.Errorf("schema validation failed: table for %T is missing", entity)
		}
		statement := &gorm.Statement{DB: migrationDB}
		if err := statement.Parse(entity); err != nil {
			return fmt.Errorf("parse schema model %T: %w", entity, err)
		}
		for _, field := range statement.Schema.Fields {
			if field.DBName == "" || field.IgnoreMigration {
				continue
			}
			if !migrator.HasColumn(entity, field.DBName) {
				return fmt.Errorf("schema validation failed: column %s.%s is missing", statement.Schema.Table, field.DBName)
			}
		}
	}
	return validateSchemaMigrationMarker(ctx, migrationDB)
}

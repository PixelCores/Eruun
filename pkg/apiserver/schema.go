package apiserver

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/mysql"
)

// MigrateSchema runs the datastore migration lifecycle without constructing
// Kubernetes, cache, messaging, API, or workflow runtime dependencies.
func MigrateSchema(ctx context.Context, cfg config.Config) error {
	if cfg.Datastore.Type != config.MYSQL {
		return fmt.Errorf("unsupported datastore type: %s", cfg.Datastore.Type)
	}
	models, err := model.BuiltinModels()
	if err != nil {
		return fmt.Errorf("build model set: %w", err)
	}
	if err := mysql.MigrateSchema(ctx, cfg.Datastore, models); err != nil {
		return fmt.Errorf("migrate mysql schema: %w", err)
	}
	return nil
}

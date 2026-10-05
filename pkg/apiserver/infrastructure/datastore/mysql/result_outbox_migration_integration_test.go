//go:build integration

package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"
)

func TestResultOutboxSchemaMigrationIntegration(t *testing.T) {
	db := integrationMigrationDB(t)
	ctx := context.Background()
	entities := []interface{}{&model.JobResultOutbox{}, &model.SystemSetting{}}
	for _, entity := range entities {
		require.False(t, db.Migrator().HasTable(entity), "requires an empty isolated schema")
	}
	require.NoError(t, db.AutoMigrate(entities...))
	t.Cleanup(func() { require.NoError(t, db.Migrator().DropTable(entities...)) })
	now := time.Now().UTC()
	legacy := &model.JobResultOutbox{BaseModel: model.BaseModel{CreateTime: now, UpdateTime: now}, ID: "before-result-lease", TaskID: "preserved-task", ExecutionKey: "preserved-execution", RunGeneration: 1, State: config.JobResultOutboxStateResultQueued, MessageID: "old-delivery"}
	require.NoError(t, db.Create(legacy).Error)
	for _, column := range []string{"job_uid", "lease_expires_at"} {
		require.NoError(t, db.Migrator().DropColumn(legacy, column))
	}
	require.NoError(t, writeSchemaMigrationMarker(ctx, db))
	require.Error(t, validateSchema(ctx, db, []model.Interface{&model.JobResultOutbox{}}), "new consumers must reject pre-lease schema")
	require.NoError(t, db.AutoMigrate(&model.JobResultOutbox{}))
	require.NoError(t, validateSchema(ctx, db, []model.Interface{&model.JobResultOutbox{}}))
	restored := &model.JobResultOutbox{}
	require.NoError(t, db.First(restored, "id = ?", legacy.ID).Error)
	require.Equal(t, legacy.TaskID, restored.TaskID)
	require.Equal(t, legacy.MessageID, restored.MessageID)
	require.Equal(t, legacy.State, restored.State)
	require.Nil(t, restored.LeaseExpiresAt, "runtime grants old rows a bounded DB-clock grace")
	require.Empty(t, restored.JobUID, "migration must not invent a cleanup identity")
}

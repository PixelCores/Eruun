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

func TestResultOutboxSchemaInitializationIntegration(t *testing.T) {
	db := integrationMigrationDB(t)
	ctx := context.Background()
	entities := []interface{}{&model.JobResultOutbox{}, &model.SystemSetting{}}
	for _, entity := range entities {
		require.False(t, db.Migrator().HasTable(entity), "requires an empty isolated schema")
	}
	require.NoError(t, db.AutoMigrate(entities...))
	t.Cleanup(func() { require.NoError(t, db.Migrator().DropTable(entities...)) })
	require.NoError(t, writeSchemaMigrationMarker(ctx, db))
	require.NoError(t, validateSchema(ctx, db, []model.Interface{&model.JobResultOutbox{}}))
	for _, column := range []string{"job_uid", "lease_expires_at", "claim_token"} {
		require.True(t, db.Migrator().HasColumn(&model.JobResultOutbox{}, column))
	}
	require.False(t, db.Migrator().HasColumn(&model.JobResultOutbox{}, "message_id"))
	for _, index := range []string{"idx_result_outbox_pending", "idx_result_outbox_state_lease"} {
		require.True(t, db.Migrator().HasIndex(&model.JobResultOutbox{}, index))
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	active := &model.JobResultOutbox{BaseModel: model.BaseModel{CreateTime: now, UpdateTime: now}, ID: "active-result", TaskID: "task", ExecutionKey: "execution", RunGeneration: 1, State: config.JobResultOutboxStateResultProcessing, ClaimToken: "independent-owner", JobUID: "job-uid", LeaseExpiresAt: &now}
	require.NoError(t, db.Create(active).Error)
	restored := &model.JobResultOutbox{}
	require.NoError(t, db.First(restored, "id = ?", active.ID).Error)
	require.Equal(t, active.ClaimToken, restored.ClaimToken)
	require.Equal(t, active.JobUID, restored.JobUID)
	require.True(t, now.Equal(*restored.LeaseExpiresAt))
	pending := &model.JobResultOutbox{BaseModel: model.BaseModel{CreateTime: now, UpdateTime: now}, ID: "pending-result", State: config.JobResultOutboxStateResultPending}
	require.NoError(t, db.Create(pending).Error)
	require.Nil(t, pending.LeaseExpiresAt)
	require.Empty(t, pending.ClaimToken)
	require.NoError(t, db.Migrator().DropColumn(active, "claim_token"))
	require.Error(t, validateSchema(ctx, db, []model.Interface{&model.JobResultOutbox{}}), "startup validation must reject a schema missing claim ownership")
}

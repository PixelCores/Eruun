package jobtest

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

func EvaluationScopeStore(t *testing.T) (*gorm.DB, *access.Store, context.Context) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "evaluation.db")), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	connection, err := db.DB()
	require.NoError(t, err)
	connection.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.JobArtifact{}, &model.Applications{}, &model.ApplicationComponent{}, &model.ResourceCreationBudget{}))
	store := access.NewStore(&sqlstore.Driver{Client: *db})
	require.NoError(t, repository.EnsureJobSchedulerPolicy(context.Background(), store))
	return db, store, access.WithScope(context.Background(), access.Scope{WorkspaceID: "space", Namespace: "space-ns"})
}

//go:build integration

package application

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

func TestApplicationTransactionsMySQL(t *testing.T) {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN not set; requires an isolated MySQL test database")
	}
	newStore := func(t *testing.T) *sqlstore.Driver {
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
		require.NoError(t, err)
		sqlDB, err := db.DB()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
		require.NoError(t, db.AutoMigrate(&model.Applications{}, &model.ApplicationComponent{}, &model.Workflow{}, &model.WorkflowQueue{}, &model.JobInfo{}))
		return &sqlstore.Driver{Client: *db}
	}
	t.Run("direct version update", func(t *testing.T) { testDirectVersionUpdateAtomicWrites(t, newStore) })
	t.Run("operation records", func(t *testing.T) { testOperationTaskAtomicWrites(t, newStore) })
}

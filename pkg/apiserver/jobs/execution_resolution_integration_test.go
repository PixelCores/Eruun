//go:build integration

package jobs

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"k8s.io/utils/ptr"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestMySQLExecutionKeyKeepsLiteralIdentity(t *testing.T) {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN not set; requires an isolated MySQL test database")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.WorkflowQueue{}, &model.JobInfo{}))
	conn, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	store := &sqlstore.Driver{Client: *db}
	for _, suffix := range []string{"MiXeD", "trailing ", " whitespace "} {
		t.Run(suffix, func(t *testing.T) {
			task := &model.WorkflowQueue{TaskID: uuid.NewString(), WorkspaceID: uuid.NewString(), Type: config.WorkflowTaskTypeJob}
			ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: task.WorkspaceID, Namespace: "test-ns", Role: "member"})
			require.NoError(t, store.Add(ctx, task))
			t.Cleanup(func() {
				require.NoError(t, store.DeleteByFilter(context.Background(), &model.JobInfo{TaskID: task.TaskID}, nil))
				require.NoError(t, store.Delete(context.Background(), task))
			})
			key := uuid.NewString() + suffix
			require.NoError(t, store.Add(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Type: string(config.JobEval), ExecutionKey: ptr.To(key)}))
			service := &Service{Store: store}
			_, selected, err := service.ResolveResultTask(ctx, task.TaskID, key)
			require.NoError(t, err)
			require.Equal(t, key, selected)
			nearby := strings.ToLower(strings.TrimSpace(key))
			require.NotEqual(t, key, nearby)
			_, _, err = service.ResolveResultTask(ctx, task.TaskID, nearby)
			require.ErrorIs(t, err, bcode.ErrNotFound, "SQL collation must not broaden a caller's execution identity")
			// Filtering before LIMIT must ignore legacy NULLs, while preserving
			// the valid identity even when they precede it in the task history.
			for range 3 {
				require.NoError(t, store.Add(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Type: string(config.JobEval)}))
			}
			_, selected, err = service.ResolveResultTask(ctx, task.TaskID, "")
			require.NoError(t, err)
			require.Equal(t, key, selected)
			rows, err := store.List(ctx, &model.JobInfo{TaskID: task.TaskID}, &datastore.ListOptions{})
			require.NoError(t, err)
			require.Len(t, rows, 4)
		})
	}
}

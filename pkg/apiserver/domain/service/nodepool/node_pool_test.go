package nodepool

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	corev1 "k8s.io/api/core/v1"
)

func TestNodePoolServicePersistenceAndErrors(t *testing.T) {
	ctx := context.Background()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "profiles.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(&model.NodePool{}))
	repo, err := repository.NewNodePoolRepositoryWithStore(&sqlstore.Driver{Client: *db})
	require.NoError(t, err)
	svc, err := NewNodePoolServiceWithRepository(repo)
	require.NoError(t, err)
	fields := UpdateNodePoolCommand{Description: "description", NodeSelector: map[string]string{"pool": "cpu"}, Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}}}
	for _, name := range []string{"z", "a"} {
		item, err := svc.Create(ctx, CreateNodePoolCommand{Name: name, UpdateNodePoolCommand: fields})
		require.NoError(t, err)
		require.Equal(t, name, item.Name)
	}
	_, err = svc.Create(ctx, CreateNodePoolCommand{Name: "a", UpdateNodePoolCommand: fields})
	require.ErrorIs(t, err, bcode.ErrNodePoolExists)
	_, err = svc.Create(ctx, CreateNodePoolCommand{Name: "invalid", UpdateNodePoolCommand: UpdateNodePoolCommand{}})
	require.ErrorIs(t, err, bcode.ErrNodePoolInvalid)
	require.NotEmpty(t, bcode.SafeClientMessage(err))
	items, err := svc.List(ctx)
	require.NoError(t, err)
	require.Equal(t, "a", items[0].Name)
	require.Equal(t, "z", items[1].Name)
	replacement := UpdateNodePoolCommand{NodeSelector: map[string]string{"pool": "new"}}
	updated, err := svc.Update(ctx, "a", replacement)
	require.NoError(t, err)
	require.Empty(t, updated.Description)
	got, err := svc.Get(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, updated.Name, got.Name)
	require.Empty(t, got.Description)
	require.Equal(t, replacement.NodeSelector, got.NodeSelector)
	require.Empty(t, got.Tolerations)
	require.True(t, updated.CreateTime.Equal(got.CreateTime))
	require.True(t, updated.UpdateTime.Equal(got.UpdateTime))
	require.NoError(t, svc.Delete(ctx, "a"))
	_, err = svc.Get(ctx, "a")
	require.ErrorIs(t, err, bcode.ErrNodePoolNotFound)
	_, err = svc.Update(ctx, "a", fields)
	require.ErrorIs(t, err, bcode.ErrNodePoolNotFound)
	require.ErrorIs(t, svc.Delete(ctx, "a"), bcode.ErrNodePoolNotFound)
	// Repository update must not recreate a configuration deleted after lookup.
	require.Error(t, repo.Update(ctx, updated))
	require.ErrorIs(t, repo.Create(ctx, &model.NodePool{Name: "z"}), datastore.ErrRecordExist)
}

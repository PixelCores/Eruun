package rbacprofile

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestRBACProfileServicePersistenceAndErrors(t *testing.T) {
	ctx := context.Background()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "profiles.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(&model.RBACProfile{}))
	repo, err := repository.NewRBACProfileRepositoryWithStore(&sqlstore.Driver{Client: *db})
	require.NoError(t, err)
	svc, err := NewRBACProfileServiceWithRepository(repo)
	require.NoError(t, err)
	fields := UpdateRBACProfileCommand{Description: "description", Rules: []spec.RBACRuleSpec{{Resources: []string{"pods"}, Verbs: []string{"get"}}}}
	for _, name := range []string{"z", "a"} {
		item, err := svc.Create(ctx, CreateRBACProfileCommand{Name: name, UpdateRBACProfileCommand: fields})
		require.NoError(t, err)
		require.Equal(t, name, item.Name)
	}
	_, err = svc.Create(ctx, CreateRBACProfileCommand{Name: "a", UpdateRBACProfileCommand: fields})
	require.ErrorIs(t, err, bcode.ErrRBACProfileExists)
	_, err = svc.Create(ctx, CreateRBACProfileCommand{Name: "invalid", UpdateRBACProfileCommand: UpdateRBACProfileCommand{}})
	require.ErrorIs(t, err, bcode.ErrRBACProfileInvalid)
	require.NotEmpty(t, bcode.SafeClientMessage(err))
	items, err := svc.List(ctx)
	require.NoError(t, err)
	require.Equal(t, "a", items[0].Name)
	require.Equal(t, "z", items[1].Name)
	replacement := UpdateRBACProfileCommand{Rules: []spec.RBACRuleSpec{{Resources: []string{"pods/log"}, Verbs: []string{"list"}}}}
	updated, err := svc.Update(ctx, "a", replacement)
	require.NoError(t, err)
	require.Empty(t, updated.Description)
	got, err := svc.Get(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, updated.Name, got.Name)
	require.Empty(t, got.Description)
	require.Equal(t, replacement.Rules, got.Rules)
	require.True(t, updated.CreateTime.Equal(got.CreateTime))
	require.True(t, updated.UpdateTime.Equal(got.UpdateTime))
	require.NoError(t, svc.Delete(ctx, "a"))
	_, err = svc.Get(ctx, "a")
	require.ErrorIs(t, err, bcode.ErrRBACProfileNotFound)
	_, err = svc.Update(ctx, "a", fields)
	require.ErrorIs(t, err, bcode.ErrRBACProfileNotFound)
	require.ErrorIs(t, svc.Delete(ctx, "a"), bcode.ErrRBACProfileNotFound)
	// Repository update must not recreate a configuration deleted after lookup.
	require.Error(t, repo.Update(ctx, updated))
	require.ErrorIs(t, repo.Create(ctx, &model.RBACProfile{Name: "z"}), datastore.ErrRecordExist)
}

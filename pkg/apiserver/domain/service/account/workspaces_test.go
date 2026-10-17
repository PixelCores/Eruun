package account

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestInvitationBeforeRegistrationOwnershipAndRemoval(t *testing.T) {
	s, _, d := testAccounts(t)
	ctx := context.Background()
	_, owner := registerTestUser(t, s, d, "email", "owner@example.com")
	w, err := s.CreateWorkspace(ctx, owner, "Team")
	require.NoError(t, err)
	_, err = s.Invite(ctx, owner, w.ID, "invited@example.com", "member")
	require.NoError(t, err)
	token := strings.Split(d.invitation, "#invitation=")[1]
	_, wrong := registerTestUser(t, s, d, "email", "wrong@example.com")
	_, err = s.AcceptInvitation(ctx, wrong, token)
	require.ErrorIs(t, err, bcode.ErrForbidden)
	_, member := registerTestUser(t, s, d, "email", "invited@example.com")
	accepted, err := s.AcceptInvitation(ctx, member, token)
	require.NoError(t, err)
	require.Equal(t, w.ID, accepted.ID)
	_, err = s.AcceptInvitation(ctx, member, token)
	require.NoError(t, err)
	rows, err := s.Members(ctx, owner, w.ID)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.ErrorIs(t, s.UpdateMember(ctx, owner, w.ID, owner.User.ID, "", true), bcode.ErrForbidden)
	require.ErrorIs(t, s.TransferWorkspace(ctx, member, w.ID, owner.User.ID), bcode.ErrForbidden)
	require.NoError(t, s.TransferWorkspace(ctx, owner, w.ID, member.User.ID))
	a, err := s.Workspace(ctx, member, w.ID)
	require.NoError(t, err)
	require.Equal(t, "owner", a.Role)
	a, err = s.Workspace(ctx, owner, w.ID)
	require.NoError(t, err)
	require.Equal(t, "admin", a.Role)
	require.NoError(t, s.UpdateMember(ctx, member, w.ID, owner.User.ID, "viewer", false))
	a, err = s.Workspace(ctx, owner, w.ID)
	require.NoError(t, err)
	require.Equal(t, "viewer", a.Role)
	require.NoError(t, s.UpdateMember(ctx, member, w.ID, owner.User.ID, "", true))
	_, err = s.Workspace(ctx, owner, w.ID)
	require.ErrorIs(t, err, bcode.ErrForbidden)
	require.Error(t, s.TransferWorkspace(ctx, member, w.ID, member.User.ID+"missing"))
}

func TestWorkspaceRoleMatrix(t *testing.T) {
	for _, actor := range []string{"owner", "admin", "member", "viewer"} {
		for _, target := range []string{"owner", "admin", "member", "viewer"} {
			expected := (actor == "owner" && target != "owner") || (actor == "admin" && (target == "member" || target == "viewer"))
			require.Equal(t, expected, canAssign(actor, target), actor+" -> "+target)
		}
	}
	s, _, d := testAccounts(t)
	ctx := context.Background()
	_, p := registerTestUser(t, s, d, "email", "personal@example.com")
	a, err := s.Workspace(ctx, p, "")
	require.NoError(t, err)
	_, err = s.Invite(ctx, p, a.Workspace.ID, "other@example.com", "member")
	require.ErrorIs(t, err, bcode.ErrForbidden)
	require.ErrorIs(t, s.TransferWorkspace(ctx, p, a.Workspace.ID, "another"), bcode.ErrForbidden)
}

func TestWorkspaceDeletionWaitsForSandboxAndOnlyDeletesItsRecords(t *testing.T) {
	s, _, delivery := testAccounts(t)
	ctx := context.Background()
	_, owner := registerTestUser(t, s, delivery, "email", "sandbox-owner@example.com")
	workspace, err := s.CreateWorkspace(ctx, owner, "Sandbox team")
	require.NoError(t, err)
	row := &model.JobSandbox{ID: "retained", WorkspaceID: workspace.ID, SlotReserved: true, State: "retained"}
	require.NoError(t, s.Repo.Store.Add(ctx, row))
	require.NoError(t, s.Repo.Store.Add(ctx, &model.JobSandbox{ID: "foreign", WorkspaceID: "foreign"}))
	called := false
	remove := func(context.Context, *model.Workspace) error { called = true; return nil }
	require.ErrorIs(t, s.DeleteWorkspace(ctx, owner, workspace.ID, remove), bcode.ErrWorkspaceNotEmpty)
	require.False(t, called)
	updated, err := s.Repo.Store.CompareAndSwap(ctx, row, "id", row.ID, map[string]interface{}{"slot_reserved": false, "state": "released"})
	require.NoError(t, err)
	require.True(t, updated)
	require.NoError(t, s.DeleteWorkspace(ctx, owner, workspace.ID, remove))
	require.True(t, called)
	count, err := s.Repo.Store.Count(ctx, &model.JobSandbox{WorkspaceID: workspace.ID}, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, s.Repo.Store.Get(ctx, &model.JobSandbox{ID: "foreign"}))
}

func TestWorkspaceDeletionWaitsForCheckpointCleanup(t *testing.T) {
	s, _, delivery := testAccounts(t)
	ctx := context.Background()
	_, owner := registerTestUser(t, s, delivery, "email", "checkpoint-owner@example.com")
	workspace, err := s.CreateWorkspace(ctx, owner, "Checkpoint team")
	require.NoError(t, err)
	row := &model.JobCheckpoint{ID: "retained", WorkspaceID: workspace.ID, State: "failed"}
	require.NoError(t, s.Repo.Store.Add(ctx, row))
	require.NoError(t, s.Repo.Store.Add(ctx, &model.JobCheckpoint{ID: "foreign", WorkspaceID: "foreign"}))
	called := false
	remove := func(context.Context, *model.Workspace) error { called = true; return nil }
	require.ErrorIs(t, s.DeleteWorkspace(ctx, owner, workspace.ID, remove), bcode.ErrWorkspaceNotEmpty)
	require.False(t, called, "a failed snapshot can still have running cloud resources")
	updated, err := s.Repo.Store.CompareAndSwap(ctx, row, "id", row.ID, map[string]interface{}{"cleaned": true})
	require.NoError(t, err)
	require.True(t, updated)
	require.NoError(t, s.DeleteWorkspace(ctx, owner, workspace.ID, remove))
	require.True(t, called)
	count, err := s.Repo.Store.Count(ctx, &model.JobCheckpoint{WorkspaceID: workspace.ID}, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, s.Repo.Store.Get(ctx, &model.JobCheckpoint{ID: "foreign"}))
}

func TestInvitationResendExpiryAndFailedDelivery(t *testing.T) {
	s, _, d := testAccounts(t)
	ctx := context.Background()
	_, p := registerTestUser(t, s, d, "email", "owner@example.com")
	_, member := registerTestUser(t, s, d, "email", "member@example.com")
	w, err := s.CreateWorkspace(ctx, p, "Team")
	require.NoError(t, err)
	_, err = s.Invite(ctx, p, w.ID, "member@example.com", "viewer")
	require.NoError(t, err)
	old := strings.Split(d.invitation, "#invitation=")[1]
	_, err = s.Invite(ctx, p, w.ID, "member@example.com", "member")
	require.NoError(t, err)
	fresh := strings.Split(d.invitation, "#invitation=")[1]
	require.NotEqual(t, old, fresh)
	_, err = s.AcceptInvitation(ctx, member, old)
	require.Error(t, err)
	s.Now = func() time.Time { return time.Now().Add(8 * 24 * time.Hour) }
	_, err = s.AcceptInvitation(ctx, member, fresh)
	require.ErrorIs(t, err, bcode.ErrAccountInput)
	s.Now = time.Now
	d.fail = true
	_, err = s.Invite(ctx, p, w.ID, "failed@example.com", "member")
	require.ErrorIs(t, err, bcode.ErrAccountDelivery)
	n, err := s.Repo.Store.Count(ctx, &model.WorkspaceInvitation{WorkspaceID: w.ID, Email: "failed@example.com"}, nil)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestWorkspaceDeleteRequiresEmptyResources(t *testing.T) {
	s, _, d := testAccounts(t)
	ctx := context.Background()
	_, p := registerTestUser(t, s, d, "email", "owner@example.com")
	w, err := s.CreateWorkspace(ctx, p, "Team")
	require.NoError(t, err)
	require.NoError(t, s.Repo.Store.Add(ctx, &model.JobDefinition{ID: "team-definition", WorkspaceID: w.ID, Name: "reusable", Revision: 1, Spec: "{}"}))
	require.NoError(t, s.Repo.Store.Add(ctx, &model.JobDefinition{ID: "foreign-definition", WorkspaceID: "foreign", Name: "reusable", Revision: 1, Spec: "{}"}))
	app := &model.Applications{ID: "app", Name: "app", WorkspaceID: w.ID, Namespace: w.Namespace}
	require.NoError(t, s.Repo.Store.Add(ctx, app))
	calls := 0
	remove := func(context.Context, *model.Workspace) error { calls++; return nil }
	require.ErrorIs(t, s.DeleteWorkspace(ctx, p, w.ID, remove), bcode.ErrWorkspaceNotEmpty)
	require.Zero(t, calls)
	require.NoError(t, s.Repo.Store.Delete(ctx, app))
	task := &model.WorkflowQueue{
		TaskID:      "resource-import-task",
		WorkspaceID: w.ID,
		Type:        config.WorkflowTaskTypeResourceImportScan,
		Status:      config.StatusWaiting,
	}
	require.NoError(t, s.Repo.Store.Add(ctx, task))
	require.ErrorIs(t, s.DeleteWorkspace(ctx, p, w.ID, remove), bcode.ErrWorkspaceNotEmpty)
	require.Zero(t, calls)
	task.Status = config.StatusCompleted
	require.NoError(t, s.Repo.Store.Put(ctx, task))
	require.NoError(t, s.Repo.Store.Add(ctx, &model.JobInfo{
		ID:          100,
		TaskID:      task.TaskID,
		WorkspaceID: w.ID,
		Type:        string(config.JobResourceImportScan),
		Status:      string(config.StatusCompleted),
	}))
	require.NoError(t, s.DeleteWorkspace(ctx, p, w.ID, remove))
	require.Equal(t, 1, calls)
	n, err := s.Repo.Store.Count(ctx, &model.WorkflowQueue{WorkspaceID: w.ID}, nil)
	require.NoError(t, err)
	require.Zero(t, n)
	n, err = s.Repo.Store.Count(ctx, &model.JobInfo{WorkspaceID: w.ID}, nil)
	require.NoError(t, err)
	require.Zero(t, n)
	n, err = s.Repo.Store.Count(ctx, &model.JobDefinition{WorkspaceID: w.ID}, nil)
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, s.Repo.Store.Get(ctx, &model.JobDefinition{ID: "foreign-definition"}))
	_, err = s.Workspace(ctx, p, w.ID)
	require.Error(t, err)
}

// The deletion path uses SQL and the injected namespace callback; it does not
// need account registration, Redis, or a listening network port.
func TestWorkspaceDeletionCleansOnlyOwnedDefinitions(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "workspace-definition.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(
		&model.Workspace{}, &model.WorkspaceMember{}, &model.WorkspaceInvitation{},
		&model.Applications{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.JobDefinition{},
		&model.JobArtifact{}, &model.ArtifactChunk{}, &model.JobDelivery{}, &model.JobSandbox{}, &model.JobCheckpoint{},
	))
	store := &sqlstore.Driver{Client: *db}
	service := New(store, nil, nil, nil)
	ctx := context.Background()
	workspace := &model.Workspace{ID: "team", Kind: "team", OwnerID: "owner", Namespace: "team-ns"}
	for _, row := range []datastore.Entity{
		workspace,
		&model.WorkspaceMember{ID: "owner-member", WorkspaceID: workspace.ID, UserID: "owner", Role: "admin"},
		&model.JobDefinition{ID: "team-definition", WorkspaceID: workspace.ID, Name: "reusable", Revision: 1, Spec: "{}"},
		&model.JobDefinition{ID: "foreign-definition", WorkspaceID: "foreign", Name: "reusable", Revision: 1, Spec: "{}"},
	} {
		require.NoError(t, store.Add(ctx, row))
	}
	now, err := store.CurrentDatabaseTime(ctx)
	require.NoError(t, err)
	principal := &Principal{User: &model.User{ID: "owner"}, Session: &model.Session{AuthenticatedAt: now.Add(-time.Second)}}
	calls := 0
	require.NoError(t, service.DeleteWorkspace(ctx, principal, workspace.ID, func(_ context.Context, deleted *model.Workspace) error {
		calls++
		require.Equal(t, workspace.ID, deleted.ID)
		return nil
	}))
	require.Equal(t, 1, calls)
	require.ErrorIs(t, store.Get(ctx, &model.Workspace{ID: workspace.ID}), datastore.ErrRecordNotExist)
	require.ErrorIs(t, store.Get(ctx, &model.JobDefinition{ID: "team-definition"}), datastore.ErrRecordNotExist)
	require.NoError(t, store.Get(ctx, &model.JobDefinition{ID: "foreign-definition"}))
}

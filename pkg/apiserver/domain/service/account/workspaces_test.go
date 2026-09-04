package account

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
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
	_, err = s.Workspace(ctx, p, w.ID)
	require.Error(t, err)
}

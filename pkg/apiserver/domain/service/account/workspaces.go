package account

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/google/uuid"
)

type WorkspaceAccess struct {
	Workspace *model.Workspace `json:"workspace"`
	Role      string           `json:"role"`
}

func workspaceRole(ctx context.Context, r repository.Accounts, p *Principal, w *model.Workspace) (string, error) {
	m := &model.WorkspaceMember{WorkspaceID: w.ID, UserID: p.User.ID}
	if err := r.One(ctx, m); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return "", bcode.ErrForbidden
		}
		return "", err
	}
	if w.OwnerID == p.User.ID {
		return "owner", nil
	}
	if m.Role != "admin" && m.Role != "member" && m.Role != "viewer" {
		return "", bcode.ErrForbidden
	}
	return m.Role, nil
}

func (s *Service) Workspace(ctx context.Context, p *Principal, id string) (*WorkspaceAccess, error) {
	w := &model.Workspace{ID: id}
	if id == "" {
		w.OwnerID = p.User.ID
		w.Kind = "personal"
	}
	if err := s.Repo.One(ctx, w); err != nil {
		return nil, err
	}
	role, err := workspaceRole(ctx, s.Repo, p, w)
	if err != nil {
		return nil, err
	}
	return &WorkspaceAccess{Workspace: w, Role: role}, nil
}

func (s *Service) Workspaces(ctx context.Context, p *Principal) ([]*WorkspaceAccess, error) {
	rows, err := s.Repo.Store.List(ctx, &model.WorkspaceMember{UserID: p.User.ID}, &datastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*WorkspaceAccess, 0, len(rows))
	for _, row := range rows {
		m := row.(*model.WorkspaceMember)
		a, e := s.Workspace(ctx, p, m.WorkspaceID)
		if e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, nil
}

func (s *Service) CreateWorkspace(ctx context.Context, p *Principal, name string) (*model.Workspace, error) {
	name = strings.TrimSpace(name)
	if name == "" || len([]rune(name)) > 128 {
		return nil, bcode.ErrAccountInput
	}
	w := &model.Workspace{ID: uuid.NewString(), Name: name, Kind: "team", OwnerID: p.User.ID}
	w.Namespace = "eruun-ws-" + strings.ReplaceAll(w.ID, "-", "")
	err := s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		u := &model.User{ID: p.User.ID}
		if e := r.Lock(ctx, u); e != nil {
			return e
		}
		if u.Disabled {
			return bcode.ErrUnauthorized
		}
		if e := r.Store.Add(ctx, w); e != nil {
			return e
		}
		return r.Store.Add(ctx, &model.WorkspaceMember{ID: uuid.NewString(), WorkspaceID: w.ID, UserID: u.ID, Role: "admin"})
	})
	return w, err
}

func (s *Service) teamMutation(ctx context.Context, p *Principal, id string, fn func(repository.Accounts, *model.Workspace, string) error) error {
	return s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		w := &model.Workspace{ID: id}
		if e := r.Lock(ctx, w); e != nil {
			return e
		}
		if w.Kind != "team" {
			return bcode.ErrForbidden
		}
		role, e := workspaceRole(ctx, r, p, w)
		if e != nil {
			return e
		}
		return fn(r, w, role)
	})
}

func (s *Service) RenameWorkspace(ctx context.Context, p *Principal, id, name string) error {
	name = strings.TrimSpace(name)
	if name == "" || len([]rune(name)) > 128 {
		return bcode.ErrAccountInput
	}
	return s.teamMutation(ctx, p, id, func(r repository.Accounts, w *model.Workspace, role string) error {
		if role != "owner" && role != "admin" {
			return bcode.ErrForbidden
		}
		return r.Update(ctx, w, map[string]interface{}{"name": name})
	})
}

func (s *Service) Members(ctx context.Context, p *Principal, id string) ([]*model.WorkspaceMember, error) {
	a, e := s.Workspace(ctx, p, id)
	if e != nil {
		return nil, e
	}
	rows, e := s.Repo.Store.List(ctx, &model.WorkspaceMember{WorkspaceID: id}, &datastore.ListOptions{})
	if e != nil {
		return nil, e
	}
	out := make([]*model.WorkspaceMember, 0, len(rows))
	for _, row := range rows {
		m := row.(*model.WorkspaceMember)
		if m.UserID == a.Workspace.OwnerID {
			m.Role = "owner"
		}
		out = append(out, m)
	}
	return out, nil
}

func canAssign(role, target string) bool {
	return (role == "owner" && (target == "admin" || target == "member" || target == "viewer")) || (role == "admin" && (target == "member" || target == "viewer"))
}

func (s *Service) UpdateMember(ctx context.Context, p *Principal, id, userID, role string, remove bool) error {
	return s.teamMutation(ctx, p, id, func(r repository.Accounts, w *model.Workspace, actor string) error {
		if userID == w.OwnerID {
			return bcode.ErrForbidden
		}
		m := &model.WorkspaceMember{WorkspaceID: id, UserID: userID}
		if e := r.One(ctx, m); e != nil {
			return e
		}
		if remove && userID == p.User.ID {
			return r.Store.Delete(ctx, m)
		}
		if !canAssign(actor, m.Role) || (!remove && !canAssign(actor, role)) {
			return bcode.ErrForbidden
		}
		if remove {
			return r.Store.Delete(ctx, m)
		}
		return r.Update(ctx, m, map[string]interface{}{"role": role})
	})
}

func (s *Service) TransferWorkspace(ctx context.Context, p *Principal, id, userID string) error {
	if !p.Recent(s.Now()) {
		return bcode.ErrAccountRecentAuth
	}
	return s.teamMutation(ctx, p, id, func(r repository.Accounts, w *model.Workspace, role string) error {
		if role != "owner" || userID == w.OwnerID {
			return bcode.ErrForbidden
		}
		member := &model.WorkspaceMember{WorkspaceID: id, UserID: userID}
		if e := r.One(ctx, member); e != nil {
			return e
		}
		u := &model.User{ID: userID}
		if e := r.One(ctx, u); e != nil {
			return e
		}
		if u.Disabled {
			return bcode.ErrAccountConflict
		}
		previous := &model.WorkspaceMember{WorkspaceID: id, UserID: w.OwnerID}
		if e := r.One(ctx, previous); e != nil {
			return e
		}
		if e := r.Update(ctx, previous, map[string]interface{}{"role": "admin"}); e != nil {
			return e
		}
		return r.Update(ctx, w, map[string]interface{}{"owner_id": userID})
	})
}

func (s *Service) Invite(ctx context.Context, p *Principal, id, email, role string) (*model.WorkspaceInvitation, error) {
	email, err := NormalizeIdentity("email", email)
	if err != nil {
		return nil, err
	}
	if s.Config.SMTP.Host == "" {
		return nil, bcode.ErrServiceUnavailable
	}
	if err = s.RateLimit(ctx, "invite:"+p.User.ID, 20, time.Hour); err != nil {
		return nil, err
	}
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	var invitation *model.WorkspaceInvitation
	err = s.teamMutation(ctx, p, id, func(r repository.Accounts, w *model.Workspace, actor string) error {
		if !canAssign(actor, role) {
			return bcode.ErrForbidden
		}
		existing := &model.WorkspaceInvitation{WorkspaceID: id, Email: email}
		e := r.One(ctx, existing)
		if e == nil {
			if existing.AcceptedBy != "" {
				member := &model.WorkspaceMember{WorkspaceID: id, UserID: existing.AcceptedBy}
				if e := r.One(ctx, member); e == nil {
					return bcode.ErrAccountConflict
				} else if !errors.Is(e, datastore.ErrRecordNotExist) {
					return e
				}
			}
			if !canAssign(actor, existing.Role) {
				return bcode.ErrForbidden
			}
			if e = r.Update(ctx, existing, map[string]interface{}{"accepted_by": "", "role": role, "token_hash": tokenHash(token), "expires_at": s.Now().UTC().Add(7 * 24 * time.Hour)}); e != nil {
				return e
			}
			existing.Role = role
			existing.AcceptedBy = ""
			existing.ExpiresAt = s.Now().UTC().Add(7 * 24 * time.Hour)
			invitation = existing
		} else if errors.Is(e, datastore.ErrRecordNotExist) {
			invitation = &model.WorkspaceInvitation{ID: uuid.NewString(), WorkspaceID: id, Email: email, Role: role, TokenHash: tokenHash(token), ExpiresAt: s.Now().UTC().Add(7 * 24 * time.Hour)}
			if e = r.Store.Add(ctx, invitation); e != nil {
				return e
			}
		} else {
			return e
		}
		// A fragment keeps the invitation credential out of HTTP request logs.
		link := s.Config.FrontendURL + "#invitation=" + url.QueryEscape(token)
		if e = s.Delivery.SendInvitation(ctx, email, link); e != nil {
			return bcode.ErrAccountDelivery
		}
		return nil
	})
	return invitation, err
}

func (s *Service) AcceptInvitation(ctx context.Context, p *Principal, token string) (*model.Workspace, error) {
	if len(token) != 43 {
		return nil, bcode.ErrAccountInput
	}
	inv := &model.WorkspaceInvitation{TokenHash: tokenHash(token)}
	if e := s.Repo.One(ctx, inv); e != nil {
		return nil, e
	}
	var result *model.Workspace
	err := s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		w := &model.Workspace{ID: inv.WorkspaceID}
		if e := r.Lock(ctx, w); e != nil {
			return e
		}
		if w.Kind != "team" {
			return bcode.ErrForbidden
		}
		// All invitation mutations take the workspace lock, including resends.
		current := &model.WorkspaceInvitation{ID: inv.ID, TokenHash: tokenHash(token)}
		if e := r.One(ctx, current); e != nil {
			return e
		}
		if current.AcceptedBy != "" {
			if current.AcceptedBy != p.User.ID {
				return bcode.ErrForbidden
			}
			if e := r.One(ctx, &model.WorkspaceMember{WorkspaceID: w.ID, UserID: p.User.ID}); e != nil {
				return bcode.ErrForbidden
			}
			result = w
			return nil
		}
		if !current.ExpiresAt.After(s.Now()) {
			return bcode.ErrAccountInput
		}
		email := &model.Identity{UserID: p.User.ID, Provider: "email", Subject: current.Email}
		if e := r.One(ctx, email); e != nil {
			if errors.Is(e, datastore.ErrRecordNotExist) {
				return bcode.ErrForbidden
			}
			return e
		}
		member := &model.WorkspaceMember{WorkspaceID: w.ID, UserID: p.User.ID}
		e := r.One(ctx, member)
		if errors.Is(e, datastore.ErrRecordNotExist) {
			if e = r.Store.Add(ctx, &model.WorkspaceMember{ID: uuid.NewString(), WorkspaceID: w.ID, UserID: p.User.ID, Role: current.Role}); e != nil {
				return e
			}
		} else if e != nil {
			return e
		}
		if e = r.Update(ctx, current, map[string]interface{}{"accepted_by": p.User.ID}); e != nil {
			return e
		}
		result = w
		return nil
	})
	return result, err
}

func (s *Service) RevokeInvitation(ctx context.Context, p *Principal, id, invitationID string) error {
	return s.teamMutation(ctx, p, id, func(r repository.Accounts, w *model.Workspace, role string) error {
		inv := &model.WorkspaceInvitation{ID: invitationID, WorkspaceID: id}
		if e := r.One(ctx, inv); e != nil {
			return e
		}
		if !canAssign(role, inv.Role) {
			return bcode.ErrForbidden
		}
		return r.Store.Delete(ctx, inv)
	})
}

// Namespace removal is injected by the API assembly, keeping Kubernetes IO out
// of the account model. The workspace lock also serializes application creation.
func (s *Service) DeleteWorkspace(ctx context.Context, p *Principal, id string, deleteNamespace func(context.Context, *model.Workspace) error) error {
	if !p.Recent(s.Now()) {
		return bcode.ErrAccountRecentAuth
	}
	return s.teamMutation(ctx, p, id, func(r repository.Accounts, w *model.Workspace, role string) error {
		if role != "owner" {
			return bcode.ErrForbidden
		}
		n, e := r.Store.Count(ctx, &model.Applications{WorkspaceID: id}, nil)
		if e != nil {
			return e
		}
		if n != 0 {
			return bcode.ErrWorkspaceNotEmpty
		}
		if deleteNamespace == nil {
			return fmt.Errorf("namespace deletion is unavailable")
		}
		if e = deleteNamespace(ctx, w); e != nil {
			return e
		}
		for _, entity := range []datastore.Entity{&model.WorkspaceInvitation{WorkspaceID: id}, &model.WorkspaceMember{WorkspaceID: id}} {
			if e = r.Store.DeleteByFilter(ctx, entity, nil); e != nil {
				return e
			}
		}
		return r.Store.Delete(ctx, w)
	})
}

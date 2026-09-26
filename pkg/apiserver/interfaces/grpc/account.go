package grpcapi

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AccountServer is a transport adapter for the existing account service.
type AccountServer struct {
	eruunv1.UnimplementedAccountServiceServer
	Accounts   *account.Service
	Namespaces *workspace.Manager
}

func principal(ctx context.Context) (*account.Principal, error) {
	p, _ := ctx.Value(principalContextKey{}).(*account.Principal)
	if p == nil {
		return nil, bcode.ErrUnauthorized
	}
	return p, nil
}

func adminPrincipal(ctx context.Context) (*account.Principal, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, err
	}
	if !p.User.SystemAdmin {
		return nil, bcode.ErrForbidden
	}
	return p, nil
}

func timeMessage(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func userMessage(u *model.User) *eruunv1.User {
	if u == nil {
		return nil
	}
	return &eruunv1.User{
		Id: u.ID, Name: u.Name, SystemAdmin: u.SystemAdmin,
		Disabled: u.Disabled, MustChangePassword: u.MustChangePassword,
		CreateTime: timeMessage(u.CreateTime), UpdateTime: timeMessage(u.UpdateTime),
	}
}

func loginMessage(result *account.Login) *eruunv1.LoginResponse {
	if result == nil {
		return &eruunv1.LoginResponse{}
	}
	return &eruunv1.LoginResponse{
		AccessToken: result.AccessToken, TokenType: result.TokenType,
		ExpiresIn: result.ExpiresIn, User: userMessage(result.User),
		RefreshToken: result.RefreshToken,
	}
}

func workspaceMessage(w *model.Workspace) *eruunv1.Workspace {
	if w == nil {
		return nil
	}
	return &eruunv1.Workspace{
		Id: w.ID, Name: w.Name, Kind: w.Kind, OwnerId: w.OwnerID,
		Namespace: w.Namespace, CreateTime: timeMessage(w.CreateTime),
		UpdateTime: timeMessage(w.UpdateTime),
	}
}

func workspaceAccessMessage(a *account.WorkspaceAccess) *eruunv1.WorkspaceAccess {
	if a == nil {
		return nil
	}
	return &eruunv1.WorkspaceAccess{Workspace: workspaceMessage(a.Workspace), Role: a.Role}
}

func (s *AccountServer) GetAuthMethods(ctx context.Context, _ *emptypb.Empty) (*eruunv1.AuthMethodsResponse, error) {
	if s.Accounts == nil || s.Accounts.Config == nil {
		return nil, rpcError(fmt.Errorf("account configuration is not initialized"))
	}
	cfg := s.Accounts.Config
	return &eruunv1.AuthMethodsResponse{
		Password: true, Email: cfg.SMTP.Host != "", Phone: cfg.SMS.AccessKeyID != "",
		Google: cfg.Google.Enabled, Github: cfg.GitHub.Enabled,
	}, nil
}

func (s *AccountServer) SendAuthCode(ctx context.Context, req *eruunv1.AuthCodeRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	if req.Purpose == "bind" {
		if _, err := principal(ctx); err != nil {
			return nil, rpcError(err)
		}
	}
	ip, err := clientIP(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := s.Accounts.SendCode(ctx, req.Purpose, req.Provider, req.Identifier, ip); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *AccountServer) Register(ctx context.Context, req *eruunv1.RegisterRequest) (*eruunv1.LoginResponse, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	result, err := s.Accounts.Register(ctx, req.Provider, req.Identifier, req.Code, req.Password, req.Name)
	return loginMessage(result), rpcError(err)
}

func (s *AccountServer) Login(ctx context.Context, req *eruunv1.LoginRequest) (*eruunv1.LoginResponse, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	identifier, err := account.NormalizeIdentity(req.Provider, req.Identifier)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := s.Accounts.RateLimit(ctx, "login:"+req.Provider+":"+identifier, 10, 15*time.Minute); err != nil {
		return nil, rpcError(err)
	}
	result, err := s.Accounts.Login(ctx, req.Provider, req.Identifier, req.Password, req.Code)
	return loginMessage(result), rpcError(err)
}

func (s *AccountServer) Refresh(ctx context.Context, req *eruunv1.RefreshRequest) (*eruunv1.LoginResponse, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	result, err := s.Accounts.Refresh(ctx, req.RefreshToken)
	return loginMessage(result), rpcError(err)
}

func (s *AccountServer) Logout(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.Logout(ctx, p))
}

func (s *AccountServer) GetMe(ctx context.Context, _ *emptypb.Empty) (*eruunv1.MeResponse, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	spaces, err := s.Accounts.Workspaces(ctx, p)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &eruunv1.MeResponse{User: userMessage(p.User)}
	for _, a := range spaces {
		resp.Workspaces = append(resp.Workspaces, workspaceAccessMessage(a))
	}
	return resp, nil
}

func (s *AccountServer) ChangePassword(ctx context.Context, req *eruunv1.ChangePasswordRequest) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.ChangePassword(ctx, p, req.Password))
}

func (s *AccountServer) ResetPassword(ctx context.Context, req *eruunv1.ResetPasswordRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.ResetPassword(ctx, req.Provider, req.Identifier, req.Code, req.Password))
}

func (s *AccountServer) ListIdentities(ctx context.Context, _ *emptypb.Empty) (*eruunv1.ListIdentitiesResponse, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	items, err := s.Accounts.Identities(ctx, p)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &eruunv1.ListIdentitiesResponse{}
	for _, item := range items {
		identity, ok := item.(*model.Identity)
		if !ok {
			return nil, rpcError(fmt.Errorf("identity list contains %T", item))
		}
		resp.Identities = append(resp.Identities, &eruunv1.Identity{
			Id: identity.ID, Provider: identity.Provider, Subject: identity.Subject,
			CreateTime: timeMessage(identity.CreateTime), UpdateTime: timeMessage(identity.UpdateTime),
		})
	}
	return resp, nil
}

func (s *AccountServer) BindIdentity(ctx context.Context, req *eruunv1.BindIdentityRequest) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.Bind(ctx, p, req.Provider, req.Identifier, req.Code))
}

func (s *AccountServer) UnbindIdentity(ctx context.Context, req *eruunv1.IdentityIDRequest) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.Unbind(ctx, p, req.IdentityId))
}

func (s *AccountServer) ListWorkspaces(ctx context.Context, _ *emptypb.Empty) (*eruunv1.ListWorkspacesResponse, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	spaces, err := s.Accounts.Workspaces(ctx, p)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &eruunv1.ListWorkspacesResponse{}
	for _, a := range spaces {
		resp.Workspaces = append(resp.Workspaces, workspaceAccessMessage(a))
	}
	return resp, nil
}

func (s *AccountServer) CreateWorkspace(ctx context.Context, req *eruunv1.WorkspaceNameRequest) (*eruunv1.Workspace, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	w, err := s.Accounts.CreateWorkspace(ctx, p, req.Name)
	return workspaceMessage(w), rpcError(err)
}

func (s *AccountServer) GetWorkspace(ctx context.Context, req *eruunv1.WorkspaceIDRequest) (*eruunv1.WorkspaceAccess, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	a, err := s.Accounts.Workspace(ctx, p, req.WorkspaceId)
	return workspaceAccessMessage(a), rpcError(err)
}

func (s *AccountServer) RenameWorkspace(ctx context.Context, req *eruunv1.RenameWorkspaceRequest) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.RenameWorkspace(ctx, p, req.WorkspaceId, req.Name))
}

func (s *AccountServer) DeleteWorkspace(ctx context.Context, req *eruunv1.WorkspaceIDRequest) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	if s.Namespaces == nil {
		return nil, rpcError(fmt.Errorf("workspace namespace manager is not initialized"))
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.DeleteWorkspace(ctx, p, req.WorkspaceId, s.Namespaces.DeleteEmpty))
}

func (s *AccountServer) ListWorkspaceMembers(ctx context.Context, req *eruunv1.WorkspaceIDRequest) (*eruunv1.ListWorkspaceMembersResponse, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	members, err := s.Accounts.Members(ctx, p, req.WorkspaceId)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &eruunv1.ListWorkspaceMembersResponse{}
	for _, m := range members {
		resp.Members = append(resp.Members, &eruunv1.WorkspaceMember{
			Id: m.ID, WorkspaceId: m.WorkspaceID, UserId: m.UserID, Role: m.Role,
			CreateTime: timeMessage(m.CreateTime), UpdateTime: timeMessage(m.UpdateTime),
		})
	}
	return resp, nil
}

func (s *AccountServer) UpdateWorkspaceMember(ctx context.Context, req *eruunv1.UpdateWorkspaceMemberRequest) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.UpdateMember(ctx, p, req.WorkspaceId, req.UserId, req.Role, false))
}

func (s *AccountServer) RemoveWorkspaceMember(ctx context.Context, req *eruunv1.WorkspaceMemberIDRequest) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.UpdateMember(ctx, p, req.WorkspaceId, req.UserId, "", true))
}

func (s *AccountServer) TransferWorkspace(ctx context.Context, req *eruunv1.TransferWorkspaceRequest) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.TransferWorkspace(ctx, p, req.WorkspaceId, req.UserId))
}

func (s *AccountServer) InviteWorkspaceMember(ctx context.Context, req *eruunv1.InviteWorkspaceMemberRequest) (*eruunv1.WorkspaceInvitation, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	invitation, err := s.Accounts.Invite(ctx, p, req.WorkspaceId, req.Email, req.Role)
	if err != nil {
		return nil, rpcError(err)
	}
	return &eruunv1.WorkspaceInvitation{
		Id: invitation.ID, WorkspaceId: invitation.WorkspaceID,
		Email: invitation.Email, Role: invitation.Role,
		ExpiresAt: timeMessage(invitation.ExpiresAt), AcceptedBy: invitation.AcceptedBy,
		CreateTime: timeMessage(invitation.CreateTime), UpdateTime: timeMessage(invitation.UpdateTime),
	}, nil
}

func (s *AccountServer) RevokeWorkspaceInvitation(ctx context.Context, req *eruunv1.WorkspaceInvitationIDRequest) (*emptypb.Empty, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.RevokeInvitation(ctx, p, req.WorkspaceId, req.InvitationId))
}

func (s *AccountServer) AcceptWorkspaceInvitation(ctx context.Context, req *eruunv1.AcceptWorkspaceInvitationRequest) (*eruunv1.Workspace, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	w, err := s.Accounts.AcceptInvitation(ctx, p, req.Token)
	return workspaceMessage(w), rpcError(err)
}

func (s *AccountServer) ListAdminUsers(ctx context.Context, req *eruunv1.ListAdminUsersRequest) (*eruunv1.ListAdminUsersResponse, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	page, size := int32(1), int32(20)
	if req != nil {
		if req.Page != nil {
			page = req.GetPage()
		}
		if req.PageSize != nil {
			size = req.GetPageSize()
		}
	}
	items, err := s.Accounts.ListAdminUsers(ctx, p, int(page), int(size))
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &eruunv1.ListAdminUsersResponse{}
	for _, u := range items {
		resp.Users = append(resp.Users, userMessage(u))
	}
	return resp, nil
}

func (s *AccountServer) SetAdminUserDisabled(ctx context.Context, req *eruunv1.SetAdminUserDisabledRequest) (*emptypb.Empty, error) {
	p, err := adminPrincipal(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	if req == nil || req.Disabled == nil || strings.TrimSpace(req.UserId) == "" {
		return nil, rpcError(bcode.ErrAccountInput)
	}
	return &emptypb.Empty{}, rpcError(s.Accounts.SetDisabled(ctx, p, req.UserId, req.GetDisabled()))
}

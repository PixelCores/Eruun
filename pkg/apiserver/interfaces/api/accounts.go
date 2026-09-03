package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/middleware"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
)

const refreshCookie = "__Secure-eruun-refresh"

type accounts struct {
	Accounts   *account.Service   `inject:""`
	Namespaces *workspace.Manager `inject:""`
}

func NewAccounts() Interface { return &accounts{} }

func (a *accounts) RegisterRoutes(g *gin.RouterGroup) {
	g.GET("/auth/methods", a.methods)
	g.POST("/auth/codes", a.codes)
	g.POST("/auth/register", a.register)
	g.POST("/auth/login", a.login)
	g.POST("/auth/oauth2/:provider/start", a.oauthStart)
	g.POST("/auth/oauth2/:provider/callback", a.oauthCallback)
	g.POST("/auth/refresh", a.refresh)
	g.POST("/auth/logout", a.logout)
	g.GET("/auth/me", a.me)
	g.PUT("/auth/password", a.password)
	g.POST("/auth/password/reset", a.resetPassword)
	g.GET("/auth/identities", a.identities)
	g.POST("/auth/identities", a.bind)
	g.DELETE("/auth/identities/:identityID", a.unbind)
	g.GET("/workspaces", a.listWorkspaces)
	g.POST("/workspaces", a.createWorkspace)
	g.GET("/workspaces/:workspaceID", a.getWorkspace)
	g.PATCH("/workspaces/:workspaceID", a.renameWorkspace)
	g.DELETE("/workspaces/:workspaceID", a.deleteWorkspace)
	g.GET("/workspaces/:workspaceID/members", a.members)
	g.PATCH("/workspaces/:workspaceID/members/:userID", a.updateMember)
	g.DELETE("/workspaces/:workspaceID/members/:userID", a.removeMember)
	g.POST("/workspaces/:workspaceID/transfer", a.transfer)
	g.POST("/workspaces/:workspaceID/invitations", a.invite)
	g.DELETE("/workspaces/:workspaceID/invitations/:invitationID", a.revokeInvitation)
	g.POST("/workspace-invitations/accept", a.acceptInvitation)
	g.GET("/admin/users", a.users)
	g.PATCH("/admin/users/:userID", a.userStatus)
}

func bindAccount[T any](c *gin.Context) (*T, bool) {
	var body T
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	if d.Decode(&body) != nil {
		bcode.ReturnError(c, bcode.ErrAccountInput)
		return nil, false
	}
	var extra interface{}
	if d.Decode(&extra) != io.EOF {
		bcode.ReturnError(c, bcode.ErrAccountInput)
		return nil, false
	}
	return &body, true
}
func accountResult(c *gin.Context, value interface{}, err error) {
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	bcode.ReturnSuccess(c, value)
}
func accountCookie(c *gin.Context, name, value, path string, maxAge int) {
	http.SetCookie(c.Writer, &http.Cookie{Name: name, Value: value, Path: path, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: maxAge})
}
func accountLoginResult(c *gin.Context, result *account.Login, err error) {
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	if result == nil {
		bcode.ReturnSuccess(c, nil)
		return
	}
	accountCookie(c, refreshCookie, result.RefreshToken, "/api/v1/auth", int((30 * 24 * time.Hour).Seconds()))
	bcode.ReturnSuccess(c, result)
}

func (a *accounts) methods(c *gin.Context) {
	cfg := a.Accounts.Config
	bcode.ReturnSuccess(c, gin.H{"password": true, "email": cfg.SMTP.Host != "", "phone": cfg.SMS.AccessKeyID != "", "google": cfg.Google.Enabled, "github": cfg.GitHub.Enabled})
}
func (a *accounts) codes(c *gin.Context) {
	r, ok := bindAccount[apis.AccountCodeRequest](c)
	if !ok {
		return
	}
	if r.Purpose == "bind" && middleware.Principal(c) == nil {
		bcode.ReturnError(c, bcode.ErrUnauthorized)
		return
	}
	accountResult(c, nil, a.Accounts.SendCode(c.Request.Context(), r.Purpose, r.Provider, r.Identifier, c.ClientIP()))
}
func (a *accounts) register(c *gin.Context) {
	r, ok := bindAccount[apis.AccountRegisterRequest](c)
	if !ok {
		return
	}
	v, e := a.Accounts.Register(c.Request.Context(), r.Provider, r.Identifier, r.Code, r.Password, r.Name)
	accountLoginResult(c, v, e)
}
func (a *accounts) login(c *gin.Context) {
	r, ok := bindAccount[apis.AccountLoginRequest](c)
	if !ok {
		return
	}
	identifier, err := account.NormalizeIdentity(r.Provider, r.Identifier)
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	if e := a.Accounts.RateLimit(c.Request.Context(), "login:"+r.Provider+":"+identifier, 10, 15*time.Minute); e != nil {
		bcode.ReturnError(c, e)
		return
	}
	v, e := a.Accounts.Login(c.Request.Context(), r.Provider, r.Identifier, r.Password, r.Code)
	accountLoginResult(c, v, e)
}
func (a *accounts) oauthStart(c *gin.Context) {
	r, ok := bindAccount[apis.OAuthStartRequest](c)
	if !ok {
		return
	}
	var p *account.Principal
	if r.Link {
		p = middleware.Principal(c)
		if p == nil {
			bcode.ReturnError(c, bcode.ErrUnauthorized)
			return
		}
	}
	provider := c.Param("provider")
	address, browser, e := a.Accounts.OAuthStart(c.Request.Context(), provider, p)
	if e != nil {
		bcode.ReturnError(c, e)
		return
	}
	accountCookie(c, "__Secure-eruun-oauth-"+provider, browser, "/api/v1/auth/oauth2", 300)
	bcode.ReturnSuccess(c, gin.H{"authorizationURL": address})
}
func (a *accounts) oauthCallback(c *gin.Context) {
	r, ok := bindAccount[apis.OAuthCallbackRequest](c)
	if !ok {
		return
	}
	provider := c.Param("provider")
	if provider != "google" && provider != "github" {
		bcode.ReturnError(c, bcode.ErrAccountInput)
		return
	}
	name := "__Secure-eruun-oauth-" + provider
	browser, _ := c.Cookie(name)
	accountCookie(c, name, "", "/api/v1/auth/oauth2", -1)
	if r.Error != "" {
		r.Code = ""
	}
	v, e := a.Accounts.OAuthCallback(c.Request.Context(), provider, r.Code, r.State, browser, middleware.Principal(c))
	accountLoginResult(c, v, e)
}
func (a *accounts) refresh(c *gin.Context) {
	token, _ := c.Cookie(refreshCookie)
	v, e := a.Accounts.Refresh(c.Request.Context(), token)
	accountLoginResult(c, v, e)
}
func (a *accounts) logout(c *gin.Context) {
	e := a.Accounts.Logout(c.Request.Context(), middleware.Principal(c))
	accountCookie(c, refreshCookie, "", "/api/v1/auth", -1)
	accountResult(c, nil, e)
}
func (a *accounts) me(c *gin.Context) {
	p := middleware.Principal(c)
	spaces, e := a.Accounts.Workspaces(c.Request.Context(), p)
	accountResult(c, gin.H{"user": p.User, "workspaces": spaces}, e)
}
func (a *accounts) password(c *gin.Context) {
	r, ok := bindAccount[apis.AccountPasswordRequest](c)
	if !ok {
		return
	}
	e := a.Accounts.ChangePassword(c.Request.Context(), middleware.Principal(c), r.Password)
	if e == nil {
		accountCookie(c, refreshCookie, "", "/api/v1/auth", -1)
	}
	accountResult(c, nil, e)
}
func (a *accounts) resetPassword(c *gin.Context) {
	r, ok := bindAccount[apis.AccountResetRequest](c)
	if !ok {
		return
	}
	accountResult(c, nil, a.Accounts.ResetPassword(c.Request.Context(), r.Provider, r.Identifier, r.Code, r.Password))
}
func (a *accounts) identities(c *gin.Context) {
	v, e := a.Accounts.Identities(c.Request.Context(), middleware.Principal(c))
	accountResult(c, v, e)
}
func (a *accounts) bind(c *gin.Context) {
	r, ok := bindAccount[apis.AccountIdentityRequest](c)
	if !ok {
		return
	}
	accountResult(c, nil, a.Accounts.Bind(c.Request.Context(), middleware.Principal(c), r.Provider, r.Identifier, r.Code))
}
func (a *accounts) unbind(c *gin.Context) {
	accountResult(c, nil, a.Accounts.Unbind(c.Request.Context(), middleware.Principal(c), c.Param("identityID")))
}
func (a *accounts) listWorkspaces(c *gin.Context) {
	v, e := a.Accounts.Workspaces(c.Request.Context(), middleware.Principal(c))
	accountResult(c, v, e)
}
func (a *accounts) createWorkspace(c *gin.Context) {
	r, ok := bindAccount[apis.WorkspaceRequest](c)
	if !ok {
		return
	}
	v, e := a.Accounts.CreateWorkspace(c.Request.Context(), middleware.Principal(c), r.Name)
	accountResult(c, v, e)
}
func (a *accounts) getWorkspace(c *gin.Context) {
	v, e := a.Accounts.Workspace(c.Request.Context(), middleware.Principal(c), c.Param("workspaceID"))
	accountResult(c, v, e)
}
func (a *accounts) renameWorkspace(c *gin.Context) {
	r, ok := bindAccount[apis.WorkspaceRequest](c)
	if !ok {
		return
	}
	accountResult(c, nil, a.Accounts.RenameWorkspace(c.Request.Context(), middleware.Principal(c), c.Param("workspaceID"), r.Name))
}
func (a *accounts) deleteWorkspace(c *gin.Context) {
	accountResult(c, nil, a.Accounts.DeleteWorkspace(c.Request.Context(), middleware.Principal(c), c.Param("workspaceID"), a.Namespaces.DeleteEmpty))
}
func (a *accounts) members(c *gin.Context) {
	v, e := a.Accounts.Members(c.Request.Context(), middleware.Principal(c), c.Param("workspaceID"))
	accountResult(c, v, e)
}
func (a *accounts) updateMember(c *gin.Context) {
	r, ok := bindAccount[apis.WorkspaceMemberRequest](c)
	if !ok {
		return
	}
	accountResult(c, nil, a.Accounts.UpdateMember(c.Request.Context(), middleware.Principal(c), c.Param("workspaceID"), c.Param("userID"), r.Role, false))
}
func (a *accounts) removeMember(c *gin.Context) {
	accountResult(c, nil, a.Accounts.UpdateMember(c.Request.Context(), middleware.Principal(c), c.Param("workspaceID"), c.Param("userID"), "", true))
}
func (a *accounts) transfer(c *gin.Context) {
	r, ok := bindAccount[apis.WorkspaceTransferRequest](c)
	if !ok {
		return
	}
	accountResult(c, nil, a.Accounts.TransferWorkspace(c.Request.Context(), middleware.Principal(c), c.Param("workspaceID"), r.UserID))
}
func (a *accounts) invite(c *gin.Context) {
	r, ok := bindAccount[apis.WorkspaceInviteRequest](c)
	if !ok {
		return
	}
	v, e := a.Accounts.Invite(c.Request.Context(), middleware.Principal(c), c.Param("workspaceID"), r.Email, r.Role)
	accountResult(c, v, e)
}
func (a *accounts) revokeInvitation(c *gin.Context) {
	accountResult(c, nil, a.Accounts.RevokeInvitation(c.Request.Context(), middleware.Principal(c), c.Param("workspaceID"), c.Param("invitationID")))
}
func (a *accounts) acceptInvitation(c *gin.Context) {
	r, ok := bindAccount[apis.WorkspaceAcceptRequest](c)
	if !ok {
		return
	}
	v, e := a.Accounts.AcceptInvitation(c.Request.Context(), middleware.Principal(c), r.Token)
	accountResult(c, v, e)
}
func (a *accounts) users(c *gin.Context) {
	page, e := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, sizeErr := strconv.Atoi(c.DefaultQuery("pageSize", "20"))
	if e != nil || sizeErr != nil || page < 1 || size < 1 || size > 100 {
		bcode.ReturnError(c, bcode.ErrAccountInput)
		return
	}
	v, e := a.Accounts.Repo.Store.List(c.Request.Context(), &model.User{}, &datastore.ListOptions{Page: page, PageSize: size, SortBy: []datastore.SortOption{{Key: "id", Order: datastore.SortOrderAscending}}})
	accountResult(c, v, e)
}
func (a *accounts) userStatus(c *gin.Context) {
	r, ok := bindAccount[apis.AccountStatusRequest](c)
	if !ok {
		return
	}
	if r.Disabled == nil {
		bcode.ReturnError(c, bcode.ErrAccountInput)
		return
	}
	accountResult(c, nil, a.Accounts.SetDisabled(c.Request.Context(), middleware.Principal(c), c.Param("userID"), *r.Disabled))
}

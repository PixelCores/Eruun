package middleware

import (
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
)

const AuthPrincipalContextKey = "auth.principal"

type AuthOptions struct{ Accounts *account.Service }

func Principal(c *gin.Context) *account.Principal {
	p, _ := c.Get(AuthPrincipalContextKey)
	value, _ := p.(*account.Principal)
	return value
}
func DefaultAuthSkipPaths() []string {
	return []string{"/api/v1/health", "/api/v1/healthz", "/api/v1/ready", "/api/v1/readyz"}
}

var publicAccountRoutes = map[string]bool{
	"GET /api/v1/auth/methods":                    true,
	"POST /api/v1/auth/codes":                     true,
	"POST /api/v1/auth/register":                  true,
	"POST /api/v1/auth/login":                     true,
	"POST /api/v1/auth/password/reset":            true,
	"POST /api/v1/auth/refresh":                   true,
	"POST /api/v1/auth/oauth2/:provider/start":    true,
	"POST /api/v1/auth/oauth2/:provider/callback": true,
}
var privateAccountRoutes = map[string]bool{
	"GET /api/v1/auth/me": true, "POST /api/v1/auth/logout": true, "PUT /api/v1/auth/password": true,
	"GET /api/v1/auth/identities": true, "POST /api/v1/auth/identities": true, "DELETE /api/v1/auth/identities/:identityID": true,
	"GET /api/v1/workspaces": true, "POST /api/v1/workspaces": true, "GET /api/v1/workspaces/:workspaceID": true, "PATCH /api/v1/workspaces/:workspaceID": true, "DELETE /api/v1/workspaces/:workspaceID": true,
	"GET /api/v1/workspaces/:workspaceID/members": true, "PATCH /api/v1/workspaces/:workspaceID/members/:userID": true, "DELETE /api/v1/workspaces/:workspaceID/members/:userID": true,
	"POST /api/v1/workspaces/:workspaceID/invitations": true, "DELETE /api/v1/workspaces/:workspaceID/invitations/:invitationID": true,
	"POST /api/v1/workspace-invitations/accept": true, "POST /api/v1/workspaces/:workspaceID/transfer": true,
}

// Each registered business route must make an explicit authorization decision.
// A route added without a policy remains denied, including for administrators.
var businessRoutes = map[string]string{
	"GET /api/v1/applications":                                                "viewer",
	"GET /api/v1/applications/templates":                                      "member",
	"GET /api/v1/cronjobs":                                                    "member",
	"GET /api/v1/scheduledjobs":                                               "member",
	"POST /api/v1/applications":                                               "member",
	"POST /api/v1/applications/create-and-exec":                               "member",
	"POST /api/v1/applications/query":                                         "member",
	"POST /api/v1/applications/convert":                                       "member",
	"POST /api/v1/applications/import/namespace":                              "system",
	"POST /api/v1/applications/import/namespace/try":                          "system",
	"GET /api/v1/applications/:appID/workflows":                               "member",
	"GET /api/v1/applications/:appID/status":                                  "viewer",
	"GET /api/v1/applications/:appID/components":                              "member",
	"GET /api/v1/applications/:appID/components/status":                       "viewer",
	"GET /api/v1/applications/:appID/components/:componentName/containers":    "member",
	"POST /api/v1/applications/components/status":                             "member",
	"GET /api/v1/applications/:appID/components/:componentName/logs":          "member",
	"POST /api/v1/applications/:appID/components/:componentName/files/export": "member",
	"POST /api/v1/applications/:appID/components/:componentName/shell/exec":   "member",
	"POST /api/v1/applications/:appID/components/:componentName/shell/stream": "member",
	"DELETE /api/v1/applications/:appID":                                      "member",
	"PUT /api/v1/applications/:appID/workflow":                                "member",
	"GET /api/v1/applications/:appID/workflow/schedules":                      "member",
	"POST /api/v1/applications/:appID/workflow/schedule":                      "member",
	"DELETE /api/v1/applications/:appID/workflow/schedule/:workflowID":        "member",
	"POST /api/v1/applications/:appID/resources/cleanup-plan":                 "member",
	"DELETE /api/v1/applications/:appID/resources":                            "member",
	"POST /api/v1/applications/:appID/database-reset":                         "member",
	"POST /api/v1/applications/:appID/log-archives":                           "member",
	"POST /api/v1/applications/:appID/restart":                                "member",
	"POST /api/v1/applications/:appID/stop":                                   "member",
	"POST /api/v1/applications/:appID/start":                                  "member",
	"POST /api/v1/applications/:appID/workflow/exec":                          "member",
	"POST /api/v1/applications/:appID/workflow/cancel":                        "member",
	"POST /api/v1/applications/:appID/workflow/tasks/cancel-all":              "member",
	"GET /api/v1/applications/:appID/workflow/tasks":                          "member",
	"POST /api/v1/workflow/tasks/:taskID/approval":                            "member",
	"GET /api/v1/workflow/tasks/:taskID/status":                               "member",
	"GET /api/v1/workflow/tasks/:taskID/stages":                               "member",
	"POST /api/v1/applications/:appID/version":                                "member",
	"POST /api/v1/applications/:appID/version/diff-update":                    "member",
	"POST /api/v1/applications/:appID/version/cancel":                         "member",
	"POST /api/v1/applications/try":                                           "member",
	"POST /api/v1/applications/:appID/workflow/try":                           "member",
	"GET /api/v1/settings":                                                    "system",
	"GET /api/v1/settings/:type":                                              "system",
	"POST /api/v1/settings":                                                   "system",
	"PUT /api/v1/settings/:type":                                              "system",
	"DELETE /api/v1/settings/:type":                                           "system",
	"GET /api/v1/programming-languages":                                       "member",
	"GET /api/v1/programming-languages/:id":                                   "member",
	"POST /api/v1/programming-languages":                                      "system",
	"PUT /api/v1/programming-languages/:id":                                   "system",
	"DELETE /api/v1/programming-languages/:id":                                "system",
	"GET /api/v1/admin/users":                                                 "system", "PATCH /api/v1/admin/users/:userID": "system",
}

// HasAuthPolicy reports whether a registered route belongs to exactly one
// explicit authorization class, including health/readiness bypasses.
func HasAuthPolicy(method, path string) bool {
	route := method + " " + path
	classes := 0
	if method == "GET" {
		for _, health := range DefaultAuthSkipPaths() {
			if path == health {
				classes++
				break
			}
		}
	}
	if publicAccountRoutes[route] {
		classes++
	}
	if privateAccountRoutes[route] {
		classes++
	}
	if _, known := businessRoutes[route]; known {
		classes++
	}
	return classes == 1
}

func Auth(opts AuthOptions) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.FullPath()
		route := c.Request.Method + " " + path
		for _, health := range DefaultAuthSkipPaths() {
			if path == health && c.Request.Method == "GET" {
				c.Next()
				return
			}
		}
		s := opts.Accounts
		if s == nil {
			bcode.ReturnError(c, bcode.ErrServiceUnavailable)
			c.Abort()
			return
		}
		c.Header("Cache-Control", "no-store")
		if strings.HasPrefix(path, "/api/v1/auth/") {
			c.Header("Cache-Control", "no-store")
			c.Header("Pragma", "no-cache")
			if c.Request.Method != "GET" {
				origin := c.GetHeader("Origin")
				if !s.Config.AllowedOrigin(origin) {
					bcode.ReturnError(c, bcode.ErrForbidden)
					c.Abort()
					return
				}
			}
			if err := s.RateLimit(c.Request.Context(), "auth-ip:"+c.ClientIP(), 60, time.Minute); err != nil {
				bcode.ReturnError(c, err)
				c.Abort()
				return
			}
		}
		token := extractBearerToken(c.GetHeader("Authorization"))
		var p *account.Principal
		if token != "" {
			var err error
			p, err = s.Authenticate(c.Request.Context(), token)
			if err != nil {
				bcode.ReturnError(c, err)
				c.Abort()
				return
			}
			c.Set(AuthPrincipalContextKey, p)
		}
		if publicAccountRoutes[route] {
			c.Next()
			return
		}
		if p == nil {
			bcode.ReturnError(c, bcode.ErrUnauthorized)
			c.Abort()
			return
		}
		if p.User.MustChangePassword && route != "GET /api/v1/auth/me" && route != "PUT /api/v1/auth/password" && route != "POST /api/v1/auth/logout" {
			bcode.ReturnError(c, bcode.ErrAccountPasswordChange)
			c.Abort()
			return
		}
		if privateAccountRoutes[route] {
			c.Next()
			return
		}
		minimum, known := businessRoutes[route]
		if !known || (minimum == "system" && !p.User.SystemAdmin) {
			bcode.ReturnError(c, bcode.ErrForbidden)
			c.Abort()
			return
		}
		if minimum == "system" && !strings.HasPrefix(path, "/api/v1/applications/") {
			c.Next()
			return
		}
		a, err := s.Workspace(c.Request.Context(), p, c.GetHeader("X-Eruun-Workspace-ID"))
		if err != nil {
			bcode.ReturnError(c, err)
			c.Abort()
			return
		}
		if a.Role == "viewer" && minimum != "viewer" {
			bcode.ReturnError(c, bcode.ErrForbidden)
			c.Abort()
			return
		}
		scope := account.Scope{UserID: p.User.ID, WorkspaceID: a.Workspace.ID, Namespace: a.Workspace.Namespace, Role: a.Role, SystemAdmin: p.User.SystemAdmin}
		scope.ClusterOperation = minimum == "system"
		ctx := account.WithScope(c.Request.Context(), scope)
		c.Request = c.Request.WithContext(ctx)
		guard := account.NewStore(s.Repo.Store)
		if appID := c.Param("appID"); appID != "" {
			if err = guard.Get(ctx, &model.Applications{ID: appID}); err != nil {
				bcode.ReturnError(c, err)
				c.Abort()
				return
			}
		}
		if taskID := c.Param("taskID"); taskID != "" {
			if err = guard.Get(ctx, &model.WorkflowQueue{TaskID: taskID}); err != nil {
				bcode.ReturnError(c, err)
				c.Abort()
				return
			}
		}
		c.Next()
	}
}

func extractBearerToken(raw string) string {
	parts := strings.Fields(raw)
	if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		return parts[1]
	}
	return ""
}

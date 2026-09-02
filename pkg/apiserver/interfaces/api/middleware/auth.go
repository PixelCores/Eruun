package middleware

import (
	"strings"

	"github.com/gin-gonic/gin"

	apiauth "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/auth"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const (
	// AuthPrincipalContextKey stores the authenticated principal in gin context.
	AuthPrincipalContextKey = "auth.principal"
)

// AuthOptions configures API auth middleware dependencies.
type AuthOptions struct {
	PolicyProvider apiauth.PolicyProvider
	Authenticator  apiauth.Authenticator
	Authorizer     apiauth.Authorizer
	SkipPaths      []string
}

// DefaultAuthSkipPaths returns routes that should bypass auth checks.
func DefaultAuthSkipPaths() []string {
	return []string{
		"/api/v1/health",
		"/api/v1/healthz",
		"/api/v1/ready",
		"/api/v1/readyz",
		"/api/v1/auth/oauth2/google/login",
		"/api/v1/auth/oauth2/google/callback",
	}
}

// Auth implements JWT authentication + route RBAC authorization.
func Auth(opts AuthOptions) gin.HandlerFunc {
	if opts.Authenticator == nil {
		opts.Authenticator = apiauth.NewJWTAuthenticator(nil)
	}
	if opts.Authorizer == nil {
		opts.Authorizer = apiauth.NewRouteAuthorizer()
	}

	skipPathSet := toPathSet(opts.SkipPaths)
	if len(skipPathSet) == 0 {
		skipPathSet = toPathSet(DefaultAuthSkipPaths())
	}

	return func(c *gin.Context) {
		fullPath := strings.TrimSpace(c.FullPath())
		if fullPath == "" {
			fullPath = strings.TrimSpace(c.Request.URL.Path)
		}
		if shouldSkipAuthPath(fullPath, skipPathSet) {
			c.Next()
			return
		}

		if opts.PolicyProvider == nil {
			bcode.ReturnError(c, bcode.ErrUnauthorized)
			c.Abort()
			return
		}

		policy, err := opts.PolicyProvider.Load(c.Request.Context())
		if err != nil {
			bcode.ReturnError(c, bcode.ErrUnauthorized)
			c.Abort()
			return
		}
		if policy == nil || !policy.Enabled {
			c.Next()
			return
		}

		token := extractBearerToken(c.GetHeader("Authorization"))
		principal, err := opts.Authenticator.Authenticate(c.Request.Context(), token, policy)
		if err != nil {
			bcode.ReturnError(c, bcode.ErrUnauthorized)
			c.Abort()
			return
		}
		c.Set(AuthPrincipalContextKey, principal)

		if err := opts.Authorizer.Authorize(c.Request.Context(), principal, c.Request.Method, fullPath, policy); err != nil {
			bcode.ReturnError(c, bcode.ErrForbidden)
			c.Abort()
			return
		}
		c.Next()
	}
}

func shouldSkipAuthPath(path string, skipPathSet map[string]struct{}) bool {
	if path == "" {
		return false
	}
	_, ok := skipPathSet[path]
	return ok
}

func extractBearerToken(rawHeader string) string {
	rawHeader = strings.TrimSpace(rawHeader)
	if rawHeader == "" {
		return ""
	}

	parts := strings.SplitN(rawHeader, " ", 2)
	if len(parts) != 2 {
		return ""
	}
	if !strings.EqualFold(strings.TrimSpace(parts[0]), "bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func toPathSet(paths []string) map[string]struct{} {
	out := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		out[path] = struct{}{}
	}
	return out
}

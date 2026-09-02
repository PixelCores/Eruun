package auth

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// Principal describes the authenticated caller identity.
type Principal struct {
	Subject string
	Roles   []string
	Claims  map[string]interface{}
}

// PolicyProvider loads API auth settings.
type PolicyProvider interface {
	Load(ctx context.Context) (*spec.APIAuthSettingSpec, error)
}

// Authenticator authenticates a bearer token to a principal.
type Authenticator interface {
	Authenticate(ctx context.Context, token string, setting *spec.APIAuthSettingSpec) (*Principal, error)
}

// Authorizer decides whether a principal can access a route.
type Authorizer interface {
	Authorize(ctx context.Context, principal *Principal, method, path string, setting *spec.APIAuthSettingSpec) error
}

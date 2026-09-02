package auth

import (
	"context"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// RouteAuthorizer implements method+path based RBAC checks.
type RouteAuthorizer struct{}

// NewRouteAuthorizer creates a RouteAuthorizer.
func NewRouteAuthorizer() *RouteAuthorizer {
	return &RouteAuthorizer{}
}

// Authorize checks whether principal roles satisfy route policy.
func (a *RouteAuthorizer) Authorize(_ context.Context, principal *Principal, method, path string, setting *spec.APIAuthSettingSpec) error {
	if setting == nil || !setting.Enabled {
		return nil
	}

	method = strings.ToUpper(strings.TrimSpace(method))
	path = strings.TrimSpace(path)

	routeRules := make([]spec.APIAuthRouteRuleSpec, 0, len(setting.Authorization.Routes))
	for _, route := range setting.Authorization.Routes {
		if strings.EqualFold(route.Method, method) && route.Path == path {
			routeRules = append(routeRules, route)
		}
	}

	defaultEffect := strings.ToLower(strings.TrimSpace(setting.Authorization.DefaultEffect))
	if defaultEffect == "" {
		defaultEffect = spec.APIAuthDefaultEffectDeny
	}
	if len(routeRules) == 0 {
		if defaultEffect == spec.APIAuthDefaultEffectAllow {
			return nil
		}
		return ErrForbidden
	}

	if principal == nil || len(principal.Roles) == 0 {
		return ErrForbidden
	}

	principalRoleSet := make(map[string]struct{}, len(principal.Roles))
	for _, role := range principal.Roles {
		role = strings.ToLower(strings.TrimSpace(role))
		if role == "" {
			continue
		}
		principalRoleSet[role] = struct{}{}
	}

	for _, route := range routeRules {
		for _, role := range route.Roles {
			if _, ok := principalRoleSet[strings.ToLower(strings.TrimSpace(role))]; ok {
				return nil
			}
		}
	}
	return ErrForbidden
}

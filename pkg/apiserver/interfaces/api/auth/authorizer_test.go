package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestRouteAuthorizer_Authorize(t *testing.T) {
	authorizer := NewRouteAuthorizer()
	setting := &spec.APIAuthSettingSpec{
		Enabled: true,
		Authorization: spec.APIAuthorizationSpec{
			DefaultEffect: spec.APIAuthDefaultEffectDeny,
			Routes: []spec.APIAuthRouteRuleSpec{
				{
					Method: "GET",
					Path:   "/api/v1/applications",
					Roles:  []string{"admin", "reader"},
				},
			},
		},
	}

	err := authorizer.Authorize(context.Background(), &Principal{Roles: []string{"reader"}}, "GET", "/api/v1/applications", setting)
	require.NoError(t, err)

	err = authorizer.Authorize(context.Background(), &Principal{Roles: []string{"writer"}}, "GET", "/api/v1/applications", setting)
	require.ErrorIs(t, err, ErrForbidden)

	err = authorizer.Authorize(context.Background(), &Principal{Roles: []string{"reader"}}, "POST", "/api/v1/applications", setting)
	require.ErrorIs(t, err, ErrForbidden)
}

func TestRouteAuthorizer_DefaultAllow(t *testing.T) {
	authorizer := NewRouteAuthorizer()
	setting := &spec.APIAuthSettingSpec{
		Enabled: true,
		Authorization: spec.APIAuthorizationSpec{
			DefaultEffect: spec.APIAuthDefaultEffectAllow,
			Routes: []spec.APIAuthRouteRuleSpec{
				{
					Method: "GET",
					Path:   "/api/v1/applications",
					Roles:  []string{"reader"},
				},
			},
		},
	}

	err := authorizer.Authorize(context.Background(), &Principal{Roles: []string{"any"}}, "GET", "/api/v1/unknown", setting)
	require.NoError(t, err)
}

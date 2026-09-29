package apiserver

import (
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/middleware"
)

func TestEveryRegisteredRouteHasExactlyOneAuthPolicy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := New(config.Config{}).(*restServer)
	server.registerAPIRoutes(false)

	routes := server.webContainer.Routes()
	require.NotEmpty(t, routes)
	for _, route := range routes {
		require.Truef(t, middleware.HasAuthPolicy(route.Method, route.Path), "route %s %s must have exactly one authorization policy", route.Method, route.Path)
	}
}

package grpcapi

import (
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/middleware"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
)

func rpcMethods(descs ...grpc.ServiceDesc) map[string]bool {
	methods := map[string]bool{}
	for _, desc := range descs {
		for _, method := range desc.Methods {
			methods["/"+desc.ServiceName+"/"+method.MethodName] = true
		}
		for _, method := range desc.Streams {
			methods["/"+desc.ServiceName+"/"+method.StreamName] = true
		}
	}
	return methods
}

func TestRouteRPCCoverageAndExceptions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	group := router.Group("/api/v1")
	for _, handler := range api.NewHandlers() {
		handler.RegisterRoutes(group)
	}
	methods := rpcMethods(
		eruunv1.AccountService_ServiceDesc, eruunv1.ApplicationService_ServiceDesc,
		eruunv1.JobsService_ServiceDesc, eruunv1.SettingsService_ServiceDesc,
		eruunv1.ProgrammingLanguagesService_ServiceDesc,
		eruunv1.NodePoolsService_ServiceDesc, eruunv1.RBACProfilesService_ServiceDesc,
	)
	require.Len(t, router.Routes(), 127)
	require.Len(t, routeRPC, 112)
	require.Len(t, grpcRouteExceptions, 11)
	require.Len(t, methods, 112)
	routes := map[string]bool{}
	for _, route := range router.Routes() {
		key := route.Method + " " + route.Path
		require.False(t, routes[key], "duplicate HTTP route %s", key)
		routes[key] = true
		method, included := routeRPC[key]
		if included {
			require.False(t, grpcRouteExceptions[key], "mapped exception %s", key)
			require.True(t, methods[method], "missing RPC for %s", key)
		} else {
			require.True(t, grpcRouteExceptions[key] || (route.Method == "GET" && middleware.IsHealthPath(route.Path)), "unreviewed route %s", key)
		}
	}
	for route := range routeRPC {
		require.True(t, routes[route], "stale route mapping %s", route)
	}
	for route := range grpcRouteExceptions {
		require.True(t, routes[route], "stale exception %s", route)
	}
	used := map[string]bool{}
	for _, method := range routeRPC {
		require.False(t, used[method], "duplicate RPC %s", method)
		used[method] = true
	}
	for method := range methods {
		require.True(t, used[method], "unmapped RPC %s", method)
	}
}

func TestHTTPAndRPCBusinessPoliciesAgree(t *testing.T) {
	for route, rpc := range routeRPC {
		verb, path, _ := strings.Cut(route, " ")
		expected := middleware.BusinessRoutePolicy(verb, path)
		actual := applicationMethodPolicies[rpc]
		if role := administrationMethodPolicies[rpc]; role != "" {
			actual = role
		}
		if role := jobsMethodPolicies[rpc]; role != "" {
			actual = role
		}
		if adminAccountMethods[rpc] {
			actual = "system"
		}
		require.Equal(t, expected, actual, "authorization drift for %s -> %s", route, rpc)
	}
}

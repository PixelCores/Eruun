package grpcapi

import (
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
	api.InitAPIBean()
	for _, handler := range api.GetRegisteredAPI() {
		handler.RegisterRoutes(group)
	}
	methods := rpcMethods(
		eruunv1.AccountService_ServiceDesc, eruunv1.ApplicationService_ServiceDesc,
		eruunv1.JobsService_ServiceDesc, eruunv1.SettingsService_ServiceDesc,
		eruunv1.ProgrammingLanguagesService_ServiceDesc, eruunv1.ResourceImportService_ServiceDesc,
	)
	require.Len(t, router.Routes(), 114)
	require.Len(t, routeRPC, 99)
	require.Len(t, grpcRouteExceptions, 15)
	require.Len(t, methods, 99)
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
			require.True(t, grpcRouteExceptions[key], "unreviewed route %s", key)
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

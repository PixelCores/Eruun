package apiserver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	grpcapi "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func grpcReadyServer(address string) *restServer {
	return &restServer{
		cfg: config.Config{GRPCBindAddr: address}, accounts: &account.Service{},
		workspaceManager: &workspace.Manager{}, grpcAdministration: &grpcapi.AdministrationServer{},
		grpcJobs: &grpcapi.JobsServer{}, grpcApplications: &grpcapi.ApplicationsServer{},
	}
}

func TestGRPCStartupFailsOnMissingDependenciesAndListenerConflict(t *testing.T) {
	err := (&restServer{cfg: config.Config{GRPCBindAddr: "127.0.0.1:0"}}).startGRPC(context.Background())
	require.ErrorContains(t, err, "dependencies are not initialized")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	err = grpcReadyServer(listener.Addr().String()).startGRPC(context.Background())
	require.ErrorContains(t, err, "listen for grpc")
}

func TestGRPCGracefulShutdownAndRoleGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- grpcReadyServer("127.0.0.1:0").startGRPC(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("grpc shutdown exceeded test deadline")
	}
	for _, role := range []config.RuntimeRole{config.RuntimeRoleController, config.RuntimeRoleScheduler, config.RuntimeRoleWorker} {
		require.False(t, (config.Config{Role: role}).RunsAPI(), "only api role starts gRPC")
	}
	require.True(t, (config.Config{Role: config.RuntimeRoleAPI}).RunsAPI())
}

type blockingService interface {
	Wait(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}
type blockedCall struct{ entered chan struct{} }

func (b *blockedCall) Wait(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	close(b.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestGRPCForcedStopAfterGraceDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	blocked := &blockedCall{entered: make(chan struct{})}
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.BlockingService", HandlerType: (*blockingService)(nil),
		Methods: []grpc.MethodDesc{{MethodName: "Wait", Handler: func(srv any, ctx context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			request := &emptypb.Empty{}
			if err := decode(request); err != nil {
				return nil, err
			}
			return srv.(blockingService).Wait(ctx, request)
		}}},
	}, blocked)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	callDone := make(chan error, 1)
	go func() {
		callDone <- conn.Invoke(context.Background(), "/test.BlockingService/Wait", &emptypb.Empty{}, &emptypb.Empty{})
	}()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking RPC did not begin")
	}
	started := time.Now()
	stopGRPCWithTimeout(server, 10*time.Millisecond)
	require.Less(t, time.Since(started), time.Second)
	select {
	case callErr := <-callDone:
		require.Error(t, callErr)
	case <-time.After(time.Second):
		t.Fatal("forced stop did not cancel RPC")
	}
	select {
	case serveErr := <-serveDone:
		require.NoError(t, serveErr)
	case <-time.After(time.Second):
		t.Fatal("forced stop did not end server")
	}
}

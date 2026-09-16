package apiserver

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	grpcapi "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

func (s *restServer) startGRPC(ctx context.Context) error {
	if s.accounts == nil || s.workspaceManager == nil || s.grpcAdministration == nil ||
		s.grpcJobs == nil || s.grpcApplications == nil {
		return fmt.Errorf("grpc API dependencies are not initialized")
	}
	listener, err := net.Listen("tcp", s.cfg.GRPCBindAddr)
	if err != nil {
		return fmt.Errorf("listen for grpc on %s: %w", s.cfg.GRPCBindAddr, err)
	}
	server := grpcapi.NewServer(s.accounts, s.workspaceManager, s.grpcAdministration, s.grpcJobs, s.grpcApplications, s.apiRateLimiter)
	shutdownComplete := make(chan struct{})
	stopShutdownWatcher := make(chan struct{})
	go func() {
		defer close(shutdownComplete)
		select {
		case <-ctx.Done():
			stopGRPCWithTimeout(server, config.DefaultHTTPShutdownTimeout)
		case <-stopShutdownWatcher:
		}
	}()
	klog.InfoS("gRPC APIs are being served", "address", s.cfg.GRPCBindAddr)
	if err := server.Serve(listener); err != nil {
		close(stopShutdownWatcher)
		return fmt.Errorf("serve grpc on %s: %w", s.cfg.GRPCBindAddr, err)
	}
	<-shutdownComplete
	return nil
}

func stopGRPCWithTimeout(server *grpc.Server, timeout time.Duration) {
	gracefulDone := make(chan struct{})
	go func() { server.GracefulStop(); close(gracefulDone) }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-gracefulDone:
	case <-timer.C:
		klog.InfoS("grpc graceful shutdown exceeded deadline; stopping active calls")
		server.Stop()
	}
}

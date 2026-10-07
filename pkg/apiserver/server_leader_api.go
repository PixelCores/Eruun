package apiserver

import (
	"context"
	"errors"
	"strconv"

	apiresponse "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/response"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// requestLeadership binds the request to the current, fully initialized term.
// A later term must never revive work accepted by a previous leader.
func (s *restServer) requestLeadership(parent context.Context) (context.Context, context.CancelFunc, bool) {
	s.leaderMu.RLock()
	defer s.leaderMu.RUnlock()
	term := s.leaderCtx
	if term == nil || term.Err() != nil {
		return nil, nil, false
	}
	if conn, ok := parent.Value(httpConnectionKey{}).(*leaderConn); ok && !conn.bind(term) {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(term, func() { cancel(bcode.ErrServiceUnavailable) })
	if term.Err() != nil {
		cancel(bcode.ErrServiceUnavailable)
	}
	return ctx, func() { stop(); cancel(context.Canceled) }, true
}

func (s *restServer) leaderAPIMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		switch c.Request.URL.Path {
		case "/api/v1/health", "/api/v1/healthz", "/api/v1/ready", "/api/v1/readyz":
			c.Next()
			return
		}
		ctx, release, ok := s.requestLeadership(c.Request.Context())
		if !ok || ctx.Err() != nil {
			if release != nil {
				release()
			}
			// A stale Service endpoint may still route a fresh connection here.
			// Do not let its next request remain pinned to this Worker.
			c.Header("Connection", "close")
			apiresponse.ReturnError(c, bcode.ErrServiceUnavailable)
			c.Abort()
			return
		}
		defer release()
		c.Request = c.Request.WithContext(ctx)
		// leaderConn closes the transport on term loss, including blocked I/O.
		c.Next()
	}
}

func leaderUnavailableRPC() error {
	st, err := status.New(codes.Unavailable, bcode.ErrServiceUnavailable.Message).WithDetails(&errdetails.ErrorInfo{
		Reason: "ERUUN_BUSINESS_ERROR", Domain: "eruun.io",
		Metadata: map[string]string{"business_code": strconv.FormatInt(int64(bcode.ErrServiceUnavailable.BusinessCode), 10)},
	})
	if err != nil {
		return status.Error(codes.Unavailable, bcode.ErrServiceUnavailable.Message)
	}
	return st.Err()
}

func (s *restServer) leaderUnaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, release, ok := s.requestLeadership(ctx)
	if !ok {
		return nil, leaderUnavailableRPC()
	}
	defer release()
	if ctx.Err() != nil {
		return nil, leaderUnavailableRPC()
	}
	response, err := handler(ctx, req)
	if errors.Is(context.Cause(ctx), bcode.ErrServiceUnavailable) {
		return nil, leaderUnavailableRPC()
	}
	return response, err
}

func (s *restServer) leaderStreamInterceptor(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, release, ok := s.requestLeadership(stream.Context())
	if !ok {
		return leaderUnavailableRPC()
	}
	defer release()
	if ctx.Err() != nil {
		return leaderUnavailableRPC()
	}
	err := handler(srv, &leaderServerStream{ServerStream: stream, ctx: ctx})
	if errors.Is(context.Cause(ctx), bcode.ErrServiceUnavailable) {
		return leaderUnavailableRPC()
	}
	return err
}

type leaderServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *leaderServerStream) Context() context.Context { return s.ctx }

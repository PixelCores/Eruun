package apiserver

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	grpcapi "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestLeaderHTTPGatesBusinessRoutesAndAllowsProbes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := &restServer{}
	router := gin.New()
	router.Use(s.leaderAPIMiddleware())
	api.NewHealth().RegisterRoutes(router.Group("/api/v1"))
	for _, path := range []string{"/api/v1/auth/login", "/api/v1/applications", "/api/v1/job-runners/token/events"} {
		router.POST(path, func(c *gin.Context) { c.Status(http.StatusAccepted) })
	}
	for _, path := range []string{"health", "healthz", "ready", "readyz"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/"+path, nil))
		require.Equal(t, http.StatusOK, w.Code, path)
	}
	for _, path := range []string{"/api/v1/auth/login", "/api/v1/applications", "/api/v1/job-runners/token/events"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, nil))
		require.Equal(t, http.StatusServiceUnavailable, w.Code, path)
		require.JSONEq(t, `{"code":503,"message":"Service unavailable","data":null}`, w.Body.String())
	}
	term, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.leaderCtx = term
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/applications", nil))
	require.Equal(t, http.StatusAccepted, w.Code)
	cancel()
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/applications", nil))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestRequestLeadershipKeepsTermsAndClientCancellationSeparate(t *testing.T) {
	s := &restServer{}
	firstTerm, loseFirstTerm := context.WithCancel(context.Background())
	defer loseFirstTerm()
	s.leaderCtx = firstTerm
	oldRequest, releaseOldRequest, ok := s.requestLeadership(context.Background())
	require.True(t, ok)
	defer releaseOldRequest()
	loseFirstTerm()
	select {
	case <-oldRequest.Done():
	case <-time.After(time.Second):
		t.Fatal("old request survived leadership loss")
	}
	require.ErrorIs(t, context.Cause(oldRequest), bcode.ErrServiceUnavailable)
	secondTerm, loseSecondTerm := context.WithCancel(context.Background())
	defer loseSecondTerm()
	s.leaderMu.Lock()
	s.leaderCtx = secondTerm
	s.leaderMu.Unlock()
	client, cancelClient := context.WithCancel(context.Background())
	newRequest, releaseNewRequest, ok := s.requestLeadership(client)
	require.True(t, ok)
	defer releaseNewRequest()
	require.NoError(t, newRequest.Err())
	require.ErrorIs(t, oldRequest.Err(), context.Canceled)
	cancelClient()
	require.ErrorIs(t, newRequest.Err(), context.Canceled)
	require.NoError(t, secondTerm.Err(), "client cancellation must not cancel leadership")
}

func TestLeaderHTTPLossCancelsRequestAndUnblocksUpload(t *testing.T) {
	term, loseLeadership := context.WithCancel(context.Background())
	defer loseLeadership()
	s := &restServer{leaderCtx: term}
	router := gin.New()
	router.Use(s.leaderAPIMiddleware())
	entered := make(chan struct{})
	done := make(chan error, 1)
	router.POST("/api/v1/upload", func(c *gin.Context) {
		close(entered)
		_, err := io.Copy(io.Discard, c.Request.Body)
		done <- err
	})
	httpServer := serveLeaderHTTPTestHandler(t, s, router)
	conn, err := net.Dial("tcp", strings.TrimPrefix(httpServer.URL, "http://"))
	require.NoError(t, err)
	defer conn.Close()
	_, err = io.WriteString(conn, "POST /api/v1/upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\nx")
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("upload did not begin")
	}
	loseLeadership()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("leadership loss did not unblock the request body")
	}
}

func TestLeaderHTTPLossCancelsHandlerContext(t *testing.T) {
	term, loseLeadership := context.WithCancel(context.Background())
	defer loseLeadership()
	s := &restServer{leaderCtx: term}
	router := gin.New()
	router.Use(s.leaderAPIMiddleware())
	entered := make(chan struct{})
	cancelled := make(chan error, 1)
	router.GET("/api/v1/stream", func(c *gin.Context) {
		close(entered)
		<-c.Request.Context().Done()
		cancelled <- context.Cause(c.Request.Context())
	})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil))
	}()
	<-entered
	loseLeadership()
	select {
	case err := <-cancelled:
		require.ErrorIs(t, err, bcode.ErrServiceUnavailable)
	case <-time.After(time.Second):
		t.Fatal("HTTP handler context survived leadership loss")
	}
	<-finished
}

func TestLeaderGRPCGatePrecedesPublicAuthenticationAndPreservesErrorDetails(t *testing.T) {
	s := &restServer{}
	var authenticationReached atomic.Bool
	server := grpcapi.NewServer(nil, nil, &grpcapi.AdministrationServer{}, &grpcapi.JobsServer{}, &grpcapi.ApplicationsServer{}, nil,
		grpc.UnaryInterceptor(s.leaderUnaryInterceptor), grpc.StreamInterceptor(s.leaderStreamInterceptor),
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			authenticationReached.Store(true)
			return handler(ctx, req)
		}))
	// Isolate interceptor ordering here; the production listener rejects
	// Worker connections before they reach authentication or RPC handlers.
	conn := serveLeaderTestGRPC(t, server, nil)
	_, err := eruunv1.NewAccountServiceClient(conn).GetAuthMethods(context.Background(), &emptypb.Empty{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	details := status.Convert(err).Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, "503", info.Metadata["business_code"])
	require.False(t, authenticationReached.Load(), "non-leader requests must not reach authentication or business handlers")
}

func TestLeaderGRPCReadyAllowsUnaryAndRejectsLostTerm(t *testing.T) {
	term, loseLeadership := context.WithCancel(context.Background())
	defer loseLeadership()
	s := &restServer{leaderCtx: term}
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		require.NoError(t, ctx.Err())
		return req, nil
	}
	response, err := s.leaderUnaryInterceptor(context.Background(), "request", nil, handler)
	require.NoError(t, err)
	require.Equal(t, "request", response)
	require.True(t, called)
	called = false
	loseLeadership()
	_, err = s.leaderUnaryInterceptor(context.Background(), "request", nil, handler)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.False(t, called)
}

func TestLeaderGRPCLossCancelsActiveUnaryAndBlockedStream(t *testing.T) {
	for _, operation := range []string{"unary", "stream"} {
		t.Run(operation, func(t *testing.T) {
			term, loseLeadership := context.WithCancel(context.Background())
			defer loseLeadership()
			s := &restServer{leaderCtx: term}
			server := grpc.NewServer(grpc.UnaryInterceptor(s.leaderUnaryInterceptor), grpc.StreamInterceptor(s.leaderStreamInterceptor))
			entered := make(chan struct{})
			transportFinished := make(chan struct{})
			server.RegisterService(&grpc.ServiceDesc{
				ServiceName: "test.Leader", HandlerType: (*blockingService)(nil),
				Methods: []grpc.MethodDesc{{MethodName: "Wait", Handler: func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					request := &emptypb.Empty{}
					if err := decode(request); err != nil {
						return nil, err
					}
					return interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: "/test.Leader/Wait"}, func(ctx context.Context, _ any) (any, error) {
						close(entered)
						<-ctx.Done()
						return nil, ctx.Err()
					})
				}}},
				Streams: []grpc.StreamDesc{{StreamName: "Upload", ClientStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
					close(entered)
					defer close(transportFinished)
					return stream.RecvMsg(&emptypb.Empty{})
				}}},
			}, &blockedCall{})
			conn := serveLeaderTestGRPC(t, server, s)
			done := make(chan error, 1)
			go func() {
				if operation == "unary" {
					done <- conn.Invoke(context.Background(), "/test.Leader/Wait", &emptypb.Empty{}, &emptypb.Empty{})
					return
				}
				stream, err := conn.NewStream(context.Background(), &grpc.StreamDesc{ClientStreams: true}, "/test.Leader/Upload")
				if err != nil {
					done <- err
					return
				}
				done <- stream.RecvMsg(&emptypb.Empty{})
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("RPC did not begin")
			}
			loseLeadership()
			select {
			case err := <-done:
				require.Equal(t, codes.Unavailable, status.Code(err))
			case <-time.After(time.Second):
				t.Fatal("RPC survived leadership loss")
			}
			if operation == "stream" {
				<-transportFinished
			}
		})
	}
}

func serveLeaderTestGRPC(t *testing.T, server *grpc.Server, node *restServer) *grpc.ClientConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	if node != nil {
		listener = &leaderListener{Listener: listener, server: node, requireLeadership: true}
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func TestLeaderHTTPLossUnblocksNetworkWrite(t *testing.T) {
	term, loseLeadership := context.WithCancel(context.Background())
	defer loseLeadership()
	node := &restServer{leaderCtx: term}
	router := gin.New()
	router.Use(node.leaderAPIMiddleware())
	entered := make(chan struct{})
	done := make(chan error, 1)
	router.GET("/api/v1/download", func(c *gin.Context) {
		close(entered)
		_, err := c.Writer.Write(make([]byte, 8<<20))
		done <- err
	})
	server := serveLeaderHTTPTestHandler(t, node, router)
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.(*net.TCPConn).SetReadBuffer(1024))
	_, err = io.WriteString(conn, "GET /api/v1/download HTTP/1.1\r\nHost: localhost\r\n\r\n")
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("download did not start")
	}
	// A peer that never reads must keep the real TCP write blocked.
	select {
	case err := <-done:
		t.Fatalf("download was not blocked before leadership loss: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	loseLeadership()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("leadership loss did not unblock the response write")
	}
	// Drain bytes already buffered before the close and verify actual EOF.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = io.Copy(io.Discard, conn)
	require.NoError(t, err)
}

func TestLeaderHTTPClientCancellationLeavesTermAndOtherRequestsAlive(t *testing.T) {
	term, loseLeadership := context.WithCancel(context.Background())
	defer loseLeadership()
	node := &restServer{leaderCtx: term}
	router := gin.New()
	router.Use(node.leaderAPIMiddleware())
	entered := make(chan struct{})
	handlerDone := make(chan error, 1)
	router.GET("/api/v1/wait", func(c *gin.Context) {
		close(entered)
		<-c.Request.Context().Done()
		handlerDone <- context.Cause(c.Request.Context())
	})
	router.GET("/api/v1/ping", func(c *gin.Context) { c.String(http.StatusOK, "alive") })
	server := serveLeaderHTTPTestHandler(t, node, router)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/wait", nil)
	require.NoError(t, err)
	client := server.Client()
	clientDone := make(chan error, 1)
	go func() {
		response, err := client.Do(request)
		if response != nil {
			response.Body.Close()
		}
		clientDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-clientDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("client request survived cancellation")
	}
	select {
	case err := <-handlerDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("handler survived client cancellation")
	}
	require.NoError(t, term.Err())
	code, body, _, err := callLeaderTransportHTTP(client, server.URL+"/api/v1/ping")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "alive", body)
}

func TestLeaderGRPCBlockedIOStopsOnTermLossAndClientCancellation(t *testing.T) {
	for _, operation := range []string{"receive", "send"} {
		for _, cancellation := range []string{"leader", "client"} {
			t.Run(operation+"/"+cancellation, func(t *testing.T) {
				term, loseLeadership := context.WithCancel(context.Background())
				defer loseLeadership()
				node := &restServer{leaderCtx: term}
				server := grpc.NewServer(grpc.StreamInterceptor(node.leaderStreamInterceptor))
				entered := make(chan struct{})
				handlerDone := make(chan error, 1)
				server.RegisterService(&grpc.ServiceDesc{
					ServiceName: "test.BlockedIO", HandlerType: (*leaderTransportPingService)(nil),
					Streams: []grpc.StreamDesc{{StreamName: "Wait", ClientStreams: true, ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
						var err error
						if operation == "receive" {
							close(entered)
							err = stream.RecvMsg(&emptypb.Empty{})
						} else {
							// The first message consumes the transport write quota;
							// the unread client stream blocks the next SendMsg.
							message := wrapperspb.Bytes(make([]byte, 1<<20))
							err = stream.SendMsg(message)
							close(entered)
							if err == nil {
								err = stream.SendMsg(message)
							}
						}
						handlerDone <- err
						return err
					}}, {StreamName: "Ping", ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
						return stream.SendMsg(&emptypb.Empty{})
					}}},
				}, struct{}{})
				conn := serveLeaderTestGRPC(t, server, node)
				ctx, cancelClient := context.WithCancel(context.Background())
				defer cancelClient()
				stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.BlockedIO/Wait")
				require.NoError(t, err)
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("stream did not enter transport operation")
				}
				select {
				case err := <-handlerDone:
					t.Fatalf("stream was not blocked before cancellation: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
				if cancellation == "leader" {
					loseLeadership()
				} else {
					cancelClient()
				}
				select {
				case err := <-handlerDone:
					require.Error(t, err, "the blocking transport operation must return before the handler exits")
				case <-time.After(time.Second):
					t.Fatal("handler transport operation survived cancellation")
				}
				err = stream.RecvMsg(&wrapperspb.BytesValue{})
				if cancellation == "leader" {
					require.Equal(t, codes.Unavailable, status.Code(err))
				} else {
					require.Equal(t, codes.Canceled, status.Code(err))
					require.NoError(t, term.Err(), "client cancellation must not cancel leadership")
					ping, err := conn.NewStream(context.Background(), &grpc.StreamDesc{ServerStreams: true}, "/test.BlockedIO/Ping")
					require.NoError(t, err)
					require.NoError(t, ping.RecvMsg(&emptypb.Empty{}), "the same connection must still serve other calls")
					require.ErrorIs(t, ping.RecvMsg(&emptypb.Empty{}), io.EOF)
				}
			})
		}
	}
}

func TestLeaderGRPCNormalStreamPreservesMultipleMessagesAndEOF(t *testing.T) {
	term, loseLeadership := context.WithCancel(context.Background())
	defer loseLeadership()
	node := &restServer{leaderCtx: term}
	server := grpc.NewServer(grpc.StreamInterceptor(node.leaderStreamInterceptor))
	handlerDone := make(chan error, 1)
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Messages", HandlerType: (*leaderTransportPingService)(nil),
		Streams: []grpc.StreamDesc{{StreamName: "Echo", ClientStreams: true, ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) (err error) {
			defer func() { handlerDone <- err }()
			for {
				message := &wrapperspb.StringValue{}
				if err = stream.RecvMsg(message); err == io.EOF {
					return nil
				} else if err != nil {
					return err
				}
				if err = stream.SendMsg(message); err != nil {
					return err
				}
			}
		}}},
	}, struct{}{})
	conn := serveLeaderTestGRPC(t, server, node)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.Messages/Echo")
	require.NoError(t, err)
	for _, value := range []string{"first", "second", "last"} {
		require.NoError(t, stream.SendMsg(wrapperspb.String(value)))
		message := &wrapperspb.StringValue{}
		require.NoError(t, stream.RecvMsg(message))
		require.Equal(t, value, message.GetValue())
	}
	require.NoError(t, stream.CloseSend())
	require.ErrorIs(t, stream.RecvMsg(&wrapperspb.StringValue{}), io.EOF)
	select {
	case err := <-handlerDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("normal stream handler did not finish")
	}
	require.NoError(t, term.Err())
}

package apiserver

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func serveLeaderTransportHTTP(t *testing.T, node *restServer, name string) *httptest.Server {
	t.Helper()
	router := gin.New()
	router.Use(node.leaderAPIMiddleware())
	router.GET("/api/v1/ping", func(c *gin.Context) { c.String(http.StatusOK, name) })
	router.GET("/api/v1/healthz", func(c *gin.Context) { c.String(http.StatusOK, "healthy") })
	server := httptest.NewUnstartedServer(router)
	server.Listener = &leaderListener{Listener: server.Listener, server: node}
	server.Config.ConnContext = node.httpConnectionContext
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func callLeaderTransportHTTP(client *http.Client, url string) (int, string, bool, error) {
	response, err := client.Get(url)
	if err != nil {
		return 0, "", false, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, string(body), response.Close, err
}

func TestLeaderTransportHTTPClientFollowsServiceAfterFailover(t *testing.T) {
	termA, loseA := context.WithCancel(context.Background())
	defer loseA()
	termB, loseB := context.WithCancel(context.Background())
	defer loseB()
	a := serveLeaderTransportHTTP(t, &restServer{leaderCtx: termA}, "leader-a")
	b := serveLeaderTransportHTTP(t, &restServer{leaderCtx: termB}, "leader-b")
	var target atomic.Value
	target.Store(a.Listener.Addr().String())
	var dials atomic.Int32
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, "tcp", target.Load().(string))
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: time.Second}
	code, body, _, err := callLeaderTransportHTTP(client, "http://runtime-service/api/v1/ping")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "leader-a", body)
	// A Service update affects only new TCP connections. Keep the same pool.
	target.Store(b.Listener.Addr().String())
	loseA()
	require.Eventually(t, func() bool {
		code, body, _, err := callLeaderTransportHTTP(client, "http://runtime-service/api/v1/ping")
		return err == nil && code == http.StatusOK && body == "leader-b"
	}, 2*time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, dials.Load(), int32(2))
}

func TestLeaderTransportHTTPWorkerRejectsKeepAliveButServesProbes(t *testing.T) {
	server := serveLeaderTransportHTTP(t, &restServer{}, "worker")
	transport := &http.Transport{}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: time.Second}
	code, body, closing, err := callLeaderTransportHTTP(client, server.URL+"/api/v1/healthz")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "healthy", body)
	require.False(t, closing)
	code, _, closing, err = callLeaderTransportHTTP(client, server.URL+"/api/v1/ping")
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.True(t, closing, "a stale Service endpoint must not keep clients attached to a Worker")
	code, _, _, err = callLeaderTransportHTTP(client, server.URL+"/api/v1/healthz")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
}

func TestLeaderTransportOldHTTPConnectionCannotCrossTerms(t *testing.T) {
	oldTerm, loseOld := context.WithCancel(context.Background())
	defer loseOld()
	node := &restServer{leaderCtx: oldTerm}
	server := serveLeaderTransportHTTP(t, node, "leader")
	var dials atomic.Int32
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: time.Second}
	_, _, _, err := callLeaderTransportHTTP(client, server.URL+"/api/v1/ping")
	require.NoError(t, err)
	// The already-bound connection must close even if this same Pod wins again.
	newTerm, loseNew := context.WithCancel(context.Background())
	defer loseNew()
	loseOld()
	node.leaderMu.Lock()
	node.leaderCtx = newTerm
	node.leaderMu.Unlock()
	require.Eventually(t, func() bool {
		code, body, _, err := callLeaderTransportHTTP(client, server.URL+"/api/v1/ping")
		return err == nil && code == http.StatusOK && body == "leader"
	}, time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, dials.Load(), int32(2), "an old transport must not survive into the next term")
}

type leaderTransportPingService interface{}

func serveLeaderTransportGRPC(t *testing.T, node *restServer, name string, entered chan<- struct{}) string {
	t.Helper()
	server := grpc.NewServer(grpc.UnaryInterceptor(node.leaderUnaryInterceptor))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.LeaderTransport", HandlerType: (*leaderTransportPingService)(nil),
		Methods: []grpc.MethodDesc{{MethodName: "Ping", Handler: func(_ any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := &emptypb.Empty{}
			if err := decode(request); err != nil {
				return nil, err
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: "/test.LeaderTransport/Ping"}, func(ctx context.Context, _ any) (any, error) {
				if entered != nil {
					entered <- struct{}{}
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return wrapperspb.String(name), nil
			})
		}}},
	}, struct{}{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(&leaderListener{Listener: listener, server: node, requireLeadership: true}) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func connectLeaderTransportGRPC(t *testing.T, target *atomic.Value, dials *atomic.Int32) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///runtime-service", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, "tcp", target.Load().(string))
		}), grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{BaseDelay: 10 * time.Millisecond, Multiplier: 1.2, MaxDelay: 50 * time.Millisecond}, MinConnectTimeout: time.Second}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func callLeaderTransportGRPC(conn *grpc.ClientConn) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	response := &wrapperspb.StringValue{}
	err := conn.Invoke(ctx, "/test.LeaderTransport/Ping", &emptypb.Empty{}, response)
	return response.GetValue(), err
}

func TestLeaderTransportGRPCClientFollowsServiceAfterFailover(t *testing.T) {
	termA, loseA := context.WithCancel(context.Background())
	defer loseA()
	termB, loseB := context.WithCancel(context.Background())
	defer loseB()
	a := serveLeaderTransportGRPC(t, &restServer{leaderCtx: termA}, "leader-a", nil)
	b := serveLeaderTransportGRPC(t, &restServer{leaderCtx: termB}, "leader-b", nil)
	var target atomic.Value
	target.Store(a)
	var dials atomic.Int32
	conn := connectLeaderTransportGRPC(t, &target, &dials)
	body, err := callLeaderTransportGRPC(conn)
	require.NoError(t, err)
	require.Equal(t, "leader-a", body)
	loseA()
	// Before EndpointSlices converge, new connections can still reach A.
	stale, err := net.Dial("tcp", a)
	require.NoError(t, err)
	defer stale.Close()
	require.NoError(t, stale.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = stale.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "Worker must reject new gRPC transports")
	target.Store(b)
	require.Eventually(t, func() bool {
		body, err := callLeaderTransportGRPC(conn)
		return err == nil && body == "leader-b"
	}, 2*time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, dials.Load(), int32(2))
}

func TestLeaderTransportGRPCWorkerCanBecomeLeader(t *testing.T) {
	node := &restServer{}
	address := serveLeaderTransportGRPC(t, node, "promoted-leader", nil)
	var target atomic.Value
	target.Store(address)
	var dials atomic.Int32
	conn := connectLeaderTransportGRPC(t, &target, &dials)
	_, err := callLeaderTransportGRPC(conn)
	require.Error(t, err, "a Worker must not accept business calls")
	term, lose := context.WithCancel(context.Background())
	defer lose()
	node.leaderMu.Lock()
	node.leaderCtx = term
	node.leaderMu.Unlock()
	require.Eventually(t, func() bool {
		body, err := callLeaderTransportGRPC(conn)
		return err == nil && body == "promoted-leader"
	}, 2*time.Second, 10*time.Millisecond)
}

func TestLeaderTransportClosesActiveGRPCCallOnLoss(t *testing.T) {
	term, lose := context.WithCancel(context.Background())
	defer lose()
	entered := make(chan struct{}, 1)
	address := serveLeaderTransportGRPC(t, &restServer{leaderCtx: term}, "leader", entered)
	var target atomic.Value
	target.Store(address)
	var dials atomic.Int32
	conn := connectLeaderTransportGRPC(t, &target, &dials)
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done <- conn.Invoke(ctx, "/test.LeaderTransport/Ping", &emptypb.Empty{}, &wrapperspb.StringValue{})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("RPC did not start")
	}
	lose()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("active RPC survived leadership loss")
	}
}

type leaderTransportCountClose struct {
	net.Conn
	closes atomic.Int32
}

func (c *leaderTransportCountClose) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

func TestLeaderTransportCloseDetachesTermAndPreventsRebinding(t *testing.T) {
	raw, peer := net.Pipe()
	defer peer.Close()
	counted := &leaderTransportCountClose{Conn: raw}
	conn := &leaderConn{Conn: counted}
	term, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.True(t, conn.bind(term))
	require.NoError(t, conn.Close())
	cancel()
	require.False(t, conn.bind(context.Background()), "closed connections cannot enter a new term")
	require.Never(t, func() bool { return counted.closes.Load() != 1 }, 50*time.Millisecond, time.Millisecond)
}

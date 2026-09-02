package utils

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func testIPAddr(rawIP string) net.IPAddr {
	return net.IPAddr{IP: net.ParseIP(rawIP)}
}

func policyHTTPTransport(t *testing.T, client *http.Client) *http.Transport {
	t.Helper()
	transport, ok := client.Transport.(*urlPolicyTransport)
	if !ok {
		t.Fatalf("expected url policy transport, got %T", client.Transport)
	}
	return transport.Transport
}

func TestValidateURLTargetRejectsPrivateByDefault(t *testing.T) {
	t.Parallel()

	_, err := ValidateURLTarget(context.Background(), "http://127.0.0.1:8080/callback", nil)
	if err == nil {
		t.Fatal("expected private target to be rejected")
	}
}

func TestValidateURLTargetAllowsPrivateWhenEnabled(t *testing.T) {
	t.Parallel()

	policy := &spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}
	_, err := ValidateURLTarget(context.Background(), "http://127.0.0.1:8080/callback", policy)
	if err != nil {
		t.Fatalf("expected private target to be allowed, got: %v", err)
	}
}

func TestValidateURLTargetAllowsPrivateHostPattern(t *testing.T) {
	t.Parallel()

	policy := &spec.URLSecurityPolicySpec{AllowedHostPatterns: []string{"localhost"}}
	_, err := ValidateURLTarget(context.Background(), "http://localhost:8080/callback", policy)
	if err != nil {
		t.Fatalf("expected localhost private target to be allowed by host pattern, got: %v", err)
	}
}

func TestValidateURLTargetAllowsPrivateCIDR(t *testing.T) {
	t.Parallel()

	policy := &spec.URLSecurityPolicySpec{AllowedCIDRs: []string{"127.0.0.1/32"}}
	_, err := ValidateURLTarget(context.Background(), "http://127.0.0.1:8080/callback", policy)
	if err != nil {
		t.Fatalf("expected private target to be allowed by cidr, got: %v", err)
	}
}

func TestValidateURLTargetAllowsPublicIP(t *testing.T) {
	t.Parallel()

	_, err := ValidateURLTarget(context.Background(), "https://8.8.8.8/dns-query", nil)
	if err != nil {
		t.Fatalf("expected public ip target to be allowed, got: %v", err)
	}
}

func TestValidateURLTargetRejectsUnsupportedScheme(t *testing.T) {
	t.Parallel()

	_, err := ValidateURLTarget(context.Background(), "ftp://example.com/resource", nil)
	if err == nil {
		t.Fatal("expected unsupported scheme to be rejected")
	}
}

func TestValidateURLTargetRejectsUnresolvedHostWhenProxyConfigured(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://proxy.example:8080")

	_, err := validateURLTarget(
		context.Background(),
		"https://proxy-only.invalid/path",
		nil,
		func(context.Context, string) ([]net.IPAddr, error) {
			return nil, errors.New("lookup failed")
		},
	)
	if err == nil {
		t.Fatal("expected unresolved host to fail closed even when an environment proxy is configured")
	}
}

func TestValidateURLTargetRejectsUnresolvedHostWithoutProxy(t *testing.T) {
	t.Parallel()

	_, err := validateURLTarget(
		context.Background(),
		"https://proxy-only.invalid/path",
		nil,
		func(context.Context, string) ([]net.IPAddr, error) {
			return nil, errors.New("lookup failed")
		},
	)
	if err == nil {
		t.Fatal("expected unresolved host to be rejected without proxy")
	}
}

func TestURLPolicyDialBlocksDNSRebinding(t *testing.T) {
	resolverCalls := 0
	resolver := func(context.Context, string) ([]net.IPAddr, error) {
		resolverCalls++
		if resolverCalls == 1 {
			return []net.IPAddr{testIPAddr("203.0.113.10")}, nil
		}
		return []net.IPAddr{testIPAddr("127.0.0.1")}, nil
	}
	dialCalled := false
	dial := func(context.Context, string, string) (net.Conn, error) {
		dialCalled = true
		return nil, errors.New("unexpected dial")
	}

	_, err := validateURLTarget(context.Background(), "https://rebind.example/path", nil, resolver)
	if err != nil {
		t.Fatalf("expected first public resolution to pass validation, got: %v", err)
	}
	client, err := newURLPolicyHTTPClient(&http.Client{}, nil, resolver, dial)
	if err != nil {
		t.Fatalf("create policy client: %v", err)
	}
	transport := policyHTTPTransport(t, client)
	_, err = transport.DialContext(context.Background(), "tcp", "rebind.example:443")
	if err == nil || !strings.Contains(err.Error(), "private target 127.0.0.1") {
		t.Fatalf("expected dial-time private resolution to be blocked, got: %v", err)
	}
	if dialCalled {
		t.Fatal("underlying dialer must not receive a blocked address")
	}
}

func TestURLPolicyDialUsesValidatedIPAddress(t *testing.T) {
	var dialAddress string
	stopDial := errors.New("stop after recording address")
	client, err := newURLPolicyHTTPClient(
		&http.Client{},
		nil,
		func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{testIPAddr("203.0.113.20")}, nil
		},
		func(_ context.Context, _, address string) (net.Conn, error) {
			dialAddress = address
			return nil, stopDial
		},
	)
	if err != nil {
		t.Fatalf("create policy client: %v", err)
	}
	transport := policyHTTPTransport(t, client)
	_, err = transport.DialContext(context.Background(), "tcp", "public.example:8443")
	if !errors.Is(err, stopDial) {
		t.Fatalf("expected recording dial error, got: %v", err)
	}
	if dialAddress != "203.0.113.20:8443" {
		t.Fatalf("expected validated IP to be dialed, got %q", dialAddress)
	}
}

func TestURLPolicyClientUsesRequestDeadlineForSameFamilyFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	requestDeadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected request deadline")
	}
	resolverDeadline := make(chan time.Time, 1)
	firstAttemptDeadline := make(chan time.Time, 1)
	var localDialer net.Dialer
	client, err := newURLPolicyHTTPClient(
		&http.Client{Transport: &http.Transport{}},
		nil,
		func(resolveCtx context.Context, _ string) ([]net.IPAddr, error) {
			deadline, hasDeadline := resolveCtx.Deadline()
			if !hasDeadline {
				return nil, errors.New("resolver did not receive request deadline")
			}
			resolverDeadline <- deadline
			return []net.IPAddr{testIPAddr("203.0.113.20"), testIPAddr("203.0.113.21")}, nil
		},
		func(attemptCtx context.Context, network, address string) (net.Conn, error) {
			switch address {
			case "203.0.113.20:80":
				deadline, hasDeadline := attemptCtx.Deadline()
				if !hasDeadline {
					return nil, errors.New("first address did not receive a partial deadline")
				}
				firstAttemptDeadline <- deadline
				<-attemptCtx.Done()
				return nil, attemptCtx.Err()
			case "203.0.113.21:80":
				return localDialer.DialContext(attemptCtx, network, server.Listener.Addr().String())
			default:
				return nil, fmt.Errorf("unexpected dial address %q", address)
			}
		},
	)
	if err != nil {
		t.Fatalf("create policy client: %v", err)
	}
	defer client.CloseIdleConnections()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://deadline.example/path", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expected second same-family address to succeed before request deadline, got: %v", err)
	}
	resp.Body.Close()

	if deadline := <-resolverDeadline; !deadline.Equal(requestDeadline) {
		t.Fatalf("expected resolver deadline %v, got %v", requestDeadline, deadline)
	}
	if deadline := <-firstAttemptDeadline; !deadline.Before(requestDeadline) {
		t.Fatalf("expected first address deadline before request deadline %v, got %v", requestDeadline, deadline)
	}
}

func TestURLPolicyClientPreservesAllowedResolvedIPv6ZoneAndBlocksByDefault(t *testing.T) {
	stopDial := errors.New("stop after recording scoped address")
	tests := []struct {
		name        string
		policy      *spec.URLSecurityPolicySpec
		wantDial    bool
		wantErrText string
	}{
		{
			name:     "allowed by cidr",
			policy:   &spec.URLSecurityPolicySpec{AllowedCIDRs: []string{"fe80::/10"}},
			wantDial: true,
		},
		{
			name:        "blocked by default",
			wantErrText: "private target fe80::1%en0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialCalled := false
			dialAddress := ""
			client, err := newURLPolicyHTTPClient(
				&http.Client{Transport: &http.Transport{}},
				tt.policy,
				func(context.Context, string) ([]net.IPAddr, error) {
					return []net.IPAddr{{IP: net.ParseIP("fe80::1"), Zone: "en0"}}, nil
				},
				func(_ context.Context, _ string, stringAddress string) (net.Conn, error) {
					dialCalled = true
					dialAddress = stringAddress
					return nil, stopDial
				},
			)
			if err != nil {
				t.Fatalf("create policy client: %v", err)
			}
			defer client.CloseIdleConnections()

			_, err = client.Get("http://scoped.example:8080/path")
			if tt.wantDial {
				if !errors.Is(err, stopDial) {
					t.Fatalf("expected recording dial error, got: %v", err)
				}
				if !dialCalled || dialAddress != "[fe80::1%en0]:8080" {
					t.Fatalf("expected scoped address to retain zone, dialCalled=%t address=%q", dialCalled, dialAddress)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("expected scoped private address to be blocked, got: %v", err)
			}
			if dialCalled {
				t.Fatal("underlying dialer must not receive a blocked scoped address")
			}
		})
	}
}

func TestValidateURLTargetAllowsScopedIPv6LiteralByCIDR(t *testing.T) {
	resolverCalled := false
	parsed, err := validateURLTarget(
		context.Background(),
		"http://[fe80::1%25en0]:8080/path",
		&spec.URLSecurityPolicySpec{AllowedCIDRs: []string{"fe80::/10"}},
		func(context.Context, string) ([]net.IPAddr, error) {
			resolverCalled = true
			return nil, errors.New("unexpected resolver call")
		},
	)
	if err != nil {
		t.Fatalf("expected scoped literal to be allowed by cidr, got: %v", err)
	}
	if resolverCalled {
		t.Fatal("scoped ip literal must not require dns resolution")
	}
	if parsed.Hostname() != "fe80::1%en0" {
		t.Fatalf("expected parsed zone to be preserved, got %q", parsed.Hostname())
	}
}

func TestDialValidatedIPsAdvancesAfterPerAddressDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	parentDeadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected parent dial deadline")
	}

	firstAddress := "203.0.113.20:443"
	secondAddress := "203.0.113.21:443"
	var peer net.Conn
	dial := func(attemptCtx context.Context, _, address string) (net.Conn, error) {
		switch address {
		case firstAddress:
			attemptDeadline, hasDeadline := attemptCtx.Deadline()
			if !hasDeadline || !attemptDeadline.Before(parentDeadline) {
				return nil, errors.New("first address did not receive a partial deadline")
			}
			<-attemptCtx.Done()
			return nil, attemptCtx.Err()
		case secondAddress:
			conn, other := net.Pipe()
			peer = other
			return conn, nil
		default:
			return nil, errors.New("unexpected dial address")
		}
	}

	conn, err := dialValidatedIPs(
		ctx,
		"tcp",
		"443",
		[]net.IPAddr{testIPAddr("203.0.113.20"), testIPAddr("203.0.113.21")},
		dial,
		urlPolicyFallbackDelay,
	)
	if err != nil {
		t.Fatalf("expected second address to succeed, got: %v", err)
	}
	if peer == nil {
		t.Fatal("expected second address to be dialed")
	}
	_ = conn.Close()
	_ = peer.Close()
}

func TestDialValidatedIPsStartsOtherAddressFamilyAfterFallbackDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	primaryCanceled := make(chan struct{}, 1)
	var fallbackPeer net.Conn
	const fallbackDelay = 20 * time.Millisecond
	startedAt := time.Now()
	dial := func(attemptCtx context.Context, _, address string) (net.Conn, error) {
		switch address {
		case "[2001:db8::1]:443":
			<-attemptCtx.Done()
			primaryCanceled <- struct{}{}
			return nil, attemptCtx.Err()
		case "203.0.113.20:443":
			conn, peer := net.Pipe()
			fallbackPeer = peer
			return conn, nil
		default:
			return nil, errors.New("unexpected dial address")
		}
	}

	conn, err := dialValidatedIPs(
		ctx,
		"tcp",
		"443",
		[]net.IPAddr{testIPAddr("2001:db8::1"), testIPAddr("203.0.113.20")},
		dial,
		fallbackDelay,
	)
	if err != nil {
		t.Fatalf("expected fallback address family to succeed, got: %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed < fallbackDelay/2 {
		t.Fatalf("fallback racer started too early: %v", elapsed)
	}
	_ = conn.Close()
	_ = fallbackPeer.Close()
	select {
	case <-primaryCanceled:
	case <-time.After(time.Second):
		t.Fatal("expected winning fallback to cancel the primary racer")
	}
}

func TestDialValidatedIPsStartsFallbackImmediatelyAfterPrimaryFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	primaryErr := errors.New("primary failed")
	var fallbackPeer net.Conn
	dial := func(_ context.Context, _, address string) (net.Conn, error) {
		if address == "[2001:db8::1]:443" {
			return nil, primaryErr
		}
		conn, peer := net.Pipe()
		fallbackPeer = peer
		return conn, nil
	}

	startedAt := time.Now()
	conn, err := dialValidatedIPs(
		ctx,
		"tcp",
		"443",
		[]net.IPAddr{testIPAddr("2001:db8::1"), testIPAddr("203.0.113.20")},
		dial,
		time.Hour,
	)
	if err != nil {
		t.Fatalf("expected immediate fallback to succeed, got: %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed >= time.Second {
		t.Fatalf("fallback waited for configured delay after primary failure: %v", elapsed)
	}
	_ = conn.Close()
	_ = fallbackPeer.Close()
}

func TestDialValidatedIPsClosesLateSuccessfulConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	primaryStarted := make(chan struct{})
	releasePrimary := make(chan struct{})
	lateConn, latePeer := net.Pipe()
	defer lateConn.Close()
	defer latePeer.Close()
	winnerConn, winnerPeer := net.Pipe()
	defer winnerPeer.Close()
	dial := func(_ context.Context, _, address string) (net.Conn, error) {
		if address == "[2001:db8::1]:443" {
			close(primaryStarted)
			<-releasePrimary
			return lateConn, nil
		}
		<-primaryStarted
		return winnerConn, nil
	}

	conn, err := dialValidatedIPs(
		ctx,
		"tcp",
		"443",
		[]net.IPAddr{testIPAddr("2001:db8::1"), testIPAddr("203.0.113.20")},
		dial,
		0,
	)
	if err != nil {
		t.Fatalf("expected fallback connection to win, got: %v", err)
	}
	_ = conn.Close()
	close(releasePrimary)
	if err := latePeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set late peer read deadline: %v", err)
	}
	_, err = latePeer.Read(make([]byte, 1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected late successful connection to close with EOF, got: %v", err)
	}
}

func TestDialValidatedIPsJoinsAddressFamilyFailures(t *testing.T) {
	primaryErr := errors.New("primary failed")
	fallbackErr := errors.New("fallback failed")
	dial := func(_ context.Context, _, address string) (net.Conn, error) {
		if address == "[2001:db8::1]:443" {
			return nil, primaryErr
		}
		return nil, fallbackErr
	}

	_, err := dialValidatedIPs(
		context.Background(),
		"tcp",
		"443",
		[]net.IPAddr{testIPAddr("2001:db8::1"), testIPAddr("203.0.113.20")},
		dial,
		0,
	)
	if !errors.Is(err, primaryErr) || !errors.Is(err, fallbackErr) {
		t.Fatalf("expected both address family errors, got: %v", err)
	}
}

func TestDialValidatedIPsPropagatesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 2)
	dial := func(attemptCtx context.Context, _, _ string) (net.Conn, error) {
		started <- struct{}{}
		<-attemptCtx.Done()
		return nil, attemptCtx.Err()
	}
	result := make(chan error, 1)
	go func() {
		_, err := dialValidatedIPs(
			ctx,
			"tcp",
			"443",
			[]net.IPAddr{testIPAddr("2001:db8::1"), testIPAddr("203.0.113.20")},
			dial,
			0,
		)
		result <- err
	}()
	startedTimer := time.NewTimer(time.Second)
	defer startedTimer.Stop()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-startedTimer.C:
			t.Fatalf("expected both dial racers to start; observed %d", i)
		}
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dial did not return after parent cancellation")
	}
}

func TestNewURLPolicyHTTPClientRejectsWrappedDefaultTransport(t *testing.T) {
	originalDefaultTransport := http.DefaultTransport
	http.DefaultTransport = http.NewFileTransport(http.Dir(t.TempDir()))
	t.Cleanup(func() {
		http.DefaultTransport = originalDefaultTransport
	})

	_, err := NewURLPolicyHTTPClient(&http.Client{}, nil)
	if err == nil || !strings.Contains(err.Error(), "http.DefaultTransport") {
		t.Fatalf("expected wrapped default transport to return an error, got: %v", err)
	}
}

func TestNewURLPolicyHTTPClientRejectsCustomTransportRoutingHooks(t *testing.T) {
	tests := []struct {
		name      string
		transport *http.Transport
	}{
		{
			name: "DialContext",
			transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("unused")
			}},
		},
		{
			name: "Dial",
			transport: &http.Transport{Dial: func(string, string) (net.Conn, error) {
				return nil, errors.New("unused")
			}},
		},
		{
			name: "DialTLSContext",
			transport: &http.Transport{DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("unused")
			}},
		},
		{
			name: "DialTLS",
			transport: &http.Transport{DialTLS: func(string, string) (net.Conn, error) {
				return nil, errors.New("unused")
			}},
		},
		{
			name: "TLSNextProto",
			transport: &http.Transport{TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{
				"h2": func(string, *tls.Conn) http.RoundTripper {
					return http.DefaultTransport
				},
			}},
		},
		{
			name: "TLSClientConfig.ServerName",
			transport: &http.Transport{TLSClientConfig: &tls.Config{
				ServerName: "override.example.com",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewURLPolicyHTTPClient(&http.Client{Transport: tt.transport}, nil)
			if err == nil || !strings.Contains(err.Error(), "custom transport") {
				t.Fatalf("expected custom %s to be rejected, got: %v", tt.name, err)
			}
		})
	}
}

func TestNewURLPolicyHTTPClientRejectsDefaultTransportTLSServerName(t *testing.T) {
	originalDefaultTransport := http.DefaultTransport
	defaultTransport := http.DefaultTransport.(*http.Transport).Clone()
	defaultTransport.TLSClientConfig = &tls.Config{ServerName: "override.example.com"}
	http.DefaultTransport = defaultTransport
	t.Cleanup(func() {
		http.DefaultTransport = originalDefaultTransport
	})

	_, err := NewURLPolicyHTTPClient(&http.Client{}, nil)
	if err == nil || !strings.Contains(err.Error(), "TLSClientConfig.ServerName") {
		t.Fatalf("expected default transport TLS ServerName override to be rejected, got: %v", err)
	}
}

func TestNewURLPolicyHTTPClientRejectsTLSNextProtoAddedAfterHTTP2Configuration(t *testing.T) {
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.DialContext = nil
	baseTransport.Dial = nil
	baseTransport.CloseIdleConnections()
	if baseTransport.TLSNextProto == nil {
		t.Skip("standard library HTTP/2 support is disabled")
	}
	baseTransport.TLSNextProto["url-policy-test"] = func(string, *tls.Conn) http.RoundTripper {
		return http.DefaultTransport
	}

	_, err := NewURLPolicyHTTPClient(&http.Client{Transport: baseTransport}, nil)
	if err == nil || !strings.Contains(err.Error(), "TLSNextProto") {
		t.Fatalf("expected TLSNextProto added after HTTP/2 configuration to be rejected, got: %v", err)
	}
}

func TestNewURLPolicyHTTPClientAllowsAutomaticallyConfiguredDefaultHTTP2Transport(t *testing.T) {
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.DialContext = nil
	baseTransport.Dial = nil
	baseTransport.CloseIdleConnections()
	if baseTransport.TLSNextProto == nil {
		t.Skip("standard library HTTP/2 support is disabled")
	}

	originalDefaultTransport := http.DefaultTransport
	http.DefaultTransport = baseTransport
	t.Cleanup(func() {
		http.DefaultTransport = originalDefaultTransport
	})

	_, err := NewURLPolicyHTTPClient(&http.Client{}, nil)
	if err != nil {
		t.Fatalf("expected automatically configured default HTTP/2 transport to remain supported, got: %v", err)
	}
}

func TestNewURLPolicyHTTPClientReusesExplicitTransportAndPreservesHTTP2(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	base := &http.Client{Transport: &http.Transport{}}
	policy := &spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}
	for attempt := 0; attempt < 2; attempt++ {
		client, err := NewURLPolicyHTTPClient(base, policy)
		if err != nil {
			t.Fatalf("construct policy client attempt %d: %v", attempt+1, err)
		}
		transport := policyHTTPTransport(t, client)
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		}
		transport.TLSClientConfig.RootCAs = roots

		resp, err := client.Get(server.URL)
		if err != nil {
			client.CloseIdleConnections()
			t.Fatalf("send HTTP/2 request attempt %d: %v", attempt+1, err)
		}
		_ = resp.Body.Close()
		client.CloseIdleConnections()
		if resp.ProtoMajor != 2 {
			t.Fatalf("expected HTTP/2 on attempt %d, got %s", attempt+1, resp.Proto)
		}
	}
}

func TestNewURLPolicyHTTPClientConcurrentlyReusesExplicitTransport(t *testing.T) {
	const clientCount = 32
	base := &http.Client{Transport: &http.Transport{}}
	start := make(chan struct{})
	results := make(chan error, clientCount)
	var wg sync.WaitGroup
	for index := 0; index < clientCount; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			client, err := NewURLPolicyHTTPClient(base, nil)
			if err == nil && client == nil {
				err = errors.New("constructed client is nil")
			}
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent policy client construction failed: %v", err)
		}
	}
}

func TestURLPolicyTransportDisablesProxyAndBlocksPrivateTarget(t *testing.T) {
	proxyCalled := false
	dialCalled := false
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.DialContext = nil
	baseTransport.Dial = nil
	baseTransport.Proxy = func(*http.Request) (*url.URL, error) {
		proxyCalled = true
		return url.Parse("http://127.0.0.1:3128")
	}
	client, err := newURLPolicyHTTPClient(
		&http.Client{Transport: baseTransport},
		nil,
		func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{testIPAddr("10.0.0.10")}, nil
		},
		func(context.Context, string, string) (net.Conn, error) {
			dialCalled = true
			return nil, errors.New("unexpected dial")
		},
	)
	if err != nil {
		t.Fatalf("create policy client: %v", err)
	}
	transport := policyHTTPTransport(t, client)
	if transport.Proxy != nil {
		t.Fatal("policy transport must disable proxy routing")
	}
	if transport.Dial != nil {
		t.Fatal("policy transport must clear the legacy Dial hook")
	}
	_, err = transport.DialContext(context.Background(), "tcp", "proxy-target.example:80")
	if err == nil || !strings.Contains(err.Error(), "private target 10.0.0.10") {
		t.Fatalf("expected private proxy-resolved target to be blocked, got: %v", err)
	}
	if proxyCalled || dialCalled {
		t.Fatalf("proxy and underlying dialer must not run, proxyCalled=%t dialCalled=%t", proxyCalled, dialCalled)
	}
}

func TestURLPolicyClientPreservesHostAndTLSSNI(t *testing.T) {
	type requestIdentity struct {
		host string
		sni  string
	}
	identity := make(chan requestIdentity, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity <- requestIdentity{host: r.Host, sni: r.TLS.ServerName}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test server url: %v", err)
	}
	serverIP := net.ParseIP(serverURL.Hostname())
	_, port, err := net.SplitHostPort(serverURL.Host)
	if err != nil {
		t.Fatalf("split test server host: %v", err)
	}
	policy := &spec.URLSecurityPolicySpec{AllowedHostPatterns: []string{"origin.example.com"}}
	baseTransport := server.Client().Transport.(*http.Transport).Clone()
	baseTransport.DialContext = nil
	baseTransport.Dial = nil
	client, err := newURLPolicyHTTPClient(
		&http.Client{Transport: baseTransport},
		policy,
		func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: serverIP}}, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("create policy client: %v", err)
	}
	resp, err := client.Get("https://origin.example.com:" + port + "/identity")
	if err != nil {
		t.Fatalf("send policy request: %v", err)
	}
	resp.Body.Close()

	seen := <-identity
	if seen.host != "origin.example.com:"+port {
		t.Fatalf("expected original Host header, got %q", seen.host)
	}
	if seen.sni != "origin.example.com" {
		t.Fatalf("expected original TLS SNI, got %q", seen.sni)
	}
}

func TestReadFileFromURLSimpleClosesIdleConnection(t *testing.T) {
	closed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}
	server.Start()
	defer server.Close()

	data, err := ReadFileFromURLSimple(
		context.Background(),
		server.URL,
		&spec.URLSecurityPolicySpec{AllowPrivateByDefault: true},
	)
	if err != nil {
		t.Fatalf("read remote file: %v", err)
	}
	if string(data) != "ok" {
		t.Fatalf("unexpected response body %q", data)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("expected one-shot fetch client to close its idle connection")
	}
}

func TestReadFileFromURLSimpleRejectsRedirectToPrivateTarget(t *testing.T) {
	t.Parallel()

	privateTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("private"))
	}))
	defer privateTarget.Close()

	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, privateTarget.URL, http.StatusFound)
	}))
	defer redirectServer.Close()

	entryURL := strings.Replace(redirectServer.URL, "127.0.0.1", "localhost", 1)
	policy := &spec.URLSecurityPolicySpec{
		AllowedHostPatterns: []string{"localhost"},
	}
	_, err := ReadFileFromURLSimple(context.Background(), entryURL, policy)
	if err == nil {
		t.Fatal("expected redirect to private target to be rejected")
	}
}

func TestReadFileFromURLForConversionRejectsRedirectToPrivateTarget(t *testing.T) {
	t.Parallel()

	privateTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("private"))
	}))
	defer privateTarget.Close()

	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, privateTarget.URL, http.StatusFound)
	}))
	defer redirectServer.Close()

	entryURL := strings.Replace(redirectServer.URL, "127.0.0.1", "localhost", 1)
	policy := &spec.URLSecurityPolicySpec{
		AllowedHostPatterns: []string{"localhost"},
	}
	_, err := ReadFileFromURLForConversion(context.Background(), entryURL, policy)
	if err == nil {
		t.Fatal("expected redirect to private target to be rejected")
	}
}

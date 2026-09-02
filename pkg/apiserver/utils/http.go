package utils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

const (
	// ConfigMapMaxSize 1MB in bytes
	ConfigMapMaxSize = 1024 * 1024
	// ConvertYAMLMaxSize 10MB in bytes
	ConvertYAMLMaxSize     = 10 * 1024 * 1024
	urlPolicyDialTimeout   = 30 * time.Second
	urlPolicyFallbackDelay = 300 * time.Millisecond
)

var sharedRemoteFetchClient = &http.Client{Timeout: 30 * time.Second}

type urlHostResolver func(context.Context, string) ([]net.IPAddr, error)
type networkDialContext func(context.Context, string, string) (net.Conn, error)

type urlPolicyRequestDeadlineKey struct{}

type urlPolicyTransport struct {
	*http.Transport
}

func (t *urlPolicyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if deadline, hasDeadline := req.Context().Deadline(); hasDeadline {
		ctx := context.WithValue(req.Context(), urlPolicyRequestDeadlineKey{}, deadline)
		req = req.WithContext(ctx)
	}
	return t.Transport.RoundTrip(req)
}

// ClientIP get client ip
func ClientIP(r *http.Request) string {
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	ip := strings.TrimSpace(strings.Split(xForwardedFor, ",")[0])
	if ip != "" {
		return ip
	}

	ip = strings.TrimSpace(r.Header.Get("X-Real-Ip"))
	if ip != "" {
		return ip
	}

	if ip, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr)); err == nil {
		return ip
	}

	return ""
}

// ResponseCapture capture response and get response info
type ResponseCapture struct {
	http.ResponseWriter
	wroteHeader bool
	status      int
	body        *bytes.Buffer
}

// NewResponseCapture new response capture
func NewResponseCapture(w http.ResponseWriter) *ResponseCapture {
	return &ResponseCapture{
		ResponseWriter: w,
		wroteHeader:    false,
		body:           new(bytes.Buffer),
	}
}

// Header return response writer header
func (c *ResponseCapture) Header() http.Header {
	return c.ResponseWriter.Header()
}

// Write data to response writer and body
func (c *ResponseCapture) Write(data []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	c.body.Write(data)
	return c.ResponseWriter.Write(data)
}

// WriteHeader write header to response writer
func (c *ResponseCapture) WriteHeader(statusCode int) {
	c.status = statusCode
	c.wroteHeader = true
	c.ResponseWriter.WriteHeader(statusCode)
}

// Bytes return response body bytes
func (c *ResponseCapture) Bytes() []byte {
	return c.body.Bytes()
}

// StatusCode return status code
func (c *ResponseCapture) StatusCode() int {
	return c.status
}

// CleanRelativePath returns the shortest path name equivalent to path
// by purely lexical processing. It make sure the provided path is rooted
// and then uses filepath.Clean and filepath.Rel to make sure the path
// doesn't include any separators or elements that shouldn't be there
// like ., .., //.
func CleanRelativePath(path string) (string, error) {
	cleanPath := filepath.Clean(filepath.Join("/", path))
	rel, err := filepath.Rel("/", cleanPath)
	if err != nil {
		// slash is prepended above therefore this is not expected to fail
		return "", err
	}

	return rel, nil
}

func isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsPrivate() {
		return true
	}
	return false
}

// ValidateURLTarget validates outbound URL safety and applies private target policy checks.
func ValidateURLTarget(ctx context.Context, rawURL string, policy *spec.URLSecurityPolicySpec) (*url.URL, error) {
	return validateURLTarget(ctx, rawURL, policy, resolveURLHostIPs)
}

func validateURLTarget(ctx context.Context, rawURL string, policy *spec.URLSecurityPolicySpec, resolver urlHostResolver) (*url.URL, error) {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("invalid url %q: %w", rawURL, err)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("unsupported url scheme %q", parsed.Scheme)
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return nil, fmt.Errorf("url %q has empty host", rawURL)
	}

	effectivePolicy, err := effectiveURLSecurityPolicy(policy)
	if err != nil {
		return nil, err
	}
	if effectivePolicy.AllowPrivateByDefault {
		return parsed, nil
	}
	if ip, ok := parseURLHostIP(host); ok {
		if err := validateResolvedURLIPs(rawURL, host, []net.IPAddr{ip}, effectivePolicy); err != nil {
			return nil, err
		}
		return parsed, nil
	}

	if resolver == nil {
		resolver = resolveURLHostIPs
	}
	ips, err := resolver(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve host %q: %w", host, err)
	}
	if err := validateResolvedURLIPs(rawURL, host, ips, effectivePolicy); err != nil {
		return nil, err
	}
	return parsed, nil
}

func effectiveURLSecurityPolicy(policy *spec.URLSecurityPolicySpec) (spec.URLSecurityPolicySpec, error) {
	effectivePolicy := spec.DefaultURLSecurityPolicy()
	if policy != nil {
		effectivePolicy = *policy
	}
	effectivePolicy = spec.NormalizeURLSecurityPolicy(effectivePolicy)
	if err := spec.ValidateURLSecurityPolicy(effectivePolicy); err != nil {
		return spec.URLSecurityPolicySpec{}, fmt.Errorf("invalid url security policy: %w", err)
	}
	return effectivePolicy, nil
}

func validateResolvedURLIPs(rawURL, host string, ips []net.IPAddr, policy spec.URLSecurityPolicySpec) error {
	if len(ips) == 0 {
		return fmt.Errorf("resolve host %q: no ip addresses returned", host)
	}
	for _, ip := range ips {
		if ip.IP == nil {
			return fmt.Errorf("resolve host %q: empty ip address returned", host)
		}
	}
	if policy.AllowPrivateByDefault {
		return nil
	}
	allowedByHostPattern := hostMatchesAllowedPatterns(host, policy.AllowedHostPatterns)
	allowedCIDRs, err := parseAllowedCIDRs(policy.AllowedCIDRs)
	if err != nil {
		return fmt.Errorf("invalid allowed cidr: %w", err)
	}
	for _, ip := range ips {
		if !isPrivateIP(ip.IP) || allowedByHostPattern || ipMatchesAllowedCIDRs(ip.IP, allowedCIDRs) {
			continue
		}
		return fmt.Errorf("url %q resolves to private target %s", rawURL, ip.String())
	}
	return nil
}

func parseURLHostIP(host string) (net.IPAddr, bool) {
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return net.IPAddr{}, false
	}
	return net.IPAddr{IP: net.IP(ip.AsSlice()), Zone: ip.Zone()}, true
}

func resolveURLHostIPs(ctx context.Context, host string) ([]net.IPAddr, error) {
	if ip, ok := parseURLHostIP(host); ok {
		return []net.IPAddr{ip}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(lookupCtx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IPAddr, 0, len(addrs))
	for _, addr := range addrs {
		if addr.IP != nil {
			ips = append(ips, addr)
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no ip addresses returned")
	}
	return ips, nil
}

// ReadFileFromURLSimple 工具：供需要从远端拉取配置文本的场景复用
func ReadFileFromURLSimple(ctx context.Context, rawURL string, policy *spec.URLSecurityPolicySpec) ([]byte, error) {
	resp, closeIdleConnections, err := getURLWithPolicy(ctx, rawURL, policy)
	if err != nil {
		return nil, err
	}
	defer closeIdleConnections()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
	}
	rd := io.LimitReader(resp.Body, ConfigMapMaxSize+1024)
	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// ReadFileFromURLForConversion fetches YAML content for the conversion API without size limits.
func ReadFileFromURLForConversion(ctx context.Context, rawURL string, policy *spec.URLSecurityPolicySpec) ([]byte, error) {
	resp, closeIdleConnections, err := getURLWithPolicy(ctx, rawURL, policy)
	if err != nil {
		return nil, err
	}
	defer closeIdleConnections()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
	}
	rd := io.LimitReader(resp.Body, ConvertYAMLMaxSize+1)
	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	if len(data) > ConvertYAMLMaxSize {
		return nil, fmt.Errorf("file size %d bytes exceeds convert yaml maximum size %d bytes", len(data), ConvertYAMLMaxSize)
	}
	return data, nil
}

func getURLWithPolicy(ctx context.Context, rawURL string, policy *spec.URLSecurityPolicySpec) (*http.Response, func(), error) {
	parsed, err := ValidateURLTarget(ctx, rawURL, policy)
	if err != nil {
		return nil, nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	client, err := NewURLPolicyHTTPClient(sharedRemoteFetchClient, policy)
	if err != nil {
		return nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		client.CloseIdleConnections()
		return nil, nil, err
	}
	return resp, client.CloseIdleConnections, nil
}

// NewURLPolicyHTTPClient clones base into a direct-only HTTP client whose
// DialContext resolves, validates, and dials the same IP address. A non-nil
// base Transport must be a *http.Transport without custom DialContext, Dial,
// DialTLSContext, DialTLS, or TLSNextProto hooks. Neither it nor the implicit
// http.DefaultTransport may override TLSClientConfig.ServerName. Request Host
// and TLS SNI remain derived from the original URL hostname.
func NewURLPolicyHTTPClient(base *http.Client, policy *spec.URLSecurityPolicySpec) (*http.Client, error) {
	return newURLPolicyHTTPClient(base, policy, resolveURLHostIPs, nil)
}

func newURLPolicyHTTPClient(base *http.Client, policy *spec.URLSecurityPolicySpec, resolver urlHostResolver, dial networkDialContext) (*http.Client, error) {
	if base == nil {
		base = &http.Client{}
	}
	effectivePolicy, err := effectiveURLSecurityPolicy(policy)
	if err != nil {
		return nil, err
	}
	transport, err := cloneURLPolicyTransport(base.Transport, effectivePolicy, resolver, dial)
	if err != nil {
		return nil, err
	}
	baseRedirect := base.CheckRedirect
	client := &http.Client{
		Transport: &urlPolicyTransport{Transport: transport},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			if _, err := validateURLTarget(req.Context(), req.URL.String(), &effectivePolicy, resolver); err != nil {
				return err
			}
			if baseRedirect != nil {
				return baseRedirect(req, via)
			}
			return nil
		},
		Jar:     base.Jar,
		Timeout: base.Timeout,
	}
	return client, nil
}

func cloneURLPolicyTransport(base http.RoundTripper, policy spec.URLSecurityPolicySpec, resolver urlHostResolver, dial networkDialContext) (*http.Transport, error) {
	var sourceTransport *http.Transport
	switch typed := base.(type) {
	case nil:
		defaultTransport, ok := http.DefaultTransport.(*http.Transport)
		if !ok || defaultTransport == nil {
			return nil, fmt.Errorf("url policy requires http.DefaultTransport to be a non-nil *http.Transport, got %T", http.DefaultTransport)
		}
		sourceTransport = defaultTransport
	case *http.Transport:
		if typed == nil {
			return nil, errors.New("url policy requires a non-nil *http.Transport")
		}
		if typed.DialContext != nil || typed.Dial != nil {
			return nil, errors.New("url policy does not support custom transport DialContext or Dial")
		}
		sourceTransport = typed
	default:
		return nil, fmt.Errorf("url policy requires *http.Transport, got %T", base)
	}

	// Clone may lazily initialize HTTP/2 on sourceTransport. Do not inspect
	// TLSNextProto until Clone returns: concurrent clones synchronize through
	// the Transport's internal once, and generated protocol hooks are then
	// distinguishable from caller-supplied hooks retained by the clone.
	transport := sourceTransport.Clone()
	if transport.DialTLSContext != nil || transport.DialTLS != nil {
		return nil, errors.New("url policy does not support custom transport DialTLSContext or DialTLS")
	}
	if err := preserveURLPolicyHTTP2(sourceTransport, transport); err != nil {
		return nil, err
	}
	if transport.TLSClientConfig != nil && transport.TLSClientConfig.ServerName != "" {
		return nil, errors.New("url policy does not support custom transport TLSClientConfig.ServerName")
	}
	if resolver == nil {
		resolver = resolveURLHostIPs
	}
	if dial == nil {
		dialer := &net.Dialer{Timeout: urlPolicyDialTimeout, KeepAlive: 30 * time.Second}
		dial = dialer.DialContext
	}
	// Environment proxies are not a trusted security boundary. Policy-bound
	// requests connect directly until an explicit trusted-proxy policy exists.
	transport.Proxy = nil
	transport.Dial = nil
	transport.DialContext = urlPolicyDialContext(policy, resolver, dial)
	// Custom TLS dialers could bypass the policy DialContext.
	transport.DialTLS = nil
	transport.DialTLSContext = nil
	return transport, nil
}

func preserveURLPolicyHTTP2(source, transport *http.Transport) error {
	// Clone retains a caller-supplied TLSNextProto map, but omits entries that
	// net/http generated after TLSNextProto was initially nil.
	if transport.TLSNextProto != nil {
		return errors.New("url policy does not support custom transport TLSNextProto")
	}
	if source.TLSNextProto == nil {
		return nil
	}

	for protocol, roundTripper := range source.TLSNextProto {
		switch protocol {
		case "h2", "unencrypted_http2":
			if roundTripper == nil {
				return errors.New("url policy does not support custom transport TLSNextProto")
			}
		default:
			return errors.New("url policy does not support custom transport TLSNextProto")
		}
	}
	if source.TLSNextProto["h2"] != nil {
		// The clone may already advertise h2 in TLSClientConfig.NextProtos while
		// omitting net/http's generated TLSNextProto map. ForceAttemptHTTP2 makes
		// the clone regenerate matching protocol handlers after DialContext is
		// replaced by the policy dialer.
		transport.ForceAttemptHTTP2 = true
	}
	return nil
}

func urlPolicyDialContext(policy spec.URLSecurityPolicySpec, resolver urlHostResolver, dial networkDialContext) networkDialContext {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("split dial address %q: %w", address, err)
		}

		dialCtx, cancel := urlPolicyDialTimeoutContext(ctx)
		defer cancel()

		var ips []net.IPAddr
		if ip, ok := parseURLHostIP(host); ok {
			ips = []net.IPAddr{ip}
		} else {
			ips, err = resolver(dialCtx, host)
			if err != nil {
				return nil, fmt.Errorf("resolve dial host %q: %w", host, err)
			}
		}
		if err := validateResolvedURLIPs("//"+address, host, ips, policy); err != nil {
			return nil, err
		}

		conn, err := dialValidatedIPs(dialCtx, network, port, ips, dial, urlPolicyFallbackDelay)
		if err != nil {
			return nil, fmt.Errorf("dial validated target %q: %w", host, err)
		}
		return conn, nil
	}
}

func urlPolicyDialTimeoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(urlPolicyDialTimeout)
	if dialDeadline, hasDeadline := ctx.Deadline(); hasDeadline && dialDeadline.Before(deadline) {
		deadline = dialDeadline
	}
	if requestDeadline, ok := ctx.Value(urlPolicyRequestDeadlineKey{}).(time.Time); ok && requestDeadline.Before(deadline) {
		deadline = requestDeadline
	}
	return context.WithDeadline(ctx, deadline)
}

func dialValidatedIPs(ctx context.Context, network, port string, ips []net.IPAddr, dial networkDialContext, fallbackDelay time.Duration) (net.Conn, error) {
	primary, fallback, err := partitionDialIPs(network, ips)
	if err != nil {
		return nil, err
	}
	if len(fallback) == 0 {
		return dialIPGroup(ctx, network, port, primary, dial)
	}

	returned := make(chan struct{})
	defer close(returned)
	type dialResult struct {
		conn    net.Conn
		err     error
		primary bool
	}
	results := make(chan dialResult)
	startRacer := func(racerCtx context.Context, candidates []net.IPAddr, primary bool) {
		conn, dialErr := dialIPGroup(racerCtx, network, port, candidates, dial)
		select {
		case results <- dialResult{conn: conn, err: dialErr, primary: primary}:
		case <-returned:
			if conn != nil {
				_ = conn.Close()
			}
		}
	}

	primaryCtx, cancelPrimary := context.WithCancel(ctx)
	defer cancelPrimary()
	go startRacer(primaryCtx, primary, true)

	fallbackTimer := time.NewTimer(fallbackDelay)
	defer fallbackTimer.Stop()
	var cancelFallback context.CancelFunc
	fallbackStarted := false
	startFallback := func() {
		if fallbackStarted {
			return
		}
		fallbackStarted = true
		fallbackCtx, cancel := context.WithCancel(ctx)
		cancelFallback = cancel
		go startRacer(fallbackCtx, fallback, false)
	}
	defer func() {
		if cancelFallback != nil {
			cancelFallback()
		}
	}()

	var primaryErr, fallbackErr error
	primaryDone := false
	fallbackDone := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-fallbackTimer.C:
			startFallback()
		case result := <-results:
			if result.err == nil {
				return result.conn, nil
			}
			if result.primary {
				primaryDone = true
				primaryErr = result.err
				if !fallbackStarted {
					fallbackTimer.Stop()
					startFallback()
				}
			} else {
				fallbackDone = true
				fallbackErr = result.err
			}
			if primaryDone && fallbackDone {
				return nil, errors.Join(primaryErr, fallbackErr)
			}
		}
	}
}

func partitionDialIPs(network string, ips []net.IPAddr) ([]net.IPAddr, []net.IPAddr, error) {
	if len(ips) == 0 {
		return nil, nil, errors.New("no validated ip addresses")
	}
	if network == "tcp4" || network == "tcp6" {
		wantIPv4 := network == "tcp4"
		filtered := make([]net.IPAddr, 0, len(ips))
		for _, ip := range ips {
			if (ip.IP.To4() != nil) == wantIPv4 {
				filtered = append(filtered, ip)
			}
		}
		if len(filtered) == 0 {
			return nil, nil, fmt.Errorf("no validated %s addresses", network)
		}
		return filtered, nil, nil
	}
	if network != "tcp" {
		return ips, nil, nil
	}

	primaryIsIPv4 := ips[0].IP.To4() != nil
	primary := make([]net.IPAddr, 0, len(ips))
	fallback := make([]net.IPAddr, 0, len(ips))
	for _, ip := range ips {
		if (ip.IP.To4() != nil) == primaryIsIPv4 {
			primary = append(primary, ip)
		} else {
			fallback = append(fallback, ip)
		}
	}
	return primary, fallback, nil
}

func dialIPGroup(ctx context.Context, network, port string, ips []net.IPAddr, dial networkDialContext) (net.Conn, error) {
	dialErrors := make([]error, 0, len(ips))
	for index, ip := range ips {
		if err := ctx.Err(); err != nil {
			dialErrors = append(dialErrors, err)
			break
		}

		attemptCtx := ctx
		cancelAttempt := func() {}
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				dialErrors = append(dialErrors, context.DeadlineExceeded)
				break
			}
			attemptTimeout := remaining / time.Duration(len(ips)-index)
			attemptCtx, cancelAttempt = context.WithTimeout(ctx, attemptTimeout)
		}

		validatedAddress := net.JoinHostPort(ip.String(), port)
		conn, dialErr := dial(attemptCtx, network, validatedAddress)
		cancelAttempt()
		if dialErr == nil {
			return conn, nil
		}
		dialErrors = append(dialErrors, fmt.Errorf("%s: %w", validatedAddress, dialErr))
	}
	return nil, errors.Join(dialErrors...)
}

func parseAllowedCIDRs(cidrs []string) ([]*net.IPNet, error) {
	if len(cidrs) == 0 {
		return nil, nil
	}
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(strings.TrimSpace(cidr))
		if err != nil {
			return nil, err
		}
		out = append(out, network)
	}
	return out, nil
}

func ipMatchesAllowedCIDRs(ip net.IP, cidrs []*net.IPNet) bool {
	for _, cidr := range cidrs {
		if cidr != nil && cidr.Contains(ip) {
			return true
		}
	}
	return false
}

func hostMatchesAllowedPatterns(host string, patterns []string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return false
	}
	for _, pattern := range patterns {
		pattern = strings.ToLower(strings.TrimSpace(pattern))
		pattern = strings.TrimSuffix(pattern, ".")
		if pattern == "" {
			continue
		}
		if strings.HasPrefix(pattern, "*.") {
			base := strings.TrimPrefix(pattern, "*.")
			if base == "" || host == base {
				continue
			}
			if strings.HasSuffix(host, "."+base) {
				return true
			}
			continue
		}
		if host == pattern {
			return true
		}
	}
	return false
}

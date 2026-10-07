package apiserver

import (
	"context"
	"net"
	"sync"
)

// A business connection belongs to one term, including while it is idle.
// Closing it on loss lets clients reconnect through the current Service route.
type leaderConn struct {
	net.Conn
	mu       sync.Mutex
	term     context.Context
	stopTerm func() bool
	closed   bool
}

func (c *leaderConn) bind(term context.Context) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || term == nil || term.Err() != nil {
		return false
	}
	if c.term != nil {
		return c.term == term
	}
	c.term = term
	c.stopTerm = context.AfterFunc(term, func() { _ = c.Close() })
	return true
}

func (c *leaderConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	stop := c.stopTerm
	c.stopTerm = nil
	c.mu.Unlock()
	if stop != nil {
		stop()
	}
	return c.Conn.Close()
}

type leaderListener struct {
	net.Listener
	server            *restServer
	requireLeadership bool
}

func (l *leaderListener) Accept() (net.Conn, error) {
	for {
		raw, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		conn := &leaderConn{Conn: raw}
		// gRPC has no Pod health endpoint: do not retain connections routed to
		// a Worker by a stale Service endpoint. HTTP binds on business admission
		// instead, so Worker health probes remain available.
		if l.requireLeadership && !conn.bind(l.server.currentLeaderTerm()) {
			_ = conn.Close()
			continue
		}
		return conn, nil
	}
}

func (s *restServer) currentLeaderTerm() context.Context {
	s.leaderMu.RLock()
	defer s.leaderMu.RUnlock()
	if s.leaderCtx == nil || s.leaderCtx.Err() != nil {
		return nil
	}
	return s.leaderCtx
}

type httpConnectionKey struct{}

func (s *restServer) httpConnectionContext(ctx context.Context, conn net.Conn) context.Context {
	if connection, ok := conn.(*leaderConn); ok {
		return context.WithValue(ctx, httpConnectionKey{}, connection)
	}
	return ctx
}

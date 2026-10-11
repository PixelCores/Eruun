package ratelimit

import (
	"net/http"
	"strings"

	"golang.org/x/time/rate"
)

// Limiter shares HTTP and gRPC operation-class budgets in one API process.
type Limiter struct {
	expensive *rate.Limiter
	read      *rate.Limiter
}

func New(qps float64, burst int) *Limiter {
	if qps <= 0 || burst <= 0 {
		return nil
	}
	return &Limiter{
		expensive: rate.NewLimiter(rate.Limit(qps), burst),
		read:      rate.NewLimiter(rate.Limit(qps*5), burst*5),
	}
}

func (l *Limiter) Allow(expensive bool) bool {
	if l == nil {
		return true
	}
	if expensive {
		return l.expensive.Allow()
	}
	return l.read.Allow()
}

// IsExpensiveRequest classifies the shared HTTP and RPC operation budget.
func IsExpensiveRequest(method, path string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
	default:
		return true
	}

	normalizedPath := strings.ToLower(strings.TrimSpace(path))
	return strings.Contains(normalizedPath, "/exec") ||
		strings.Contains(normalizedPath, "/logs") ||
		strings.Contains(normalizedPath, "/shell/stream") ||
		strings.Contains(normalizedPath, "/files/export")
}

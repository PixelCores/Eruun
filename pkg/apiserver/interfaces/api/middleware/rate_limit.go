package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

const (
	defaultRateLimitReadMultiplier = 5
)

// RateLimitOptions configures API request throttling by operation class.
type RateLimitOptions struct {
	QPS       float64
	Burst     int
	SkipPaths []string
}

// DefaultRateLimitSkipPaths returns routes that should bypass request throttling.
func DefaultRateLimitSkipPaths() []string {
	return []string{
		"/api/v1/health",
		"/api/v1/healthz",
		"/api/v1/ready",
		"/api/v1/readyz",
	}
}

// RateLimit throttles API requests by operation class.
func RateLimit(opts RateLimitOptions) gin.HandlerFunc {
	if opts.QPS <= 0 || opts.Burst <= 0 {
		return func(c *gin.Context) {
			c.Next()
		}
	}

	expensiveLimiter := rate.NewLimiter(rate.Limit(opts.QPS), opts.Burst)
	readLimiter := rate.NewLimiter(rate.Limit(opts.QPS*defaultRateLimitReadMultiplier), opts.Burst*defaultRateLimitReadMultiplier)
	skipPathSet := toPathSet(opts.SkipPaths)

	return func(c *gin.Context) {
		method := ""
		if c.Request != nil {
			method = c.Request.Method
		}

		fullPath := strings.TrimSpace(c.FullPath())
		if fullPath == "" && c.Request != nil && c.Request.URL != nil {
			fullPath = strings.TrimSpace(c.Request.URL.Path)
		}
		if shouldSkipAuthPath(fullPath, skipPathSet) {
			c.Next()
			return
		}

		limiter := readLimiter
		if isExpensiveRateLimitRequest(method, fullPath) {
			limiter = expensiveLimiter
		}
		if !limiter.Allow() {
			c.AbortWithStatus(http.StatusTooManyRequests)
			return
		}

		c.Next()
	}
}

func isExpensiveRateLimitRequest(method, path string) bool {
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

package middleware

import (
	"net/http"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/ratelimit"
	"github.com/gin-gonic/gin"
)

const defaultRateLimitReadMultiplier = 5

// RateLimitOptions configures API request throttling by operation class.
type RateLimitOptions struct {
	QPS       float64
	Burst     int
	SkipPaths []string
	Shared    *ratelimit.Limiter
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
	limiter := opts.Shared
	if limiter == nil {
		limiter = ratelimit.New(opts.QPS, opts.Burst)
	}
	if limiter == nil {
		return func(c *gin.Context) {
			c.Next()
		}
	}

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
		if pathInSet(fullPath, skipPathSet) {
			c.Next()
			return
		}

		if !limiter.Allow(isExpensiveRateLimitRequest(method, fullPath)) {
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

func pathInSet(path string, set map[string]struct{}) bool {
	_, ok := set[path]
	return path != "" && ok
}
func toPathSet(paths []string) map[string]struct{} {
	result := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if path != "" {
			result[path] = struct{}{}
		}
	}
	return result
}

package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/ratelimit"
)

// RateLimit uses the same limiter as gRPC; a nil limiter disables throttling.
func RateLimit(limiter *ratelimit.Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := strings.TrimSpace(c.FullPath())
		if path == "" {
			path = c.Request.URL.Path
		}
		if !IsHealthPath(path) && !limiter.Allow(ratelimit.IsExpensiveRequest(c.Request.Method, path)) {
			c.AbortWithStatus(http.StatusTooManyRequests)
			return
		}
		c.Next()
	}
}

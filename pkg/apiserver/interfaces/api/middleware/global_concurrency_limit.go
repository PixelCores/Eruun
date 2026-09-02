package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// GlobalConcurrencyLimitOptions configures process-local API request concurrency.
type GlobalConcurrencyLimitOptions struct {
	MaxConcurrent int
	SkipPaths     []string
}

// GlobalConcurrencyLimit caps in-flight requests for the current process.
func GlobalConcurrencyLimit(opts GlobalConcurrencyLimitOptions) gin.HandlerFunc {
	if opts.MaxConcurrent <= 0 {
		return func(c *gin.Context) {
			c.Next()
		}
	}

	limiter := make(chan struct{}, opts.MaxConcurrent)
	skipPathSet := toPathSet(opts.SkipPaths)

	return func(c *gin.Context) {
		fullPath := strings.TrimSpace(c.FullPath())
		if fullPath == "" && c.Request != nil && c.Request.URL != nil {
			fullPath = strings.TrimSpace(c.Request.URL.Path)
		}
		if shouldSkipAuthPath(fullPath, skipPathSet) {
			c.Next()
			return
		}

		select {
		case limiter <- struct{}{}:
			defer func() {
				<-limiter
			}()
			c.Next()
		default:
			c.AbortWithStatus(http.StatusServiceUnavailable)
		}
	}
}

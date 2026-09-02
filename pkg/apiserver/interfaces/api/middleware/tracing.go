package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/klog/v2"
)

// Logging is a gin middleware that logs request details along with tracing information.
func Logging() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		latency := time.Since(start)

		span := trace.SpanFromContext(c.Request.Context())
		spanCtx := span.SpanContext()

		fields := []any{
			"status", c.Writer.Status(),
			"method", c.Request.Method,
			"path", path,
			"ip", c.ClientIP(),
			"latency", latency.String(),
			"user-agent", c.Request.UserAgent(),
		}
		if spanCtx.IsValid() {
			fields = append(fields,
				"traceID", spanCtx.TraceID().String(),
				"spanID", spanCtx.SpanID().String(),
			)
		}

		if isLowVerbosityRequestLog(c.Request.Method, path) {
			klog.V(4).InfoS("HTTP request", fields...)
			return
		}
		klog.InfoS("HTTP request", fields...)
	}
}

func isLowVerbosityRequestLog(method, path string) bool {
	switch strings.TrimSpace(path) {
	case "/health", "/healthz", "/ready", "/readyz",
		"/api/v1/health", "/api/v1/healthz", "/api/v1/ready", "/api/v1/readyz":
		return true
	case "/api/v1/applications/components/status":
		return strings.EqualFold(strings.TrimSpace(method), http.MethodPost)
	default:
		return false
	}
}

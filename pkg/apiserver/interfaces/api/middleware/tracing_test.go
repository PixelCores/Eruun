package middleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"k8s.io/klog/v2"
)

func TestLoggingMiddleware(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)

	var writer *httptest.ResponseRecorder
	logOutput := captureKlogOutput(t, func() {
		gin.SetMode(gin.TestMode)
		router := gin.New()
		router.Use(otelgin.Middleware("test-service"))
		router.Use(Logging())
		router.GET("/ping", func(c *gin.Context) {
			c.String(http.StatusOK, "pong")
		})

		writer = httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(context.Background(), "GET", "/ping", nil)
		router.ServeHTTP(writer, req)
	})

	assert.Equal(t, http.StatusOK, writer.Code)

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	span := spans[0]

	traceID := span.SpanContext.TraceID().String()
	assert.True(t, strings.Contains(logOutput, traceID), "Log output should contain the traceID")
	assert.True(t, strings.Contains(logOutput, `status=200`), "Log output should contain the status code")
	assert.True(t, strings.Contains(logOutput, `path="/ping"`), "Log output should contain the path")

	t.Logf("Captured log: %s", logOutput)
	t.Logf("Captured traceID: %s", traceID)
}

func TestLoggingMiddlewareLowersHealthCheckVerbosity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(Logging())
	healthCheckPaths := []string{
		"/api/v1/health",
		"/api/v1/healthz",
		"/api/v1/ready",
		"/api/v1/readyz",
		"/health",
		"/healthz",
		"/ready",
		"/readyz",
	}
	for _, path := range healthCheckPaths {
		path := path
		router.GET(path, func(c *gin.Context) {
			c.String(http.StatusOK, "ok")
		})
	}

	logOutput := captureKlogOutput(t, func() {
		for _, path := range healthCheckPaths {
			writer := httptest.NewRecorder()
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
			router.ServeHTTP(writer, req)
			assert.Equal(t, http.StatusOK, writer.Code)
		}
	})

	for _, path := range healthCheckPaths {
		assert.NotContains(t, logOutput, `path="`+path+`"`)
	}
}

func TestLoggingMiddlewareLowersBatchStatusPostVerbosity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(Logging())
	handler := func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	}
	router.POST("/api/v1/applications/components/status", handler)
	router.GET("/api/v1/applications/components/status", handler)
	router.POST("/api/v1/applications/app-1/components/status", handler)

	logOutput := captureKlogOutput(t, func() {
		requests := []struct {
			method string
			path   string
		}{
			{method: http.MethodPost, path: "/api/v1/applications/components/status"},
			{method: http.MethodGet, path: "/api/v1/applications/components/status"},
			{method: http.MethodPost, path: "/api/v1/applications/app-1/components/status"},
		}
		for _, request := range requests {
			writer := httptest.NewRecorder()
			req, _ := http.NewRequestWithContext(context.Background(), request.method, request.path, nil)
			router.ServeHTTP(writer, req)
			assert.Equal(t, http.StatusOK, writer.Code)
		}
	})

	assert.NotContains(t, logOutput, `method="POST" path="/api/v1/applications/components/status"`)
	assert.Contains(t, logOutput, `method="GET" path="/api/v1/applications/components/status"`)
	assert.Contains(t, logOutput, `method="POST" path="/api/v1/applications/app-1/components/status"`)
}

func captureKlogOutput(t *testing.T, fn func()) string {
	t.Helper()

	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)

	os.Stderr = w
	klog.SetOutput(w)
	defer func() {
		os.Stderr = oldStderr
		klog.SetOutput(oldStderr)
		_ = r.Close()
		_ = w.Close()
	}()

	fn()

	klog.Flush()
	require.NoError(t, w.Close())
	os.Stderr = oldStderr
	klog.SetOutput(oldStderr)

	var logBuf bytes.Buffer
	_, err = io.Copy(&logBuf, r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return logBuf.String()
}

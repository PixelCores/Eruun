package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGzipSkipsComponentFilesExportRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(Gzip())
	r.POST("/applications/:appID/components/:componentName/files/export", func(c *gin.Context) {
		c.Header("Content-Type", "application/zip")
		_, _ = c.Writer.Write([]byte("zipdata"))
	})

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/components/api/files/export", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("expected no content-encoding, got %q", got)
	}
	if body := resp.Body.String(); body != "zipdata" {
		t.Fatalf("unexpected body: %q", body)
	}
}

func TestGzipSkipsComponentShellStreamRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(Gzip())
	r.POST("/applications/:appID/components/:componentName/shell/stream", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.Write([]byte("event: stdout\ndata: {\"chunk\":\"hello\"}\n\n"))
	})

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/components/api/shell/stream", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("expected no content-encoding, got %q", got)
	}
	if body := resp.Body.String(); body != "event: stdout\ndata: {\"chunk\":\"hello\"}\n\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

func TestGzipSkipsComponentLogsRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(Gzip())
	r.GET("/applications/:appID/components/:componentName/logs", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.Write([]byte("data: hello\n\n"))
	})

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components/api/logs", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("expected no content-encoding, got %q", got)
	}
	if body := resp.Body.String(); body != "data: hello\n\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

func TestGzipSkipsLogArchivesRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(Gzip())
	r.POST("/applications/:appID/log-archives", func(c *gin.Context) {
		c.Header("Content-Type", "application/zip")
		_, _ = c.Writer.Write([]byte("zipdata"))
	})

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/log-archives", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("expected no content-encoding, got %q", got)
	}
	if body := resp.Body.String(); body != "zipdata" {
		t.Fatalf("unexpected body: %q", body)
	}
}

func TestGzipCompressesOtherRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(Gzip())
	r.GET("/applications/:appID/components", func(c *gin.Context) {
		_, _ = c.Writer.Write([]byte("plain-text"))
	})

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if got := resp.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip content-encoding, got %q", got)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	defer gz.Close()
	data, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gzip body: %v", err)
	}
	if string(data) != "plain-text" {
		t.Fatalf("unexpected decompressed body: %q", string(data))
	}
}

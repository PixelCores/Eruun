package middleware

import (
	"bytes"
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

func TestGzipPreservesResponseStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(Gzip())
	r.GET("/unauthorized", func(c *gin.Context) {
		c.JSON(http.StatusUnauthorized, gin.H{"code": http.StatusUnauthorized})
	})

	req := httptest.NewRequest(http.MethodGet, "/unauthorized", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected HTTP %d, got %d", http.StatusUnauthorized, resp.Code)
	}
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
	if string(data) != `{"code":401}` {
		t.Fatalf("unexpected decompressed body: %q", string(data))
	}
}

func TestGzipWriteStringUsesCompressedBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip())
	r.GET("/string", func(c *gin.Context) {
		c.Header("Content-Length", "5")
		_, err := io.WriteString(c.Writer, "hello")
		if err != nil {
			t.Errorf("write string: %v", err)
		}
	})
	request := httptest.NewRequest(http.MethodGet, "/string", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)

	if got := response.Result().Header.Get("Content-Length"); got != "" {
		t.Fatalf("uncompressed Content-Length must be removed, got %q", got)
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil || string(body) != "hello" {
		t.Fatalf("decompressed body = %q, error = %v", body, err)
	}
}

func TestGzipNegotiatesAcceptEncoding(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct {
		accept string
		gzip   bool
	}{
		{accept: "gzip", gzip: true},
		{accept: "GZIP; q=0.5", gzip: true},
		{accept: "br, gzip;q=0.1", gzip: true},
		{accept: "*", gzip: true},
		{accept: "gzip;q=0"},
		{accept: "gzip;q=0.0, *;q=1"},
		{accept: "gzip;q=invalid"},
		{accept: "gzip;q=NaN"},
		{accept: "gzip;q=2"},
		{accept: "notgzip"},
		{accept: ""},
	} {
		t.Run(tt.accept, func(t *testing.T) {
			r := gin.New()
			r.Use(Gzip())
			r.GET("/body", func(c *gin.Context) { c.Data(http.StatusOK, "text/plain", []byte("body")) })
			request := httptest.NewRequest(http.MethodGet, "/body", nil)
			request.Header.Set("Accept-Encoding", tt.accept)
			response := httptest.NewRecorder()
			r.ServeHTTP(response, request)
			if got := response.Header().Get("Content-Encoding") == "gzip"; got != tt.gzip {
				t.Fatalf("gzip encoding = %v, want %v", got, tt.gzip)
			}
			if response.Header().Get("Vary") != "Accept-Encoding" {
				t.Fatalf("response must vary with encoding: %v", response.Header())
			}
		})
	}
}

func TestGzipPreservesResponsesWithoutBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, status := range []int{http.StatusNoContent, http.StatusNotModified} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			r := gin.New()
			r.Use(Gzip())
			r.GET("/empty", func(c *gin.Context) { c.Status(status) })
			request := httptest.NewRequest(http.MethodGet, "/empty", nil)
			request.Header.Set("Accept-Encoding", "gzip")
			response := httptest.NewRecorder()
			r.ServeHTTP(response, request)
			if response.Code != status || response.Body.Len() != 0 || response.Header().Get("Content-Encoding") != "" {
				t.Fatalf("unexpected response: status = %d, body = %q, headers = %v", response.Code, response.Body.String(), response.Header())
			}
		})
	}
}

func TestGzipPreservesHandlerContentEncoding(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip())
	r.GET("/encoded", func(c *gin.Context) {
		c.Header("Content-Encoding", "br")
		c.Data(http.StatusOK, "application/octet-stream", []byte("encoded-body"))
	})
	request := httptest.NewRequest(http.MethodGet, "/encoded", nil)
	request.Header.Set("Accept-Encoding", "gzip, br")
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if response.Header().Get("Content-Encoding") != "br" || response.Body.String() != "encoded-body" {
		t.Fatalf("already encoded response was modified: %v %q", response.Header(), response.Body.String())
	}
}

func TestGzipFlushDeliversBodyBeforeHandlerReturns(t *testing.T) {
	gin.SetMode(gin.TestMode)
	response := httptest.NewRecorder()
	r := gin.New()
	r.Use(Gzip())
	r.GET("/flush", func(c *gin.Context) {
		if _, err := c.Writer.Write([]byte("first")); err != nil {
			t.Fatalf("write response: %v", err)
		}
		c.Writer.Flush()
		reader, err := gzip.NewReader(bytes.NewReader(response.Body.Bytes()))
		if err != nil {
			t.Fatalf("create gzip reader before handler completion: %v", err)
		}
		defer reader.Close()
		body := make([]byte, 5)
		if _, err := io.ReadFull(reader, body); err != nil || string(body) != "first" {
			t.Fatalf("flushed body = %q, error = %v", body, err)
		}
	})
	request := httptest.NewRequest(http.MethodGet, "/flush", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	r.ServeHTTP(response, request)
}

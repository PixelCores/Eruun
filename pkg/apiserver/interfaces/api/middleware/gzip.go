package middleware

import (
	"compress/gzip"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

const (
	gzipSkipComponentFilesExportSuffix = "/applications/:appID/components/:componentName/files/export"
	gzipSkipComponentLogsSuffix        = "/applications/:appID/components/:componentName/logs"
	gzipSkipComponentShellStreamSuffix = "/applications/:appID/components/:componentName/shell/stream"
	gzipSkipLogArchivesSuffix          = "/applications/:appID/log-archives"
)

// Gzip is a minimal gzip middleware for gin that compresses responses when
// the client advertises gzip support and the response isn't already encoded.
func Gzip() gin.HandlerFunc {
	return func(c *gin.Context) {
		if shouldSkipGzip(c) {
			c.Next()
			return
		}

		c.Writer.Header().Add("Vary", "Accept-Encoding")
		if c.Request.Method == http.MethodHead || !acceptsGzip(strings.Join(c.Request.Header.Values("Accept-Encoding"), ",")) {
			c.Next()
			return
		}

		w := &gzipWriter{ResponseWriter: c.Writer}
		defer func() {
			if w.Writer != nil {
				if err := w.Writer.Close(); err != nil {
					klog.V(4).InfoS("close gzip response failed", "err", err)
				}
			}
		}()
		c.Writer = w
		c.Next()
	}
}

func shouldSkipGzip(c *gin.Context) bool {
	fullPath := c.FullPath()
	return strings.HasSuffix(fullPath, gzipSkipComponentFilesExportSuffix) ||
		strings.HasSuffix(fullPath, gzipSkipComponentLogsSuffix) ||
		strings.HasSuffix(fullPath, gzipSkipComponentShellStreamSuffix) ||
		strings.HasSuffix(fullPath, gzipSkipLogArchivesSuffix)
}

// acceptsGzip honors explicit quality values before the wildcard encoding.
func acceptsGzip(header string) bool {
	wildcard := false
	for _, value := range strings.Split(header, ",") {
		encoding, params, err := mime.ParseMediaType(strings.TrimSpace(value))
		if err != nil || (encoding != "gzip" && encoding != "*") {
			continue
		}
		quality := 1.0
		if raw, ok := params["q"]; ok {
			quality, err = strconv.ParseFloat(raw, 64)
		}
		accepted := err == nil && quality > 0 && quality <= 1
		if encoding == "gzip" {
			return accepted
		}
		wildcard = accepted
	}
	return wildcard
}

type gzipWriter struct {
	gin.ResponseWriter
	Writer *gzip.Writer
}

func (w *gzipWriter) WriteHeaderNow() {
	if !w.Written() {
		status := w.Status()
		if status >= http.StatusOK && status != http.StatusNoContent && status != http.StatusNotModified && w.Header().Get("Content-Encoding") == "" {
			w.Header().Del("Content-Length")
			w.Header().Set("Content-Encoding", "gzip")
			w.Writer = gzip.NewWriter(w.ResponseWriter)
		}
	}
	w.ResponseWriter.WriteHeaderNow()
}

func (w *gzipWriter) Write(data []byte) (int, error) {
	if !w.Written() && w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", http.DetectContentType(data))
	}
	w.WriteHeaderNow()
	if w.Writer == nil {
		return w.ResponseWriter.Write(data)
	}
	return w.Writer.Write(data)
}

func (w *gzipWriter) WriteString(data string) (int, error) {
	return w.Write([]byte(data))
}

func (w *gzipWriter) Flush() {
	w.WriteHeaderNow()
	if w.Writer != nil {
		if err := w.Writer.Flush(); err != nil {
			klog.V(4).InfoS("flush gzip response failed", "err", err)
		}
	}
	w.ResponseWriter.Flush()
}

func (w *gzipWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

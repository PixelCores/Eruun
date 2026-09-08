package middleware

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"

	"github.com/gin-gonic/gin"
)

const ArchiveTransferTimeout = spec.JobArchiveTimeoutSeconds * time.Second

// Archive transfers have bounded streaming budgets separate from ordinary API
// requests. Set deadlines before authentication or body reads; the HTTP server
// restores its normal deadlines when processing the next request.
func prepareArchiveDeadline(c *gin.Context) bool {
	route := c.Request.Method + " " + c.FullPath()
	switch route {
	case "POST /api/v1/job-datasets", "POST /api/v1/job-runners/:taskID/results",
		"GET /api/v1/job-runners/:taskID/dataset", "GET /api/v1/job-datasets/:datasetID/download",
		"GET /api/v1/jobs/:taskID/results/:artifactID/download", "GET /api/v1/jobs/:taskID/deliveries/:target/download":
	default:
		return true
	}
	controller := http.NewResponseController(c.Writer)
	deadline := time.Now().Add(ArchiveTransferTimeout)
	for _, set := range []func(time.Time) error{controller.SetReadDeadline, controller.SetWriteDeadline} {
		if err := set(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
			c.AbortWithStatus(http.StatusInternalServerError)
			return false
		}
	}
	// ErrNotSupported is expected for httptest recorders and response writers
	// without deadline support. The production net/http writer supports both.
	return true
}

// RequestBodyLimit caps request body size via MaxBytesReader.
func RequestBodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !prepareArchiveDeadline(c) {
			return
		}
		archiveLimit := int64(0)
		if c.Request.Method == http.MethodPost {
			switch c.FullPath() {
			case "/api/v1/job-datasets":
				archiveLimit = 64 << 20
			case "/api/v1/job-runners/:taskID/results":
				archiveLimit = 512 << 20
			}
		}
		if archiveLimit > 0 {
			if c.Request.ContentLength > archiveLimit {
				c.AbortWithStatus(http.StatusRequestEntityTooLarge)
				return
			}
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, archiveLimit)
			c.Next()
			return
		}
		if maxBytes <= 0 || c.Request == nil || c.Request.Body == nil {
			c.Next()
			return
		}

		if c.Request.ContentLength > maxBytes {
			c.AbortWithStatus(http.StatusRequestEntityTooLarge)
			return
		}

		// For chunked/unknown-length bodies, eagerly read and validate size so
		// non-body-reading handlers cannot bypass the global size cap.
		if c.Request.ContentLength < 0 {
			body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxBytes+1))
			_ = c.Request.Body.Close()
			if err != nil {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			if int64(len(body)) > maxBytes {
				c.AbortWithStatus(http.StatusRequestEntityTooLarge)
				return
			}
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			c.Request.ContentLength = int64(len(body))
		}

		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}

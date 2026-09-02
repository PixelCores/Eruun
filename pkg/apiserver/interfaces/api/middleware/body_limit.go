package middleware

import (
	"bytes"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

// RequestBodyLimit caps request body size via MaxBytesReader.
func RequestBodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
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

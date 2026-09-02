package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func setupSSEStream(c *gin.Context, podName, containerName string, unsupportedErr error) (http.Flusher, bool) {
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		bcode.ReturnError(c, unsupportedErr)
		return nil, false
	}
	if err := http.NewResponseController(c.Writer).SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		klog.V(4).InfoS("clear SSE response write deadline failed", "err", err)
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Header("X-Eruun-Pod", podName)
	c.Header("X-Eruun-Container", containerName)
	c.Status(http.StatusOK)
	c.Writer.Flush()

	return flusher, true
}

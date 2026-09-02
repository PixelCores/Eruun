package api

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (app *applications) streamComponentLogs(c *gin.Context) {
	appID, componentName, ok := componentRouteParams(c)
	if !ok {
		return
	}
	requestedContainer := strings.TrimSpace(c.Query("container"))
	stream, err := app.ApplicationService.StreamComponentLogs(c.Request.Context(), appID, componentName, requestedContainer)
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	defer func() {
		if closeErr := stream.Close(); closeErr != nil {
			klog.V(4).InfoS("close component log stream failed", "err", closeErr, "appID", appID, "component", componentName)
		}
	}()
	flusher, ok := setupSSEStream(c, stream.PodName, stream.ContainerName, bcode.ErrComponentLogUnavailable)
	if !ok {
		return
	}

	if err := streamSSELines(c.Request.Context(), stream.Reader, c.Writer, flusher); err != nil && !errors.Is(err, context.Canceled) {
		klog.V(4).InfoS("component log stream ended", "err", err, "appID", appID, "component", componentName, "pod", stream.PodName, "container", stream.ContainerName)
	}
}

func streamSSELines(ctx context.Context, reader io.Reader, writer io.Writer, flusher http.Flusher) error {
	buf := bufio.NewReader(reader)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line, err := buf.ReadString('\n')
		if line != "" {
			payload := strings.TrimRight(line, "\r\n")
			if _, writeErr := fmt.Fprintf(writer, "data: %s\n\n", payload); writeErr != nil {
				return writeErr
			}
			flusher.Flush()
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

func (app *applications) streamComponentShellScript(c *gin.Context) {
	appID, componentName, ok := componentRouteParams(c)
	if !ok {
		return
	}

	req, ok := validatedComponentShellScriptRequest(c)
	if !ok {
		return
	}

	stream, err := app.ApplicationService.StreamComponentShellScript(c.Request.Context(), appID, componentName, *req)
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	flusher, ok := setupSSEStream(c, stream.PodName, stream.ContainerName, fmt.Errorf("response writer does not support streaming"))
	if !ok {
		return
	}

	if err := streamShellScriptSSEEvents(c.Request.Context(), stream, c.Writer, flusher); err != nil && !errors.Is(err, context.Canceled) {
		klog.V(4).InfoS("component shell stream ended", "err", err, "appID", appID, "component", componentName, "pod", stream.PodName, "container", stream.ContainerName)
	}
}

func streamShellScriptSSEEvents(ctx context.Context, stream *service.ComponentShellScriptStream, writer io.Writer, flusher http.Flusher) error {
	if stream == nil {
		return fmt.Errorf("shell stream is nil")
	}
	if stream.Events == nil {
		return fmt.Errorf("shell stream events are empty")
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-stream.Events:
			if !ok {
				return nil
			}
			if err := writeShellStreamSSEEvent(writer, flusher, event); err != nil {
				return err
			}
		}
	}
}

func writeShellStreamSSEEvent(writer io.Writer, flusher http.Flusher, event kube.PodShellStreamEvent) error {
	payload := map[string]interface{}{}
	switch event.Type {
	case kube.PodShellStreamEventStdout, kube.PodShellStreamEventStderr:
		payload["chunk"] = event.Chunk
	case kube.PodShellStreamEventExit:
		payload["exitCode"] = event.ExitCode
		payload["succeeded"] = event.Succeeded
	case kube.PodShellStreamEventError:
		payload["message"] = event.Message
	default:
		if event.Chunk != "" {
			payload["chunk"] = event.Chunk
		}
		payload["exitCode"] = event.ExitCode
		payload["succeeded"] = event.Succeeded
		if event.Message != "" {
			payload["message"] = event.Message
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal shell stream event: %w", err)
	}
	if _, err := fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event.Type, body); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

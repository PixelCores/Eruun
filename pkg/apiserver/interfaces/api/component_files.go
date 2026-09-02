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

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

func (app *applications) exportComponentFilesZip(c *gin.Context) {
	appID, componentName, ok := componentRouteParams(c)
	if !ok {
		return
	}

	req, ok := bindRequest[apis.ExportComponentFilesRequest](c, bcode.ErrApplicationConfig, false)
	if !ok {
		return
	}
	if strings.TrimSpace(req.Path) == "" {
		bcode.ReturnError(c, bcode.ErrComponentFilePathInvalid)
		return
	}

	archive, err := app.ApplicationService.ExportComponentFilesZip(c.Request.Context(), appID, componentName, *req)
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	writeComponentArchiveStream(c, archive, appID, componentName, "component-export")
}

func writeComponentArchiveStream(c *gin.Context, archive *service.ComponentFileArchiveStream, appID, componentName, fallbackBaseName string) {
	if archive == nil || archive.Reader == nil {
		bcode.ReturnError(c, fmt.Errorf("component file archive stream is empty"))
		return
	}
	defer func() {
		if closeErr := archive.Close(); closeErr != nil {
			klog.V(4).InfoS("close component file archive failed", "err", closeErr, "appID", appID, "component", componentName)
		}
	}()

	contentType := strings.TrimSpace(archive.ContentType)
	if contentType == "" {
		contentType = "application/zip"
	}
	fileName := strings.TrimSpace(archive.FileName)
	if fileName == "" {
		baseName := strings.TrimSpace(fallbackBaseName)
		if baseName == "" {
			baseName = "component-export"
		}
		if strings.HasPrefix(contentType, "multipart/") {
			fileName = baseName + ".multipart"
		} else {
			fileName = baseName + ".zip"
		}
	}
	buffered := bufio.NewReader(archive.Reader)
	if _, err := buffered.Peek(1); err != nil {
		if errors.Is(err, io.EOF) {
			bcode.ReturnError(c, fmt.Errorf("component file archive stream is empty"))
			return
		}
		if kube.IsArchivePathInvalidError(err) || kube.IsArchivePathLookupError(err) {
			bcode.ReturnError(c, bcode.ErrComponentFilePathInvalid)
			return
		}
		bcode.ReturnError(c, err)
		return
	}

	c.Header("Content-Type", contentType)
	if fileName != "" {
		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
	}
	c.Header("Cache-Control", "no-store")
	c.Header("X-Eruun-Pod", archive.PodName)
	c.Header("X-Eruun-Container", archive.ContainerName)
	c.Status(http.StatusOK)

	if _, err := io.Copy(c.Writer, buffered); err != nil && !errors.Is(err, context.Canceled) {
		klog.V(4).InfoS("component file archive stream ended", "err", err, "appID", appID, "component", componentName, "pod", archive.PodName, "container", archive.ContainerName)
	}
}

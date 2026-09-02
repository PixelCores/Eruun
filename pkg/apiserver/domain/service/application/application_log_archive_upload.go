package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (c *applicationsServiceImpl) DownloadLogArchive(ctx context.Context, appID string, req apisv1.LogArchiveDownloadRequest) (*ComponentFileArchiveStream, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	if req.JobType != "" && req.JobType != config.JobLogArchiveUpload {
		return nil, fmt.Errorf("%w: log archive jobType must be %q", bcode.ErrApplicationConfig, config.JobLogArchiveUpload)
	}
	if len(req.Components) != 1 {
		return nil, fmt.Errorf("%w: log archive download requires exactly one component", bcode.ErrApplicationConfig)
	}

	componentName := strings.TrimSpace(req.Components[0])
	if componentName == "" {
		return nil, fmt.Errorf("%w: log archive component name is required", bcode.ErrApplicationConfig)
	}

	targetPath := strings.TrimSpace(req.Path)
	if targetPath == "" {
		return nil, bcode.ErrComponentFilePathInvalid
	}

	app, err := c.AppRepo.FindByID(ctx, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}

	component, err := c.ComponentRepo.FindByName(ctx, app.ID, componentName)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, fmt.Errorf("%w: component %q not found", bcode.ErrApplicationConfig, componentName)
		}
		return nil, err
	}
	if !logArchiveDownloadComponentUsesPods(component) {
		return nil, fmt.Errorf("%w: component %q does not use pods", bcode.ErrApplicationConfig, componentName)
	}

	return c.ExportComponentFilesZip(ctx, app.ID, component.Name, apisv1.ExportComponentFilesRequest{
		Path:      targetPath,
		Container: strings.TrimSpace(req.Container),
	})
}

func logArchiveDownloadComponentUsesPods(component *model.ApplicationComponent) bool {
	return component != nil && config.ComponentTypeUsesPods(component.ComponentType)
}

package api

import (
	"context"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/gin-gonic/gin"
)

func (app *applications) listComponentContainers(c *gin.Context) {
	handleComponentResult(
		c,
		func(ctx context.Context, appID, componentName string) (*apis.ComponentContainersResponse, error) {
			return app.ApplicationService.ListComponentContainers(ctx, appID, componentName)
		},
	)
}

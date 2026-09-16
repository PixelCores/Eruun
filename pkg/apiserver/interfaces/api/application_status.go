package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/applicationstatus"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

type applicationRuntimeComponentReader interface {
	ListApplicationRuntimeComponents(context.Context, string) ([]*model.ApplicationComponent, error)
}

func (app *applications) statusRules() applicationstatus.Service {
	return applicationstatus.Service{Applications: app.ApplicationService, Runtime: app.RuntimeComponentReader}
}

func (app *applications) listApplicationComponents(c *gin.Context) {
	handlePathResult(c, appIDPathParam, func(ctx context.Context, appID string) (apis.ListApplicationComponentsResponse, error) {
		components, err := app.ApplicationService.ListApplicationComponents(ctx, appID)
		if err != nil {
			return apis.ListApplicationComponentsResponse{}, err
		}
		resp, err := assembler.ConvertComponentModelsToDTO(components)
		if err != nil {
			klog.ErrorS(err, "convert components dto failed", "appID", appID)
			return apis.ListApplicationComponentsResponse{}, err
		}
		return apis.ListApplicationComponentsResponse{Components: resp}, nil
	})
}

func (app *applications) listBatchApplicationComponentStatus(c *gin.Context) {
	req, ok := bindRequest[apis.BatchApplicationComponentStatusRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	if len(req.AppIDs) == 0 {
		bcode.ReturnError(c, bcode.ErrApplicationConfig)
		return
	}
	bcode.ReturnSuccess(c, app.statusRules().Batch(c.Request.Context(), req.AppIDs, batchLookupErrorMessage))
}

func (app *applications) getApplicationStatus(c *gin.Context) {
	handlePathResult(c, appIDPathParam, app.applicationStatus)
}

func (app *applications) applicationStatus(ctx context.Context, appID string) (apis.ApplicationStatusResponse, error) {
	return app.statusRules().GetStatus(ctx, appID)
}

func (app *applications) getApplicationComponentStatus(c *gin.Context) {
	handlePathResult(c, appIDPathParam, app.applicationComponentStatus)
}

func (app *applications) applicationComponentStatus(ctx context.Context, appID string) (apis.ApplicationComponentStatusResponse, error) {
	return app.statusRules().GetComponentStatus(ctx, appID)
}

func (app *applications) applicationAggregateStatus(ctx context.Context, appID string, components []*model.ApplicationComponent) (string, error) {
	return app.statusRules().Aggregate(ctx, appID, components)
}

func aggregateApplicationStatus(components []*model.ApplicationComponent) string {
	return aggregateApplicationStatusWithReferenceTime(components, time.Now())
}

func aggregateApplicationStatusWithReferenceTime(components []*model.ApplicationComponent, now time.Time) string {
	return applicationstatus.AggregateWithReferenceTime(components, now)
}

func batchLookupErrorMessage(err error) string {
	var bc *bcode.Bcode
	if errors.As(err, &bc) && bc != nil {
		return bc.Message
	}
	return strings.TrimSpace(err.Error())
}

package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type resourceImports struct {
	Service service.ResourceImportService `inject:""`
}

func NewResourceImports() Interface { return &resourceImports{} }

func (r *resourceImports) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/resource-import/jobs/scan", r.submitScanJob)
	group.POST("/resource-import/jobs/manage", r.submitManageJob)
	group.GET("/resource-import/jobs/:taskID", r.getJob)
}

func (r *resourceImports) submitScanJob(c *gin.Context) {
	req, ok := bindStrictJSON[apisv1.ResourceImportScanJobRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	result, err := r.Service.SubmitScanJob(c.Request.Context(), *req)
	respondWithAccepted(c, result, err)
}

func (r *resourceImports) submitManageJob(c *gin.Context) {
	req, ok := bindStrictJSON[apisv1.ResourceImportManageJobRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	result, err := r.Service.SubmitManageJob(c.Request.Context(), *req)
	respondWithAccepted(c, result, err)
}

func (r *resourceImports) getJob(c *gin.Context) {
	taskID, ok := taskIDPathParam(c)
	if !ok {
		return
	}
	result, err := r.Service.GetJob(c.Request.Context(), taskID)
	respondWithResult(c, result, err)
}

func respondWithAccepted(c *gin.Context, result *apisv1.ResourceImportJobAcceptedResponse, err error) {
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	bcode.ReturnResponse(c, http.StatusAccepted, bcode.SuccessCode, "", result)
}

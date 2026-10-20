package api

import (
	"context"
	"net/http"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/nodepool"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	apiresponse "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/response"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
)

type nodepools struct {
	Service nodepool.NodePoolService `inject:""`
}

func NewNodePools() Interface { return &nodepools{} }

func (h *nodepools) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/node-pools", h.options)
	group.GET("/admin/node-pools", h.list)
	group.GET("/admin/node-pools/:name", h.get)
	group.POST("/admin/node-pools", h.create)
	group.PUT("/admin/node-pools/:name", h.update)
	group.DELETE("/admin/node-pools/:name", h.delete)
}

func (h *nodepools) list(c *gin.Context) {
	handleContextResult(c, func(ctx context.Context) (apis.ListNodePoolsResponse, error) {
		items, err := h.Service.List(ctx)
		return apis.ListNodePoolsResponse{NodePools: assembler.NodePoolModelsToDTO(items)}, err
	})
}

func (h *nodepools) options(c *gin.Context) {
	handleContextResult(c, func(ctx context.Context) (apis.ListNodePoolOptionsResponse, error) {
		items, err := h.Service.List(ctx)
		return apis.ListNodePoolOptionsResponse{NodePools: assembler.NodePoolModelsToOptions(items)}, err
	})
}

func (h *nodepools) get(c *gin.Context) {
	item, err := h.Service.Get(c.Request.Context(), c.Param("name"))
	respondWithResult(c, assembler.NodePoolModelToDTO(item), err)
}

func (h *nodepools) create(c *gin.Context) {
	req, ok := bindAndValidateStrictJSON[apis.CreateNodePoolRequest](c, bcode.ErrNodePoolInvalid, true)
	if !ok {
		return
	}
	item, err := h.Service.Create(c.Request.Context(), assembler.CreateNodePoolCommand(*req))
	if err != nil {
		apiresponse.ReturnError(c, err)
		return
	}
	apiresponse.ReturnResponse(c, http.StatusCreated, apiresponse.SuccessCode, "", assembler.NodePoolModelToDTO(item))
}

func (h *nodepools) update(c *gin.Context) {
	req, ok := bindAndValidateStrictJSON[apis.UpdateNodePoolRequest](c, bcode.ErrNodePoolInvalid, true)
	if !ok {
		return
	}
	item, err := h.Service.Update(c.Request.Context(), c.Param("name"), assembler.UpdateNodePoolCommand(*req))
	respondWithResult(c, assembler.NodePoolModelToDTO(item), err)
}

func (h *nodepools) delete(c *gin.Context) {
	if err := h.Service.Delete(c.Request.Context(), c.Param("name")); err != nil {
		apiresponse.ReturnError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

package api

import (
	"context"
	"net/http"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/rbacprofile"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	apiresponse "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/response"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
)

type rbacprofiles struct {
	Service rbacprofile.RBACProfileService `inject:""`
}

func NewRBACProfiles() Interface { return &rbacprofiles{} }

func (h *rbacprofiles) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/rbac-profiles", h.options)
	group.GET("/admin/rbac-profiles", h.list)
	group.GET("/admin/rbac-profiles/:name", h.get)
	group.POST("/admin/rbac-profiles", h.create)
	group.PUT("/admin/rbac-profiles/:name", h.update)
	group.DELETE("/admin/rbac-profiles/:name", h.delete)
}

func (h *rbacprofiles) list(c *gin.Context) {
	handleContextResult(c, func(ctx context.Context) (apis.ListRBACProfilesResponse, error) {
		items, err := h.Service.List(ctx)
		return apis.ListRBACProfilesResponse{RBACProfiles: assembler.RBACProfileModelsToDTO(items)}, err
	})
}

func (h *rbacprofiles) options(c *gin.Context) {
	handleContextResult(c, func(ctx context.Context) (apis.ListRBACProfileOptionsResponse, error) {
		items, err := h.Service.List(ctx)
		return apis.ListRBACProfileOptionsResponse{RBACProfiles: assembler.RBACProfileModelsToOptions(items)}, err
	})
}

func (h *rbacprofiles) get(c *gin.Context) {
	item, err := h.Service.Get(c.Request.Context(), c.Param("name"))
	respondWithResult(c, assembler.RBACProfileModelToDTO(item), err)
}

func (h *rbacprofiles) create(c *gin.Context) {
	req, ok := bindAndValidateStrictJSON[apis.CreateRBACProfileRequest](c, bcode.ErrRBACProfileInvalid, true)
	if !ok {
		return
	}
	item, err := h.Service.Create(c.Request.Context(), assembler.CreateRBACProfileCommand(*req))
	if err != nil {
		apiresponse.ReturnError(c, err)
		return
	}
	apiresponse.ReturnResponse(c, http.StatusCreated, apiresponse.SuccessCode, "", assembler.RBACProfileModelToDTO(item))
}

func (h *rbacprofiles) update(c *gin.Context) {
	req, ok := bindAndValidateStrictJSON[apis.UpdateRBACProfileRequest](c, bcode.ErrRBACProfileInvalid, true)
	if !ok {
		return
	}
	item, err := h.Service.Update(c.Request.Context(), c.Param("name"), assembler.UpdateRBACProfileCommand(*req))
	respondWithResult(c, assembler.RBACProfileModelToDTO(item), err)
}

func (h *rbacprofiles) delete(c *gin.Context) {
	if err := h.Service.Delete(c.Request.Context(), c.Param("name")); err != nil {
		apiresponse.ReturnError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

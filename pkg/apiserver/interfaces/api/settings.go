package api

import (
	"context"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type settings struct {
	SystemSettingService service.SystemSettingService `inject:""`
}

// NewSettings creates a new settings API handler.
func NewSettings() Interface {
	return &settings{}
}

func (s *settings) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/settings", s.listSettings)
	group.GET("/settings/:type", s.getSetting)
	group.POST("/settings", s.createSetting)
	group.PUT("/settings/:type", s.updateSetting)
	group.DELETE("/settings/:type", s.deleteSetting)
	// Temporary disable authz management routes on master; uncomment when API authz APIs should be enabled again.
	// group.GET("/authz/routes", s.listAPIAuthorization)
	// group.PUT("/authz/routes", s.upsertAPIAuthorizationRoute)
	// group.DELETE("/authz/routes", s.deleteAPIAuthorizationRoute)
	// group.PATCH("/authz/default-effect", s.updateAPIAuthorizationDefaultEffect)
}

func (s *settings) listSettings(c *gin.Context) {
	handleContextResult(c, func(ctx context.Context) (apis.ListSystemSettingResponse, error) {
		items, err := s.SystemSettingService.List(ctx)
		return apis.ListSystemSettingResponse{Settings: items}, err
	})
}

func (s *settings) getSetting(c *gin.Context) {
	handlePathResult(c, settingTypePathParam, s.SystemSettingService.Get)
}

func (s *settings) createSetting(c *gin.Context) {
	handleBoundResult(
		c,
		validatedRequestBody[apis.CreateSystemSettingRequest](bcode.ErrSystemSettingValueInvalid, false),
		func(ctx context.Context, req *apis.CreateSystemSettingRequest) (*apis.SystemSetting, error) {
			return s.SystemSettingService.Create(ctx, *req)
		},
	)
}

func (s *settings) updateSetting(c *gin.Context) {
	handlePathBoundResult(
		c,
		settingTypePathParam,
		validatedRequestBody[apis.UpdateSystemSettingRequest](bcode.ErrSystemSettingValueInvalid, false),
		func(ctx context.Context, settingType string, req *apis.UpdateSystemSettingRequest) (*apis.SystemSetting, error) {
			return s.SystemSettingService.Update(ctx, settingType, *req)
		},
	)
}

func (s *settings) deleteSetting(c *gin.Context) {
	handlePathResult(c, settingTypePathParam, func(ctx context.Context, settingType string) (gin.H, error) {
		if err := s.SystemSettingService.Delete(ctx, settingType); err != nil {
			return nil, err
		}
		return gin.H{"type": settingType}, nil
	})
}

func (s *settings) listAPIAuthorization(c *gin.Context) {
	handleContextResult(c, s.SystemSettingService.GetAPIAuthorization)
}

func (s *settings) upsertAPIAuthorizationRoute(c *gin.Context) {
	handleBoundResult(
		c,
		validatedRequestBody[apis.UpsertAPIAuthorizationRouteRequest](bcode.ErrSystemSettingValueInvalid, false),
		func(ctx context.Context, req *apis.UpsertAPIAuthorizationRouteRequest) (*apis.APIAuthorizationPolicy, error) {
			return s.SystemSettingService.UpsertAPIAuthorizationRoute(ctx, *req)
		},
	)
}

func (s *settings) deleteAPIAuthorizationRoute(c *gin.Context) {
	method := strings.TrimSpace(c.Query("method"))
	path := strings.TrimSpace(c.Query("path"))
	resp, err := s.SystemSettingService.DeleteAPIAuthorizationRoute(c.Request.Context(), method, path)
	respondWithResult(c, resp, err)
}

func (s *settings) updateAPIAuthorizationDefaultEffect(c *gin.Context) {
	handleBoundResult(
		c,
		validatedRequestBody[apis.UpdateAPIAuthorizationDefaultEffectRequest](bcode.ErrSystemSettingValueInvalid, false),
		func(ctx context.Context, req *apis.UpdateAPIAuthorizationDefaultEffectRequest) (*apis.APIAuthorizationPolicy, error) {
			return s.SystemSettingService.UpdateAPIAuthorizationDefaultEffect(ctx, *req)
		},
	)
}

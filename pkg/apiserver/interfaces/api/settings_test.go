package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type mockSettingService struct {
	createFn                            func(context.Context, apis.CreateSystemSettingRequest) (*apis.SystemSetting, error)
	updateFn                            func(context.Context, string, apis.UpdateSystemSettingRequest) (*apis.SystemSetting, error)
	deleteFn                            func(context.Context, string) error
	getFn                               func(context.Context, string) (*apis.SystemSetting, error)
	listFn                              func(context.Context) ([]*apis.SystemSetting, error)
	getAPIAuthorizationFn               func(context.Context) (*apis.APIAuthorizationPolicy, error)
	upsertAPIAuthorizationRouteFn       func(context.Context, apis.UpsertAPIAuthorizationRouteRequest) (*apis.APIAuthorizationPolicy, error)
	deleteAPIAuthorizationRouteFn       func(context.Context, string, string) (*apis.APIAuthorizationPolicy, error)
	updateAPIAuthorizationDefaultEffect func(context.Context, apis.UpdateAPIAuthorizationDefaultEffectRequest) (*apis.APIAuthorizationPolicy, error)
}

func (m *mockSettingService) Create(ctx context.Context, req apis.CreateSystemSettingRequest) (*apis.SystemSetting, error) {
	if m.createFn == nil {
		return nil, nil
	}
	return m.createFn(ctx, req)
}

func (m *mockSettingService) Update(ctx context.Context, settingType string, req apis.UpdateSystemSettingRequest) (*apis.SystemSetting, error) {
	if m.updateFn == nil {
		return nil, nil
	}
	return m.updateFn(ctx, settingType, req)
}

func (m *mockSettingService) Delete(ctx context.Context, settingType string) error {
	if m.deleteFn == nil {
		return nil
	}
	return m.deleteFn(ctx, settingType)
}

func (m *mockSettingService) Get(ctx context.Context, settingType string) (*apis.SystemSetting, error) {
	if m.getFn == nil {
		return nil, nil
	}
	return m.getFn(ctx, settingType)
}

func (m *mockSettingService) List(ctx context.Context) ([]*apis.SystemSetting, error) {
	if m.listFn == nil {
		return nil, nil
	}
	return m.listFn(ctx)
}

func (m *mockSettingService) GetAPIAuthorization(ctx context.Context) (*apis.APIAuthorizationPolicy, error) {
	if m.getAPIAuthorizationFn == nil {
		return nil, nil
	}
	return m.getAPIAuthorizationFn(ctx)
}

func (m *mockSettingService) UpsertAPIAuthorizationRoute(ctx context.Context, req apis.UpsertAPIAuthorizationRouteRequest) (*apis.APIAuthorizationPolicy, error) {
	if m.upsertAPIAuthorizationRouteFn == nil {
		return nil, nil
	}
	return m.upsertAPIAuthorizationRouteFn(ctx, req)
}

func (m *mockSettingService) DeleteAPIAuthorizationRoute(ctx context.Context, method, path string) (*apis.APIAuthorizationPolicy, error) {
	if m.deleteAPIAuthorizationRouteFn == nil {
		return nil, nil
	}
	return m.deleteAPIAuthorizationRouteFn(ctx, method, path)
}

func (m *mockSettingService) UpdateAPIAuthorizationDefaultEffect(ctx context.Context, req apis.UpdateAPIAuthorizationDefaultEffectRequest) (*apis.APIAuthorizationPolicy, error) {
	if m.updateAPIAuthorizationDefaultEffect == nil {
		return nil, nil
	}
	return m.updateAPIAuthorizationDefaultEffect(ctx, req)
}

func TestSettingsAPI_CreateAndGet(t *testing.T) {
	gin.SetMode(gin.TestMode)

	createdValue := json.RawMessage(`{"nodeSelector":{"node.kubernetes.io/test":"on"}}`)
	service := &mockSettingService{
		createFn: func(_ context.Context, req apis.CreateSystemSettingRequest) (*apis.SystemSetting, error) {
			require.Equal(t, model.SystemSettingTypeNodeSelector, req.Type)
			return &apis.SystemSetting{
				Type:       req.Type,
				Value:      req.Value,
				CreateTime: time.Now(),
				UpdateTime: time.Now(),
			}, nil
		},
		getFn: func(_ context.Context, settingType string) (*apis.SystemSetting, error) {
			require.Equal(t, model.SystemSettingTypeNodeSelector, settingType)
			return &apis.SystemSetting{Type: settingType, Value: createdValue}, nil
		},
		updateFn: func(context.Context, string, apis.UpdateSystemSettingRequest) (*apis.SystemSetting, error) {
			return nil, nil
		},
		deleteFn: func(context.Context, string) error { return nil },
		listFn:   func(context.Context) ([]*apis.SystemSetting, error) { return nil, nil },
	}

	h := &settings{SystemSettingService: service}
	r := gin.New()
	r.POST("/settings", h.createSetting)
	r.GET("/settings/:type", h.getSetting)

	body := `{"type":"nodeSelector","value":{"nodeSelector":{"node.kubernetes.io/test":"on"}}}`
	req := httptest.NewRequest(http.MethodPost, "/settings", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var created apis.SystemSetting
	requireSuccessResponse(t, resp.Body.Bytes(), &created)
	require.JSONEq(t, string(createdValue), string(created.Value))

	getReq := httptest.NewRequest(http.MethodGet, "/settings/nodeSelector", nil)
	getResp := httptest.NewRecorder()
	r.ServeHTTP(getResp, getReq)

	require.Equal(t, http.StatusOK, getResp.Code)
	var got apis.SystemSetting
	requireSuccessResponse(t, getResp.Body.Bytes(), &got)
	require.Equal(t, model.SystemSettingTypeNodeSelector, got.Type)
}

func TestSettingsAPI_List(t *testing.T) {
	gin.SetMode(gin.TestMode)

	service := &mockSettingService{
		listFn: func(context.Context) ([]*apis.SystemSetting, error) {
			return []*apis.SystemSetting{{Type: model.SystemSettingTypeRBACPolicies}}, nil
		},
		createFn: func(context.Context, apis.CreateSystemSettingRequest) (*apis.SystemSetting, error) { return nil, nil },
		updateFn: func(context.Context, string, apis.UpdateSystemSettingRequest) (*apis.SystemSetting, error) {
			return nil, nil
		},
		deleteFn: func(context.Context, string) error { return nil },
		getFn:    func(context.Context, string) (*apis.SystemSetting, error) { return nil, nil },
	}

	h := &settings{SystemSettingService: service}
	r := gin.New()
	r.GET("/settings", h.listSettings)

	req := httptest.NewRequest(http.MethodGet, "/settings", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.ListSystemSettingResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Settings, 1)
	require.Equal(t, model.SystemSettingTypeRBACPolicies, payload.Settings[0].Type)
}

func TestSettingsAPI_ManageAPIAuthorization(t *testing.T) {
	gin.SetMode(gin.TestMode)

	service := &mockSettingService{
		getAPIAuthorizationFn: func(context.Context) (*apis.APIAuthorizationPolicy, error) {
			return &apis.APIAuthorizationPolicy{
				DefaultEffect: "deny",
				Routes: []apis.APIAuthorizationRoute{
					{Method: "GET", Path: "/api/v1/applications", Roles: []string{"reader"}},
				},
			}, nil
		},
		upsertAPIAuthorizationRouteFn: func(_ context.Context, req apis.UpsertAPIAuthorizationRouteRequest) (*apis.APIAuthorizationPolicy, error) {
			require.Equal(t, "POST", req.Method)
			require.Equal(t, "/api/v1/applications", req.Path)
			require.Equal(t, []string{"admin"}, req.Roles)
			return &apis.APIAuthorizationPolicy{
				DefaultEffect: "deny",
				Routes: []apis.APIAuthorizationRoute{
					{Method: "GET", Path: "/api/v1/applications", Roles: []string{"reader"}},
					{Method: "POST", Path: "/api/v1/applications", Roles: []string{"admin"}},
				},
			}, nil
		},
		deleteAPIAuthorizationRouteFn: func(_ context.Context, method, path string) (*apis.APIAuthorizationPolicy, error) {
			require.Equal(t, "POST", method)
			require.Equal(t, "/api/v1/applications", path)
			return &apis.APIAuthorizationPolicy{
				DefaultEffect: "deny",
				Routes: []apis.APIAuthorizationRoute{
					{Method: "GET", Path: "/api/v1/applications", Roles: []string{"reader"}},
				},
			}, nil
		},
		updateAPIAuthorizationDefaultEffect: func(_ context.Context, req apis.UpdateAPIAuthorizationDefaultEffectRequest) (*apis.APIAuthorizationPolicy, error) {
			require.Equal(t, "allow", req.DefaultEffect)
			return &apis.APIAuthorizationPolicy{
				DefaultEffect: "allow",
				Routes: []apis.APIAuthorizationRoute{
					{Method: "GET", Path: "/api/v1/applications", Roles: []string{"reader"}},
				},
			}, nil
		},
	}

	h := &settings{SystemSettingService: service}
	r := gin.New()
	r.GET("/authz/routes", h.listAPIAuthorization)
	r.PUT("/authz/routes", h.upsertAPIAuthorizationRoute)
	r.DELETE("/authz/routes", h.deleteAPIAuthorizationRoute)
	r.PATCH("/authz/default-effect", h.updateAPIAuthorizationDefaultEffect)

	getReq := httptest.NewRequest(http.MethodGet, "/authz/routes", nil)
	getResp := httptest.NewRecorder()
	r.ServeHTTP(getResp, getReq)
	require.Equal(t, http.StatusOK, getResp.Code)
	var listed apis.APIAuthorizationPolicy
	requireSuccessResponse(t, getResp.Body.Bytes(), &listed)
	require.Equal(t, "deny", listed.DefaultEffect)
	require.Len(t, listed.Routes, 1)

	upsertReq := httptest.NewRequest(http.MethodPut, "/authz/routes", strings.NewReader(`{"method":"POST","path":"/api/v1/applications","roles":["admin"]}`))
	upsertReq.Header.Set("Content-Type", "application/json")
	upsertResp := httptest.NewRecorder()
	r.ServeHTTP(upsertResp, upsertReq)
	require.Equal(t, http.StatusOK, upsertResp.Code)
	var upserted apis.APIAuthorizationPolicy
	requireSuccessResponse(t, upsertResp.Body.Bytes(), &upserted)
	require.Len(t, upserted.Routes, 2)

	deleteReq := httptest.NewRequest(http.MethodDelete, "/authz/routes?method=POST&path=/api/v1/applications", nil)
	deleteResp := httptest.NewRecorder()
	r.ServeHTTP(deleteResp, deleteReq)
	require.Equal(t, http.StatusOK, deleteResp.Code)
	var deleted apis.APIAuthorizationPolicy
	requireSuccessResponse(t, deleteResp.Body.Bytes(), &deleted)
	require.Len(t, deleted.Routes, 1)

	patchReq := httptest.NewRequest(http.MethodPatch, "/authz/default-effect", strings.NewReader(`{"defaultEffect":"allow"}`))
	patchReq.Header.Set("Content-Type", "application/json")
	patchResp := httptest.NewRecorder()
	r.ServeHTTP(patchResp, patchReq)
	require.Equal(t, http.StatusOK, patchResp.Code)
	var patched apis.APIAuthorizationPolicy
	requireSuccessResponse(t, patchResp.Body.Bytes(), &patched)
	require.Equal(t, "allow", patched.DefaultEffect)
}

func TestSettingsAPI_UpsertAPIAuthorizationRouteInvalidPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &settings{SystemSettingService: &mockSettingService{}}
	r := gin.New()
	r.PUT("/authz/routes", h.upsertAPIAuthorizationRoute)

	req := httptest.NewRequest(http.MethodPut, "/authz/routes", strings.NewReader(`{"method":"GET","path":"/api/v1/applications","roles":[]}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrSystemSettingValueInvalid.BusinessCode, envelope.Code)
}

func TestSettingsAPI_CreateConnectivityCheckFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	service := &mockSettingService{
		createFn: func(context.Context, apis.CreateSystemSettingRequest) (*apis.SystemSetting, error) {
			return nil, bcode.ErrSystemSettingConnectivityCheckFailed
		},
	}

	h := &settings{SystemSettingService: service}
	r := gin.New()
	r.POST("/settings", h.createSetting)

	req := httptest.NewRequest(http.MethodPost, "/settings", strings.NewReader(`{"type":"aliyunCloud","value":{"accessKeyId":"ak","accessKeySecret":"sk","regionId":"cn-hangzhou"}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrSystemSettingConnectivityCheckFailed.BusinessCode, envelope.Code)
}

func TestSettingsAPI_UpdateConnectivityCheckFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	service := &mockSettingService{
		updateFn: func(context.Context, string, apis.UpdateSystemSettingRequest) (*apis.SystemSetting, error) {
			return nil, bcode.ErrSystemSettingConnectivityCheckFailed
		},
	}

	h := &settings{SystemSettingService: service}
	r := gin.New()
	r.PUT("/settings/:type", h.updateSetting)

	req := httptest.NewRequest(http.MethodPut, "/settings/aliyunCloud", strings.NewReader(`{"value":{"accessKeyId":"ak","accessKeySecret":"sk","regionId":"cn-hangzhou"}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrSystemSettingConnectivityCheckFailed.BusinessCode, envelope.Code)
}

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
	createFn func(context.Context, apis.CreateSystemSettingRequest) (*apis.SystemSetting, error)
	updateFn func(context.Context, string, apis.UpdateSystemSettingRequest) (*apis.SystemSetting, error)
	deleteFn func(context.Context, string) error
	getFn    func(context.Context, string) (*apis.SystemSetting, error)
	listFn   func(context.Context) ([]*apis.SystemSetting, error)
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

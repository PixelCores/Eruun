package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type mockProgrammingLanguageService struct {
	listFn   func(context.Context) ([]*model.ProgrammingLanguage, error)
	getFn    func(context.Context, string) (*model.ProgrammingLanguage, error)
	createFn func(context.Context, service.CreateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error)
	updateFn func(context.Context, string, service.UpdateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error)
	deleteFn func(context.Context, string) error
}

func (m *mockProgrammingLanguageService) List(ctx context.Context) ([]*model.ProgrammingLanguage, error) {
	if m.listFn == nil {
		return nil, nil
	}
	return m.listFn(ctx)
}

func (m *mockProgrammingLanguageService) Get(ctx context.Context, id string) (*model.ProgrammingLanguage, error) {
	if m.getFn == nil {
		return nil, nil
	}
	return m.getFn(ctx, id)
}

func (m *mockProgrammingLanguageService) Create(ctx context.Context, req service.CreateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error) {
	if m.createFn == nil {
		return nil, nil
	}
	return m.createFn(ctx, req)
}

func (m *mockProgrammingLanguageService) Update(ctx context.Context, id string, req service.UpdateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error) {
	if m.updateFn == nil {
		return nil, nil
	}
	return m.updateFn(ctx, id, req)
}

func (m *mockProgrammingLanguageService) Delete(ctx context.Context, id string) error {
	if m.deleteFn == nil {
		return nil
	}
	return m.deleteFn(ctx, id)
}

func TestProgrammingLanguageAPI_CRUDSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)

	service := &mockProgrammingLanguageService{
		listFn: func(context.Context) ([]*model.ProgrammingLanguage, error) {
			return []*model.ProgrammingLanguage{{ID: "lang-1", Code: "net"}}, nil
		},
		getFn: func(_ context.Context, id string) (*model.ProgrammingLanguage, error) {
			require.Equal(t, "lang-1", id)
			return &model.ProgrammingLanguage{ID: id, Code: "net"}, nil
		},
		createFn: func(_ context.Context, req service.CreateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error) {
			require.Equal(t, ".NET", req.Name)
			require.NotNil(t, req.Enabled)
			return &model.ProgrammingLanguage{ID: "lang-1", Code: "net", Enabled: req.Enabled}, nil
		},
		updateFn: func(_ context.Context, id string, req service.UpdateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error) {
			require.Equal(t, "lang-1", id)
			require.NotNil(t, req.Name)
			return &model.ProgrammingLanguage{ID: id, Code: "net", Name: *req.Name}, nil
		},
		deleteFn: func(_ context.Context, id string) error {
			require.Equal(t, "lang-1", id)
			return nil
		},
	}
	r := programmingLanguageTestRouter(service)

	createReq := httptest.NewRequest(http.MethodPost, "/programming-languages", strings.NewReader(`{"name":".NET","version":"8.0","enabled":true,"cpuReq":"100m","memReq":"0Mi"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createResp := httptest.NewRecorder()
	r.ServeHTTP(createResp, createReq)
	require.Equal(t, http.StatusOK, createResp.Code)
	var created apis.ProgrammingLanguage
	requireSuccessResponse(t, createResp.Body.Bytes(), &created)
	require.Equal(t, "net", created.Code)

	listReq := httptest.NewRequest(http.MethodGet, "/programming-languages", nil)
	listResp := httptest.NewRecorder()
	r.ServeHTTP(listResp, listReq)
	require.Equal(t, http.StatusOK, listResp.Code)
	var listed apis.ListProgrammingLanguagesResponse
	requireSuccessResponse(t, listResp.Body.Bytes(), &listed)
	require.Len(t, listed.Languages, 1)

	getReq := httptest.NewRequest(http.MethodGet, "/programming-languages/lang-1", nil)
	getResp := httptest.NewRecorder()
	r.ServeHTTP(getResp, getReq)
	require.Equal(t, http.StatusOK, getResp.Code)
	var got apis.ProgrammingLanguage
	requireSuccessResponse(t, getResp.Body.Bytes(), &got)
	require.Equal(t, "lang-1", got.ID)

	updateReq := httptest.NewRequest(http.MethodPut, "/programming-languages/lang-1", strings.NewReader(`{"name":"Dotnet"}`))
	updateReq.Header.Set("Content-Type", "application/json")
	updateResp := httptest.NewRecorder()
	r.ServeHTTP(updateResp, updateReq)
	require.Equal(t, http.StatusOK, updateResp.Code)
	var updated apis.ProgrammingLanguage
	requireSuccessResponse(t, updateResp.Body.Bytes(), &updated)
	require.Equal(t, "Dotnet", updated.Name)

	deleteReq := httptest.NewRequest(http.MethodDelete, "/programming-languages/lang-1", nil)
	deleteResp := httptest.NewRecorder()
	r.ServeHTTP(deleteResp, deleteReq)
	require.Equal(t, http.StatusOK, deleteResp.Code)
	var deleted apis.DeleteProgrammingLanguageResponse
	requireSuccessResponse(t, deleteResp.Body.Bytes(), &deleted)
	require.Equal(t, "lang-1", deleted.ID)
}

func TestProgrammingLanguageAPI_CreateRejectsClientCode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := programmingLanguageTestRouter(&mockProgrammingLanguageService{})

	req := httptest.NewRequest(http.MethodPost, "/programming-languages", strings.NewReader(`{"code":"dotnet","name":".NET","version":"8.0","enabled":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrProgrammingLanguageInvalid.BusinessCode, envelope.Code)
}

func TestProgrammingLanguageAPI_UpdateBindsEmptyResourceRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	service := &mockProgrammingLanguageService{
		updateFn: func(_ context.Context, id string, req service.UpdateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error) {
			require.Equal(t, "lang-1", id)
			require.NotNil(t, req.CPUReq)
			require.NotNil(t, req.MemReq)
			require.Empty(t, *req.CPUReq)
			require.Empty(t, *req.MemReq)
			return &model.ProgrammingLanguage{ID: id, Code: "golang", CPUReq: *req.CPUReq, MemReq: *req.MemReq}, nil
		},
	}
	r := programmingLanguageTestRouter(service)

	req := httptest.NewRequest(http.MethodPut, "/programming-languages/lang-1", strings.NewReader(`{"cpuReq":"","memReq":""}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var updated apis.ProgrammingLanguage
	requireSuccessResponse(t, resp.Body.Bytes(), &updated)
	require.Empty(t, updated.CPUReq)
	require.Empty(t, updated.MemReq)
}

func TestProgrammingLanguageAPI_ErrorMapping(t *testing.T) {
	gin.SetMode(gin.TestMode)
	service := &mockProgrammingLanguageService{
		getFn: func(context.Context, string) (*model.ProgrammingLanguage, error) {
			return nil, bcode.ErrProgrammingLanguageNotFound
		},
		createFn: func(context.Context, service.CreateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error) {
			return nil, bcode.ErrProgrammingLanguageExists
		},
		updateFn: func(context.Context, string, service.UpdateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error) {
			return nil, bcode.ErrProgrammingLanguageExists
		},
	}
	r := programmingLanguageTestRouter(service)

	getReq := httptest.NewRequest(http.MethodGet, "/programming-languages/missing", nil)
	getResp := httptest.NewRecorder()
	r.ServeHTTP(getResp, getReq)
	require.Equal(t, http.StatusNotFound, getResp.Code)
	getEnvelope := decodeResponse(t, getResp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrProgrammingLanguageNotFound.BusinessCode, getEnvelope.Code)

	createReq := httptest.NewRequest(http.MethodPost, "/programming-languages", strings.NewReader(`{"name":".NET","version":"8.0","enabled":true}`))
	createReq.Header.Set("Content-Type", "application/json")
	createResp := httptest.NewRecorder()
	r.ServeHTTP(createResp, createReq)
	require.Equal(t, http.StatusConflict, createResp.Code)
	createEnvelope := decodeResponse(t, createResp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrProgrammingLanguageExists.BusinessCode, createEnvelope.Code)

	updateReq := httptest.NewRequest(http.MethodPut, "/programming-languages/lang-1", strings.NewReader(`{"name":"Go"}`))
	updateReq.Header.Set("Content-Type", "application/json")
	updateResp := httptest.NewRecorder()
	r.ServeHTTP(updateResp, updateReq)
	require.Equal(t, http.StatusConflict, updateResp.Code)
	updateEnvelope := decodeResponse(t, updateResp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrProgrammingLanguageExists.BusinessCode, updateEnvelope.Code)
}

func programmingLanguageTestRouter(service *mockProgrammingLanguageService) *gin.Engine {
	h := &programmingLanguages{ProgrammingLanguageService: service}
	r := gin.New()
	r.GET("/programming-languages", h.listProgrammingLanguages)
	r.GET("/programming-languages/:id", h.getProgrammingLanguage)
	r.POST("/programming-languages", h.createProgrammingLanguage)
	r.PUT("/programming-languages/:id", h.updateProgrammingLanguage)
	r.DELETE("/programming-languages/:id", h.deleteProgrammingLanguage)
	return r
}

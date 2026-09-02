package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type handlerResultTestRequest struct {
	Name string `json:"name" binding:"required"`
}

func TestHandleContextResultReturnsSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/result", func(c *gin.Context) {
		handleContextResult(c, func(context.Context) (gin.H, error) {
			return gin.H{"name": "demo"}, nil
		})
	})

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/result", nil))

	var payload map[string]string
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload["name"] != "demo" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestHandleContextResultReturnsError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/result", func(c *gin.Context) {
		handleContextResult(c, func(context.Context) (gin.H, error) {
			return nil, bcode.ErrApplicationNotExist
		})
	})

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/result", nil))

	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrApplicationNotExist.BusinessCode {
		t.Fatalf("unexpected response code: %d", envelope.Code)
	}
}

func TestHandlePathBoundResultSkipsRunOnPathError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	called := false
	r := gin.New()
	r.POST("/apps/:appID", func(c *gin.Context) {
		handlePathBoundResult(
			c,
			appIDPathParam,
			validatedRequestBody[handlerResultTestRequest](bcode.ErrApplicationConfig, true),
			func(context.Context, string, *handlerResultTestRequest) (gin.H, error) {
				called = true
				return gin.H{}, nil
			},
		)
	})

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/apps/%20", strings.NewReader(`{"name":"demo"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(resp, req)

	if called {
		t.Fatalf("run should not be called when path binding fails")
	}
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrApplicationNotExist.BusinessCode {
		t.Fatalf("unexpected response code: %d", envelope.Code)
	}
}

func TestHandleBoundResultSkipsRunOnBindError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	called := false
	r := gin.New()
	r.POST("/result", func(c *gin.Context) {
		handleBoundResult(
			c,
			validatedRequestBody[handlerResultTestRequest](bcode.ErrApplicationConfig, true),
			func(context.Context, *handlerResultTestRequest) (gin.H, error) {
				called = true
				return gin.H{}, errors.New("unexpected call")
			},
		)
	})

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/result", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(resp, req)

	if called {
		t.Fatalf("run should not be called when request binding fails")
	}
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrApplicationConfig.BusinessCode {
		t.Fatalf("unexpected response code: %d", envelope.Code)
	}
}

func TestHandleComponentResultReturnsSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/apps/:appID/components/:componentName", func(c *gin.Context) {
		handleComponentResult(c, func(_ context.Context, appID, componentName string) (gin.H, error) {
			return gin.H{"appID": appID, "componentName": componentName}, nil
		})
	})

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/apps/app-1/components/api", nil))

	var payload map[string]string
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload["appID"] != "app-1" || payload["componentName"] != "api" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestHandleComponentBoundResultSkipsRunOnComponentPathError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	called := false
	r := gin.New()
	r.POST("/apps/:appID/components/:componentName", func(c *gin.Context) {
		handleComponentBoundResult(
			c,
			validatedRequestBody[handlerResultTestRequest](bcode.ErrApplicationConfig, true),
			func(context.Context, string, string, *handlerResultTestRequest) (gin.H, error) {
				called = true
				return gin.H{}, nil
			},
		)
	})

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/apps/app-1/components/%20", strings.NewReader(`{"name":"demo"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(resp, req)

	if called {
		t.Fatalf("run should not be called when component path binding fails")
	}
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrComponentNotFound.BusinessCode {
		t.Fatalf("unexpected response code: %d", envelope.Code)
	}
}

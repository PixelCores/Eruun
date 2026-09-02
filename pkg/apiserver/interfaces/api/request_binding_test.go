package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type bindingTestRequest struct {
	Name string `json:"name" validate:"required"`
}

func TestBindAndValidateRejectsInvalidPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.POST("/binding", func(c *gin.Context) {
		if _, ok := bindAndValidate[bindingTestRequest](c, bcode.ErrApplicationConfig, true); !ok {
			return
		}
		bcode.ReturnSuccess(c, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodPost, "/binding", strings.NewReader(`{"name":`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
}

func TestBindAndValidateRejectsValidationFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.POST("/binding", func(c *gin.Context) {
		if _, ok := bindAndValidate[bindingTestRequest](c, bcode.ErrApplicationConfig, false); !ok {
			return
		}
		bcode.ReturnSuccess(c, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodPost, "/binding", strings.NewReader(`{"name":""}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
}

func TestBindJSONAllowEOFAcceptsEmptyBody(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.DELETE("/binding", func(c *gin.Context) {
		req, ok := bindJSONAllowEOF[bindingTestRequest](c, bcode.ErrApplicationConfig, true)
		if !ok {
			return
		}
		bcode.ReturnSuccess(c, gin.H{"name": req.Name})
	})

	req := httptest.NewRequest(http.MethodDelete, "/binding", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload struct {
		Name string `json:"name"`
	}
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Empty(t, payload.Name)
}

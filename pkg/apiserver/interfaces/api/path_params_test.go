package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestAppIDPathParamRejectsMissingValue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.GET("/applications/:appID", func(c *gin.Context) {
		if _, ok := appIDPathParam(c); !ok {
			return
		}
		bcode.ReturnSuccess(c, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/applications/%20", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationNotExist.BusinessCode, envelope.Code)
}

func TestComponentRouteParamsRejectsMissingComponentName(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.GET("/applications/:appID/components/:componentName/logs", func(c *gin.Context) {
		if _, _, ok := componentRouteParams(c); !ok {
			return
		}
		bcode.ReturnSuccess(c, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components/%20/logs", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusNotFound, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrComponentNotFound.BusinessCode, envelope.Code)
}

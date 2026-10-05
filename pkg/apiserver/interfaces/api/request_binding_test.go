package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"

	apiresponse "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/response"
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
		apiresponse.ReturnSuccess(c, gin.H{"ok": true})
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
		apiresponse.ReturnSuccess(c, gin.H{"ok": true})
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
		apiresponse.ReturnSuccess(c, gin.H{"name": req.Name})
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

func TestStrictJSONBindingBodyContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	binders := []struct {
		name     string
		bind     func(*gin.Context, error, bool) (*bindingTestRequest, bool)
		allowEOF bool
	}{
		{name: "required", bind: bindStrictJSON[bindingTestRequest]},
		{name: "optional", bind: bindStrictJSONAllowEOF[bindingTestRequest], allowEOF: true},
	}
	cases := []struct {
		name     string
		body     string
		valid    bool
		empty    bool
		wantName string
	}{
		{name: "empty", empty: true},
		{name: "whitespace", body: " \n\t", empty: true},
		{name: "object", body: `{"name":"demo"}`, valid: true, wantName: "demo"},
		{name: "trailing whitespace", body: "{\"name\":\"demo\"} \n", valid: true, wantName: "demo"},
		{name: "empty object", body: `{}`, valid: true},
		{name: "null", body: `null`, valid: true},
		{name: "unknown field", body: `{"name":"demo","extra":true}`},
		{name: "truncated", body: `{"name":`},
		{name: "second object", body: `{"name":"demo"} {}`},
		{name: "second null", body: `{"name":"demo"} null`},
		{name: "second scalar", body: `{"name":"demo"} 1`},
		{name: "trailing garbage", body: `{"name":"demo"} x`},
	}
	for _, binder := range binders {
		t.Run(binder.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					resp := httptest.NewRecorder()
					ctx, _ := gin.CreateTestContext(resp)
					ctx.Request = httptest.NewRequest(http.MethodPost, "/binding", strings.NewReader(tc.body))
					req, ok := binder.bind(ctx, bcode.ErrApplicationConfig, false)
					wantOK := tc.valid || (tc.empty && binder.allowEOF)
					require.Equal(t, wantOK, ok)
					if wantOK {
						require.NotNil(t, req)
						require.Equal(t, tc.wantName, req.Name)
						return
					}
					require.Nil(t, req)
					require.Equal(t, http.StatusBadRequest, resp.Code)
					envelope := decodeResponse(t, resp.Body.Bytes(), nil)
					require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
				})
			}
		})
	}
}

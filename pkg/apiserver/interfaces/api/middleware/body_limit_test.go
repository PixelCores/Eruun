package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestRequestBodyLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	handlerCalled := 0
	router.Use(RequestBodyLimit(5))
	router.POST("/upload", func(c *gin.Context) {
		handlerCalled++
		if _, err := io.ReadAll(c.Request.Body); err != nil {
			return
		}
		c.Status(http.StatusOK)
	})

	overLimitReq := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader("123456"))
	overLimitResp := httptest.NewRecorder()
	router.ServeHTTP(overLimitResp, overLimitReq)
	require.Equal(t, http.StatusRequestEntityTooLarge, overLimitResp.Code)
	require.Equal(t, 0, handlerCalled)

	withinLimitReq := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader("12345"))
	withinLimitResp := httptest.NewRecorder()
	router.ServeHTTP(withinLimitResp, withinLimitReq)
	require.Equal(t, http.StatusOK, withinLimitResp.Code)
	require.Equal(t, 1, handlerCalled)
}

func TestRequestBodyLimit_RejectChunkedBodyBeforeHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	handlerCalled := 0
	router.Use(RequestBodyLimit(5))
	router.POST("/restart", func(c *gin.Context) {
		handlerCalled++
		c.Status(http.StatusOK)
	})

	overLimitReq := httptest.NewRequest(http.MethodPost, "/restart", strings.NewReader("123456"))
	overLimitReq.ContentLength = -1
	overLimitReq.TransferEncoding = []string{"chunked"}
	overLimitResp := httptest.NewRecorder()
	router.ServeHTTP(overLimitResp, overLimitReq)
	require.Equal(t, http.StatusRequestEntityTooLarge, overLimitResp.Code)
	require.Equal(t, 0, handlerCalled)

	withinLimitReq := httptest.NewRequest(http.MethodPost, "/restart", strings.NewReader("12345"))
	withinLimitReq.ContentLength = -1
	withinLimitReq.TransferEncoding = []string{"chunked"}
	withinLimitResp := httptest.NewRecorder()
	router.ServeHTTP(withinLimitResp, withinLimitReq)
	require.Equal(t, http.StatusOK, withinLimitResp.Code)
	require.Equal(t, 1, handlerCalled)
}

func TestRequestBodyLimit_WithCORSHeadersWhenCORSRunsFirst(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(CORS(CORSOptions{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{"POST", "OPTIONS"},
		AllowHeaders: []string{"Content-Type", "Origin"},
	}))
	router.Use(RequestBodyLimit(5))
	router.POST("/upload", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader("123456"))
	req.Header.Set("Origin", "https://example.com")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.Code)
	require.Equal(t, "*", resp.Header().Get("Access-Control-Allow-Origin"))
}

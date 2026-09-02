package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestRateLimitRejectsOverBurstForExpensiveOperations(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	handlerCalled := 0
	router.Use(RateLimit(RateLimitOptions{QPS: 0.001, Burst: 1}))
	router.POST("/api/v1/applications", func(c *gin.Context) {
		handlerCalled++
		c.Status(http.StatusOK)
	})

	firstResp := performRateLimitRequest(router, http.MethodPost, "/api/v1/applications")
	require.Equal(t, http.StatusOK, firstResp.Code)

	secondResp := performRateLimitRequest(router, http.MethodPost, "/api/v1/applications")
	require.Equal(t, http.StatusTooManyRequests, secondResp.Code)
	require.Equal(t, 1, handlerCalled)
}

func TestRateLimitDoesNotUseForwardedHeadersAsFreshBuckets(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(RateLimit(RateLimitOptions{QPS: 0.001, Burst: 1}))
	router.POST("/api/v1/applications", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	firstReq := httptest.NewRequest(http.MethodPost, "/api/v1/applications", nil)
	firstReq.Header.Set("X-Forwarded-For", "10.0.0.1")
	firstResp := httptest.NewRecorder()
	router.ServeHTTP(firstResp, firstReq)
	require.Equal(t, http.StatusOK, firstResp.Code)

	secondReq := httptest.NewRequest(http.MethodPost, "/api/v1/applications", nil)
	secondReq.Header.Set("X-Forwarded-For", "10.0.0.2")
	secondResp := httptest.NewRecorder()
	router.ServeHTTP(secondResp, secondReq)
	require.Equal(t, http.StatusTooManyRequests, secondResp.Code)
}

func TestRateLimitAllowsMoreReadRequestsThanExpensiveOperations(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(RateLimit(RateLimitOptions{QPS: 0.001, Burst: 1}))
	router.GET("/api/v1/applications", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	for i := 0; i < defaultRateLimitReadMultiplier; i++ {
		resp := performRateLimitRequest(router, http.MethodGet, "/api/v1/applications")
		require.Equal(t, http.StatusOK, resp.Code)
	}

	overLimitResp := performRateLimitRequest(router, http.MethodGet, "/api/v1/applications")
	require.Equal(t, http.StatusTooManyRequests, overLimitResp.Code)
}

func TestRateLimitTreatsLogReadsAsExpensiveOperations(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(RateLimit(RateLimitOptions{QPS: 0.001, Burst: 1}))
	router.GET("/api/v1/applications/:appID/components/:componentName/logs", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	firstResp := performRateLimitRequest(router, http.MethodGet, "/api/v1/applications/app-1/components/web/logs")
	require.Equal(t, http.StatusOK, firstResp.Code)

	secondResp := performRateLimitRequest(router, http.MethodGet, "/api/v1/applications/app-1/components/web/logs")
	require.Equal(t, http.StatusTooManyRequests, secondResp.Code)
}

func TestRateLimitSkipsHealthPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	handlerCalled := 0
	router.Use(RateLimit(RateLimitOptions{
		QPS:       0.001,
		Burst:     1,
		SkipPaths: DefaultRateLimitSkipPaths(),
	}))
	router.GET("/api/v1/health", func(c *gin.Context) {
		handlerCalled++
		c.Status(http.StatusOK)
	})

	firstResp := performRateLimitRequest(router, http.MethodGet, "/api/v1/health")
	require.Equal(t, http.StatusOK, firstResp.Code)

	secondResp := performRateLimitRequest(router, http.MethodGet, "/api/v1/health")
	require.Equal(t, http.StatusOK, secondResp.Code)
	require.Equal(t, 2, handlerCalled)
}

func TestRateLimitDisabledWhenQPSNotPositive(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	handlerCalled := 0
	router.Use(RateLimit(RateLimitOptions{QPS: 0, Burst: 1}))
	router.GET("/api/v1/apps", func(c *gin.Context) {
		handlerCalled++
		c.Status(http.StatusOK)
	})

	firstResp := performRateLimitRequest(router, http.MethodGet, "/api/v1/apps")
	require.Equal(t, http.StatusOK, firstResp.Code)

	secondResp := performRateLimitRequest(router, http.MethodGet, "/api/v1/apps")
	require.Equal(t, http.StatusOK, secondResp.Code)
	require.Equal(t, 2, handlerCalled)
}

func performRateLimitRequest(router *gin.Engine, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

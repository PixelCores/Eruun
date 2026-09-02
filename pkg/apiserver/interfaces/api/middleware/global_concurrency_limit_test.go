package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGlobalConcurrencyLimitRejectsWhenFull(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(GlobalConcurrencyLimit(GlobalConcurrencyLimitOptions{MaxConcurrent: 1}))

	started := make(chan struct{})
	release := make(chan struct{})
	var handlerCalls int32

	router.GET("/api/v1/slow", func(c *gin.Context) {
		atomic.AddInt32(&handlerCalls, 1)
		close(started)
		<-release
		c.Status(http.StatusOK)
	})
	router.GET("/api/v1/fast", func(c *gin.Context) {
		atomic.AddInt32(&handlerCalls, 1)
		c.Status(http.StatusOK)
	})

	var wg sync.WaitGroup
	slowResp := make(chan *httptest.ResponseRecorder, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		slowResp <- performGlobalConcurrencyLimitRequest(router, http.MethodGet, "/api/v1/slow")
	}()

	require.Eventually(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	rejectedResp := performGlobalConcurrencyLimitRequest(router, http.MethodGet, "/api/v1/fast")
	require.Equal(t, http.StatusServiceUnavailable, rejectedResp.Code)
	require.Equal(t, int32(1), atomic.LoadInt32(&handlerCalls))

	close(release)
	wg.Wait()
	require.Equal(t, http.StatusOK, (<-slowResp).Code)

	releasedResp := performGlobalConcurrencyLimitRequest(router, http.MethodGet, "/api/v1/fast")
	require.Equal(t, http.StatusOK, releasedResp.Code)
	require.Equal(t, int32(2), atomic.LoadInt32(&handlerCalls))
}

func TestGlobalConcurrencyLimitDisabledWhenMaxConcurrentNotPositive(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	handlerCalls := 0
	router.Use(GlobalConcurrencyLimit(GlobalConcurrencyLimitOptions{MaxConcurrent: 0}))
	router.GET("/api/v1/apps", func(c *gin.Context) {
		handlerCalls++
		c.Status(http.StatusOK)
	})

	firstResp := performGlobalConcurrencyLimitRequest(router, http.MethodGet, "/api/v1/apps")
	require.Equal(t, http.StatusOK, firstResp.Code)

	secondResp := performGlobalConcurrencyLimitRequest(router, http.MethodGet, "/api/v1/apps")
	require.Equal(t, http.StatusOK, secondResp.Code)
	require.Equal(t, 2, handlerCalls)
}

func TestGlobalConcurrencyLimitSkipsConfiguredPathsWhenFull(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(GlobalConcurrencyLimit(GlobalConcurrencyLimitOptions{
		MaxConcurrent: 1,
		SkipPaths:     []string{"/api/v1/health"},
	}))

	started := make(chan struct{})
	release := make(chan struct{})
	var handlerCalls int32

	router.GET("/api/v1/slow", func(c *gin.Context) {
		atomic.AddInt32(&handlerCalls, 1)
		close(started)
		<-release
		c.Status(http.StatusOK)
	})
	router.GET("/api/v1/health", func(c *gin.Context) {
		atomic.AddInt32(&handlerCalls, 1)
		c.Status(http.StatusOK)
	})

	var wg sync.WaitGroup
	slowResp := make(chan *httptest.ResponseRecorder, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		slowResp <- performGlobalConcurrencyLimitRequest(router, http.MethodGet, "/api/v1/slow")
	}()

	require.Eventually(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	resp := performGlobalConcurrencyLimitRequest(router, http.MethodGet, "/api/v1/health")
	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, int32(2), atomic.LoadInt32(&handlerCalls))

	close(release)
	wg.Wait()
	require.Equal(t, http.StatusOK, (<-slowResp).Code)
}

func TestGlobalConcurrencyLimitReleasesAfterAbort(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(GlobalConcurrencyLimit(GlobalConcurrencyLimitOptions{MaxConcurrent: 1}))
	router.GET("/api/v1/abort", func(c *gin.Context) {
		c.AbortWithStatus(http.StatusBadRequest)
	})
	router.GET("/api/v1/ok", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	abortedResp := performGlobalConcurrencyLimitRequest(router, http.MethodGet, "/api/v1/abort")
	require.Equal(t, http.StatusBadRequest, abortedResp.Code)

	releasedResp := performGlobalConcurrencyLimitRequest(router, http.MethodGet, "/api/v1/ok")
	require.Equal(t, http.StatusOK, releasedResp.Code)
}

func performGlobalConcurrencyLimitRequest(router *gin.Engine, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

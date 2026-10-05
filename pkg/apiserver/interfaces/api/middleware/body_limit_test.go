package middleware

import (
	"errors"
	"fmt"
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

func TestRequestBodyLimitCapsRunnerEventsAt64KiB(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestBodyLimit(24 << 20))
	router.POST("/api/v1/job-runners/:taskID/events", func(c *gin.Context) {
		if _, err := io.ReadAll(c.Request.Body); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				c.Status(http.StatusRequestEntityTooLarge)
				return
			}
		}
		c.Status(http.StatusOK)
	})

	for _, tc := range []struct {
		size int
		want int
	}{{64 << 10, http.StatusOK}, {(64 << 10) + 1, http.StatusRequestEntityTooLarge}} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/job-runners/task/events", strings.NewReader(strings.Repeat("x", tc.size)))
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, tc.want, response.Code)
	}
}

type checkpointBody struct{}

func (checkpointBody) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestCheckpointUploadBodyLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, chunked := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			size int64
			want int
		}{
			{"above ordinary limit", 25 << 20, http.StatusOK},
			{"checkpoint limit", 64 << 20, http.StatusOK},
			{"over checkpoint limit", (64 << 20) + 1, http.StatusRequestEntityTooLarge},
		} {
			t.Run(fmt.Sprintf("%s/chunked=%t", tc.name, chunked), func(t *testing.T) {
				router := gin.New()
				router.Use(RequestBodyLimit(24 << 20))
				router.POST("/api/v1/job-runners/:taskID/checkpoints/:checkpointID", func(c *gin.Context) {
					_, err := io.Copy(io.Discard, c.Request.Body)
					if err != nil {
						var tooLarge *http.MaxBytesError
						require.ErrorAs(t, err, &tooLarge)
						c.Status(http.StatusRequestEntityTooLarge)
						return
					}
					c.Status(http.StatusOK)
				})
				request := httptest.NewRequest(http.MethodPost, "/api/v1/job-runners/task/checkpoints/point", io.LimitReader(checkpointBody{}, tc.size))
				request.ContentLength = tc.size
				if chunked {
					request.ContentLength = -1
					request.TransferEncoding = []string{"chunked"}
				}
				response := httptest.NewRecorder()
				router.ServeHTTP(response, request)
				require.Equal(t, tc.want, response.Code)
			})
		}
	}
}

func TestCheckpointControlRoutesKeepOrdinaryBodyLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestBodyLimit(8))
	called := false
	router.GET("/api/v1/job-runners/:taskID/checkpoints/:checkpointID", func(c *gin.Context) { called = true })
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/job-runners/task/checkpoints/point", strings.NewReader("123456789")))
	require.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
	require.False(t, called)
}

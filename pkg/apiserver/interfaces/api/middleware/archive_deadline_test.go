package middleware

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func deadlineServer(t *testing.T, router http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(router)
	server.Config.ReadTimeout = 40 * time.Millisecond
	server.Config.WriteTimeout = 40 * time.Millisecond
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func TestArchiveUploadsOverrideServerReadDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		route, url string
		archive    bool
	}{
		{"/api/v1/job-datasets", "/api/v1/job-datasets", true},
		{"/api/v1/job-runners/:taskID/results", "/api/v1/job-runners/test/results", true},
		{"/ordinary", "/ordinary", false},
	} {
		t.Run(test.route, func(t *testing.T) {
			router := gin.New()
			router.Use(RequestBodyLimit(8))
			readResult := make(chan error, 1)
			router.POST(test.route, func(c *gin.Context) {
				_, err := io.Copy(io.Discard, c.Request.Body)
				readResult <- err
				if err == nil {
					c.String(http.StatusOK, "stored")
				} else {
					c.Status(http.StatusBadRequest)
				}
			})
			server := deadlineServer(t, router)
			reader, writer := io.Pipe()
			request, err := http.NewRequest(http.MethodPost, server.URL+test.url, reader)
			require.NoError(t, err)
			request.ContentLength = 2
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer writer.Close()
				if _, err := writer.Write([]byte("a")); err != nil {
					return
				}
				time.Sleep(120 * time.Millisecond)
				_, _ = writer.Write([]byte("b"))
			}()
			client := server.Client()
			client.Timeout = 2 * time.Second
			response, requestErr := client.Do(request)
			if response != nil {
				defer response.Body.Close()
			}
			_ = reader.Close()
			<-done
			select {
			case readErr := <-readResult:
				if test.archive {
					require.NoError(t, readErr)
					require.NoError(t, requestErr)
					require.Equal(t, http.StatusOK, response.StatusCode)
				} else {
					var timeout net.Error
					require.ErrorAs(t, readErr, &timeout)
					require.True(t, timeout.Timeout())
				}
			case <-time.After(time.Second):
				t.Fatal("body handler did not finish")
			}
		})
	}
}

func TestArchiveDownloadsOverrideServerWriteDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		route, url string
		archive    bool
	}{
		{"/api/v1/job-runners/:taskID/dataset", "/api/v1/job-runners/test/dataset", true},
		{"/api/v1/job-datasets/:datasetID/download", "/api/v1/job-datasets/test/download", true},
		{"/api/v1/jobs/:taskID/results/:artifactID/download", "/api/v1/jobs/test/results/source/download", true},
		{"/api/v1/jobs/:taskID/deliveries/:target/download", "/api/v1/jobs/test/deliveries/database/download", true},
		{"/ordinary", "/ordinary", false},
	} {
		t.Run(test.route, func(t *testing.T) {
			router := gin.New()
			router.Use(RequestBodyLimit(8))
			router.GET(test.route, func(c *gin.Context) { time.Sleep(120 * time.Millisecond); c.String(http.StatusOK, "archive") })
			server := deadlineServer(t, router)
			client := server.Client()
			client.Timeout = 2 * time.Second
			response, err := client.Get(server.URL + test.url)
			if !test.archive {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, "archive", string(body))
		})
	}
}

type failingDeadlineWriter struct{ *httptest.ResponseRecorder }

func (w failingDeadlineWriter) SetReadDeadline(time.Time) error {
	return errors.New("deadline rejected")
}

func TestArchiveDeadlineFailureDoesNotReadUpload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestBodyLimit(8))
	called := false
	router.POST("/api/v1/job-datasets", func(c *gin.Context) { called = true; c.Status(http.StatusOK) })
	recorder := httptest.NewRecorder()
	router.ServeHTTP(failingDeadlineWriter{recorder}, httptest.NewRequest(http.MethodPost, "/api/v1/job-datasets", nil))
	require.False(t, called)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

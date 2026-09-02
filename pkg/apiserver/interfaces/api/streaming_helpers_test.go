package api

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestSetupSSEStreamClearsHTTPWriteDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)

	releaseSecondEvent := make(chan struct{})
	var releaseSecondEventOnce sync.Once
	releaseSecond := func() {
		releaseSecondEventOnce.Do(func() {
			close(releaseSecondEvent)
		})
	}
	router := gin.New()
	router.GET("/stream", func(c *gin.Context) {
		flusher, ok := setupSSEStream(c, "pod-api", "api", fmt.Errorf("response writer does not support streaming"))
		if !ok {
			return
		}
		if _, err := fmt.Fprint(c.Writer, "data: first\n\n"); err != nil {
			return
		}
		flusher.Flush()

		<-releaseSecondEvent
		if _, err := fmt.Fprint(c.Writer, "data: second\n\n"); err != nil {
			return
		}
		flusher.Flush()
	})

	server := httptest.NewUnstartedServer(router)
	server.Config.WriteTimeout = 100 * time.Millisecond
	server.Start()
	defer func() {
		releaseSecond()
		server.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/stream", nil)
	if err != nil {
		t.Fatalf("create stream request: %v", err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %s", resp.Status)
	}

	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read first event: %v", err)
	}
	if first != "data: first\n" {
		t.Fatalf("unexpected first event: %q", first)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("read first event terminator: %v", err)
	}

	time.Sleep(2 * server.Config.WriteTimeout)
	releaseSecond()
	second, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read second event after write timeout: %v", err)
	}
	if second != "data: second\n" {
		t.Fatalf("unexpected second event: %q", second)
	}
}

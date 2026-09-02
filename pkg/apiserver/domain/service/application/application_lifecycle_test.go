package application

import (
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func newLifecycleCallbackServer(t *testing.T) (*httptest.Server, <-chan string) {
	t.Helper()
	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		select {
		case received <- string(body):
		default:
		}
	}))
	t.Cleanup(server.Close)
	return server, received
}

func requireLifecycleCallback(t *testing.T, received <-chan string, event, status, taskID string, workflowType config.WorkflowTaskType) {
	t.Helper()
	select {
	case body := <-received:
		require.Contains(t, body, `"event":"`+event+`"`)
		require.Contains(t, body, `"status":"`+status+`"`)
		require.Contains(t, body, `"taskId":"`+taskID+`"`)
		require.Contains(t, body, `"workflowId":""`)
		require.Contains(t, body, `"workflowType":"`+string(workflowType)+`"`)
	case <-time.After(2 * time.Second):
		t.Fatalf("lifecycle callback %s not received", event)
	}
}

func requireNoCallbackReceived(t *testing.T, received <-chan string) {
	t.Helper()
	select {
	case body := <-received:
		t.Fatalf("unexpected callback received: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
}

func captureLifecycleKlogOutput(t *testing.T, fn func()) string {
	t.Helper()
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)

	os.Stderr = w
	klog.SetOutput(w)
	defer func() {
		os.Stderr = oldStderr
		klog.SetOutput(oldStderr)
		_ = r.Close()
		_ = w.Close()
	}()

	fn()
	klog.Flush()
	require.NoError(t, w.Close())
	os.Stderr = oldStderr
	klog.SetOutput(oldStderr)

	output, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return string(output)
}

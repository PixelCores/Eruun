package profiling

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProfilingServerStopsOnCancellation(t *testing.T) {
	originalAddr := Addr
	t.Cleanup(func() { Addr = originalAddr })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	Addr = listener.Addr().String()
	require.NoError(t, listener.Close())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errors := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		StartProfilingServer(ctx, errors)
	}()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	require.Eventually(t, func() bool {
		response, err := client.Get("http://" + Addr + "/mem/stat")
		if err != nil {
			return false
		}
		require.NoError(t, response.Body.Close())
		return response.StatusCode == http.StatusOK
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("profiling server did not stop after cancellation")
	}
	select {
	case err := <-errors:
		t.Fatalf("normal cancellation reported error: %v", err)
	default:
	}
	listener, err = net.Listen("tcp", Addr)
	require.NoError(t, err, "profiling listener must be released")
	require.NoError(t, listener.Close())
}

func TestProfilingStartupFailureDoesNotBlockCancellation(t *testing.T) {
	originalAddr := Addr
	t.Cleanup(func() { Addr = originalAddr })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	Addr = listener.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errors := make(chan error)
	done := make(chan struct{})
	go func() {
		defer close(done)
		StartProfilingServer(ctx, errors)
	}()
	select {
	case err := <-errors:
		require.ErrorContains(t, err, "serve profiling")
	case <-time.After(time.Second):
		t.Fatal("profiling startup error was not reported")
	}
	<-done

	done = make(chan struct{})
	go func() {
		defer close(done)
		StartProfilingServer(ctx, errors)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("profiling error notification blocked cancellation")
	}
}

package async

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBoundedExecutorSubmitRunsTask(t *testing.T) {
	exec := NewBoundedExecutor("test-run", 1, 1)
	t.Cleanup(exec.Close)

	done := make(chan struct{})
	err := exec.Submit(context.Background(), func() {
		close(done)
	})
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("task was not executed")
	}
}

func TestBoundedExecutorSubmitBlocksWhenQueueFull(t *testing.T) {
	exec := NewBoundedExecutor("test-block", 1, 1)
	t.Cleanup(exec.Close)

	release := make(chan struct{})
	firstStarted := make(chan struct{})
	require.NoError(t, exec.Submit(context.Background(), func() {
		close(firstStarted)
		<-release
	}))
	<-firstStarted

	// Fill queue with second task.
	require.NoError(t, exec.Submit(context.Background(), func() {}))

	thirdAccepted := make(chan struct{})
	go func() {
		_ = exec.Submit(context.Background(), func() {})
		close(thirdAccepted)
	}()

	select {
	case <-thirdAccepted:
		t.Fatal("third task should block while queue is full")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-thirdAccepted:
	case <-time.After(time.Second):
		t.Fatal("third task was not accepted after queue was released")
	}
}

func TestBoundedExecutorSubmitReturnsContextErrorWhenQueueFull(t *testing.T) {
	exec := NewBoundedExecutor("test-ctx", 1, 1)
	t.Cleanup(exec.Close)

	release := make(chan struct{})
	firstStarted := make(chan struct{})
	require.NoError(t, exec.Submit(context.Background(), func() {
		close(firstStarted)
		<-release
	}))
	<-firstStarted
	require.NoError(t, exec.Submit(context.Background(), func() {}))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := exec.Submit(ctx, func() {})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded))

	close(release)
}

func TestBoundedExecutorSubmitDoesNotAcceptAlreadyCanceledContext(t *testing.T) {
	exec := NewBoundedExecutor("test-canceled", 1, 1)
	t.Cleanup(exec.Close)

	called := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := exec.Submit(ctx, func() {
		called <- struct{}{}
	})
	require.ErrorIs(t, err, context.Canceled)

	select {
	case <-called:
		t.Fatal("task should not run when context is already canceled")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestBoundedExecutorRecoversTaskPanic(t *testing.T) {
	exec := NewBoundedExecutor("test-panic", 1, 1)
	t.Cleanup(exec.Close)

	require.NoError(t, exec.Submit(context.Background(), func() {
		panic("boom")
	}))

	done := make(chan struct{})
	require.NoError(t, exec.Submit(context.Background(), func() {
		close(done)
	}))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("executor worker did not continue after panic")
	}
}

func TestBoundedExecutorCloseRejectsSubmit(t *testing.T) {
	exec := NewBoundedExecutor("test-close", 1, 1)
	exec.Close()

	for range 100 {
		err := exec.Submit(context.Background(), func() {})
		require.ErrorIs(t, err, ErrExecutorClosed)
	}
}

func TestBoundedExecutorCloseUnblocksBlockedSubmit(t *testing.T) {
	exec := &BoundedExecutor{
		name:  "test-close-unblock",
		tasks: make(chan TaskFunc, 1),
		stop:  make(chan struct{}),
	}
	require.NoError(t, exec.Submit(context.Background(), func() {}))

	submitDone := make(chan error, 1)
	go func() {
		submitDone <- exec.Submit(context.Background(), func() {})
	}()

	select {
	case err := <-submitDone:
		t.Fatalf("submit should block on full queue before close, got err=%v", err)
	case <-time.After(100 * time.Millisecond):
	}

	closeDone := make(chan struct{})
	go func() {
		exec.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("close should not block while submit is waiting on full queue")
	}

	select {
	case err := <-submitDone:
		require.ErrorIs(t, err, ErrExecutorClosed)
	case <-time.After(time.Second):
		t.Fatal("submit did not unblock after close")
	}
}

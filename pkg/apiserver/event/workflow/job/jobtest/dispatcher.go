package jobtest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type DispatcherAckCall struct {
	Group string
	Ids   []string
}

type DispatcherAckQueue struct {
	EnsureGroupErr error
	AckErr         error
	AckCalls       []DispatcherAckCall
}

func TestResultExecutionKey(taskID string) string {
	return "execution-" + taskID
}

func ResultTestPod(job *batchv1.Job) *corev1.Pod {
	pod := SucceededPodForJob(job, job.Name+"-pod", job.UID)
	pod.Spec.Containers = []corev1.Container{{Name: "main"}}
	return pod
}

func (q *DispatcherAckQueue) EnsureGroup(context.Context, string) error { return q.EnsureGroupErr }

func (q *DispatcherAckQueue) Enqueue(context.Context, []byte) (string, error) {
	return "", nil
}

func (q *DispatcherAckQueue) ReadGroup(context.Context, string, string, int, time.Duration) ([]msg.Message, error) {
	return nil, nil
}

func (q *DispatcherAckQueue) Ack(_ context.Context, group string, ids ...string) error {
	q.AckCalls = append(q.AckCalls, DispatcherAckCall{
		Group: group,
		Ids:   append([]string(nil), ids...),
	})
	return q.AckErr
}

func (q *DispatcherAckQueue) AutoClaim(context.Context, string, string, time.Duration, int) ([]msg.Message, error) {
	return nil, nil
}

func (q *DispatcherAckQueue) Close(context.Context) error { return nil }

func (q *DispatcherAckQueue) Stats(context.Context, string) (int64, int64, error) { return 0, 0, nil }

type BlockingDispatcherQueue struct {
	ReadStarted      chan struct{}
	AutoClaimStarted chan struct{}
	ReadOnce         sync.Once
	AutoClaimOnce    sync.Once
}

func NewBlockingDispatcherQueue() *BlockingDispatcherQueue {
	return &BlockingDispatcherQueue{
		ReadStarted:      make(chan struct{}),
		AutoClaimStarted: make(chan struct{}),
	}
}

func (q *BlockingDispatcherQueue) EnsureGroup(context.Context, string) error { return nil }

func (q *BlockingDispatcherQueue) Enqueue(context.Context, []byte) (string, error) {
	return "", nil
}

func (q *BlockingDispatcherQueue) ReadGroup(ctx context.Context, _ string, _ string, _ int, _ time.Duration) ([]msg.Message, error) {
	q.ReadOnce.Do(func() { close(q.ReadStarted) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *BlockingDispatcherQueue) Ack(context.Context, string, ...string) error { return nil }

func (q *BlockingDispatcherQueue) AutoClaim(ctx context.Context, _ string, _ string, _ time.Duration, _ int) ([]msg.Message, error) {
	q.AutoClaimOnce.Do(func() { close(q.AutoClaimStarted) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *BlockingDispatcherQueue) Close(context.Context) error { return nil }

func (q *BlockingDispatcherQueue) Stats(context.Context, string) (int64, int64, error) {
	return 0, 0, nil
}

func RequireStillRunning(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("expected dispatcher to keep running")
	default:
	}
}

func RequireClosed(t *testing.T, done <-chan struct{}) {
	t.Helper()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

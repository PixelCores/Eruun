package jobtest

import (
	"context"
	"time"

	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type EnqueueCaptureQueue struct {
	EnqueueID  string
	EnqueueErr error
	Enqueued   [][]byte
	AckErr     error
}

func (q *EnqueueCaptureQueue) EnsureGroup(context.Context, string) error { return nil }

func (q *EnqueueCaptureQueue) Enqueue(_ context.Context, payload []byte) (string, error) {
	q.Enqueued = append(q.Enqueued, append([]byte(nil), payload...))
	if q.EnqueueErr != nil {
		return "", q.EnqueueErr
	}
	if q.EnqueueID == "" {
		return "1-0", nil
	}
	return q.EnqueueID, nil
}

func (q *EnqueueCaptureQueue) ReadGroup(context.Context, string, string, int, time.Duration) ([]msg.Message, error) {
	return nil, nil
}

func (q *EnqueueCaptureQueue) Ack(context.Context, string, ...string) error { return q.AckErr }

func (q *EnqueueCaptureQueue) AutoClaim(context.Context, string, string, time.Duration, int) ([]msg.Message, error) {
	return nil, nil
}

func (q *EnqueueCaptureQueue) Close(context.Context) error { return nil }

func (q *EnqueueCaptureQueue) Stats(context.Context, string) (int64, int64, error) { return 0, 0, nil }

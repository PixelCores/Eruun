package messaging

import (
	"context"
	"time"
)

// Message represents a queue message with its ID and raw payload.
type Message struct {
	ID      string
	Payload []byte
}

// Queue abstracts a work queue with stream semantics (enqueue, group read, ack).
type Queue interface {
	// EnsureGroup ensures the underlying stream and the specified consumer group exist.
	EnsureGroup(ctx context.Context, group string) error
	// Enqueue pushes a payload to the stream and returns the message ID.
	Enqueue(ctx context.Context, payload []byte) (string, error)
	// ReadGroup reads messages for a consumer in a group.
	ReadGroup(ctx context.Context, group, consumer string, count int, block time.Duration) ([]Message, error)
	// Ack acknowledges a processed message by ID.
	Ack(ctx context.Context, group string, ids ...string) error
	// AutoClaim claims stale pending messages (idle >= minIdle) to the given consumer and returns them.
	AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, count int) ([]Message, error)
	// Close releases any underlying resources.
	Close(ctx context.Context) error
	// Stats returns stream backlog size and pending count for a group.
	Stats(ctx context.Context, group string) (backlog int64, pending int64, err error)
}

// MessageHandlingLifecycle is an optional extension for queue implementations
// that need to track per-message handling state locally.
type MessageHandlingLifecycle interface {
	MarkMessageHandlingStart(id string)
	MarkMessageHandlingDone(id string, acked bool)
}

// MarkMessageHandlingStart marks the beginning of message handling if the queue
// implementation supports lifecycle tracking.
func MarkMessageHandlingStart(queue Queue, id string) {
	if queue == nil || id == "" {
		return
	}
	lifecycle, ok := queue.(MessageHandlingLifecycle)
	if !ok {
		return
	}
	lifecycle.MarkMessageHandlingStart(id)
}

// MarkMessageHandlingDone marks message handling completion if the queue
// implementation supports lifecycle tracking.
func MarkMessageHandlingDone(queue Queue, id string, acked bool) {
	if queue == nil || id == "" {
		return
	}
	lifecycle, ok := queue.(MessageHandlingLifecycle)
	if !ok {
		return
	}
	lifecycle.MarkMessageHandlingDone(id, acked)
}

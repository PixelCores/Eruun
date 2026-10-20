package jobdelay

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

func TestDelayDispatcherHandleMessageAcksInvalidPayload(t *testing.T) {
	queue := &jobtest.DispatcherAckQueue{}
	dispatcher := &DelayDispatcher{
		queue: queue,
		group: "delay-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "delay-1",
		Payload: []byte(`{"job":`),
	})

	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, "delay-workers", queue.AckCalls[0].Group)
	require.Equal(t, []string{"delay-1"}, queue.AckCalls[0].Ids)
}

func TestDelayDispatcherHandleMessageAcksEmptyPayload(t *testing.T) {
	queue := &jobtest.DispatcherAckQueue{}
	dispatcher := &DelayDispatcher{
		queue: queue,
		group: "delay-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "delay-empty",
		Payload: nil,
	})

	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, "delay-workers", queue.AckCalls[0].Group)
	require.Equal(t, []string{"delay-empty"}, queue.AckCalls[0].Ids)
}

func TestDelayDispatcherHandleMessageAcksMissingJob(t *testing.T) {
	queue := &jobtest.DispatcherAckQueue{}
	dispatcher := &DelayDispatcher{
		queue: queue,
		group: "delay-workers",
	}
	raw, err := json.Marshal(&jobinfo.DelayJobPayload{})
	require.NoError(t, err)

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "delay-2",
		Payload: raw,
	})

	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, []string{"delay-2"}, queue.AckCalls[0].Ids)
}

func TestDelayDispatcherAcksUnsupportedNotificationVersions(t *testing.T) {
	for _, raw := range []string{
		`{"version":3,"taskId":"task-1","executionKey":"key-1","runGeneration":1}`,
		`{"version":2,"taskId":"task-1","executionKey":"key-1","runGeneration":1,"job":null}`,
		`{"version":2,"taskId":"task-1","executionKey":"key-1"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			queue := &jobtest.DispatcherAckQueue{}
			dispatcher := &DelayDispatcher{queue: queue, group: "delay-workers"}
			dispatcher.handleMessage(context.Background(), msg.Message{ID: "unsupported", Payload: []byte(raw)})
			require.Len(t, queue.AckCalls, 1)
			require.Equal(t, []string{"unsupported"}, queue.AckCalls[0].Ids)
		})
	}
}

func TestDelayDispatcherFinishRequeuesWhenAckFails(t *testing.T) {
	queue := &jobtest.DispatcherAckQueue{AckErr: errors.New("ack failed")}
	dispatcher := &DelayDispatcher{
		queue:      queue,
		group:      "delay-workers",
		backoffMin: time.Second,
		backoffMax: 8 * time.Second,
		pending: map[string]struct{}{
			"delay-3": {},
		},
		wake: make(chan struct{}, 1),
	}
	item := &delayItem{
		msgID:     "delay-3",
		executeAt: time.Now().Unix(),
	}

	dispatcher.finish(context.Background(), item)

	require.Len(t, queue.AckCalls, 1)
	require.Len(t, dispatcher.items, 1)
	require.Equal(t, "delay-3", dispatcher.items[0].msgID)
	_, stillPending := dispatcher.pending["delay-3"]
	require.True(t, stillPending)
}

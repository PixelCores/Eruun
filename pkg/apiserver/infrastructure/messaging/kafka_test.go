package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
)

type mockKafkaWriter struct {
	mu         sync.Mutex
	writeErr   error
	writeCalls []kafka.Message
	closeErr   error
}

func (m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCalls = append(m.writeCalls, msgs...)
	return m.writeErr
}

func (m *mockKafkaWriter) Close() error {
	return m.closeErr
}

type mockKafkaReader struct {
	mu          sync.Mutex
	fetchQueue  []kafka.Message
	fetchErr    error
	commitErr   error
	commitFn    func(msgs ...kafka.Message) error
	commitCalls []kafka.Message
	closeErr    error
	closeCalls  int
	stats       kafka.ReaderStats
}

func (m *mockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	m.mu.Lock()
	if len(m.fetchQueue) > 0 {
		msg := m.fetchQueue[0]
		m.fetchQueue = m.fetchQueue[1:]
		m.mu.Unlock()
		return msg, nil
	}
	fetchErr := m.fetchErr
	m.mu.Unlock()
	if fetchErr != nil {
		return kafka.Message{}, fetchErr
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (m *mockKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitCalls = append(m.commitCalls, msgs...)
	if m.commitFn != nil {
		return m.commitFn(msgs...)
	}
	return m.commitErr
}

func (m *mockKafkaReader) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	return m.closeErr
}

func (m *mockKafkaReader) Stats() kafka.ReaderStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats
}

func useMockKafkaWriterFactory(t *testing.T) {
	t.Helper()
	old := kafkaWriterFactory
	kafkaWriterFactory = func(cfg KafkaConfig) kafkaWriter {
		return &mockKafkaWriter{}
	}
	t.Cleanup(func() {
		kafkaWriterFactory = old
	})
}

func TestNewKafkaQueueValidation(t *testing.T) {
	useMockKafkaWriterFactory(t)

	tests := []struct {
		name    string
		cfg     KafkaConfig
		wantErr bool
	}{
		{
			name: "empty brokers",
			cfg: KafkaConfig{
				Brokers: []string{},
				Topic:   "test-topic",
			},
			wantErr: true,
		},
		{
			name: "empty topic",
			cfg: KafkaConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "",
			},
			wantErr: true,
		},
		{
			name: "valid config with defaults",
			cfg: KafkaConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kq, err := NewKafkaQueue(tt.cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, kq)
		})
	}
}

func TestKafkaQueueDefaults(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	})
	require.NoError(t, err)
	require.Equal(t, defaultKafkaGroupID, kq.cfg.GroupID)
	require.Equal(t, "earliest", kq.cfg.AutoOffsetReset)
}

func TestKafkaQueueEnqueueReturnsCorrelationIDAndWritesHeader(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	})
	require.NoError(t, err)

	correlationID, err := kq.Enqueue(context.Background(), []byte("payload"))
	require.NoError(t, err)
	_, parseErr := uuid.Parse(correlationID)
	require.NoError(t, parseErr)

	writer, ok := kq.writer.(*mockKafkaWriter)
	require.True(t, ok)
	writer.mu.Lock()
	calls := append([]kafka.Message(nil), writer.writeCalls...)
	writer.mu.Unlock()
	require.Len(t, calls, 1)
	require.Equal(t, []byte(correlationID), calls[0].Key)
	require.Equal(t, []byte("payload"), calls[0].Value)

	foundHeader := false
	for _, header := range calls[0].Headers {
		if header.Key == correlationHeaderKey {
			foundHeader = true
			require.Equal(t, []byte(correlationID), header.Value)
			break
		}
	}
	require.True(t, foundHeader)
}

func TestKafkaQueueEnsureGroupDerivationAndSwitch(t *testing.T) {
	useMockKafkaWriterFactory(t)

	oldReaderFactory := kafkaReaderFactory
	var readerCfgs []kafka.ReaderConfig
	reader1 := &mockKafkaReader{}
	reader2 := &mockKafkaReader{}
	reader3 := &mockKafkaReader{}
	readers := []kafkaReader{reader1, reader2, reader3}
	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReader {
		readerCfgs = append(readerCfgs, cfg)
		idx := len(readerCfgs) - 1
		return readers[idx]
	}
	t.Cleanup(func() {
		kafkaReaderFactory = oldReaderFactory
	})

	kq, err := NewKafkaQueue(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "base-group",
	})
	require.NoError(t, err)

	require.NoError(t, kq.EnsureGroup(context.Background(), workflowDispatchGroup))
	require.Len(t, readerCfgs, 1)
	require.Equal(t, "base-group", readerCfgs[0].GroupID)

	kq.storePending("0:1", kafka.Message{Partition: 0, Offset: 1, Value: []byte("one")})
	require.Len(t, kq.pendingMessages, 1)

	require.NoError(t, kq.EnsureGroup(context.Background(), workflowDispatchGroup))
	require.Len(t, readerCfgs, 1, "same group should not recreate reader")

	require.NoError(t, kq.EnsureGroup(context.Background(), delayDispatchGroup))
	require.Len(t, readerCfgs, 2)
	require.Equal(t, "base-group.delay", readerCfgs[1].GroupID)
	require.Equal(t, 1, reader1.closeCalls)
	require.Empty(t, kq.pendingMessages, "switching group should reset pending state")

	require.NoError(t, kq.EnsureGroup(context.Background(), resultDispatchGroup))
	require.Len(t, readerCfgs, 3)
	require.Equal(t, "base-group.result", readerCfgs[2].GroupID)
	require.Equal(t, 1, reader2.closeCalls)
}

func TestKafkaQueueAckCommitsContiguousOffsetsOnly(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	reader := &mockKafkaReader{}
	kq.reader = reader

	kq.storePending("0:1", kafka.Message{Partition: 0, Offset: 1, Value: []byte("a")})
	kq.storePending("0:2", kafka.Message{Partition: 0, Offset: 2, Value: []byte("b")})
	kq.storePending("0:3", kafka.Message{Partition: 0, Offset: 3, Value: []byte("c")})

	require.NoError(t, kq.Ack(context.Background(), "group", "0:3"))
	require.Empty(t, reader.commitCalls, "out-of-order ack must not commit higher offset")

	require.NoError(t, kq.Ack(context.Background(), "group", "0:1"))
	require.Len(t, reader.commitCalls, 1)
	require.EqualValues(t, 1, reader.commitCalls[0].Offset)

	require.NoError(t, kq.Ack(context.Background(), "group", "0:2"))
	require.Len(t, reader.commitCalls, 2)
	require.EqualValues(t, 3, reader.commitCalls[1].Offset)
	require.Empty(t, kq.pendingMessages)
}

func TestKafkaQueueAckCommitsPerPartitionIndependently(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	reader := &mockKafkaReader{}
	kq.reader = reader

	kq.storePending("0:10", kafka.Message{Partition: 0, Offset: 10, Value: []byte("p0")})
	kq.storePending("1:20", kafka.Message{Partition: 1, Offset: 20, Value: []byte("p1")})

	require.NoError(t, kq.Ack(context.Background(), "group", "1:20"))
	require.Len(t, reader.commitCalls, 1)
	require.EqualValues(t, 1, reader.commitCalls[0].Partition)
	require.EqualValues(t, 20, reader.commitCalls[0].Offset)
	require.Contains(t, kq.pendingMessages, "0:10")
	require.NotContains(t, kq.pendingMessages, "1:20")
}

func TestKafkaQueueAckBlockedHeadCompactsAckedPendingTail(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	reader := &mockKafkaReader{}
	kq.reader = reader

	kq.storePending("0:1", kafka.Message{Partition: 0, Offset: 1, Value: []byte("head")})
	kq.storePending("0:2", kafka.Message{Partition: 0, Offset: 2, Value: []byte("tail-2")})
	kq.storePending("0:3", kafka.Message{Partition: 0, Offset: 3, Value: []byte("tail-3")})
	kq.storePending("0:4", kafka.Message{Partition: 0, Offset: 4, Value: []byte("tail-4")})

	require.NoError(t, kq.Ack(context.Background(), "group", "0:2", "0:3", "0:4"))
	require.Empty(t, reader.commitCalls, "head message is not acked, so commit must stay blocked")
	require.Len(t, kq.pendingMessages, 2, "acked tail should be compacted to avoid unbounded pending growth")
	require.Contains(t, kq.pendingMessages, "0:1")
	require.Contains(t, kq.pendingMessages, "0:4")

	require.NoError(t, kq.Ack(context.Background(), "group", "0:1"))
	require.Len(t, reader.commitCalls, 1)
	require.EqualValues(t, 4, reader.commitCalls[0].Offset, "head release should allow commit to compacted tail")
	require.Empty(t, kq.pendingMessages)
}

func TestKafkaQueueAckUnknownIDDoesNotFail(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	kq.reader = &mockKafkaReader{}

	err = kq.Ack(context.Background(), "group", "unknown-id")
	require.NoError(t, err)
}

func TestKafkaQueueAckCommitFailureKeepsPending(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	kq.reader = &mockKafkaReader{commitErr: errors.New("commit failed")}

	kq.storePending("0:1", kafka.Message{Partition: 0, Offset: 1, Value: []byte("a")})
	err = kq.Ack(context.Background(), "group", "0:1")
	require.Error(t, err)
	require.Contains(t, kq.pendingMessages, "0:1")
	require.False(t, kq.pendingMessages["0:1"].acked)

	kq.MarkMessageHandlingDone("0:1", false)
	claimed, claimErr := kq.AutoClaim(context.Background(), "group", "consumer", 0, 1)
	require.NoError(t, claimErr)
	require.Len(t, claimed, 1)
	require.Equal(t, "0:1", claimed[0].ID)
}

func TestKafkaQueueAutoClaimStaleMessagesAndRefreshIdle(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)

	kq.storePending("0:1", kafka.Message{Partition: 0, Offset: 1, Value: []byte("old")})
	kq.storePending("0:2", kafka.Message{Partition: 0, Offset: 2, Value: []byte("new")})

	kq.pendingMessages["0:1"].lastDeliveredAt = time.Now().Add(-2 * time.Minute)
	kq.pendingMessages["0:2"].lastDeliveredAt = time.Now()
	kq.pendingMessages["0:1"].inFlight = false
	kq.pendingMessages["0:2"].inFlight = false

	claimed, err := kq.AutoClaim(context.Background(), "group", "consumer", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, "0:1", claimed[0].ID)

	claimedAgain, err := kq.AutoClaim(context.Background(), "group", "consumer", time.Minute, 10)
	require.NoError(t, err)
	require.Empty(t, claimedAgain, "claimed message idle timestamp should be refreshed")
}

func TestKafkaQueueAutoClaimSkipsInFlightMessages(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)

	kq.storePending("0:1", kafka.Message{Partition: 0, Offset: 1, Value: []byte("active")})
	kq.storePending("0:2", kafka.Message{Partition: 0, Offset: 2, Value: []byte("stale")})
	kq.pendingMessages["0:1"].lastDeliveredAt = time.Now().Add(-2 * time.Minute)
	kq.pendingMessages["0:2"].lastDeliveredAt = time.Now().Add(-2 * time.Minute)
	kq.pendingMessages["0:1"].inFlight = true
	kq.pendingMessages["0:2"].inFlight = false

	claimed, err := kq.AutoClaim(context.Background(), "group", "consumer", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, "0:2", claimed[0].ID)
}

func TestKafkaQueueMarkMessageHandlingDoneRefreshesIdleWindow(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)

	kq.storePending("0:1", kafka.Message{Partition: 0, Offset: 1, Value: []byte("payload")})
	kq.pendingMessages["0:1"].lastDeliveredAt = time.Now().Add(-2 * time.Minute)
	kq.pendingMessages["0:1"].inFlight = false

	claimed, err := kq.AutoClaim(context.Background(), "group", "consumer", time.Minute, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	kq.MarkMessageHandlingDone("0:1", false)
	rec := kq.pendingMessages["0:1"]
	require.NotNil(t, rec)
	require.False(t, rec.inFlight)
	require.WithinDuration(t, time.Now(), rec.lastDeliveredAt, time.Second)

	claimedAgain, err := kq.AutoClaim(context.Background(), "group", "consumer", time.Minute, 1)
	require.NoError(t, err)
	require.Empty(t, claimedAgain)
}

func TestKafkaQueueReadGroupRequiresEnsureGroup(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)

	_, err = kq.ReadGroup(context.Background(), "group", "consumer", 1, 10*time.Millisecond)
	require.Error(t, err)
}

func TestKafkaQueueReadGroupSkipsReadinessProbeMessages(t *testing.T) {
	useMockKafkaWriterFactory(t)

	oldReaderFactory := kafkaReaderFactory
	reader := &mockKafkaReader{
		fetchQueue: []kafka.Message{
			{
				Partition: 0,
				Offset:    10,
				Headers: []kafka.Header{
					{Key: "eruun-readiness-probe", Value: []byte("readiness")},
					{Key: "eruun-readiness-probe-id", Value: []byte("probe-1")},
				},
			},
			{
				Partition: 0,
				Offset:    11,
				Headers: []kafka.Header{
					{Key: correlationHeaderKey, Value: []byte("task-1")},
				},
				Value: []byte("payload-1"),
			},
		},
	}
	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReader { return reader }
	t.Cleanup(func() {
		kafkaReaderFactory = oldReaderFactory
	})

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	require.NoError(t, kq.EnsureGroup(context.Background(), "group"))

	msgs, err := kq.ReadGroup(context.Background(), "group", "consumer", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "task-1", msgs[0].ID)
	require.Equal(t, []byte("payload-1"), msgs[0].Payload)
	require.Len(t, reader.commitCalls, 1)
	require.EqualValues(t, 10, reader.commitCalls[0].Offset)
}

func TestKafkaQueueReadGroupReadinessProbeDoesNotCommitPastUnackedBusinessMessage(t *testing.T) {
	useMockKafkaWriterFactory(t)

	oldReaderFactory := kafkaReaderFactory
	reader := &mockKafkaReader{
		fetchQueue: []kafka.Message{
			{
				Partition: 0,
				Offset:    10,
				Headers: []kafka.Header{
					{Key: correlationHeaderKey, Value: []byte("task-1")},
				},
				Value: []byte("payload-1"),
			},
			{
				Partition: 0,
				Offset:    11,
				Headers: []kafka.Header{
					{Key: "eruun-readiness-probe", Value: []byte("readiness")},
					{Key: "eruun-readiness-probe-id", Value: []byte("probe-1")},
				},
			},
		},
	}
	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReader { return reader }
	t.Cleanup(func() {
		kafkaReaderFactory = oldReaderFactory
	})

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	require.NoError(t, kq.EnsureGroup(context.Background(), "group"))

	msgs, err := kq.ReadGroup(context.Background(), "group", "consumer", 2, 10*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "task-1", msgs[0].ID)
	require.Empty(t, reader.commitCalls, "probe offset must not commit past earlier unacked business message")
	require.Contains(t, kq.pendingMessages, "task-1")
	require.Contains(t, kq.pendingMessages, "0:11")
	require.True(t, kq.pendingMessages["0:11"].acked)

	require.NoError(t, kq.Ack(context.Background(), "group", "task-1"))
	require.Len(t, reader.commitCalls, 1)
	require.EqualValues(t, 11, reader.commitCalls[0].Offset)
	require.Empty(t, kq.pendingMessages)
}

func TestKafkaQueueReadGroupFailsWhenProbeCommitFails(t *testing.T) {
	useMockKafkaWriterFactory(t)

	oldReaderFactory := kafkaReaderFactory
	reader := &mockKafkaReader{
		fetchQueue: []kafka.Message{
			{
				Partition: 1,
				Offset:    22,
				Headers: []kafka.Header{
					{Key: "eruun-readiness-probe", Value: []byte("readiness")},
					{Key: "eruun-readiness-probe-id", Value: []byte("probe-2")},
				},
			},
		},
		commitErr: errors.New("commit failed"),
	}
	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReader { return reader }
	t.Cleanup(func() {
		kafkaReaderFactory = oldReaderFactory
	})

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	require.NoError(t, kq.EnsureGroup(context.Background(), "group"))

	_, err = kq.ReadGroup(context.Background(), "group", "consumer", 1, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit kafka readiness probe")
}

func TestKafkaQueueReadGroupReturnsPartialBatchWhenProbeCommitFails(t *testing.T) {
	useMockKafkaWriterFactory(t)

	oldReaderFactory := kafkaReaderFactory
	reader := &mockKafkaReader{
		fetchQueue: []kafka.Message{
			{
				Partition: 0,
				Offset:    10,
				Headers: []kafka.Header{
					{Key: correlationHeaderKey, Value: []byte("task-1")},
				},
				Value: []byte("payload-1"),
			},
			{
				Partition: 1,
				Offset:    22,
				Headers: []kafka.Header{
					{Key: "eruun-readiness-probe", Value: []byte("readiness")},
					{Key: "eruun-readiness-probe-id", Value: []byte("probe-2")},
				},
			},
		},
		commitErr: errors.New("commit failed"),
	}
	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReader { return reader }
	t.Cleanup(func() {
		kafkaReaderFactory = oldReaderFactory
	})

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	require.NoError(t, kq.EnsureGroup(context.Background(), "group"))

	msgs, err := kq.ReadGroup(context.Background(), "group", "consumer", 2, time.Second)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "task-1", msgs[0].ID)
	require.Len(t, reader.commitCalls, 1)
	require.EqualValues(t, 22, reader.commitCalls[0].Offset)
	require.Contains(t, kq.pendingMessages, "task-1")
}

func TestKafkaQueueReadGroupProbeCommitUsesReadCtxNotCallerCtx(t *testing.T) {
	useMockKafkaWriterFactory(t)

	oldReaderFactory := kafkaReaderFactory
	commitCh := make(chan struct{})
	reader := &mockKafkaReader{
		fetchQueue: []kafka.Message{
			{
				Partition: 0,
				Offset:    10,
				Headers: []kafka.Header{
					{Key: "eruun-readiness-probe", Value: []byte("readiness")},
					{Key: "eruun-readiness-probe-id", Value: []byte("probe-slow")},
				},
			},
		},
		commitFn: func(msgs ...kafka.Message) error {
			// Block until the context passed to CommitMessages is cancelled.
			// If the caller's long-lived context were used, this would hang forever.
			<-commitCh
			return context.DeadlineExceeded
		},
	}
	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReader { return reader }
	t.Cleanup(func() {
		kafkaReaderFactory = oldReaderFactory
	})

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	require.NoError(t, kq.EnsureGroup(context.Background(), "group"))

	block := 100 * time.Millisecond
	done := make(chan struct{})
	go func() {
		defer close(done)
		// The caller context has no deadline — only the block timeout should bound the call.
		_, _ = kq.ReadGroup(context.Background(), "group", "consumer", 1, block)
	}()

	// Unblock the commit after a short wait so the goroutine can finish.
	go func() {
		time.Sleep(block + 200*time.Millisecond)
		close(commitCh)
	}()

	select {
	case <-done:
		// ReadGroup returned within a reasonable time — bounded by readCtx.
	case <-time.After(3 * time.Second):
		close(commitCh) // unblock so goroutine exits
		t.Fatal("ReadGroup did not return within expected timeout; probe commit likely used caller context instead of readCtx")
	}
}

func TestKafkaQueueStatsWithPendingAndReaderLag(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"})
	require.NoError(t, err)
	kq.reader = &mockKafkaReader{stats: kafka.ReaderStats{Lag: 8}}
	kq.storePending("0:1", kafka.Message{Partition: 0, Offset: 1})
	kq.storePending("0:2", kafka.Message{Partition: 0, Offset: 2})

	backlog, pending, err := kq.Stats(context.Background(), "group")
	require.NoError(t, err)
	require.EqualValues(t, 8, backlog)
	require.EqualValues(t, 2, pending)
}

func TestKafkaQueueUsesClientProbeHeaders(t *testing.T) {
	require.True(t, clients.IsKafkaReadinessProbe([]kafka.Header{
		{Key: "eruun-readiness-probe", Value: []byte("readiness")},
	}))
	require.False(t, clients.IsKafkaReadinessProbe([]kafka.Header{
		{Key: correlationHeaderKey, Value: []byte("task-1")},
	}))
}

func TestKafkaQueueMessageIDUsesCorrelationWhenPresent(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	})
	require.NoError(t, err)

	msgID := kq.messageID(kafka.Message{
		Partition: 7,
		Offset:    42,
		Headers: []kafka.Header{
			{Key: correlationHeaderKey, Value: []byte("cid-from-header")},
		},
	})
	require.Equal(t, "cid-from-header", msgID)

	msgID = kq.messageID(kafka.Message{
		Partition: 8,
		Offset:    99,
		Key:       []byte("cid-from-key"),
	})
	require.Equal(t, "8:99", msgID)

	msgID = kq.messageID(kafka.Message{Partition: 7, Offset: 42})
	require.Equal(t, "7:42", msgID)
}

func TestKafkaQueueMessageIDWithoutCorrelationUsesPartitionOffset(t *testing.T) {
	useMockKafkaWriterFactory(t)

	kq, err := NewKafkaQueue(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	})
	require.NoError(t, err)

	msgID1 := kq.messageID(kafka.Message{
		Partition: 0,
		Offset:    101,
		Key:       []byte("business-key"),
	})
	msgID2 := kq.messageID(kafka.Message{
		Partition: 0,
		Offset:    102,
		Key:       []byte("business-key"),
	})

	require.Equal(t, "0:101", msgID1)
	require.Equal(t, "0:102", msgID2)
	require.NotEqual(t, msgID1, msgID2)
}

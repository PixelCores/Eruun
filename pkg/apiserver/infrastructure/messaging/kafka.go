package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
)

const (
	defaultKafkaGroupID   = "eruun-workflow-workers"
	workflowDispatchGroup = "workflow-workers"
	delayDispatchGroup    = "job-delay-dispatcher"
	resultDispatchGroup   = "job-result-dispatcher"
	delayGroupSuffix      = "delay"
	resultGroupSuffix     = "result"
	correlationHeaderKey  = "eruun-correlation-id"
)

var (
	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReader { return kafka.NewReader(cfg) }
	kafkaWriterFactory = defaultKafkaWriterFactory
)

// KafkaConfig holds Kafka-specific configuration options.
type KafkaConfig struct {
	Brokers         []string
	Topic           string
	GroupID         string
	AutoOffsetReset string // "earliest" or "latest"
}

type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
	Stats() kafka.ReaderStats
}

type kafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type pendingMessage struct {
	id              string
	msg             kafka.Message
	acked           bool
	inFlight        bool
	fetchedAt       time.Time
	lastDeliveredAt time.Time
}

// KafkaQueue implements Queue using Kafka Consumer Groups.
// It uses kafka-go library for both producing and consuming messages.
type KafkaQueue struct {
	cfg    KafkaConfig
	writer kafkaWriter

	// reader is lazily initialized when EnsureGroup is called.
	mu          sync.RWMutex
	reader      kafkaReader
	readerGroup string

	// pendingMessages tracks messages that have been read but not yet committed.
	pendingMu          sync.Mutex
	pendingMessages    map[string]*pendingMessage
	pendingByPartition map[int]map[int64]*pendingMessage
}

func defaultKafkaWriterFactory(cfg KafkaConfig) kafkaWriter {
	return &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1, // Send immediately for low latency
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
}

// NewKafkaQueue creates a new KafkaQueue with the given configuration.
// The writer is initialized immediately, but the reader is created lazily
// when EnsureGroup is called to set up the consumer group.
func NewKafkaQueue(cfg KafkaConfig) (*KafkaQueue, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers cannot be empty")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka topic cannot be empty")
	}
	if strings.TrimSpace(cfg.GroupID) == "" {
		cfg.GroupID = defaultKafkaGroupID
	}
	if cfg.AutoOffsetReset == "" {
		cfg.AutoOffsetReset = "earliest"
	}

	return &KafkaQueue{
		cfg:                cfg,
		writer:             kafkaWriterFactory(cfg),
		pendingMessages:    make(map[string]*pendingMessage),
		pendingByPartition: make(map[int]map[int64]*pendingMessage),
	}, nil
}

// EnsureGroup ensures the consumer group exists and initializes the reader.
// In Kafka, consumer groups are created automatically when a consumer joins,
// so this method primarily initializes the reader with the specified group.
func (k *KafkaQueue) EnsureGroup(ctx context.Context, group string) error {
	effectiveGroup := k.deriveGroupID(group)

	k.mu.Lock()
	defer k.mu.Unlock()

	if k.reader != nil && k.readerGroup == effectiveGroup {
		return nil
	}

	if k.reader != nil {
		if err := k.reader.Close(); err != nil {
			return fmt.Errorf("close kafka reader before group switch: %w", err)
		}
		k.reader = nil
		k.readerGroup = ""
	}

	startOffset := kafka.FirstOffset
	if strings.EqualFold(strings.TrimSpace(k.cfg.AutoOffsetReset), "latest") {
		startOffset = kafka.LastOffset
	}

	k.reader = kafkaReaderFactory(kafka.ReaderConfig{
		Brokers:        k.cfg.Brokers,
		Topic:          k.cfg.Topic,
		GroupID:        effectiveGroup,
		StartOffset:    startOffset,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0, // Disable auto-commit, we commit manually on Ack
	})
	k.readerGroup = effectiveGroup
	k.resetPending()

	klog.V(2).Infof("kafka reader initialized for topic=%s group=%s", k.cfg.Topic, effectiveGroup)
	return nil
}

func (k *KafkaQueue) deriveGroupID(group string) string {
	base := strings.TrimSpace(k.cfg.GroupID)
	if base == "" {
		base = defaultKafkaGroupID
	}
	role := strings.TrimSpace(group)
	if role == "" || role == base || role == workflowDispatchGroup {
		return base
	}

	switch role {
	case delayDispatchGroup:
		return fmt.Sprintf("%s.%s", base, delayGroupSuffix)
	case resultDispatchGroup:
		return fmt.Sprintf("%s.%s", base, resultGroupSuffix)
	}

	if strings.HasPrefix(role, base+".") {
		return role
	}
	normalized := strings.NewReplacer(" ", "-", "\t", "-", "\n", "-").Replace(role)
	return fmt.Sprintf("%s.%s", base, normalized)
}

// Enqueue pushes a payload to the Kafka topic and returns a correlation ID.
func (k *KafkaQueue) Enqueue(ctx context.Context, payload []byte) (string, error) {
	correlationID := uuid.NewString()
	msg := kafka.Message{
		Key:   []byte(correlationID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: correlationHeaderKey, Value: []byte(correlationID)},
		},
	}
	if err := k.writer.WriteMessages(ctx, msg); err != nil {
		return "", err
	}
	return correlationID, nil
}

// ReadGroup reads messages for a consumer in a group.
// The consumer parameter is ignored as Kafka manages consumers within groups automatically.
func (k *KafkaQueue) ReadGroup(ctx context.Context, group, consumer string, count int, block time.Duration) ([]Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	k.mu.RLock()
	reader := k.reader
	k.mu.RUnlock()

	if reader == nil {
		return nil, errors.New("kafka reader not initialized, call EnsureGroup first")
	}

	readCtx, cancel := context.WithTimeout(ctx, block)
	defer cancel()

	var messages []Message
	for len(messages) < count {
		msg, err := reader.FetchMessage(readCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			if len(messages) > 0 {
				klog.V(4).Infof("kafka read partial: got %d messages before error: %v", len(messages), err)
				break
			}
			return nil, err
		}
		if clients.IsKafkaReadinessProbe(msg.Headers) {
			if err := k.ackInternalMessage(readCtx, reader, msg); err != nil {
				if len(messages) > 0 {
					klog.V(4).Infof("kafka read partial: got %d messages before readiness probe ack error: %v", len(messages), err)
					break
				}
				return nil, err
			}
			klog.V(4).Infof("kafka queue skipped readiness probe topic=%s partition=%d offset=%d", k.cfg.Topic, msg.Partition, msg.Offset)
			continue
		}

		msgID := k.messageID(msg)
		k.storePending(msgID, msg)
		messages = append(messages, Message{ID: msgID, Payload: msg.Value})
	}

	return messages, nil
}

func (k *KafkaQueue) storePending(id string, msg kafka.Message) {
	now := time.Now()
	k.pendingMu.Lock()
	defer k.pendingMu.Unlock()

	rec, exists := k.pendingMessages[id]
	if !exists || rec == nil {
		rec = &pendingMessage{id: id}
		k.pendingMessages[id] = rec
	}
	rec.msg = msg
	rec.acked = false
	rec.inFlight = true
	rec.fetchedAt = now
	rec.lastDeliveredAt = now

	if _, ok := k.pendingByPartition[msg.Partition]; !ok {
		k.pendingByPartition[msg.Partition] = make(map[int64]*pendingMessage)
	}
	k.pendingByPartition[msg.Partition][msg.Offset] = rec
}

func (k *KafkaQueue) ackInternalMessage(ctx context.Context, reader kafkaReader, msg kafka.Message) error {
	now := time.Now()
	id := k.messageID(msg)

	k.pendingMu.Lock()
	defer k.pendingMu.Unlock()

	rec, exists := k.pendingMessages[id]
	if !exists || rec == nil {
		rec = &pendingMessage{id: id}
		k.pendingMessages[id] = rec
	}
	rec.msg = msg
	rec.acked = true
	rec.inFlight = false
	rec.fetchedAt = now
	rec.lastDeliveredAt = now

	if _, ok := k.pendingByPartition[msg.Partition]; !ok {
		k.pendingByPartition[msg.Partition] = make(map[int64]*pendingMessage)
	}
	k.pendingByPartition[msg.Partition][msg.Offset] = rec

	candidate, ok := k.commitCandidateLocked(msg.Partition)
	if !ok || candidate == nil {
		k.compactAckedPendingLocked(msg.Partition)
		return nil
	}
	if err := reader.CommitMessages(ctx, candidate.msg); err != nil {
		return fmt.Errorf("commit kafka readiness probe partition %d offset %d: %w", candidate.msg.Partition, candidate.msg.Offset, err)
	}
	k.clearCommittedLocked(msg.Partition, candidate.msg.Offset)
	k.compactAckedPendingLocked(msg.Partition)
	return nil
}

// MarkMessageHandlingStart marks a pending message as actively handled.
func (k *KafkaQueue) MarkMessageHandlingStart(id string) {
	if strings.TrimSpace(id) == "" {
		return
	}
	now := time.Now()
	k.pendingMu.Lock()
	defer k.pendingMu.Unlock()
	rec, ok := k.pendingMessages[id]
	if !ok || rec == nil {
		return
	}
	rec.inFlight = true
	rec.lastDeliveredAt = now
}

// MarkMessageHandlingDone marks active handling completion.
// For non-acked messages, the idle timer starts from handling completion.
func (k *KafkaQueue) MarkMessageHandlingDone(id string, acked bool) {
	if strings.TrimSpace(id) == "" {
		return
	}
	now := time.Now()
	k.pendingMu.Lock()
	defer k.pendingMu.Unlock()
	rec, ok := k.pendingMessages[id]
	if !ok || rec == nil {
		return
	}
	rec.inFlight = false
	if acked {
		return
	}
	rec.lastDeliveredAt = now
}

// Ack acknowledges processed messages by committing contiguous offsets.
func (k *KafkaQueue) Ack(ctx context.Context, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}

	k.mu.RLock()
	reader := k.reader
	k.mu.RUnlock()
	if reader == nil {
		return errors.New("kafka reader not initialized")
	}

	k.pendingMu.Lock()
	defer k.pendingMu.Unlock()

	partitions := make(map[int]struct{})
	previousAckState := make(map[*pendingMessage]bool)
	for _, id := range ids {
		rec, ok := k.pendingMessages[id]
		if !ok || rec == nil {
			klog.V(4).Infof("kafka ack: message %s not found in pending, may be already acked", id)
			continue
		}
		if _, exists := previousAckState[rec]; !exists {
			previousAckState[rec] = rec.acked
		}
		rec.acked = true
		partitions[rec.msg.Partition] = struct{}{}
	}

	var commitErrs []error
	for partition := range partitions {
		candidate, ok := k.commitCandidateLocked(partition)
		if !ok || candidate == nil {
			k.compactAckedPendingLocked(partition)
			continue
		}
		if err := reader.CommitMessages(ctx, candidate.msg); err != nil {
			k.rollbackPartitionAckStateLocked(partition, previousAckState)
			commitErrs = append(commitErrs, fmt.Errorf("commit partition %d offset %d: %w", partition, candidate.msg.Offset, err))
			continue
		}
		k.clearCommittedLocked(partition, candidate.msg.Offset)
		k.compactAckedPendingLocked(partition)
	}

	if len(commitErrs) > 0 {
		return errors.Join(commitErrs...)
	}
	return nil
}

func (k *KafkaQueue) rollbackPartitionAckStateLocked(partition int, previous map[*pendingMessage]bool) {
	for rec, acked := range previous {
		if rec == nil || rec.msg.Partition != partition {
			continue
		}
		rec.acked = acked
	}
}

func (k *KafkaQueue) commitCandidateLocked(partition int) (*pendingMessage, bool) {
	records, ok := k.pendingByPartition[partition]
	if !ok || len(records) == 0 {
		return nil, false
	}

	offsets := make([]int64, 0, len(records))
	for offset := range records {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	first := records[offsets[0]]
	if first == nil || !first.acked {
		return nil, false
	}
	candidate := first
	for _, offset := range offsets[1:] {
		rec := records[offset]
		if rec == nil || !rec.acked {
			break
		}
		candidate = rec
	}
	return candidate, true
}

func (k *KafkaQueue) clearCommittedLocked(partition int, committedOffset int64) {
	records, ok := k.pendingByPartition[partition]
	if !ok {
		return
	}
	for offset, rec := range records {
		if offset > committedOffset {
			continue
		}
		delete(records, offset)
		if rec != nil {
			delete(k.pendingMessages, rec.id)
		}
	}
	if len(records) == 0 {
		delete(k.pendingByPartition, partition)
	}
}

// compactAckedPendingLocked keeps only the tail record for each consecutive
// run of acked messages in a partition to cap pending memory under head-of-line
// blocking while still preserving enough information to advance commits later.
func (k *KafkaQueue) compactAckedPendingLocked(partition int) {
	records, ok := k.pendingByPartition[partition]
	if !ok || len(records) < 2 {
		return
	}

	offsets := make([]int64, 0, len(records))
	for offset := range records {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	var previousAckedOffset int64
	hasPreviousAcked := false
	for _, offset := range offsets {
		rec := records[offset]
		if rec == nil {
			hasPreviousAcked = false
			continue
		}
		if !rec.acked {
			hasPreviousAcked = false
			continue
		}

		// Payload bytes are not needed once message handling has completed.
		rec.msg.Value = nil
		rec.msg.Key = nil
		rec.msg.Headers = nil

		if !hasPreviousAcked {
			previousAckedOffset = offset
			hasPreviousAcked = true
			continue
		}

		if prev := records[previousAckedOffset]; prev != nil {
			delete(k.pendingMessages, prev.id)
		}
		delete(records, previousAckedOffset)
		previousAckedOffset = offset
	}
}

// AutoClaim returns stale unacked messages from local pending state.
func (k *KafkaQueue) AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, count int) ([]Message, error) {
	if count <= 0 {
		return nil, nil
	}
	if minIdle < 0 {
		minIdle = 0
	}

	now := time.Now()
	k.pendingMu.Lock()
	defer k.pendingMu.Unlock()

	candidates := make([]*pendingMessage, 0, len(k.pendingMessages))
	for _, rec := range k.pendingMessages {
		if rec == nil || rec.acked {
			continue
		}
		if rec.inFlight {
			continue
		}
		if now.Sub(rec.lastDeliveredAt) < minIdle {
			continue
		}
		candidates = append(candidates, rec)
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].lastDeliveredAt.Equal(candidates[j].lastDeliveredAt) {
			if candidates[i].msg.Partition == candidates[j].msg.Partition {
				return candidates[i].msg.Offset < candidates[j].msg.Offset
			}
			return candidates[i].msg.Partition < candidates[j].msg.Partition
		}
		return candidates[i].lastDeliveredAt.Before(candidates[j].lastDeliveredAt)
	})

	if len(candidates) > count {
		candidates = candidates[:count]
	}

	messages := make([]Message, 0, len(candidates))
	for _, rec := range candidates {
		rec.inFlight = true
		rec.lastDeliveredAt = now
		messages = append(messages, Message{ID: rec.id, Payload: rec.msg.Value})
	}
	return messages, nil
}

// Close releases the Kafka writer and reader resources.
func (k *KafkaQueue) Close(ctx context.Context) error {
	var errs []error

	if k.writer != nil {
		if err := k.writer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	k.mu.Lock()
	if k.reader != nil {
		if err := k.reader.Close(); err != nil {
			errs = append(errs, err)
		}
		k.reader = nil
		k.readerGroup = ""
	}
	k.mu.Unlock()

	k.resetPending()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (k *KafkaQueue) resetPending() {
	k.pendingMu.Lock()
	defer k.pendingMu.Unlock()
	k.pendingMessages = make(map[string]*pendingMessage)
	k.pendingByPartition = make(map[int]map[int64]*pendingMessage)
}

// Stats returns queue stats for the consumer group.
func (k *KafkaQueue) Stats(ctx context.Context, group string) (backlog int64, pending int64, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	k.mu.RLock()
	reader := k.reader
	k.mu.RUnlock()

	if reader != nil {
		backlog = reader.Stats().Lag
		if backlog < 0 {
			backlog = 0
		}
	}

	k.pendingMu.Lock()
	pending = int64(len(k.pendingMessages))
	k.pendingMu.Unlock()

	return backlog, pending, nil
}

func (k *KafkaQueue) messageID(msg kafka.Message) string {
	for _, header := range msg.Headers {
		if header.Key != correlationHeaderKey {
			continue
		}
		if correlation := strings.TrimSpace(string(header.Value)); correlation != "" {
			return correlation
		}
	}
	return fmt.Sprintf("%d:%d", msg.Partition, msg.Offset)
}

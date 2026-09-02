package clients

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"k8s.io/klog/v2"
)

var (
	kafkaMu     sync.Mutex
	kafkaDialer *kafka.Dialer
	kafkaConns  map[string]*kafka.Conn // broker address -> connection

	kafkaProbeTimeout = 5 * time.Second
	kafkaDialContext  = func(d *kafka.Dialer, ctx context.Context, network, address string) (*kafka.Conn, error) {
		return d.DialContext(ctx, network, address)
	}
	kafkaReadPartitions = func(conn *kafka.Conn, topic string) ([]kafka.Partition, error) {
		return conn.ReadPartitions(topic)
	}
	kafkaReadController = func(conn *kafka.Conn) (kafka.Broker, error) {
		return conn.Controller()
	}
	kafkaCreateTopics = func(conn *kafka.Conn, topics ...kafka.TopicConfig) error {
		return conn.CreateTopics(topics...)
	}
	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReadCloser {
		return kafka.NewReader(cfg)
	}
	kafkaProbeWriterFactory = func(cfg kafka.WriterConfig) kafkaProbeWriteCloser {
		return newKafkaProbeWriter(cfg)
	}
)

const (
	kafkaReadinessProbeHeaderKey   = "eruun-readiness-probe"
	kafkaReadinessProbeIDHeaderKey = "eruun-readiness-probe-id"
	kafkaReadinessProbeKind        = "readiness"
)

type kafkaReadCloser interface {
	SetOffset(offset int64) error
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type kafkaProbeWriteCloser interface {
	WriteProbe(ctx context.Context, msg kafka.Message) (kafka.Message, error)
	Close() error
}

type kafkaProbeWriter struct {
	writer *kafka.Writer

	mu            sync.Mutex
	producedProbe *kafka.Message
	completionErr error
}

func newKafkaProbeWriter(cfg kafka.WriterConfig) *kafkaProbeWriter {
	writer := kafka.NewWriter(cfg)
	probeWriter := &kafkaProbeWriter{writer: writer}
	writer.Completion = probeWriter.complete
	return probeWriter
}

func (w *kafkaProbeWriter) complete(messages []kafka.Message, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		w.completionErr = err
		return
	}
	if len(messages) == 0 {
		return
	}
	msg := messages[0]
	w.producedProbe = &msg
}

func (w *kafkaProbeWriter) WriteProbe(ctx context.Context, msg kafka.Message) (kafka.Message, error) {
	w.mu.Lock()
	w.producedProbe = nil
	w.completionErr = nil
	w.mu.Unlock()

	if err := w.writer.WriteMessages(ctx, msg); err != nil {
		return kafka.Message{}, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.completionErr != nil {
		return kafka.Message{}, w.completionErr
	}
	if w.producedProbe == nil {
		return kafka.Message{}, fmt.Errorf("missing produce metadata")
	}
	return *w.producedProbe, nil
}

func (w *kafkaProbeWriter) Close() error {
	return w.writer.Close()
}

func init() {
	kafkaConns = make(map[string]*kafka.Conn)
}

// KafkaConfig holds the configuration for Kafka client initialization.
type KafkaConfig struct {
	Brokers                []string
	Topic                  string
	Topics                 []string
	TopicPartitions        int
	TopicReplicationFactor int
}

// EnsureKafka validates the Kafka brokers connectivity and returns the dialer.
// It performs a health check by connecting to one of the brokers.
// The connection is cached for reuse.
func EnsureKafka(cfg KafkaConfig) (*kafka.Dialer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers cannot be empty")
	}

	kafkaMu.Lock()
	defer kafkaMu.Unlock()

	if kafkaDialer == nil {
		dialer, err := initKafkaDialerLocked(cfg.Brokers)
		if err != nil {
			return nil, err
		}
		kafkaDialer = dialer
	}

	for _, topic := range normalizeKafkaTopics(cfg.Topic, cfg.Topics...) {
		partitions := cfg.TopicPartitions
		if partitions <= 0 {
			partitions = 1
		}
		replication := cfg.TopicReplicationFactor
		if replication <= 0 {
			replication = 1
		}
		if err := ensureKafkaTopicLocked(kafkaDialer, cfg.Brokers, topic, partitions, replication); err != nil {
			return nil, err
		}
	}

	return kafkaDialer, nil
}

func initKafkaDialerLocked(brokers []string) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	var lastErr error
	for _, broker := range brokers {
		probeCtx, cancel := context.WithTimeout(context.Background(), kafkaProbeTimeout)
		conn, err := kafkaDialContext(dialer, probeCtx, "tcp", broker)
		cancel()
		if err != nil {
			lastErr = err
			klog.V(4).Infof("failed to connect to kafka broker %s: %v", broker, err)
			continue
		}

		kafkaConns[broker] = conn
		klog.V(2).Infof("kafka dialer initialized, connected to broker: %s", broker)
		return dialer, nil
	}
	return nil, fmt.Errorf("failed to connect to any kafka broker: %w", lastErr)
}

func ensureKafkaTopicLocked(dialer *kafka.Dialer, brokers []string, topic string, partitions, replication int) error {
	exists, err := kafkaTopicExists(context.Background(), dialer, brokers, topic)
	if err != nil {
		return fmt.Errorf("check kafka topic %s: %w", topic, err)
	}
	if exists {
		return nil
	}

	if err := createKafkaTopic(context.Background(), dialer, brokers, topic, partitions, replication); err != nil {
		return fmt.Errorf("create kafka topic %s: %w", topic, err)
	}

	exists, err = kafkaTopicExists(context.Background(), dialer, brokers, topic)
	if err != nil {
		return fmt.Errorf("verify kafka topic %s after create: %w", topic, err)
	}
	if !exists {
		return fmt.Errorf("kafka topic %s still unavailable after create", topic)
	}
	klog.Infof("kafka topic ensured: %s (partitions=%d replication=%d)", topic, partitions, replication)
	return nil
}

// CloseKafkaConnections closes all cached Kafka connections.
// This should be called during graceful shutdown.
func CloseKafkaConnections() {
	kafkaMu.Lock()
	defer kafkaMu.Unlock()

	for addr, conn := range kafkaConns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil {
			klog.Warningf("failed to close kafka connection to %s: %v", addr, err)
		}
	}
	kafkaConns = make(map[string]*kafka.Conn)
	kafkaDialer = nil
}

// CheckKafkaHealth performs a broker-only Kafka health check.
func CheckKafkaHealth(ctx context.Context, brokers []string) error {
	return checkKafkaBrokerHealth(normalizeContext(ctx), newKafkaDialer(), brokers)
}

// CheckKafkaTopicHealth validates that the target topic exists and metadata is readable.
func CheckKafkaTopicHealth(ctx context.Context, brokers []string, topic string) error {
	return checkKafkaTopicHealth(normalizeContext(ctx), newKafkaDialer(), brokers, topic)
}

// CheckKafkaReadiness validates broker connectivity, topic metadata, and real topic
// produce/read behavior for the configured Kafka topics.
func CheckKafkaReadiness(ctx context.Context, cfg KafkaConfig) error {
	ctx = normalizeContext(ctx)
	dialer := newKafkaDialer()

	if err := checkKafkaBrokerHealth(ctx, dialer, cfg.Brokers); err != nil {
		return fmt.Errorf("check kafka brokers: %w", err)
	}

	for _, topic := range normalizeKafkaTopics(cfg.Topic, cfg.Topics...) {
		if err := checkKafkaTopicHealth(ctx, dialer, cfg.Brokers, topic); err != nil {
			return err
		}
		if err := smokeKafkaTopicReadiness(ctx, dialer, cfg.Brokers, topic); err != nil {
			return err
		}
	}

	return nil
}

// IsKafkaReadinessProbe returns true when the Kafka message headers mark it as
// an internal readiness probe.
func IsKafkaReadinessProbe(headers []kafka.Header) bool {
	return kafkaHeaderValue(headers, kafkaReadinessProbeHeaderKey) == kafkaReadinessProbeKind
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func newKafkaDialer() *kafka.Dialer {
	return &kafka.Dialer{
		Timeout:   kafkaProbeTimeout,
		DualStack: true,
	}
}

func normalizeKafkaTopics(topic string, topics ...string) []string {
	seen := make(map[string]struct{}, len(topics)+1)
	normalized := make([]string, 0, len(topics)+1)
	for _, candidate := range append([]string{topic}, topics...) {
		name := strings.TrimSpace(candidate)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized
}

func checkKafkaBrokerHealth(ctx context.Context, dialer *kafka.Dialer, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}

	var lastErr error
	for _, broker := range brokers {
		conn, err := kafkaDialContext(dialer, ctx, "tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}

		_, err = kafkaReadController(conn)
		closeKafkaConn(conn)
		if err != nil {
			lastErr = err
			continue
		}
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("unable to connect to any kafka broker: %w", lastErr)
	}
	return fmt.Errorf("unable to connect to any kafka broker")
}

func checkKafkaTopicHealth(ctx context.Context, dialer *kafka.Dialer, brokers []string, topic string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return fmt.Errorf("kafka topic cannot be empty")
	}

	exists, err := kafkaTopicExists(ctx, dialer, brokers, topic)
	if err != nil {
		return fmt.Errorf("check kafka topic %s metadata: %w", topic, err)
	}
	if !exists {
		return fmt.Errorf("kafka topic %s not found", topic)
	}
	return nil
}

func smokeKafkaTopicReadiness(ctx context.Context, dialer *kafka.Dialer, brokers []string, topic string) error {
	probeID := uuid.NewString()

	writer := kafkaProbeWriterFactory(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: int(kafka.RequireOne),
		Dialer:       dialer,
	})
	defer func() {
		if err := writer.Close(); err != nil {
			klog.V(4).Infof("close kafka readiness writer failed: %v", err)
		}
	}()

	probeMsg := kafka.Message{
		Key:   []byte(probeID),
		Value: []byte(probeID),
		Headers: []kafka.Header{
			{Key: kafkaReadinessProbeHeaderKey, Value: []byte(kafkaReadinessProbeKind)},
			{Key: kafkaReadinessProbeIDHeaderKey, Value: []byte(probeID)},
		},
	}
	producedProbe, err := writer.WriteProbe(ctx, probeMsg)
	if err != nil {
		return fmt.Errorf("produce readiness probe: %w", err)
	}
	if kafkaHeaderValue(producedProbe.Headers, kafkaReadinessProbeIDHeaderKey) != probeID || !IsKafkaReadinessProbe(producedProbe.Headers) {
		return fmt.Errorf("produce readiness probe: missing produce metadata")
	}

	reader := kafkaReaderFactory(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: producedProbe.Partition,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   250 * time.Millisecond,
		Dialer:    dialer,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			klog.V(4).Infof("close kafka readiness reader failed: %v", err)
		}
	}()
	if err := reader.SetOffset(producedProbe.Offset); err != nil {
		return fmt.Errorf("prepare readiness reader offset: %w", err)
	}

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("consume readiness probe: %w", err)
		}
		if kafkaHeaderValue(msg.Headers, kafkaReadinessProbeIDHeaderKey) == probeID && IsKafkaReadinessProbe(msg.Headers) {
			return nil
		}
	}
}

func kafkaHeaderValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if header.Key != key {
			continue
		}
		return strings.TrimSpace(string(header.Value))
	}
	return ""
}

func kafkaTopicExists(ctx context.Context, dialer *kafka.Dialer, brokers []string, topic string) (bool, error) {
	var (
		lastErr      error
		topicMissing bool
	)
	for _, broker := range brokers {
		conn, err := kafkaDialContext(dialer, ctx, "tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}

		partitions, readErr := kafkaReadPartitions(conn, topic)
		closeKafkaConn(conn)
		if readErr != nil {
			if isUnknownTopicOrPartitionErr(readErr) {
				topicMissing = true
				continue
			}
			lastErr = readErr
			continue
		}
		if hasTopicPartitions(partitions, topic) {
			return true, nil
		}
	}

	if topicMissing {
		return false, nil
	}
	if lastErr != nil {
		return false, lastErr
	}
	return false, nil
}

func isUnknownTopicOrPartitionErr(err error) bool {
	return errors.Is(err, kafka.UnknownTopicOrPartition)
}

func hasTopicPartitions(partitions []kafka.Partition, topic string) bool {
	for _, partition := range partitions {
		if partition.Topic == topic {
			return true
		}
	}
	return false
}

func createKafkaTopic(ctx context.Context, dialer *kafka.Dialer, brokers []string, topic string, partitions, replication int) error {
	var lastErr error
	for _, broker := range brokers {
		conn, err := kafkaDialContext(dialer, ctx, "tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}

		controller, controllerErr := kafkaReadController(conn)
		closeKafkaConn(conn)
		if controllerErr != nil {
			lastErr = controllerErr
			continue
		}

		controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
		controllerConn, dialErr := kafkaDialContext(dialer, ctx, "tcp", controllerAddr)
		if dialErr != nil {
			lastErr = dialErr
			continue
		}
		createErr := kafkaCreateTopics(controllerConn, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replication,
		})
		closeKafkaConn(controllerConn)
		if createErr != nil {
			if isTopicAlreadyExistsErr(createErr) {
				return nil
			}
			lastErr = createErr
			continue
		}
		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("unable to connect to any kafka broker")
}

func isTopicAlreadyExistsErr(err error) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(err.Error())), "already exists")
}

func closeKafkaConn(conn *kafka.Conn) {
	if conn == nil {
		return
	}
	if err := conn.Close(); err != nil {
		klog.V(4).Infof("close kafka connection failed: %v", err)
	}
}

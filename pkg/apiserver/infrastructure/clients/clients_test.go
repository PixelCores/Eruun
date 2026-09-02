package clients

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

type kafkaFetchResult struct {
	msg kafka.Message
	err error
}

type mockKafkaReadCloser struct {
	mu           sync.Mutex
	fetchQueue   []kafkaFetchResult
	setOffsetErr error
	setOffsets   []int64
	commitErr    error
	commitCalls  []kafka.Message
	closeCalls   int
}

func (m *mockKafkaReadCloser) SetOffset(offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setOffsets = append(m.setOffsets, offset)
	return m.setOffsetErr
}

func (m *mockKafkaReadCloser) FetchMessage(ctx context.Context) (kafka.Message, error) {
	m.mu.Lock()
	if len(m.fetchQueue) == 0 {
		m.mu.Unlock()
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	next := m.fetchQueue[0]
	m.fetchQueue = m.fetchQueue[1:]
	m.mu.Unlock()
	return next.msg, next.err
}

func (m *mockKafkaReadCloser) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitCalls = append(m.commitCalls, msgs...)
	return m.commitErr
}

func (m *mockKafkaReadCloser) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	return nil
}

type mockKafkaProbeWriteCloser struct {
	mu         sync.Mutex
	writeErr   error
	writeCalls []kafka.Message
	writeFn    func(msg kafka.Message) (kafka.Message, error)
	closeCalls int
}

func (m *mockKafkaProbeWriteCloser) WriteProbe(ctx context.Context, msg kafka.Message) (kafka.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCalls = append(m.writeCalls, msg)
	if m.writeFn != nil {
		return m.writeFn(msg)
	}
	if m.writeErr != nil {
		return kafka.Message{}, m.writeErr
	}
	return kafka.Message{}, nil
}

func (m *mockKafkaProbeWriteCloser) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	return nil
}

func resetKafkaClientStateForTest() {
	kafkaMu.Lock()
	defer kafkaMu.Unlock()
	kafkaDialer = nil
	kafkaConns = map[string]*kafka.Conn{}
}

func TestEnsureKafkaEmptyBrokers(t *testing.T) {
	CloseKafkaConnections()

	_, err := EnsureKafka(KafkaConfig{})
	require.Error(t, err)
}

func TestCheckKafkaHealthNoBrokers(t *testing.T) {
	err := CheckKafkaHealth(context.Background(), nil)
	require.Error(t, err)
}

func TestCloseKafkaConnectionsResetsDialer(t *testing.T) {
	kafkaDialer = &kafka.Dialer{}
	kafkaConns = map[string]*kafka.Conn{}

	CloseKafkaConnections()

	require.Nil(t, kafkaDialer)
	require.Empty(t, kafkaConns)
}

func TestEnsureKafkaTriesNextBrokerAfterFailure(t *testing.T) {
	resetKafkaClientStateForTest()

	oldDialFn := kafkaDialContext
	oldProbeTimeout := kafkaProbeTimeout
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaProbeTimeout = oldProbeTimeout
		resetKafkaClientStateForTest()
	})

	var calls []string
	kafkaProbeTimeout = 20 * time.Millisecond
	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, address string) (*kafka.Conn, error) {
		calls = append(calls, address)
		if len(calls) == 1 {
			return nil, errors.New("first broker unavailable")
		}
		return &kafka.Conn{}, nil
	}

	dialer, err := EnsureKafka(KafkaConfig{
		Brokers: []string{"broker-1:9092", "broker-2:9092"},
	})
	require.NoError(t, err)
	require.NotNil(t, dialer)
	require.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, calls)
}

func TestEnsureKafkaUsesIndependentProbeContextPerBroker(t *testing.T) {
	resetKafkaClientStateForTest()

	oldDialFn := kafkaDialContext
	oldProbeTimeout := kafkaProbeTimeout
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaProbeTimeout = oldProbeTimeout
		resetKafkaClientStateForTest()
	})

	kafkaProbeTimeout = 20 * time.Millisecond
	callCount := 0
	secondCallStartedWithCanceledCtx := false
	kafkaDialContext = func(_ *kafka.Dialer, ctx context.Context, _, _ string) (*kafka.Conn, error) {
		callCount++
		if callCount == 1 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		secondCallStartedWithCanceledCtx = ctx.Err() != nil
		return nil, errors.New("second broker unavailable")
	}

	_, err := EnsureKafka(KafkaConfig{
		Brokers: []string{"broker-1:9092", "broker-2:9092"},
	})
	require.Error(t, err)
	require.Equal(t, 2, callCount)
	require.False(t, secondCallStartedWithCanceledCtx)
}

func TestEnsureKafkaTopicExistsSkipsCreate(t *testing.T) {
	resetKafkaClientStateForTest()

	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	oldReadController := kafkaReadController
	oldCreateTopics := kafkaCreateTopics
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
		kafkaReadController = oldReadController
		kafkaCreateTopics = oldCreateTopics
		resetKafkaClientStateForTest()
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	kafkaReadPartitions = func(_ *kafka.Conn, topic string) ([]kafka.Partition, error) {
		return []kafka.Partition{{Topic: topic, ID: 0}}, nil
	}
	createCalled := false
	kafkaReadController = func(_ *kafka.Conn) (kafka.Broker, error) {
		return kafka.Broker{Host: "controller", Port: 9093}, nil
	}
	kafkaCreateTopics = func(_ *kafka.Conn, _ ...kafka.TopicConfig) error {
		createCalled = true
		return nil
	}

	_, err := EnsureKafka(KafkaConfig{
		Brokers:                []string{"broker-1:9092"},
		Topic:                  "eruun.workflow.dispatch",
		TopicPartitions:        3,
		TopicReplicationFactor: 2,
	})
	require.NoError(t, err)
	require.False(t, createCalled)
}

func TestEnsureKafkaTopicUnknownMetadataCreatesTopic(t *testing.T) {
	resetKafkaClientStateForTest()

	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	oldReadController := kafkaReadController
	oldCreateTopics := kafkaCreateTopics
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
		kafkaReadController = oldReadController
		kafkaCreateTopics = oldCreateTopics
		resetKafkaClientStateForTest()
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	readCalls := 0
	kafkaReadPartitions = func(_ *kafka.Conn, topic string) ([]kafka.Partition, error) {
		readCalls++
		if readCalls == 1 {
			return nil, kafka.UnknownTopicOrPartition
		}
		return []kafka.Partition{{Topic: topic, ID: 0}}, nil
	}
	kafkaReadController = func(_ *kafka.Conn) (kafka.Broker, error) {
		return kafka.Broker{Host: "controller", Port: 9093}, nil
	}
	var created []kafka.TopicConfig
	kafkaCreateTopics = func(_ *kafka.Conn, topics ...kafka.TopicConfig) error {
		created = append(created, topics...)
		return nil
	}

	_, err := EnsureKafka(KafkaConfig{
		Brokers:                []string{"broker-1:9092"},
		Topic:                  "eruun.workflow.dispatch",
		TopicPartitions:        3,
		TopicReplicationFactor: 2,
	})
	require.NoError(t, err)
	require.Len(t, created, 1)
	require.Equal(t, "eruun.workflow.dispatch", created[0].Topic)
	require.Equal(t, 3, created[0].NumPartitions)
	require.Equal(t, 2, created[0].ReplicationFactor)
}

func TestEnsureKafkaTopicCreateFailure(t *testing.T) {
	resetKafkaClientStateForTest()

	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	oldReadController := kafkaReadController
	oldCreateTopics := kafkaCreateTopics
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
		kafkaReadController = oldReadController
		kafkaCreateTopics = oldCreateTopics
		resetKafkaClientStateForTest()
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	kafkaReadPartitions = func(_ *kafka.Conn, _ string) ([]kafka.Partition, error) {
		return nil, nil
	}
	kafkaReadController = func(_ *kafka.Conn) (kafka.Broker, error) {
		return kafka.Broker{Host: "controller", Port: 9093}, nil
	}
	kafkaCreateTopics = func(_ *kafka.Conn, _ ...kafka.TopicConfig) error {
		return errors.New("create denied")
	}

	_, err := EnsureKafka(KafkaConfig{
		Brokers:                []string{"broker-1:9092"},
		Topic:                  "eruun.workflow.dispatch",
		TopicPartitions:        3,
		TopicReplicationFactor: 2,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "create kafka topic")
}

func TestEnsureKafkaEnsuresAllConfiguredTopics(t *testing.T) {
	resetKafkaClientStateForTest()

	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	oldReadController := kafkaReadController
	oldCreateTopics := kafkaCreateTopics
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
		kafkaReadController = oldReadController
		kafkaCreateTopics = oldCreateTopics
		resetKafkaClientStateForTest()
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	var topicChecks []string
	kafkaReadPartitions = func(_ *kafka.Conn, topic string) ([]kafka.Partition, error) {
		topicChecks = append(topicChecks, topic)
		return []kafka.Partition{{Topic: topic, ID: 0}}, nil
	}
	kafkaReadController = func(_ *kafka.Conn) (kafka.Broker, error) {
		return kafka.Broker{Host: "controller", Port: 9093}, nil
	}
	kafkaCreateTopics = func(_ *kafka.Conn, _ ...kafka.TopicConfig) error { return nil }

	_, err := EnsureKafka(KafkaConfig{
		Brokers: []string{"broker-1:9092"},
		Topics: []string{
			"eruun.workflow.dispatch",
			"eruun.job.delay",
			"eruun.job.result",
		},
		TopicPartitions:        3,
		TopicReplicationFactor: 2,
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"eruun.workflow.dispatch",
		"eruun.job.delay",
		"eruun.job.result",
	}, topicChecks)
}

func TestCheckKafkaTopicHealthValidation(t *testing.T) {
	require.Error(t, CheckKafkaTopicHealth(context.Background(), nil, "topic-a"))
	require.Error(t, CheckKafkaTopicHealth(context.Background(), []string{"broker-1:9092"}, ""))
}

func TestCheckKafkaTopicHealthTopicNotFound(t *testing.T) {
	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	kafkaReadPartitions = func(_ *kafka.Conn, _ string) ([]kafka.Partition, error) {
		return nil, nil
	}

	err := CheckKafkaTopicHealth(context.Background(), []string{"broker-1:9092"}, "topic-a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestCheckKafkaTopicHealthUnknownTopicMetadataReturnsNotFound(t *testing.T) {
	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	kafkaReadPartitions = func(_ *kafka.Conn, _ string) ([]kafka.Partition, error) {
		return nil, kafka.UnknownTopicOrPartition
	}

	err := CheckKafkaTopicHealth(context.Background(), []string{"broker-1:9092"}, "topic-a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestCheckKafkaReadinessSmokeSuccess(t *testing.T) {
	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	oldReadController := kafkaReadController
	oldReaderFactory := kafkaReaderFactory
	oldWriterFactory := kafkaProbeWriterFactory
	var readerCfgs []kafka.ReaderConfig
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
		kafkaReadController = oldReadController
		kafkaReaderFactory = oldReaderFactory
		kafkaProbeWriterFactory = oldWriterFactory
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	kafkaReadPartitions = func(_ *kafka.Conn, topic string) ([]kafka.Partition, error) {
		return []kafka.Partition{{Topic: topic, ID: 0}}, nil
	}
	kafkaReadController = func(_ *kafka.Conn) (kafka.Broker, error) {
		return kafka.Broker{Host: "controller", Port: 9093}, nil
	}

	reader := &mockKafkaReadCloser{
		fetchQueue: []kafkaFetchResult{},
	}
	writer := &mockKafkaProbeWriteCloser{
		writeFn: func(msg kafka.Message) (kafka.Message, error) {
			produced := msg
			produced.Partition = 1
			produced.Offset = 42
			reader.mu.Lock()
			reader.fetchQueue = append(reader.fetchQueue, kafkaFetchResult{msg: produced})
			reader.mu.Unlock()
			return produced, nil
		},
	}
	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReadCloser {
		readerCfgs = append(readerCfgs, cfg)
		return reader
	}
	kafkaProbeWriterFactory = func(cfg kafka.WriterConfig) kafkaProbeWriteCloser {
		return writer
	}

	err := CheckKafkaReadiness(context.Background(), KafkaConfig{
		Brokers: []string{"broker-1:9092"},
		Topics:  []string{"eruun.workflow.dispatch"},
	})
	require.NoError(t, err)
	require.Len(t, writer.writeCalls, 1)
	require.True(t, IsKafkaReadinessProbe(writer.writeCalls[0].Headers))
	require.Empty(t, reader.commitCalls)
	require.Len(t, readerCfgs, 1)
	require.Empty(t, readerCfgs[0].GroupID)
	require.Equal(t, 1, readerCfgs[0].Partition)
	require.Equal(t, []int64{42}, reader.setOffsets)
}

func TestCheckKafkaReadinessSmokeFailsWithoutProduceMetadata(t *testing.T) {
	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	oldReadController := kafkaReadController
	oldReaderFactory := kafkaReaderFactory
	oldWriterFactory := kafkaProbeWriterFactory
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
		kafkaReadController = oldReadController
		kafkaReaderFactory = oldReaderFactory
		kafkaProbeWriterFactory = oldWriterFactory
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	kafkaReadPartitions = func(_ *kafka.Conn, topic string) ([]kafka.Partition, error) {
		return []kafka.Partition{{Topic: topic, ID: 0}}, nil
	}
	kafkaReadController = func(_ *kafka.Conn) (kafka.Broker, error) {
		return kafka.Broker{Host: "controller", Port: 9093}, nil
	}

	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReadCloser {
		return &mockKafkaReadCloser{}
	}
	kafkaProbeWriterFactory = func(cfg kafka.WriterConfig) kafkaProbeWriteCloser {
		return &mockKafkaProbeWriteCloser{}
	}

	err := CheckKafkaReadiness(context.Background(), KafkaConfig{
		Brokers: []string{"broker-1:9092"},
		Topics:  []string{"eruun.workflow.dispatch"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing produce metadata")
}

func TestCheckKafkaReadinessSmokeProduceFailure(t *testing.T) {
	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	oldReadController := kafkaReadController
	oldReaderFactory := kafkaReaderFactory
	oldWriterFactory := kafkaProbeWriterFactory
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
		kafkaReadController = oldReadController
		kafkaReaderFactory = oldReaderFactory
		kafkaProbeWriterFactory = oldWriterFactory
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	kafkaReadPartitions = func(_ *kafka.Conn, topic string) ([]kafka.Partition, error) {
		return []kafka.Partition{{Topic: topic, ID: 0}}, nil
	}
	kafkaReadController = func(_ *kafka.Conn) (kafka.Broker, error) {
		return kafka.Broker{Host: "controller", Port: 9093}, nil
	}

	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReadCloser {
		return &mockKafkaReadCloser{fetchQueue: []kafkaFetchResult{{err: context.DeadlineExceeded}}}
	}
	kafkaProbeWriterFactory = func(cfg kafka.WriterConfig) kafkaProbeWriteCloser {
		return &mockKafkaProbeWriteCloser{writeErr: errors.New("produce denied")}
	}

	err := CheckKafkaReadiness(context.Background(), KafkaConfig{
		Brokers: []string{"broker-1:9092"},
		Topics:  []string{"eruun.workflow.dispatch"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "produce readiness probe")
}

func TestCheckKafkaReadinessSmokeConsumeFailure(t *testing.T) {
	oldDialFn := kafkaDialContext
	oldReadPartitions := kafkaReadPartitions
	oldReadController := kafkaReadController
	oldReaderFactory := kafkaReaderFactory
	oldWriterFactory := kafkaProbeWriterFactory
	t.Cleanup(func() {
		kafkaDialContext = oldDialFn
		kafkaReadPartitions = oldReadPartitions
		kafkaReadController = oldReadController
		kafkaReaderFactory = oldReaderFactory
		kafkaProbeWriterFactory = oldWriterFactory
	})

	kafkaDialContext = func(_ *kafka.Dialer, _ context.Context, _, _ string) (*kafka.Conn, error) {
		return nil, nil
	}
	kafkaReadPartitions = func(_ *kafka.Conn, topic string) ([]kafka.Partition, error) {
		return []kafka.Partition{{Topic: topic, ID: 0}}, nil
	}
	kafkaReadController = func(_ *kafka.Conn) (kafka.Broker, error) {
		return kafka.Broker{Host: "controller", Port: 9093}, nil
	}

	kafkaReaderFactory = func(cfg kafka.ReaderConfig) kafkaReadCloser {
		return &mockKafkaReadCloser{
			fetchQueue: []kafkaFetchResult{
				{err: context.DeadlineExceeded},
				{err: errors.New("read denied")},
			},
		}
	}
	kafkaProbeWriterFactory = func(cfg kafka.WriterConfig) kafkaProbeWriteCloser {
		return &mockKafkaProbeWriteCloser{
			writeFn: func(msg kafka.Message) (kafka.Message, error) {
				msg.Partition = 0
				msg.Offset = 10
				return msg, nil
			},
		}
	}

	err := CheckKafkaReadiness(context.Background(), KafkaConfig{
		Brokers: []string{"broker-1:9092"},
		Topics:  []string{"eruun.workflow.dispatch"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "consume readiness probe")
}

func TestGetKubeConfigRequiresSet(t *testing.T) {
	kubeConfig = nil
	_, err := GetKubeConfig()
	require.Error(t, err)
}

func TestGetKubeConfigReturnsConfiguredValue(t *testing.T) {
	cfg := &rest.Config{Host: "https://example.invalid"}
	kubeConfig = cfg

	got, err := GetKubeConfig()
	require.NoError(t, err)
	require.Equal(t, cfg, got)
}

func TestGetKubeClientReturnsInjectedClient(t *testing.T) {
	kubeClient = nil
	defer func() { kubeClient = nil }()

	client := fake.NewSimpleClientset()
	SetKubeClient(client)

	got, err := GetKubeClient()
	require.NoError(t, err)
	require.Equal(t, client, got)
}

func TestNewRedisClientPingFailure(t *testing.T) {
	_, err := NewRedisClient(config.RedisCacheConfig{
		CacheHost: "127.0.0.1",
		CacheProt: 0,
		CacheDB:   0,
	})
	require.Error(t, err)
}

//go:build integration
// +build integration

package messaging

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestKafkaIntegrationOutOfOrderAckDoesNotSkipLowerOffsets(t *testing.T) {
	broker := integrationKafkaBroker(t)
	topic := fmt.Sprintf("eruun-it-ack-%d", time.Now().UnixNano())
	require.NoError(t, createKafkaTopic(broker, topic))

	queue, err := NewKafkaQueue(KafkaConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: "eruun-it-group",
	})
	require.NoError(t, err)
	require.NoError(t, queue.EnsureGroup(context.Background(), workflowDispatchGroup))

	for i := 0; i < 3; i++ {
		_, err = queue.Enqueue(context.Background(), []byte(fmt.Sprintf("payload-%d", i)))
		require.NoError(t, err)
	}

	msgs, err := queue.ReadGroup(context.Background(), workflowDispatchGroup, "it-consumer", 3, 4*time.Second)
	require.NoError(t, err)
	require.Len(t, msgs, 3)

	highMsg := msgs[len(msgs)-1]

	require.NoError(t, queue.Ack(context.Background(), workflowDispatchGroup, highMsg.ID))
	require.NoError(t, queue.Close(context.Background()))

	queue2, err := NewKafkaQueue(KafkaConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: "eruun-it-group",
	})
	require.NoError(t, err)
	require.NoError(t, queue2.EnsureGroup(context.Background(), workflowDispatchGroup))

	remaining, err := queue2.ReadGroup(context.Background(), workflowDispatchGroup, "it-consumer-2", 3, 4*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, remaining, "higher offset ack must not cause lower offsets to be skipped")

	ids := make([]string, 0, len(remaining))
	for _, msg := range remaining {
		ids = append(ids, msg.ID)
	}
	require.NoError(t, queue2.Ack(context.Background(), workflowDispatchGroup, ids...))
	require.NoError(t, queue2.Close(context.Background()))
}

func TestKafkaIntegrationAutoClaimReturnsStalePending(t *testing.T) {
	broker := integrationKafkaBroker(t)
	topic := fmt.Sprintf("eruun-it-claim-%d", time.Now().UnixNano())
	require.NoError(t, createKafkaTopic(broker, topic))

	queue, err := NewKafkaQueue(KafkaConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: "eruun-it-group",
	})
	require.NoError(t, err)
	require.NoError(t, queue.EnsureGroup(context.Background(), workflowDispatchGroup))

	_, err = queue.Enqueue(context.Background(), []byte("claim-me"))
	require.NoError(t, err)

	msgs, err := queue.ReadGroup(context.Background(), workflowDispatchGroup, "it-consumer", 1, 4*time.Second)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	queue.MarkMessageHandlingDone(msgs[0].ID, false)

	claimedNow, err := queue.AutoClaim(context.Background(), workflowDispatchGroup, "it-consumer", 500*time.Millisecond, 1)
	require.NoError(t, err)
	require.Empty(t, claimedNow)

	time.Sleep(700 * time.Millisecond)
	claimedLater, err := queue.AutoClaim(context.Background(), workflowDispatchGroup, "it-consumer", 500*time.Millisecond, 1)
	require.NoError(t, err)
	require.Len(t, claimedLater, 1)
	require.Equal(t, msgs[0].ID, claimedLater[0].ID)

	require.NoError(t, queue.Ack(context.Background(), workflowDispatchGroup, claimedLater[0].ID))
	require.NoError(t, queue.Close(context.Background()))
}

func integrationKafkaBroker(t *testing.T) string {
	t.Helper()
	broker, _ := os.LookupEnv("KAFKA_BROKER")
	broker = strings.TrimSpace(broker)
	if broker == "" {
		t.Skip("KAFKA_BROKER not set; skip Kafka integration tests")
	}
	return broker
}

func createKafkaTopic(broker, topic string) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return nil
	}
	return err
}

//go:build integration

package messaging

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisStreamsIntegrationRecreatesMissingGroupFromBacklog(t *testing.T) {
	client := integrationRedisClient(t)
	defer client.Close()

	ctx := context.Background()
	key := fmt.Sprintf("eruun-it-group-recovery-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })
	queue, err := NewRedisStreamsWithClient(client, key, 0)
	require.NoError(t, err)

	want := map[string]bool{"before-group-1": true, "before-group-2": true}
	for payload := range want {
		_, err = queue.Enqueue(ctx, []byte(payload))
		require.NoError(t, err)
	}

	messages, err := queue.ReadGroup(ctx, "workers", "worker-1", 10, 100*time.Millisecond)
	require.NoError(t, err)
	require.ElementsMatch(t, mapKeys(want), messagePayloads(messages))
	require.NoError(t, queue.Ack(ctx, "workers", messageIDs(messages)...))

	require.EqualValues(t, 1, client.XGroupDestroy(ctx, key, "workers").Val())
	_, err = queue.Enqueue(ctx, []byte("after-group-loss"))
	require.NoError(t, err)

	messages, err = queue.ReadGroup(ctx, "workers", "worker-2", 10, 100*time.Millisecond)
	require.NoError(t, err)
	require.Contains(t, messagePayloads(messages), "after-group-loss")
	require.NoError(t, queue.Ack(ctx, "workers", messageIDs(messages)...))
}

func TestRedisStreamsIntegrationAutoClaimAdvancesCursor(t *testing.T) {
	client := integrationRedisClient(t)
	defer client.Close()

	ctx := context.Background()
	key := fmt.Sprintf("eruun-it-claim-cursor-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })
	queue, err := NewRedisStreamsWithClient(client, key, 0)
	require.NoError(t, err)
	require.NoError(t, queue.EnsureGroup(ctx, "workers"))

	for i := 0; i < 5; i++ {
		_, err = queue.Enqueue(ctx, []byte(fmt.Sprintf("payload-%d", i)))
		require.NoError(t, err)
	}
	pending, err := queue.ReadGroup(ctx, "workers", "stale-worker", 5, 100*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, pending, 5)
	time.Sleep(20 * time.Millisecond)

	claimedIDs := make(map[string]struct{}, len(pending))
	for i := 0; i < 3; i++ {
		claimed, claimErr := queue.AutoClaim(ctx, "workers", "replacement-worker", 10*time.Millisecond, 2)
		require.NoError(t, claimErr)
		for _, message := range claimed {
			_, duplicate := claimedIDs[message.ID]
			require.False(t, duplicate, "claim cursor returned the same pending record twice")
			claimedIDs[message.ID] = struct{}{}
		}
	}
	require.Len(t, claimedIDs, len(pending))

	ids := make([]string, 0, len(claimedIDs))
	for id := range claimedIDs {
		ids = append(ids, id)
	}
	require.NoError(t, queue.Ack(ctx, "workers", ids...))
}

func integrationRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := strings.TrimSpace(os.Getenv("REDIS_TEST_ADDR"))
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR not set; requires an isolated Redis instance")
	}
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_TEST_PASSWORD"),
	})
	require.NoError(t, client.Ping(context.Background()).Err())
	return client
}

func messageIDs(messages []Message) []string {
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.ID)
	}
	return ids
}

func messagePayloads(messages []Message) []string {
	payloads := make([]string, 0, len(messages))
	for _, message := range messages {
		payloads = append(payloads, string(message.Payload))
	}
	sort.Strings(payloads)
	return payloads
}

func mapKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

package cancelsignal

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

func RedisClientForCancelSignal(ctx context.Context, cacheStore cache.ICache) (*redis.Client, error) {
	if cacheStore == nil {
		return nil, bcode.ErrWorkflowCancelSignalUnavailable
	}
	redisClient := cacheStore.GetRedisClient()
	if redisClient == nil {
		return nil, bcode.ErrWorkflowCancelSignalUnavailable
	}
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", bcode.ErrWorkflowCancelSignalUnavailable, err)
	}
	return redisClient, nil
}

func PublishWorkflowCancelSignal(ctx context.Context, taskID, reason string, redisClient *redis.Client) error {
	if err := signal.CancelWithClient(ctx, taskID, reason, redisClient); err != nil {
		return fmt.Errorf("publish workflow cancel signal: %w", err)
	}
	return nil
}

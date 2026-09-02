package clients

import (
	"context"
	"fmt"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/redis/go-redis/v9"
)

// NewRedisClient builds a redis client from cfg and verifies connectivity.
func NewRedisClient(cfg config.RedisCacheConfig) (*redis.Client, error) {
	addr := fmt.Sprintf("%s:%d", cfg.CacheHost, cfg.CacheProt)
	cli := redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: cfg.UserName,
		Password: cfg.Password,
		DB:       int(cfg.CacheDB),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		_ = cli.Close()
		return nil, err
	}
	return cli, nil
}

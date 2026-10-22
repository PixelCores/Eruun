package clients

import (
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestNewRedisClientPingFailure(t *testing.T) {
	_, err := NewRedisClient(config.RedisCacheConfig{
		CacheHost: "127.0.0.1",
		CacheProt: 0,
		CacheDB:   0,
	})
	require.Error(t, err)
}

func TestNewRedisClientPreservesTimeoutAndRetryDefaults(t *testing.T) {
	server := miniredis.RunT(t)
	port, err := strconv.Atoi(server.Port())
	require.NoError(t, err)

	client, err := NewRedisClient(config.RedisCacheConfig{
		CacheHost: server.Host(),
		CacheProt: port,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	options := client.Options()
	require.True(t, options.ContextTimeoutEnabled)
	require.Equal(t, 3*time.Second, options.ReadTimeout)
	require.Equal(t, 3*time.Second, options.WriteTimeout)
	require.Equal(t, 8*time.Millisecond, options.MinRetryBackoff)
	require.Equal(t, 512*time.Millisecond, options.MaxRetryBackoff)
}

package clients

import (
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

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

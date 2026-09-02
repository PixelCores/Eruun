package config

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveWorkflowCallbackTimeoutMax(t *testing.T) {
	require.Equal(t, DefaultWorkflowCallbackTimeoutMax, ResolveWorkflowCallbackTimeoutMax(nil))

	cfg := NewConfig()
	cfg.Workflow.CallbackTimeoutMax = 12 * time.Hour
	require.Equal(t, 12*time.Hour, ResolveWorkflowCallbackTimeoutMax(cfg))
}

func TestResolveWorkflowCallbackTimeoutMaxSeconds(t *testing.T) {
	require.Equal(t, int64(0), ResolveWorkflowCallbackTimeoutMaxSeconds(0))
	require.Equal(t, int64(1), ResolveWorkflowCallbackTimeoutMaxSeconds(500*time.Millisecond))
	require.Equal(t, int64(2), ResolveWorkflowCallbackTimeoutMaxSeconds(1500*time.Millisecond))
}

func TestClampWorkflowCallbackTimeoutSeconds(t *testing.T) {
	require.Equal(t, int64(0), ClampWorkflowCallbackTimeoutSeconds(0, 72*time.Hour))
	require.Equal(t, int64(10), ClampWorkflowCallbackTimeoutSeconds(10, 72*time.Hour))
	require.Equal(t, int64(72*3600), ClampWorkflowCallbackTimeoutSeconds(72*3600+1, 72*time.Hour))
	require.Equal(t, int64(1), ClampWorkflowCallbackTimeoutSeconds(3600, 500*time.Millisecond))

	overflowInput := int64(math.MaxInt64)
	capped := ClampWorkflowCallbackTimeoutSeconds(overflowInput, 0)
	require.Equal(t, maxDurationSeconds, capped)
}

func TestResolveWorkflowCallbackTimeout(t *testing.T) {
	require.Equal(t, DefaultWorkflowCallbackTimeout, ResolveWorkflowCallbackTimeout(0, 72*time.Hour))
	require.Equal(t, 30*time.Second, ResolveWorkflowCallbackTimeout(30, 72*time.Hour))
	require.Equal(t, 72*time.Hour, ResolveWorkflowCallbackTimeout(72*3600+30, 72*time.Hour))
	require.Equal(t, 500*time.Millisecond, ResolveWorkflowCallbackTimeout(3600, 500*time.Millisecond))

	// When max is lower than default timeout, default path should still respect max.
	require.Equal(t, 5*time.Second, ResolveWorkflowCallbackTimeout(0, 5*time.Second))
}

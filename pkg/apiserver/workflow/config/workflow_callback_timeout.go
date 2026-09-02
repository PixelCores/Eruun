package config

import (
	"math"
	"time"
)

const (
	DefaultWorkflowCallbackTimeout    = 10 * time.Second // 回调默认超时时间
	DefaultWorkflowCallbackTimeoutMax = 72 * time.Hour   // 回调超时上限（默认 3 天）
)

const maxDurationSeconds = math.MaxInt64 / int64(time.Second)

// ResolveWorkflowCallbackTimeoutMax returns the effective callback timeout upper bound.
func ResolveWorkflowCallbackTimeoutMax(cfg RuntimeConfig) time.Duration {
	if cfg.CallbackTimeoutMax > 0 {
		return cfg.CallbackTimeoutMax
	}
	return DefaultWorkflowCallbackTimeoutMax
}

// ResolveWorkflowCallbackTimeoutMaxSeconds serializes max timeout with second granularity.
// Positive values below 1s are rounded up to 1s so caps are never disabled accidentally.
func ResolveWorkflowCallbackTimeoutMaxSeconds(max time.Duration) int64 {
	if max <= 0 {
		return 0
	}
	if max < time.Second {
		return 1
	}
	maxSeconds := int64(max / time.Second)
	if max%time.Second != 0 && maxSeconds < maxDurationSeconds {
		maxSeconds++
	}
	return maxSeconds
}

// ClampWorkflowCallbackTimeoutSeconds caps callback timeout seconds to avoid overflow
// and enforce the configured upper bound.
func ClampWorkflowCallbackTimeoutSeconds(seconds int64, max time.Duration) int64 {
	if seconds <= 0 {
		return 0
	}
	if seconds > maxDurationSeconds {
		seconds = maxDurationSeconds
	}
	if max > 0 {
		maxSeconds := ResolveWorkflowCallbackTimeoutMaxSeconds(max)
		if maxSeconds > 0 && seconds > maxSeconds {
			return maxSeconds
		}
	}
	return seconds
}

// ResolveWorkflowCallbackTimeout converts timeout seconds to a duration with default
// and max-capping behavior applied.
func ResolveWorkflowCallbackTimeout(seconds int64, max time.Duration) time.Duration {
	if seconds > 0 {
		cappedSeconds := ClampWorkflowCallbackTimeoutSeconds(seconds, max)
		timeout := time.Duration(cappedSeconds) * time.Second
		if max > 0 && timeout > max {
			return max
		}
		return timeout
	}
	timeout := DefaultWorkflowCallbackTimeout
	if max > 0 && timeout > max {
		return max
	}
	return timeout
}

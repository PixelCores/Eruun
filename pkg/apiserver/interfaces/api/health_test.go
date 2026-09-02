package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

type mockHealthQueue struct {
	statsError error
}

type mockRuntimeReadiness struct {
	ready  bool
	reason string
}

func (m mockRuntimeReadiness) RuntimeReady() (bool, string) {
	return m.ready, m.reason
}

func (m *mockHealthQueue) EnsureGroup(ctx context.Context, group string) error { return nil }
func (m *mockHealthQueue) Enqueue(ctx context.Context, payload []byte) (string, error) {
	return "", nil
}
func (m *mockHealthQueue) ReadGroup(ctx context.Context, group, consumer string, count int, block time.Duration) ([]msg.Message, error) {
	return nil, nil
}
func (m *mockHealthQueue) Ack(ctx context.Context, group string, ids ...string) error { return nil }
func (m *mockHealthQueue) AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, count int) ([]msg.Message, error) {
	return nil, nil
}
func (m *mockHealthQueue) Close(ctx context.Context) error { return nil }
func (m *mockHealthQueue) Stats(ctx context.Context, group string) (int64, int64, error) {
	if m.statsError != nil {
		return 0, 0, m.statsError
	}
	return 10, 5, nil
}

func TestHealthCheck(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{}
	r := gin.New()
	r.GET("/health", h.healthCheck)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload map[string]string
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "healthy", payload["status"])
}

func TestHealthzCheck(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{}
	r := gin.New()
	r.GET("/healthz", h.healthCheck)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload map[string]string
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "healthy", payload["status"])
}

func TestReadinessCheckWithHealthyQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{
		Queues: &msg.RuntimeQueues{Dispatch: &mockHealthQueue{}},
		Cfg: &config.Config{
			Role:      config.RuntimeRoleScheduler,
			Messaging: config.MessagingConfig{Type: "redis"},
		},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload map[string]string
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "ready", payload["status"])
}

func TestReadinessCheckReportsRuntimeRole(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{
		Queues: &msg.RuntimeQueues{
			Dispatch: &mockHealthQueue{},
			Delay:    &mockHealthQueue{},
		},
		Runtime: mockRuntimeReadiness{ready: true},
		Cfg:     &config.Config{Role: config.RuntimeRoleWorker},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload map[string]string
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "worker", payload["role"])
}

func TestReadinessCheckRejectsInitializingRuntimeLeader(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{
		Runtime: mockRuntimeReadiness{reason: "scheduler leader is still initializing"},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Contains(t, envelope.Message, "scheduler leader is still initializing")
}

func TestReadinessCheckWithUnhealthyQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{
		Queues: &msg.RuntimeQueues{Dispatch: &mockHealthQueue{statsError: errors.New("connection refused")}},
		Cfg: &config.Config{
			Role:      config.RuntimeRoleScheduler,
			Messaging: config.MessagingConfig{Type: "redis"},
		},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
	require.Contains(t, envelope.Message, "not ready")
	require.Contains(t, envelope.Message, "queue connection failed")
	require.Equal(t, "null", string(envelope.Data))
}

func TestReadinessCheckWithNilQueueWithoutExternalQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload map[string]string
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "ready", payload["status"])
}

func TestReadinessCheckAPIRedisOutageAndRecovery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldCheck := checkKafkaReadiness
	checkKafkaReadiness = func(context.Context, clients.KafkaConfig) error {
		return errors.New("API readiness must not probe Kafka")
	}
	t.Cleanup(func() { checkKafkaReadiness = oldCheck })

	for _, backend := range []string{config.REDIS, config.KAFKA} {
		t.Run(backend, func(t *testing.T) {
			redisServer := miniredis.RunT(t)
			redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr(), MaxRetries: -1})
			t.Cleanup(func() { require.NoError(t, redisClient.Close()) })

			h := &health{
				Cache:   cache.NewRedisICacheWithClient(redisClient, false),
				Runtime: mockRuntimeReadiness{ready: true},
				Cfg: &config.Config{
					Role:      config.RuntimeRoleAPI,
					Messaging: config.MessagingConfig{Type: backend},
				},
			}
			r := gin.New()
			h.RegisterRoutes(r.Group("/api/v1"))
			checkReadiness := func(wantStatus int) {
				t.Helper()
				for _, path := range []string{"/api/v1/ready", "/api/v1/readyz"} {
					resp := httptest.NewRecorder()
					r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, path, nil))
					require.Equal(t, wantStatus, resp.Code, path)
					if wantStatus == http.StatusOK {
						var payload map[string]string
						requireSuccessResponse(t, resp.Body.Bytes(), &payload)
						require.Equal(t, "ready", payload["status"])
						require.Equal(t, "api", payload["role"])
					} else {
						envelope := decodeResponse(t, resp.Body.Bytes(), nil)
						require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
						require.Equal(t, "not ready: redis connection failed", envelope.Message)
						require.Equal(t, "null", string(envelope.Data))
					}
				}
			}

			checkReadiness(http.StatusOK)
			redisServer.Close()
			checkReadiness(http.StatusServiceUnavailable)
			for _, path := range []string{"/api/v1/health", "/api/v1/healthz"} {
				resp := httptest.NewRecorder()
				r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, path, nil))
				require.Equal(t, http.StatusOK, resp.Code, path)
			}
			require.NoError(t, redisServer.Restart())
			checkReadiness(http.StatusOK)

			// Unused queue failures must not affect API readiness.
			failedQueue := &mockHealthQueue{statsError: errors.New("queue down")}
			h.Queues = &msg.RuntimeQueues{Dispatch: failedQueue, Delay: failedQueue, Result: failedQueue}
			checkReadiness(http.StatusOK)
		})
	}
}

func TestReadinessCheckAPIRequiresRedisClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, tc := range []struct {
		name  string
		cache cache.ICache
	}{
		{name: "missing cache"},
		{name: "missing redis client", cache: cache.NewMemCache(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &health{Cache: tc.cache, Cfg: config.NewConfig()}
			r := gin.New()
			r.GET("/ready", h.readinessCheck)
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/ready", nil))

			require.Equal(t, http.StatusServiceUnavailable, resp.Code)
			envelope := decodeResponse(t, resp.Body.Bytes(), nil)
			require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
			require.Equal(t, "not ready: redis client is not configured", envelope.Message)
		})
	}
}

func TestReadinessCheckControllerRequiresOnlyDelayAndResultQueues(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{
		Queues: &msg.RuntimeQueues{},
		Cfg: &config.Config{
			Role:      config.RuntimeRoleController,
			Messaging: config.MessagingConfig{Type: "redis"},
		},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Contains(t, envelope.Message, "delay")
	require.Contains(t, envelope.Message, "result")
	require.NotContains(t, envelope.Message, "dispatch")
}

func TestReadinessCheckWithExternalDelayQueueStatsError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldCheck := checkKafkaReadiness
	checkKafkaReadiness = func(ctx context.Context, cfg clients.KafkaConfig) error { return nil }
	t.Cleanup(func() {
		checkKafkaReadiness = oldCheck
	})

	h := &health{
		Queues: &msg.RuntimeQueues{
			Delay:  &mockHealthQueue{statsError: errors.New("delay queue down")},
			Result: &mockHealthQueue{},
		},
		Cfg: &config.Config{
			Role:      config.RuntimeRoleController,
			Messaging: config.MessagingConfig{Type: "kafka"},
		},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
	require.Contains(t, envelope.Message, "delay queue connection failed")
}

func TestReadinessCheckWithKafkaBrokerConnectivityFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldCheck := checkKafkaReadiness
	checkKafkaReadiness = func(ctx context.Context, cfg clients.KafkaConfig) error {
		return errors.New("dial failed")
	}
	t.Cleanup(func() {
		checkKafkaReadiness = oldCheck
	})

	h := &health{
		Queues: &msg.RuntimeQueues{Dispatch: &mockHealthQueue{}},
		Cfg: &config.Config{
			Role:      config.RuntimeRoleScheduler,
			Messaging: config.MessagingConfig{Type: "kafka", KafkaBrokers: []string{"127.0.0.1:1"}},
		},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
	require.Contains(t, envelope.Message, "kafka readiness failed")
}

func TestReadinessCheckWithKafkaQueueStatsFailureAfterBrokerHealthPasses(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldCheck := checkKafkaReadiness
	checkKafkaReadiness = func(ctx context.Context, cfg clients.KafkaConfig) error { return nil }
	t.Cleanup(func() {
		checkKafkaReadiness = oldCheck
	})

	h := &health{
		Queues: &msg.RuntimeQueues{Dispatch: &mockHealthQueue{statsError: errors.New("queue stats failed")}},
		Cfg: &config.Config{
			Role:      config.RuntimeRoleScheduler,
			Messaging: config.MessagingConfig{Type: "kafka", KafkaBrokers: []string{"127.0.0.1:9092"}},
		},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
	require.Contains(t, envelope.Message, "dispatch queue connection failed")
}

func TestReadinessCheckWithNilQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	// Nil queue should be considered ready (no dependency)
	require.Equal(t, http.StatusOK, resp.Code)
	var payload map[string]string
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "ready", payload["status"])
}

func TestReadinessCheckWithKafkaTopicHealthFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldCheck := checkKafkaReadiness
	checkKafkaReadiness = func(ctx context.Context, cfg clients.KafkaConfig) error {
		return errors.New("topic metadata unavailable")
	}
	t.Cleanup(func() {
		checkKafkaReadiness = oldCheck
	})

	h := &health{
		Queues: &msg.RuntimeQueues{Dispatch: &mockHealthQueue{}},
		Cfg: &config.Config{
			Role: config.RuntimeRoleScheduler,
			Messaging: config.MessagingConfig{
				Type:         "kafka",
				KafkaBrokers: []string{"127.0.0.1:9092"},
			},
		},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
	require.Contains(t, envelope.Message, "kafka readiness failed")
}

func TestHealthGetName(t *testing.T) {
	h := &health{}
	require.Equal(t, "health", h.GetName())
}

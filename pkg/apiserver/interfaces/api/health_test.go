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
)

type mockHealthQueue struct {
	statsError error
}

type mockRuntimeReadiness struct {
	ready  bool
	reason string
	role   string
}

type mockHealthDatabase struct {
	err            error
	waitForContext bool
}

func (m *mockHealthDatabase) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	if m.waitForContext {
		<-ctx.Done()
		return time.Time{}, ctx.Err()
	}
	if m.err != nil {
		return time.Time{}, m.err
	}
	return time.Now().UTC(), nil
}

func (m mockRuntimeReadiness) RuntimeRole() string {
	if m.role == "" {
		return "worker"
	}
	return m.role
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

func TestReadinessCheckWithHealthyQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{
		Queues: &msg.RuntimeQueues{Dispatch: &mockHealthQueue{}, Delay: &mockHealthQueue{}},
		Cfg: &config.Config{
			Messaging: config.MessagingConfig{Type: "redis"},
		},
	}
	r := gin.New()
	redisServer := miniredis.RunT(t)
	h.RedisClient = redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = h.RedisClient.Close() })
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload map[string]string
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "ready", payload["status"])
}

func TestReadinessCheckDatabaseOutageTimeoutAndRecovery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database := &mockHealthDatabase{err: errors.New("database unavailable")}
	h := &health{Database: database}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	check := func(ctx context.Context, wantStatus int) {
		t.Helper()
		resp := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ready", nil).WithContext(ctx)
		r.ServeHTTP(resp, req)
		require.Equal(t, wantStatus, resp.Code)
	}

	check(context.Background(), http.StatusServiceUnavailable)
	database.err = nil
	check(context.Background(), http.StatusOK)
	database.waitForContext = true
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	check(ctx, http.StatusServiceUnavailable)
	require.Less(t, time.Since(started), time.Second)

	resp := httptest.NewRecorder()
	r.GET("/health", h.healthCheck)
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusOK, resp.Code)
}

func TestReadinessCheckReportsRuntimeRole(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, role := range []string{"worker", "leader"} {
		t.Run(role, func(t *testing.T) {
			redisServer := miniredis.RunT(t)
			h := &health{
				Queues:      &msg.RuntimeQueues{Dispatch: &mockHealthQueue{}, Delay: &mockHealthQueue{}},
				RedisClient: redis.NewClient(&redis.Options{Addr: redisServer.Addr()}),
				Runtime:     mockRuntimeReadiness{ready: true, role: role},
				Cfg:         &config.Config{Messaging: config.MessagingConfig{Type: config.REDIS}},
			}
			t.Cleanup(func() { _ = h.RedisClient.Close() })
			r := gin.New()
			r.GET("/ready", h.readinessCheck)

			req := httptest.NewRequest(http.MethodGet, "/ready", nil)
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			require.Equal(t, http.StatusOK, resp.Code)
			var payload map[string]string
			requireSuccessResponse(t, resp.Body.Bytes(), &payload)
			require.Equal(t, role, payload["role"])
		})
	}
}

func TestReadinessCheckRejectsInitializingRuntimeLeader(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{
		Runtime: mockRuntimeReadiness{reason: "leader is still initializing"},
	}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Contains(t, envelope.Message, "leader is still initializing")
}

func TestReadinessCheckWithUnhealthyQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	for _, name := range []string{"dispatch", "delay"} {
		t.Run(name, func(t *testing.T) {
			queues := map[string]*mockHealthQueue{"dispatch": {}, "delay": {}}
			queues[name].statsError = errors.New("connection refused")
			h := &health{
				Queues:      &msg.RuntimeQueues{Dispatch: queues["dispatch"], Delay: queues["delay"]},
				Cfg:         &config.Config{Messaging: config.MessagingConfig{Type: config.REDIS}},
				RedisClient: redisClient,
			}
			r := gin.New()
			r.GET("/ready", h.readinessCheck)
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/ready", nil))
			require.Equal(t, http.StatusServiceUnavailable, resp.Code)
			envelope := decodeResponse(t, resp.Body.Bytes(), nil)
			require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
			require.Equal(t, "not ready: "+name+" queue connection failed", envelope.Message)
			require.Equal(t, "null", string(envelope.Data))
		})
	}
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

func TestReadinessCheckRedisOutageAndRecovery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldCheck := checkKafkaReadiness
	checkKafkaReadiness = func(context.Context, clients.KafkaConfig) error { return nil }
	t.Cleanup(func() { checkKafkaReadiness = oldCheck })

	for _, backend := range []string{config.REDIS, config.KAFKA} {
		t.Run(backend, func(t *testing.T) {
			redisServer := miniredis.RunT(t)
			redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr(), MaxRetries: -1})
			t.Cleanup(func() { require.NoError(t, redisClient.Close()) })

			h := &health{
				RedisClient: redisClient,
				Queues:      &msg.RuntimeQueues{Dispatch: &mockHealthQueue{}, Delay: &mockHealthQueue{}},
				Runtime:     mockRuntimeReadiness{ready: true},
				Cfg: &config.Config{
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
						require.Equal(t, "worker", payload["role"])
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
		})
	}
}

func TestReadinessCheckRequiresRedisClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{Cfg: config.NewConfig()}
	r := gin.New()
	r.GET("/ready", h.readinessCheck)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/ready", nil))

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
	require.Equal(t, "not ready: redis client is not configured", envelope.Message)
}

func TestReadinessCheckRequiresAllRuntimeQueues(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &health{
		Queues: &msg.RuntimeQueues{},
		Cfg: &config.Config{
			Messaging: config.MessagingConfig{Type: "redis"},
		},
	}
	r := gin.New()
	redisServer := miniredis.RunT(t)
	h.RedisClient = redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = h.RedisClient.Close() })
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Contains(t, envelope.Message, "delay")
	require.Contains(t, envelope.Message, "dispatch")
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
			Dispatch: &mockHealthQueue{},
			Delay:    &mockHealthQueue{statsError: errors.New("delay queue down")},
		},
		Cfg: &config.Config{
			Messaging: config.MessagingConfig{Type: "kafka"},
		},
	}
	r := gin.New()
	redisServer := miniredis.RunT(t)
	h.RedisClient = redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = h.RedisClient.Close() })
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
		Queues: &msg.RuntimeQueues{Dispatch: &mockHealthQueue{}, Delay: &mockHealthQueue{}},
		Cfg: &config.Config{
			Messaging: config.MessagingConfig{Type: "kafka", KafkaBrokers: []string{"127.0.0.1:1"}},
		},
	}
	r := gin.New()
	redisServer := miniredis.RunT(t)
	h.RedisClient = redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = h.RedisClient.Close() })
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
		Queues: &msg.RuntimeQueues{Dispatch: &mockHealthQueue{statsError: errors.New("queue stats failed")}, Delay: &mockHealthQueue{}},
		Cfg: &config.Config{
			Messaging: config.MessagingConfig{Type: "kafka", KafkaBrokers: []string{"127.0.0.1:9092"}},
		},
	}
	r := gin.New()
	redisServer := miniredis.RunT(t)
	h.RedisClient = redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = h.RedisClient.Close() })
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
	require.Contains(t, envelope.Message, "dispatch queue connection failed")
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
		Queues: &msg.RuntimeQueues{Dispatch: &mockHealthQueue{}, Delay: &mockHealthQueue{}},
		Cfg: &config.Config{
			Messaging: config.MessagingConfig{
				Type:         "kafka",
				KafkaBrokers: []string{"127.0.0.1:9092"},
			},
		},
	}
	r := gin.New()
	redisServer := miniredis.RunT(t)
	h.RedisClient = redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = h.RedisClient.Close() })
	r.GET("/ready", h.readinessCheck)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrServiceUnavailable.BusinessCode, envelope.Code)
	require.Contains(t, envelope.Message, "kafka readiness failed")
}

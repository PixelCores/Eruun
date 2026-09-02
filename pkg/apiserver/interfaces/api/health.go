package api

import (
	"strings"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

var checkKafkaReadiness = clients.CheckKafkaReadiness

type RuntimeReadiness interface {
	RuntimeReady() (bool, string)
}

// health provides health check endpoints for Kubernetes probes.
type health struct {
	Queue       msg.Queue        `inject:"queue"`
	DelayQueue  msg.Queue        `inject:"delayQueue"`
	ResultQueue msg.Queue        `inject:"resultQueue"`
	Cfg         *config.Config   `inject:""`
	Runtime     RuntimeReadiness `inject:"runtimeReadiness"`
}

// GetName returns the API name for registration.
func (h *health) GetName() string {
	return "health"
}

// RegisterRoutes registers health check endpoints.
func (h *health) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/health", h.healthCheck)
	group.GET("/healthz", h.healthCheck)
	group.GET("/ready", h.readinessCheck)
	group.GET("/readyz", h.readinessCheck)
}

// healthCheck returns a simple health status (liveness probe).
// This endpoint always returns OK if the server is running.
func (h *health) healthCheck(c *gin.Context) {
	bcode.ReturnSuccess(c, gin.H{
		"status": "healthy",
	})
}

// readinessCheck checks if the server is ready to accept traffic.
// It verifies connectivity to dependencies like the message queue.
func (h *health) readinessCheck(c *gin.Context) {
	ctx := c.Request.Context()
	if h.Runtime != nil {
		if ready, reason := h.Runtime.RuntimeReady(); !ready {
			bcode.ReturnErrorWithMessage(c, bcode.ErrServiceUnavailable, "not ready: "+reason)
			return
		}
	}
	externalQueue := h.Cfg != nil && h.Cfg.HasExternalQueue()
	isKafka := h.Cfg != nil && strings.EqualFold(strings.TrimSpace(h.Cfg.Messaging.Type), "kafka")

	if externalQueue {
		var degraded []string
		if !queueIsReady(h.Queue) {
			degraded = append(degraded, "dispatch")
		}
		if !queueIsReady(h.DelayQueue) {
			degraded = append(degraded, "delay")
		}
		if !queueIsReady(h.ResultQueue) {
			degraded = append(degraded, "result")
		}
		if len(degraded) > 0 {
			bcode.ReturnErrorWithMessage(c, bcode.ErrServiceUnavailable, "not ready: external queue degraded ("+strings.Join(degraded, ", ")+")")
			return
		}
	}

	if isKafka {
		if err := checkKafkaReadiness(ctx, clients.KafkaConfig{
			Brokers: h.Cfg.Messaging.KafkaBrokers,
			Topics:  kafkaTopicsForHealth(h.Cfg),
		}); err != nil {
			klog.V(4).InfoS("readiness check failed", "dependency", "kafka", "err", err)
			bcode.ReturnErrorWithMessage(c, bcode.ErrServiceUnavailable, "not ready: kafka readiness failed")
			return
		}
	}

	checks := []struct {
		name  string
		queue msg.Queue
		group string
	}{
		{name: "dispatch", queue: h.Queue, group: config.WorkflowWorkerQueueGroup},
	}
	if externalQueue {
		checks = append(checks,
			struct {
				name  string
				queue msg.Queue
				group string
			}{name: "delay", queue: h.DelayQueue, group: config.DelayQueueGroup},
			struct {
				name  string
				queue msg.Queue
				group string
			}{name: "result", queue: h.ResultQueue, group: config.ResultQueueGroup},
		)
	}

	for _, check := range checks {
		if !queueIsReady(check.queue) {
			continue
		}
		if _, _, err := check.queue.Stats(ctx, check.group); err != nil {
			klog.V(4).InfoS("readiness check failed", "dependency", check.name, "group", check.group, "err", err)
			bcode.ReturnErrorWithMessage(c, bcode.ErrServiceUnavailable, "not ready: "+check.name+" queue connection failed")
			return
		}
	}

	role := config.RuntimeRoleAPI
	if h.Cfg != nil {
		role = h.Cfg.NormalizedRole()
	}
	bcode.ReturnSuccess(c, gin.H{
		"status": "ready",
		"role":   role,
	})
}

func queueIsReady(queue msg.Queue) bool {
	return queue != nil
}

func kafkaTopicsForHealth(cfg *config.Config) []string {
	if cfg == nil {
		return nil
	}
	return config.KafkaTopics(cfg.Messaging.ChannelPrefix)
}

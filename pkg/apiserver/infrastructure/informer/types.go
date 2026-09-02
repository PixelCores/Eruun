package informer

import (
	"context"
	"sync"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

// ComponentReadyObserver is the worker-facing readiness contract. Implementations
// may use informer snapshots or direct Kubernetes API observation.
type ComponentReadyObserver interface {
	WaitForComponentReady(ctx context.Context, appID, componentName string, desiredReplicas int32, timeout time.Duration) error
	WaitForComponentReadyWithOptions(ctx context.Context, appID, componentName string, desiredReplicas int32, options ComponentReadyWaitOptions, timeout time.Duration) error
}

// ResourceType 资源类型
type ResourceType string

const (
	ResourceTypeComponent ResourceType = "Component"
)

// WaitEntry 等待条目
type WaitEntry struct {
	Key                 string            // namespace/name
	ResourceType        ResourceType      // 资源类型
	ReadyChan           chan struct{}     // 关闭表示资源就绪
	ErrorChan           chan error        // 错误通道
	CreatedAt           time.Time         // 创建时间
	DesiredReplicas     int32             // 期望副本数（仅用于组件等待）
	ExpectedImages      []string          // 期望 Pod 镜像；为空时只按组件 Ready 聚合
	ExpectedAnnotations map[string]string // 期望 Pod 注解；为空时不按注解过滤
	mu                  sync.Mutex        // 保护 closed 字段
	closed              bool              // 是否已关闭
}

// ComponentReadyWaitOptions filters component readiness to a specific Pod generation.
type ComponentReadyWaitOptions struct {
	ExpectedImages      []string
	ExpectedAnnotations map[string]string
}

// Close 安全关闭 WaitEntry
func (e *WaitEntry) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return
	}
	e.closed = true
	close(e.ReadyChan)
}

// SendError 发送错误并关闭
func (e *WaitEntry) SendError(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return
	}
	e.closed = true
	select {
	case e.ErrorChan <- err:
	default:
	}
	close(e.ErrorChan)
}

// IsClosed 检查是否已关闭
func (e *WaitEntry) IsClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

// ComponentStatusUpdate 组件状态更新信息（传递给数据库同步）
type ComponentStatusUpdate struct {
	AppID         string                  // 应用 ID
	ComponentID   int                     // 组件 ID
	ComponentName string                  // 组件名称
	Status        *config.ComponentStatus // 运行状态
	ReadyReplicas *int32                  // 就绪副本数
	Replicas      *int32                  // 期望副本数
	LastAbnormal  *string                 // 最近一次异常信息（为空表示清空）
}

// StatusSyncFunc 状态同步回调函数类型
type StatusSyncFunc func(update *ComponentStatusUpdate)

// PodRestartMonitorConfig controls Pod restart threshold detection.
type PodRestartMonitorConfig struct {
	Enabled   bool
	Window    time.Duration
	Threshold int
}

// PodRestartMonitorConfigFunc loads the current Pod restart monitor configuration.
type PodRestartMonitorConfigFunc func(ctx context.Context) (PodRestartMonitorConfig, error)

// DeploymentPodRestartEvent describes a Pod restart threshold event.
type DeploymentPodRestartEvent struct {
	Namespace     string
	PodName       string
	AppID         string
	ComponentName string
	ComponentID   int
	Window        time.Duration
	Threshold     int
	RestartCount  int
	OccurredAt    time.Time
}

// DeploymentPodRestartTriggerFunc handles Pod restart threshold events.
type DeploymentPodRestartTriggerFunc func(event DeploymentPodRestartEvent)

// WaitError 等待错误（携带状态）
type WaitError struct {
	Status         config.Status
	Err            error
	AbnormalReason string
}

func (e *WaitError) Error() string { return e.Err.Error() }
func (e *WaitError) Unwrap() error { return e.Err }

// NewWaitError 创建带状态的等待错误
func NewWaitError(status config.Status, err error) error {
	return NewWaitErrorWithAbnormal(status, err, "")
}

// NewWaitErrorWithAbnormal 创建带状态和异常原因的等待错误
func NewWaitErrorWithAbnormal(status config.Status, err error, abnormalReason string) error {
	if err == nil {
		return nil
	}
	return &WaitError{
		Status:         status,
		Err:            err,
		AbnormalReason: abnormalReason,
	}
}

// ExtractWaitError 提取 WaitError
func ExtractWaitError(err error) (*WaitError, bool) {
	if err == nil {
		return nil, false
	}
	if we, ok := err.(*WaitError); ok {
		return we, true
	}
	return nil, false
}

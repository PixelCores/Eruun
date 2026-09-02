package informer

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// Manager 管理所有 Informer
type Manager struct {
	client           kubernetes.Interface
	factory          informers.SharedInformerFactory
	stopCh           chan struct{}
	waiter           *ResourceReadyWaiter
	mu               sync.RWMutex
	started          bool
	waiterGeneration uint64
	resyncPeriod     time.Duration
	labelSelector    string
}

// ManagerOption 配置选项
type ManagerOption func(*Manager)

// WithResyncPeriod 设置重新同步周期
func WithResyncPeriod(d time.Duration) ManagerOption {
	return func(m *Manager) {
		m.resyncPeriod = d
	}
}

// WithLabelSelector 设置标签过滤器（减少内存消耗）
func WithLabelSelector(selector string) ManagerOption {
	return func(m *Manager) {
		m.labelSelector = selector
	}
}

// NewManager 创建 Informer 管理器
func NewManager(client kubernetes.Interface, opts ...ManagerOption) *Manager {
	m := &Manager{
		client:       client,
		waiter:       NewResourceReadyWaiter(),
		resyncPeriod: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

func (m *Manager) resetRuntime() {
	m.stopCh = make(chan struct{})
	m.factory = m.newFactory()
}

func (m *Manager) newFactory() informers.SharedInformerFactory {
	if m.labelSelector != "" {
		klog.V(2).Infof("Informer manager created with label selector: %s", m.labelSelector)
		return informers.NewSharedInformerFactoryWithOptions(
			m.client,
			m.resyncPeriod,
			informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
				opts.LabelSelector = m.labelSelector
			}),
		)
	}
	klog.V(2).Info("Informer manager created without label selector")
	return informers.NewSharedInformerFactory(m.client, m.resyncPeriod)
}

// GetWaiter 获取资源等待器
func (m *Manager) GetWaiter() *ResourceReadyWaiter {
	return m.waiter
}

// Start 启动 Informer
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		klog.V(2).Info("Informer manager already started")
		return nil
	}
	m.resetRuntime()
	generation := m.waiter.beginPodSnapshotGeneration()
	m.waiterGeneration = generation
	factory := m.factory
	stopCh := m.stopCh
	m.started = true
	m.mu.Unlock()

	klog.Info("Starting informer manager...")

	// 设置 Pod Informer
	podInformer := factory.Core().V1().Pods().Informer()
	_, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				m.waiter.onPodAddForGeneration(generation, pod)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok1 := oldObj.(*corev1.Pod)
			newPod, ok2 := newObj.(*corev1.Pod)
			if ok1 && ok2 {
				m.waiter.onPodUpdateForGeneration(generation, oldPod, newPod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				m.waiter.onPodDeleteForGeneration(generation, pod)
			} else if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				if pod, ok := tombstone.Obj.(*corev1.Pod); ok {
					m.waiter.onPodDeleteForGeneration(generation, pod)
				}
			}
		},
	})
	if err != nil {
		m.stopGeneration(generation)
		return fmt.Errorf("failed to add pod event handler: %w", err)
	}
	klog.V(2).Info("Pod informer event handler registered")

	// 启动所有 Informer
	factory.Start(stopCh)

	go func() {
		select {
		case <-ctx.Done():
			klog.Info("Context cancelled, stopping informer manager...")
			m.stopGeneration(generation)
		case <-stopCh:
		}
	}()

	// 等待缓存同步
	klog.Info("Waiting for informer caches to sync...")
	synced := factory.WaitForCacheSync(stopCh)
	for typ, ok := range synced {
		if !ok {
			m.stopGeneration(generation)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("failed to sync cache for %v", typ)
		}
		klog.V(2).Infof("Cache synced for %v", typ)
	}
	klog.Info("All informer caches synced successfully")

	return nil
}

// Stop 停止所有 Informer
func (m *Manager) Stop() {
	m.stopGeneration(0)
}

func (m *Manager) stopGeneration(generation uint64) {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	if generation != 0 && m.waiterGeneration != generation {
		m.mu.Unlock()
		return
	}
	stopCh := m.stopCh
	activeGeneration := m.waiterGeneration
	m.started = false
	m.waiterGeneration = 0
	m.stopCh = nil
	m.factory = nil
	m.waiter.endPodSnapshotGeneration(activeGeneration)
	m.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	klog.Info("Informer manager stopped")
}

// IsStarted 检查是否已启动
func (m *Manager) IsStarted() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.started
}

// GetStats 获取统计信息（用于监控）
func (m *Manager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"started":        m.IsStarted(),
		"pendingWaiters": m.waiter.GetPendingCount(),
		"pendingKeys":    m.waiter.GetPendingKeys(),
		"labelSelector":  m.labelSelector,
		"resyncPeriod":   m.resyncPeriod.String(),
	}
}

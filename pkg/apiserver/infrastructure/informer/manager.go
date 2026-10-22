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
	syncer           *ComponentStatusSyncer
	mu               sync.RWMutex
	started          bool
	syncerGeneration uint64
	syncTimeout      time.Duration
	labelSelector    string
}

const (
	managerResyncPeriod       = 30 * time.Second
	defaultManagerSyncTimeout = 30 * time.Second
)

// NewManager 创建 Informer 管理器；labelSelector 限定监听的对象以减少内存消耗
func NewManager(client kubernetes.Interface, labelSelector string) *Manager {
	return &Manager{
		client:        client,
		syncer:        NewComponentStatusSyncer(),
		syncTimeout:   defaultManagerSyncTimeout,
		labelSelector: labelSelector,
	}
}

func (m *Manager) resetRuntime() {
	m.stopCh = make(chan struct{})
	m.factory = m.newFactory()
}

func (m *Manager) newFactory() informers.SharedInformerFactory {
	klog.V(2).Infof("Informer manager created with label selector: %q", m.labelSelector)
	return informers.NewSharedInformerFactoryWithOptions(
		m.client,
		managerResyncPeriod,
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = m.labelSelector
		}),
	)
}

// StatusSyncer returns the Pod event handler that syncs component status and
// monitors Deployment Pod restarts while this node is Leader.
func (m *Manager) StatusSyncer() *ComponentStatusSyncer {
	return m.syncer
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
	generation := m.syncer.beginPodSnapshotGeneration()
	m.syncerGeneration = generation
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
				m.syncer.onPodAddForGeneration(generation, pod)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok1 := oldObj.(*corev1.Pod)
			newPod, ok2 := newObj.(*corev1.Pod)
			if ok1 && ok2 {
				m.syncer.onPodUpdateForGeneration(generation, oldPod, newPod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				m.syncer.onPodDeleteForGeneration(generation, pod)
			} else if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				if pod, ok := tombstone.Obj.(*corev1.Pod); ok {
					m.syncer.onPodDeleteForGeneration(generation, pod)
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
	syncTimeout := m.syncTimeout
	if syncTimeout <= 0 {
		syncTimeout = defaultManagerSyncTimeout
	}
	syncCtx, cancelSync := context.WithTimeout(ctx, syncTimeout)
	defer cancelSync()
	synced := factory.WaitForCacheSync(syncCtx.Done())
	for typ, ok := range synced {
		if !ok {
			m.stopGeneration(generation)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if syncCtx.Err() != nil {
				return fmt.Errorf("sync informer cache for %v within %s: %w", typ, syncTimeout, syncCtx.Err())
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
	if generation != 0 && m.syncerGeneration != generation {
		m.mu.Unlock()
		return
	}
	stopCh := m.stopCh
	activeGeneration := m.syncerGeneration
	m.started = false
	m.syncerGeneration = 0
	m.stopCh = nil
	m.factory = nil
	m.syncer.endPodSnapshotGeneration(activeGeneration)
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

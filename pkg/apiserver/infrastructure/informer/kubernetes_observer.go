package informer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

const (
	defaultWorkloadObserverPollInterval = 2 * time.Second
	defaultWorkloadObserverSyncTimeout  = 30 * time.Second
)

// KubernetesWorkloadObserver observes readiness from one shared Pod informer
// cache per worker process. This keeps Worker correctness independent from the
// Controller leader without issuing a cluster-wide Pod List for every Job poll.
type KubernetesWorkloadObserver struct {
	client       kubernetes.Interface
	factory      k8sinformers.SharedInformerFactory
	podInformer  cache.SharedIndexInformer
	podLister    corelisters.PodLister
	pollInterval time.Duration
	syncTimeout  time.Duration
	startOnce    sync.Once
	startErr     error
	synced       atomic.Bool
}

func NewKubernetesWorkloadObserver(client kubernetes.Interface) *KubernetesWorkloadObserver {
	if client == nil {
		return &KubernetesWorkloadObserver{
			pollInterval: defaultWorkloadObserverPollInterval,
			syncTimeout:  defaultWorkloadObserverSyncTimeout,
		}
	}
	factory := k8sinformers.NewSharedInformerFactoryWithOptions(
		client,
		0,
		k8sinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = config.LabelAppID
		}),
	)
	pods := factory.Core().V1().Pods()
	return &KubernetesWorkloadObserver{
		client:       client,
		factory:      factory,
		podInformer:  pods.Informer(),
		podLister:    pods.Lister(),
		pollInterval: defaultWorkloadObserverPollInterval,
		syncTimeout:  defaultWorkloadObserverSyncTimeout,
	}
}

// Start begins the shared List/Watch and waits for the initial Pod snapshot.
// A failed initial sync is fatal for a Worker because readiness decisions must
// never be made from an uninitialized cache.
func (o *KubernetesWorkloadObserver) Start(ctx context.Context) error {
	if o == nil || o.client == nil || o.factory == nil || o.podInformer == nil || o.podLister == nil {
		return fmt.Errorf("kubernetes workload observer is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	syncTimeout := o.syncTimeout
	if syncTimeout <= 0 {
		syncTimeout = defaultWorkloadObserverSyncTimeout
	}
	o.startOnce.Do(func() {
		o.factory.Start(ctx.Done())
		syncCtx, cancelSync := context.WithTimeout(ctx, syncTimeout)
		defer cancelSync()
		if !cache.WaitForCacheSync(syncCtx.Done(), o.podInformer.HasSynced) {
			o.startErr = fmt.Errorf("synchronize kubernetes workload observer pod cache within %s: %w", syncTimeout, syncCtx.Err())
			return
		}
		o.synced.Store(true)
	})
	return o.startErr
}

func (o *KubernetesWorkloadObserver) WaitForComponentReady(ctx context.Context, appID, componentName string, desiredReplicas int32, timeout time.Duration) error {
	return o.WaitForComponentReadyWithOptions(ctx, appID, componentName, desiredReplicas, ComponentReadyWaitOptions{}, timeout)
}

func (o *KubernetesWorkloadObserver) WaitForComponentReadyWithOptions(ctx context.Context, appID, componentName string, desiredReplicas int32, options ComponentReadyWaitOptions, timeout time.Duration) error {
	if o == nil || o.client == nil || o.podLister == nil {
		return fmt.Errorf("kubernetes workload observer is not configured")
	}
	if !o.synced.Load() {
		return fmt.Errorf("kubernetes workload observer pod cache is not synchronized")
	}
	if desiredReplicas <= 0 || timeout <= 0 {
		return fmt.Errorf("component %s/%s requires positive replicas and timeout", appID, componentName)
	}
	options = normalizeComponentReadyWaitOptions(options)
	selector := labels.Set{
		config.LabelAppID:         appID,
		config.LabelComponentName: componentName,
	}.AsSelector()
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	lastAbnormal := ""
	var lastCacheErr error
	for {
		pods, err := o.podLister.List(selector)
		if err != nil {
			if waitCtx.Err() != nil {
				break
			}
			lastCacheErr = err
		} else {
			lastCacheErr = nil
			ready := int32(0)
			currentAbnormal := ""
			for _, pod := range pods {
				if !podImagesContainAll(podImageSet(pod), options.ExpectedImages) || !podAnnotationsContainAll(podAnnotations(pod), options.ExpectedAnnotations) {
					continue
				}
				if abnormal := strings.TrimSpace(kube.ExtractPodAbnormalReason(pod)); abnormal != "" {
					currentAbnormal = abnormal
				}
				if isPodReady(pod) {
					ready++
				}
			}
			lastAbnormal = currentAbnormal
			if ready >= desiredReplicas {
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return NewWaitError(config.StatusCancelled, fmt.Errorf("component %s/%s cancelled: %w", appID, componentName, ctx.Err()))
			}
			if lastAbnormal != "" {
				return NewWaitErrorWithAbnormal(config.StatusFailed, fmt.Errorf("component %s/%s timeout after %v with abnormal pod state: %s", appID, componentName, timeout, lastAbnormal), lastAbnormal)
			}
			if lastCacheErr != nil {
				return NewWaitError(config.StatusTimeout, fmt.Errorf("component %s/%s timeout after %v; last pod cache error: %w", appID, componentName, timeout, lastCacheErr))
			}
			return NewWaitError(config.StatusTimeout, fmt.Errorf("component %s/%s timeout after %v", appID, componentName, timeout))
		case <-ticker.C:
		}
	}
	return NewWaitError(config.StatusTimeout, fmt.Errorf("component %s/%s timeout after %v", appID, componentName, timeout))
}

var _ ComponentReadyObserver = (*ResourceReadyWaiter)(nil)
var _ ComponentReadyObserver = (*KubernetesWorkloadObserver)(nil)

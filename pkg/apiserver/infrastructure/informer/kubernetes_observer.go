package informer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

const (
	defaultWorkloadObserverPollInterval = 2 * time.Second
	defaultWorkloadObserverSyncTimeout  = 30 * time.Second
)

// KubernetesWorkloadObserver shares application Pod readiness and managed Job
// snapshots within a Worker, independently from the Controller leader.
type KubernetesWorkloadObserver struct {
	client       kubernetes.Interface
	factory      k8sinformers.SharedInformerFactory
	podInformer  cache.SharedIndexInformer
	podLister    corelisters.PodLister
	jobFactory   k8sinformers.SharedInformerFactory
	jobInformer  cache.SharedIndexInformer
	jobLister    batchlisters.JobLister
	jobWaitersMu sync.Mutex
	jobWaiters   map[string]map[chan struct{}]struct{}
	runDone      <-chan struct{}
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
		k8sinformers.WithTransform(stripObserverManagedFields),
	)
	pods := factory.Core().V1().Pods()
	jobFactory := k8sinformers.NewSharedInformerFactoryWithOptions(client, 0,
		k8sinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labels.Set{config.LabelManagedBy: config.ManagedByEruun}.String()
		}),
		k8sinformers.WithTransform(stripObserverManagedFields),
	)
	jobs := jobFactory.Batch().V1().Jobs()
	return &KubernetesWorkloadObserver{
		client:       client,
		factory:      factory,
		podInformer:  pods.Informer(),
		podLister:    pods.Lister(),
		jobFactory:   jobFactory,
		jobInformer:  jobs.Informer(),
		jobLister:    jobs.Lister(),
		jobWaiters:   make(map[string]map[chan struct{}]struct{}),
		pollInterval: defaultWorkloadObserverPollInterval,
		syncTimeout:  defaultWorkloadObserverSyncTimeout,
	}
}

func stripObserverManagedFields(obj interface{}) (interface{}, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	accessor.SetManagedFields(nil)
	return obj, nil
}

// Start begins the shared List/Watch and waits for the initial Pod and Job snapshots.
// A failed initial sync is fatal for a Worker because readiness decisions must
// never be made from an uninitialized cache.
func (o *KubernetesWorkloadObserver) Start(ctx context.Context) error {
	if o == nil || o.client == nil || o.factory == nil || o.podInformer == nil || o.podLister == nil || o.jobFactory == nil || o.jobInformer == nil {
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
		o.runDone = ctx.Done()
		_, err := o.jobInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    o.notifyJobWaiters,
			UpdateFunc: func(_, obj interface{}) { o.notifyJobWaiters(obj) },
			DeleteFunc: o.notifyJobWaiters,
		})
		if err != nil {
			o.startErr = fmt.Errorf("register kubernetes Job observer: %w", err)
			return
		}
		o.factory.Start(ctx.Done())
		o.jobFactory.Start(ctx.Done())
		syncCtx, cancelSync := context.WithTimeout(ctx, syncTimeout)
		defer cancelSync()
		if !cache.WaitForCacheSync(syncCtx.Done(), o.podInformer.HasSynced) {
			o.startErr = fmt.Errorf("synchronize kubernetes workload observer pod cache within %s: %w", syncTimeout, syncCtx.Err())
			return
		}
		if !cache.WaitForCacheSync(syncCtx.Done(), o.jobInformer.HasSynced) {
			o.startErr = fmt.Errorf("synchronize kubernetes workload observer Job cache within %s: %w", syncTimeout, syncCtx.Err())
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

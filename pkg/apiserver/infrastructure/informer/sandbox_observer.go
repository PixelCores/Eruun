package informer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

var ErrSandboxObservationUnavailable = errors.New("Sandbox observation is unavailable")

// SandboxObserver returns shared lifecycle snapshots. Absence is unknown;
// callers must use fresh identity checks before destructive or execution actions.
type SandboxObserver interface {
	Sandbox(namespace, name string) (*unstructured.Unstructured, error)
	SandboxPods(namespace, sandboxID string) ([]*corev1.Pod, error)
}

var observedSandboxResource = schema.GroupVersionResource{Group: "agents.kruise.io", Version: "v1alpha1", Resource: "sandboxes"}

const sandboxPodIndex = "eruunSandbox"

// KubernetesSandboxObserver owns only the Sandbox GVR and task-labeled Pods.
// It is shared by the API or Controller process that consumes these snapshots.
// A missing CRD leaves Sandbox requests unavailable while ordinary Jobs remain
// usable; the standard reflector retries List/Watch for this exact resource.
type KubernetesSandboxObserver struct {
	factory         dynamicinformer.DynamicSharedInformerFactory
	podFactory      k8sinformers.SharedInformerFactory
	sandboxInformer cache.SharedIndexInformer
	podInformer     cache.SharedIndexInformer
	startOnce       sync.Once
	synced          atomic.Bool
	runDone         <-chan struct{}
}

func NewKubernetesSandboxObserver(client dynamic.Interface, pods kubernetes.Interface) (*KubernetesSandboxObserver, error) {
	if client == nil || pods == nil {
		return nil, fmt.Errorf("Sandbox observation requires Kubernetes dynamic and typed clients")
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, 0, metav1.NamespaceAll,
		func(options *metav1.ListOptions) {
			options.LabelSelector = labels.Set{config.LabelManagedBy: config.ManagedByEruun}.String()
		})
	sandboxes := factory.ForResource(observedSandboxResource).Informer()
	if err := sandboxes.SetTransform(stripObserverManagedFields); err != nil {
		return nil, fmt.Errorf("configure Sandbox cache transform: %w", err)
	}
	podFactory := k8sinformers.NewSharedInformerFactoryWithOptions(pods, 0,
		k8sinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = "eruun.io/task-id"
		}),
		k8sinformers.WithTransform(stripObserverManagedFields))
	podInformer := podFactory.Core().V1().Pods().Informer()
	if err := podInformer.AddIndexers(cache.Indexers{
		sandboxPodIndex: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return nil, fmt.Errorf("Sandbox Pod cache received %T", obj)
			}
			id := pod.Labels["eruun.io/sandbox-id"]
			if id == "" {
				return nil, nil
			}
			return []string{pod.Namespace + "/" + id}, nil
		},
	}); err != nil {
		return nil, fmt.Errorf("index Sandbox Pods: %w", err)
	}
	return &KubernetesSandboxObserver{
		factory: factory, podFactory: podFactory,
		sandboxInformer: sandboxes, podInformer: podInformer,
	}, nil
}

// Run keeps both shared caches alive until the owning runtime stops. Initial
// synchronization happens in this background goroutine, never in an HTTP request.
func (o *KubernetesSandboxObserver) Run(ctx context.Context) {
	o.startOnce.Do(func() {
		o.runDone = ctx.Done()
		o.factory.Start(ctx.Done())
		o.podFactory.Start(ctx.Done())
		defer o.factory.Shutdown()
		defer o.podFactory.Shutdown()
		if !cache.WaitForCacheSync(ctx.Done(), o.sandboxInformer.HasSynced, o.podInformer.HasSynced) {
			return
		}
		o.synced.Store(true)
		defer o.synced.Store(false)
		<-ctx.Done()
	})
}

func (o *KubernetesSandboxObserver) available() error {
	if o == nil || !o.synced.Load() {
		return fmt.Errorf("%w: initial Sandbox and Pod snapshots have not synchronized", ErrSandboxObservationUnavailable)
	}
	select {
	case <-o.runDone:
		return fmt.Errorf("%w: observer stopped", ErrSandboxObservationUnavailable)
	default:
		return nil
	}
}

func (o *KubernetesSandboxObserver) Sandbox(namespace, name string) (*unstructured.Unstructured, error) {
	if err := o.available(); err != nil {
		return nil, err
	}
	if namespace == "" || name == "" {
		return nil, fmt.Errorf("Sandbox observation requires namespace and name")
	}
	obj, exists, err := o.sandboxInformer.GetIndexer().GetByKey(namespace + "/" + name)
	if err != nil {
		return nil, fmt.Errorf("read Sandbox snapshot: %w", err)
	}
	if !exists {
		return nil, apierrors.NewNotFound(observedSandboxResource.GroupResource(), name)
	}
	sandbox, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("Sandbox cache received %T", obj)
	}
	return sandbox.DeepCopy(), nil
}

func (o *KubernetesSandboxObserver) SandboxPods(namespace, sandboxID string) ([]*corev1.Pod, error) {
	if err := o.available(); err != nil {
		return nil, err
	}
	if namespace == "" || sandboxID == "" {
		return nil, fmt.Errorf("Sandbox Pod observation requires namespace and sandbox ID")
	}
	objects, err := o.podInformer.GetIndexer().ByIndex(sandboxPodIndex, namespace+"/"+sandboxID)
	if err != nil {
		return nil, fmt.Errorf("read Sandbox Pod snapshots: %w", err)
	}
	pods := make([]*corev1.Pod, 0, len(objects))
	for _, object := range objects {
		pod, ok := object.(*corev1.Pod)
		if !ok {
			return nil, fmt.Errorf("Sandbox Pod cache received %T", object)
		}
		pods = append(pods, pod.DeepCopy())
	}
	return pods, nil
}

var _ SandboxObserver = (*KubernetesSandboxObserver)(nil)

// Package runtime contains Kubernetes-side coordination for imported resources
// after a user has explicitly enrolled them for management.
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

const (
	defaultBindingReloadInterval = 30 * time.Second
	defaultMaxQueueItems         = 4096
	maxPodLabelRetries           = 5
)

// SourceBinding is the immutable Kubernetes controller identity that an
// adopted component owns.  A Kubernetes UID, rather than an application ID or
// a generated resource name, is the ownership fact used by the coordinator.
type SourceBinding struct {
	Namespace          string
	AppID              string
	ComponentID        int
	ComponentName      string
	ManagementMode     config.ManagementMode
	WorkloadAPIVersion string
	WorkloadKind       string
	WorkloadName       string
	WorkloadUID        types.UID
}

func (b SourceBinding) readOnly() bool {
	return b.ManagementMode == config.ManagementModeObserve
}

func (b SourceBinding) valid() bool {
	return b.labelClaimValid() &&
		strings.TrimSpace(b.WorkloadKind) != "" &&
		strings.TrimSpace(string(b.WorkloadUID)) != ""
}

func (b SourceBinding) labelClaimValid() bool {
	return strings.TrimSpace(b.Namespace) != "" &&
		strings.TrimSpace(b.AppID) != "" &&
		b.ComponentID > 0 &&
		strings.TrimSpace(b.ComponentName) != ""
}

func (b SourceBinding) controllerKey() string {
	return strings.ToLower(strings.TrimSpace(b.Namespace)) + "/" +
		strings.ToLower(strings.TrimSpace(b.WorkloadKind)) + "/" +
		string(b.WorkloadUID)
}

// BindingLoader returns current component label claims for every application.
// Adopted and observe components may populate source workload identity; observe
// bindings are used only for synthetic status events and never authorize writes.
type BindingLoader func(context.Context) ([]SourceBinding, error)

// PodObservationFunc receives synthetic managed-label views of read-only
// source Pods. A nil newPod represents deletion.
type PodObservationFunc func(oldPod, newPod *corev1.Pod)

type observedPodState struct {
	binding SourceBinding
	pod     *corev1.Pod
}

// PodCoordinator observes Pods in namespaces with source bindings. It patches
// only adopted Pod metadata after verifying the owner UID chain; observe
// bindings emit read-only synthetic events. It never reads or writes controller
// PodTemplates, so initial adoption cannot cause a rollout.
type PodCoordinator struct {
	client kubernetes.Interface
	load   BindingLoader

	reloadInterval time.Duration
	maxQueueItems  int
	observe        PodObservationFunc

	queue workqueue.RateLimitingInterface

	observeMu        sync.Mutex
	mu               sync.RWMutex
	bindingsByNS     map[string]map[string]SourceBinding
	labelClaimsByNS  map[string]map[string]struct{}
	namespaceCancels map[string]context.CancelFunc
	pending          map[string]uint64
	podScanOffsets   map[string]int
	observedPods     map[string]observedPodState
	namespaceOffset  int
}

// PodCoordinatorOption configures a coordinator. Options are intentionally
// small so the default runtime behavior remains conservative.
type PodCoordinatorOption func(*PodCoordinator)

// WithBindingReloadInterval changes the database binding reload cadence.
func WithBindingReloadInterval(interval time.Duration) PodCoordinatorOption {
	return func(c *PodCoordinator) {
		if interval > 0 {
			c.reloadInterval = interval
		}
	}
}

// WithMaxQueueItems bounds distinct queued Pods and prevents a noisy namespace
// from consuming unbounded memory.
func WithMaxQueueItems(max int) PodCoordinatorOption {
	return func(c *PodCoordinator) {
		if max > 0 {
			c.maxQueueItems = max
		}
	}
}

// WithPodObservationFunc connects read-only source Pod events to the runtime
// status tracker without changing Kubernetes metadata.
func WithPodObservationFunc(observe PodObservationFunc) PodCoordinatorOption {
	return func(c *PodCoordinator) {
		c.observe = observe
	}
}

// NewPodCoordinator creates a leader-scoped coordinator. Run owns all watches
// and returns when its context is cancelled (for example on leader loss).
func NewPodCoordinator(client kubernetes.Interface, loader BindingLoader, opts ...PodCoordinatorOption) *PodCoordinator {
	c := &PodCoordinator{
		client:           client,
		load:             loader,
		reloadInterval:   defaultBindingReloadInterval,
		maxQueueItems:    defaultMaxQueueItems,
		queue:            workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "namespace-adoption-pod-labeler"),
		bindingsByNS:     make(map[string]map[string]SourceBinding),
		labelClaimsByNS:  make(map[string]map[string]struct{}),
		namespaceCancels: make(map[string]context.CancelFunc),
		pending:          make(map[string]uint64),
		podScanOffsets:   make(map[string]int),
		observedPods:     make(map[string]observedPodState),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run starts namespace-local Pod watches and a single bounded worker. It is
// safe to construct a new coordinator after every leader election; cancellation
// stops all watches before another leader starts its own coordinator.
func (c *PodCoordinator) Run(ctx context.Context) {
	if c == nil || c.client == nil || c.load == nil {
		return
	}
	c.reload(ctx)
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		c.runWorker(ctx)
	}()
	defer func() {
		c.shutdown()
		worker.Wait()
	}()

	ticker := time.NewTicker(c.reloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reload(ctx)
		}
	}
}

func (c *PodCoordinator) shutdown() {
	c.mu.Lock()
	for namespace, cancel := range c.namespaceCancels {
		cancel()
		delete(c.namespaceCancels, namespace)
	}
	c.mu.Unlock()
	c.queue.ShutDown()
}

func (c *PodCoordinator) reload(ctx context.Context) {
	bindings, err := c.load(ctx)
	if err != nil {
		// Persisted source bindings are the authorization fact for Pod metadata
		// writes. A failed refresh must not leave the last successful snapshot
		// active after an application may have been detached in the database.
		c.replaceBindings(nil, nil)
		klog.ErrorS(err, "reload namespace adoption source bindings")
		return
	}

	uniqueByUID := make(map[types.UID]SourceBinding)
	ambiguousUIDs := make(map[types.UID]struct{})
	labelClaimsByNamespace := make(map[string]map[string]struct{})
	for _, binding := range bindings {
		if binding.labelClaimValid() {
			namespace := strings.TrimSpace(binding.Namespace)
			if labelClaimsByNamespace[namespace] == nil {
				labelClaimsByNamespace[namespace] = make(map[string]struct{})
			}
			labelClaimsByNamespace[namespace][managedLabelClaimKey(expectedLabels(binding))] = struct{}{}
		}
		if !binding.valid() {
			continue
		}
		uid := binding.WorkloadUID
		if _, ambiguous := ambiguousUIDs[uid]; ambiguous {
			continue
		}
		if existing, exists := uniqueByUID[uid]; exists {
			if existing == binding {
				continue
			}
			// Kubernetes UIDs are cluster-global. Once conflicting persisted
			// bindings are observed, tombstone the UID for the entire reload so
			// a third duplicate cannot accidentally make it writable again.
			delete(uniqueByUID, uid)
			ambiguousUIDs[uid] = struct{}{}
			klog.ErrorS(
				fmt.Errorf("duplicate source workload UID"),
				"skip ambiguous adopted source binding",
				"uid", uid,
				"existingAppID", existing.AppID,
				"conflictingAppID", binding.AppID,
			)
			continue
		}
		uniqueByUID[uid] = binding
	}

	byNamespace := make(map[string]map[string]SourceBinding)
	for _, binding := range uniqueByUID {
		namespace := strings.TrimSpace(binding.Namespace)
		if byNamespace[namespace] == nil {
			byNamespace[namespace] = make(map[string]SourceBinding)
		}
		byNamespace[namespace][binding.controllerKey()] = binding
	}
	c.replaceBindings(byNamespace, labelClaimsByNamespace)

	for _, namespace := range c.namespacesForNextScan() {
		c.ensureNamespaceWatch(ctx, namespace)
		c.enqueueNamespacePods(ctx, namespace)
	}
}

func (c *PodCoordinator) replaceBindings(
	byNamespace map[string]map[string]SourceBinding,
	labelClaimsByNamespace map[string]map[string]struct{},
) {
	if byNamespace == nil {
		byNamespace = make(map[string]map[string]SourceBinding)
	}
	if labelClaimsByNamespace == nil {
		labelClaimsByNamespace = make(map[string]map[string]struct{})
	}
	removedObserved := make([]*corev1.Pod, 0)
	c.observeMu.Lock()
	defer c.observeMu.Unlock()
	c.mu.Lock()
	c.bindingsByNS = byNamespace
	c.labelClaimsByNS = labelClaimsByNamespace
	for key, observed := range c.observedPods {
		current, active := byNamespace[observed.binding.Namespace][observed.binding.controllerKey()]
		if active && current == observed.binding && current.readOnly() {
			continue
		}
		delete(c.observedPods, key)
		removedObserved = append(removedObserved, observed.pod)
	}
	for namespace, cancel := range c.namespaceCancels {
		if _, active := byNamespace[namespace]; active {
			continue
		}
		cancel()
		delete(c.namespaceCancels, namespace)
		delete(c.podScanOffsets, namespace)
	}
	for namespace := range c.podScanOffsets {
		if _, active := byNamespace[namespace]; !active {
			delete(c.podScanOffsets, namespace)
		}
	}
	c.mu.Unlock()
	for _, pod := range removedObserved {
		c.notifyObservedPod(pod, nil)
	}
}

func (c *PodCoordinator) namespacesForNextScan() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	namespaces := make([]string, 0, len(c.bindingsByNS))
	for namespace := range c.bindingsByNS {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	if len(namespaces) == 0 {
		c.namespaceOffset = 0
		return namespaces
	}
	start := c.namespaceOffset % len(namespaces)
	rotated := append(append(make([]string, 0, len(namespaces)), namespaces[start:]...), namespaces[:start]...)
	c.namespaceOffset = (start + 1) % len(namespaces)
	return rotated
}

func (c *PodCoordinator) ensureNamespaceWatch(parent context.Context, namespace string) {
	c.mu.Lock()
	if _, exists := c.namespaceCancels[namespace]; exists {
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	c.namespaceCancels[namespace] = cancel
	c.mu.Unlock()

	factory := informers.NewSharedInformerFactoryWithOptions(c.client, 30*time.Second, informers.WithNamespace(namespace))
	pods := factory.Core().V1().Pods().Informer()
	_, err := pods.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueuePod(obj) },
		UpdateFunc: func(_, obj interface{}) { c.enqueuePod(obj) },
		DeleteFunc: func(obj interface{}) { c.handleDeletedPod(obj) },
	})
	if err != nil {
		klog.ErrorS(err, "register namespace adoption pod handler", "namespace", namespace)
		cancel()
		c.mu.Lock()
		delete(c.namespaceCancels, namespace)
		c.mu.Unlock()
		return
	}

	go factory.Start(ctx.Done())
}

func (c *PodCoordinator) enqueueNamespacePods(ctx context.Context, namespace string) {
	pods, err := c.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "list pods after adoption source binding reload", "namespace", namespace)
		return
	}
	if len(pods.Items) == 0 {
		c.mu.Lock()
		c.podScanOffsets[namespace] = 0
		c.mu.Unlock()
		return
	}
	sort.Slice(pods.Items, func(left, right int) bool {
		return pods.Items[left].Name < pods.Items[right].Name
	})
	c.mu.Lock()
	start := c.podScanOffsets[namespace] % len(pods.Items)
	c.mu.Unlock()
	next := start
	for scanned := 0; scanned < len(pods.Items); scanned++ {
		index := (start + scanned) % len(pods.Items)
		if !c.enqueuePodIfCapacity(&pods.Items[index]) {
			next = index
			break
		}
		next = (index + 1) % len(pods.Items)
	}
	c.mu.Lock()
	c.podScanOffsets[namespace] = next
	c.mu.Unlock()
}

func (c *PodCoordinator) enqueuePod(obj interface{}) {
	c.enqueuePodIfCapacity(obj)
}

func (c *PodCoordinator) handleDeletedPod(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil || strings.TrimSpace(pod.Namespace) == "" || strings.TrimSpace(pod.Name) == "" {
		return
	}
	// Deletion does not require an API read or Kubernetes write. Invalidate an
	// in-flight reconciliation before removing the synthetic observation so an
	// older GET result must be followed by a final NotFound check.
	key := pod.Namespace + "/" + pod.Name
	c.mu.Lock()
	if generation, pending := c.pending[key]; pending {
		c.pending[key] = generation + 1
	}
	c.mu.Unlock()
	c.deleteObservedPod(key)
}

// enqueuePodIfCapacity returns false only when the bounded queue is full. A
// namespace scan can then preserve its cursor and retry the same Pod after the
// worker has released capacity.
func (c *PodCoordinator) enqueuePodIfCapacity(obj interface{}) bool {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil || strings.TrimSpace(pod.Namespace) == "" || strings.TrimSpace(pod.Name) == "" {
		return true
	}
	if !c.hasNamespace(pod.Namespace) {
		return true
	}
	key := pod.Namespace + "/" + pod.Name
	c.mu.Lock()
	if generation, exists := c.pending[key]; exists {
		// Track updates while the key is queued, processing, or waiting for a
		// retry. The worker compares this generation after reconciliation and
		// requeues without releasing the key's bounded-capacity reservation.
		c.pending[key] = generation + 1
		c.mu.Unlock()
		return true
	}
	if len(c.pending) >= c.maxQueueItems {
		c.mu.Unlock()
		return false
	}
	c.pending[key] = 1
	c.mu.Unlock()
	c.queue.Add(key)
	return true
}

func (c *PodCoordinator) hasNamespace(namespace string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.bindingsByNS[namespace]
	return ok
}

func (c *PodCoordinator) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *PodCoordinator) processNext(ctx context.Context) bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(item)
	key, ok := item.(string)
	if !ok {
		c.queue.Forget(item)
		return true
	}
	c.mu.RLock()
	generation := c.pending[key]
	c.mu.RUnlock()
	if err := c.reconcilePod(ctx, key); err != nil {
		if c.queue.NumRequeues(item) < maxPodLabelRetries && ctx.Err() == nil {
			c.queue.AddRateLimited(item)
			return true
		}
		klog.ErrorS(err, "reconcile adopted pod labels", "pod", key)
	}
	c.queue.Forget(item)
	if c.finishPendingAttempt(key, generation) {
		c.queue.Add(key)
	}
	return true
}

func (c *PodCoordinator) finishPendingAttempt(key string, processedGeneration uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	currentGeneration, tracked := c.pending[key]
	if !tracked {
		return false
	}
	if currentGeneration != processedGeneration {
		return true
	}
	delete(c.pending, key)
	return false
}

func (c *PodCoordinator) reconcilePod(ctx context.Context, key string) error {
	namespace, name, found := strings.Cut(key, "/")
	if !found || namespace == "" || name == "" {
		return nil
	}
	pod, err := c.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		c.deleteObservedPod(key)
		return nil
	}
	if err != nil {
		return err
	}
	binding, found, err := c.bindingForPod(ctx, pod)
	if err != nil {
		return err
	}
	if !found {
		c.deleteObservedPod(key)
		return nil
	}
	if binding.readOnly() {
		if labelsConflict(pod.Labels, binding) {
			c.deleteObservedPod(key)
			klog.Warningf("refusing observed pod identity conflict namespace=%s pod=%s appID=%s component=%s", namespace, name, binding.AppID, binding.ComponentName)
			return nil
		}
		c.updateObservedPod(key, pod, binding)
		return nil
	}
	c.deleteObservedPod(key)
	if labelsConflict(pod.Labels, binding) && !c.labelsAreDetachedBinding(namespace, pod.Labels) {
		klog.Warningf("refusing adopted pod label conflict namespace=%s pod=%s appID=%s component=%s", namespace, name, binding.AppID, binding.ComponentName)
		return nil
	}
	if labelsComplete(pod.Labels, binding) {
		return nil
	}

	podUID := strings.TrimSpace(string(pod.UID))
	resourceVersion := strings.TrimSpace(pod.ResourceVersion)
	if podUID == "" || resourceVersion == "" {
		return fmt.Errorf("refuse adopted pod label patch without UID and resourceVersion")
	}
	labels := make(map[string]string, len(pod.Labels)+3)
	for key, value := range pod.Labels {
		labels[key] = value
	}
	for key, value := range expectedLabels(binding) {
		labels[key] = value
	}
	payload, err := json.Marshal([]map[string]interface{}{
		{"op": "test", "path": "/metadata/uid", "value": podUID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": resourceVersion},
		{"op": "add", "path": "/metadata/labels", "value": labels},
	})
	if err != nil {
		return fmt.Errorf("marshal adopted pod labels: %w", err)
	}
	if _, err := c.client.CoreV1().Pods(namespace).Patch(ctx, name, types.JSONPatchType, payload, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("patch adopted pod metadata: %w", err)
	}
	return nil
}

func (c *PodCoordinator) updateObservedPod(key string, pod *corev1.Pod, binding SourceBinding) {
	if c == nil || pod == nil || c.observe == nil {
		return
	}
	c.observeMu.Lock()
	defer c.observeMu.Unlock()
	next := syntheticObservedPodFromSource(pod, binding)
	c.mu.Lock()
	current, active := c.bindingsByNS[binding.Namespace][binding.controllerKey()]
	if !active || current != binding || !current.readOnly() {
		c.mu.Unlock()
		return
	}
	previous, found := c.observedPods[key]
	c.observedPods[key] = observedPodState{binding: binding, pod: next}
	c.mu.Unlock()
	if found && previous.binding == binding && previous.pod != nil && previous.pod.UID == next.UID {
		c.notifyObservedPod(previous.pod, next)
		return
	}
	if found {
		c.notifyObservedPod(previous.pod, nil)
	}
	c.notifyObservedPod(nil, next)
}

func (c *PodCoordinator) deleteObservedPod(key string) {
	if c == nil || c.observe == nil {
		return
	}
	c.observeMu.Lock()
	defer c.observeMu.Unlock()
	c.mu.Lock()
	observed, found := c.observedPods[key]
	if found {
		delete(c.observedPods, key)
	}
	c.mu.Unlock()
	if found {
		c.notifyObservedPod(observed.pod, nil)
	}
}

func (c *PodCoordinator) notifyObservedPod(oldPod, newPod *corev1.Pod) {
	if c != nil && c.observe != nil {
		c.observe(oldPod, newPod)
	}
}

func syntheticObservedPodFromSource(pod *corev1.Pod, binding SourceBinding) *corev1.Pod {
	if pod == nil {
		return nil
	}
	result := pod.DeepCopy()
	if result.Labels == nil {
		result.Labels = make(map[string]string)
	}
	for key, value := range expectedLabels(binding) {
		result.Labels[key] = value
	}
	return result
}

func (c *PodCoordinator) bindingForPod(ctx context.Context, pod *corev1.Pod) (SourceBinding, bool, error) {
	if pod == nil {
		return SourceBinding{}, false, nil
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		switch strings.ToLower(owner.Kind) {
		case "statefulset", "deployment", "daemonset":
			if binding, ok := c.bindingForController(pod.Namespace, owner.Kind, owner.UID); ok {
				return binding, true, nil
			}
		case "job":
			if binding, ok := c.bindingForController(pod.Namespace, owner.Kind, owner.UID); ok {
				return binding, true, nil
			}
			job, err := c.client.BatchV1().Jobs(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return SourceBinding{}, false, fmt.Errorf("get pod job owner %s: %w", owner.Name, err)
			}
			if job.UID != owner.UID {
				continue
			}
			for _, jobOwner := range job.OwnerReferences {
				if jobOwner.Controller != nil && *jobOwner.Controller && strings.EqualFold(jobOwner.Kind, "CronJob") {
					if binding, ok := c.bindingForController(pod.Namespace, jobOwner.Kind, jobOwner.UID); ok {
						return binding, true, nil
					}
				}
			}
		case "replicaset":
			rs, err := c.client.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return SourceBinding{}, false, fmt.Errorf("get pod replicaset owner %s: %w", owner.Name, err)
			}
			if rs.UID != owner.UID {
				continue
			}
			for _, rsOwner := range rs.OwnerReferences {
				if rsOwner.Controller != nil && *rsOwner.Controller && strings.EqualFold(rsOwner.Kind, "Deployment") {
					if binding, ok := c.bindingForController(pod.Namespace, rsOwner.Kind, rsOwner.UID); ok {
						return binding, true, nil
					}
				}
			}
		}
	}
	return SourceBinding{}, false, nil
}

func (c *PodCoordinator) bindingForController(namespace, kind string, uid types.UID) (SourceBinding, bool) {
	key := strings.ToLower(strings.TrimSpace(namespace)) + "/" + strings.ToLower(strings.TrimSpace(kind)) + "/" + string(uid)
	c.mu.RLock()
	defer c.mu.RUnlock()
	binding, found := c.bindingsByNS[namespace][key]
	return binding, found
}

func expectedLabels(binding SourceBinding) map[string]string {
	return map[string]string{
		config.LabelAppID:         binding.AppID,
		config.LabelComponentName: naming.BoundedLabelValue(binding.ComponentName),
		config.LabelComponentID:   strconv.Itoa(binding.ComponentID),
	}
}

func labelsComplete(labels map[string]string, binding SourceBinding) bool {
	for key, value := range expectedLabels(binding) {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func labelsConflict(labels map[string]string, binding SourceBinding) bool {
	if len(labels) == 0 {
		return false
	}
	for key, value := range expectedLabels(binding) {
		if existing := strings.TrimSpace(labels[key]); existing != "" && existing != value {
			return true
		}
	}
	return false
}

func (c *PodCoordinator) labelsAreDetachedBinding(namespace string, labels map[string]string) bool {
	if c == nil || strings.TrimSpace(namespace) == "" {
		return false
	}
	for _, key := range []string{
		config.LabelAppID,
		config.LabelComponentName,
		config.LabelComponentID,
	} {
		if strings.TrimSpace(labels[key]) == "" {
			return false
		}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, claimed := c.labelClaimsByNS[namespace][managedLabelClaimKey(labels)]; claimed {
		return false
	}
	return true
}

func managedLabelClaimKey(labels map[string]string) string {
	return strings.Join([]string{
		strings.TrimSpace(labels[config.LabelAppID]),
		strings.TrimSpace(labels[config.LabelComponentName]),
		strings.TrimSpace(labels[config.LabelComponentID]),
	}, "\x00")
}

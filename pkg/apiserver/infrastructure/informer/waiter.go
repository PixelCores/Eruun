package informer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/async"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

const (
	statusSyncSubmitTimeout = 100 * time.Millisecond
	podRestartConfigTimeout = 2 * time.Second
	podOwnerKindReplicaSet  = "ReplicaSet"
)

// ResourceReadyWaiter 资源就绪等待器 - 基于 Informer 事件驱动
type ResourceReadyWaiter struct {
	// waiters 存储等待中的资源
	// key 格式: "resourceType/namespace/name"（组件等待使用 appID 作为 namespace）
	waiters sync.Map
	// statusSyncFunc 状态同步回调（更新数据库）
	statusSyncFunc StatusSyncFunc
	// statusSyncExecutor 为状态同步回调提供有界异步执行，避免散点裸协程。
	statusSyncExecutor *async.BoundedExecutor
	statusSyncMu       sync.Mutex
	statusSyncLanes    map[componentStatusSyncKey]*componentStatusSyncLane
	statusSyncEpoch    uint64
	statusSyncSignal   chan struct{}
	statusSyncStop     chan struct{}
	statusSyncWG       sync.WaitGroup
	podGenerationMu    sync.RWMutex
	podGeneration      uint64
	podGenerationLive  bool
	closeOnce          sync.Once
	pods               *podTracker
	podRestarts        *podRestartTracker
	podRestartConfigFn PodRestartMonitorConfigFunc
	podRestartTrigger  DeploymentPodRestartTriggerFunc
	now                func() time.Time
}

type componentStatusSyncKey struct {
	appID       string
	componentID int
}

type componentStatusSyncLane struct {
	latest *ComponentStatusUpdate
	epoch  uint64
	active bool
}

type podStatusInfo struct {
	componentKey   string
	appID          string
	componentName  string
	componentID    int
	ready          bool
	images         map[string]struct{}
	annotations    map[string]string
	abnormalReason string
	updatedAt      time.Time
}

type componentSnapshot struct {
	appID         string
	componentName string
	componentID   int
	readyCount    int32
	totalCount    int32
	lastAbnormal  string
}

type podTracker struct {
	mu   sync.Mutex
	pods map[string]podStatusInfo
}

type podRestartEventMeta struct {
	namespace     string
	podName       string
	appID         string
	componentName string
	componentID   int
}

type podRestartState struct {
	lastRestartTotal int32
	restartTimes     []time.Time
	lastTriggeredAt  time.Time
}

type podRestartTracker struct {
	mu   sync.Mutex
	pods map[string]*podRestartState
}

func newPodTracker() *podTracker {
	return &podTracker{pods: make(map[string]podStatusInfo)}
}

func newPodRestartTracker() *podRestartTracker {
	return &podRestartTracker{pods: make(map[string]*podRestartState)}
}

func (t *podTracker) reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pods = make(map[string]podStatusInfo)
}

func (t *podRestartTracker) reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pods = make(map[string]*podRestartState)
}

// NewResourceReadyWaiter 创建等待器
func NewResourceReadyWaiter() *ResourceReadyWaiter {
	waiter := &ResourceReadyWaiter{
		pods:               newPodTracker(),
		podRestarts:        newPodRestartTracker(),
		statusSyncExecutor: async.NewBoundedExecutor("informer-status-sync", 2, 256),
		statusSyncLanes:    make(map[componentStatusSyncKey]*componentStatusSyncLane),
		statusSyncSignal:   make(chan struct{}, 1),
		statusSyncStop:     make(chan struct{}),
		now:                time.Now,
	}
	waiter.statusSyncWG.Add(1)
	go waiter.runStatusSyncRetry()
	return waiter
}

// Close releases async resources created by waiter.
func (w *ResourceReadyWaiter) Close() {
	if w == nil {
		return
	}
	w.closeOnce.Do(func() {
		close(w.statusSyncStop)
		w.fencePodSnapshotGenerations()
		if w.statusSyncExecutor != nil {
			w.statusSyncExecutor.Close()
		}
		w.statusSyncWG.Wait()
		w.statusSyncMu.Lock()
		w.statusSyncEpoch++
		w.statusSyncLanes = make(map[componentStatusSyncKey]*componentStatusSyncLane)
		w.statusSyncMu.Unlock()
	})
}

// ResetPodSnapshots clears informer-derived pod state while preserving waiters and callbacks.
func (w *ResourceReadyWaiter) ResetPodSnapshots() {
	if w == nil {
		return
	}
	w.podGenerationMu.Lock()
	w.podGeneration++
	w.podGenerationLive = false
	w.resetPodSnapshotsLocked()
	w.podGenerationMu.Unlock()
}

func (w *ResourceReadyWaiter) beginPodSnapshotGeneration() uint64 {
	if w == nil {
		return 0
	}
	w.podGenerationMu.Lock()
	defer w.podGenerationMu.Unlock()
	w.podGeneration++
	w.podGenerationLive = true
	w.resetPodSnapshotsLocked()
	return w.podGeneration
}

func (w *ResourceReadyWaiter) endPodSnapshotGeneration(generation uint64) {
	if w == nil || generation == 0 {
		return
	}
	w.podGenerationMu.Lock()
	defer w.podGenerationMu.Unlock()
	if !w.podGenerationLive || w.podGeneration != generation {
		return
	}
	w.podGeneration++
	w.podGenerationLive = false
	w.resetPodSnapshotsLocked()
}

func (w *ResourceReadyWaiter) fencePodSnapshotGenerations() {
	if w == nil {
		return
	}
	w.podGenerationMu.Lock()
	defer w.podGenerationMu.Unlock()
	w.podGeneration++
	w.podGenerationLive = false
	w.resetPodSnapshotsLocked()
}

func (w *ResourceReadyWaiter) resetPodSnapshotsLocked() {
	w.resetStatusSyncGeneration()
	w.pods.reset()
	w.podRestarts.reset()
}

// SetStatusSyncFunc 设置状态同步回调函数
func (w *ResourceReadyWaiter) SetStatusSyncFunc(fn StatusSyncFunc) {
	w.statusSyncFunc = fn
}

// SetPodRestartMonitorConfigFunc 设置 Pod 重启监控配置读取回调。
func (w *ResourceReadyWaiter) SetPodRestartMonitorConfigFunc(fn PodRestartMonitorConfigFunc) {
	w.podRestartConfigFn = fn
}

// SetDeploymentPodRestartTriggerFunc 设置 Deployment Pod 重启阈值触发回调。
func (w *ResourceReadyWaiter) SetDeploymentPodRestartTriggerFunc(fn DeploymentPodRestartTriggerFunc) {
	w.podRestartTrigger = fn
}

// buildKey 构建唯一键
func buildKey(resourceType ResourceType, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", resourceType, namespace, name)
}

func buildPodKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func buildComponentKey(appID, componentName string) string {
	return fmt.Sprintf("%s/%s", appID, componentName)
}

// WaitForComponentReady 等待组件就绪（基于 Pod 事件）
func (w *ResourceReadyWaiter) WaitForComponentReady(ctx context.Context, appID, componentName string, desiredReplicas int32, timeout time.Duration) error {
	return w.WaitForComponentReadyWithOptions(ctx, appID, componentName, desiredReplicas, ComponentReadyWaitOptions{}, timeout)
}

// WaitForComponentReadyWithImages waits for ready pods whose Pod spec contains all expected images.
// Empty expectedImages preserves the original component-level readiness behavior.
func (w *ResourceReadyWaiter) WaitForComponentReadyWithImages(ctx context.Context, appID, componentName string, desiredReplicas int32, expectedImages []string, timeout time.Duration) error {
	return w.WaitForComponentReadyWithOptions(ctx, appID, componentName, desiredReplicas, ComponentReadyWaitOptions{
		ExpectedImages: expectedImages,
	}, timeout)
}

// WaitForComponentReadyWithOptions waits for ready pods matching the provided filters.
// Empty filters preserve the original component-level readiness behavior.
func (w *ResourceReadyWaiter) WaitForComponentReadyWithOptions(ctx context.Context, appID, componentName string, desiredReplicas int32, options ComponentReadyWaitOptions, timeout time.Duration) error {
	if desiredReplicas <= 0 {
		return fmt.Errorf("component %s/%s desired replicas must be greater than 0", appID, componentName)
	}
	key := buildKey(ResourceTypeComponent, appID, componentName)
	normalizedOptions := normalizeComponentReadyWaitOptions(options)

	entry := &WaitEntry{
		Key:                 key,
		ResourceType:        ResourceTypeComponent,
		ReadyChan:           make(chan struct{}),
		ErrorChan:           make(chan error, 1),
		CreatedAt:           time.Now(),
		DesiredReplicas:     desiredReplicas,
		ExpectedImages:      normalizedOptions.ExpectedImages,
		ExpectedAnnotations: normalizedOptions.ExpectedAnnotations,
	}

	// 注册等待
	w.waiters.Store(key, entry)
	defer w.waiters.Delete(key)

	klog.V(4).Infof("Waiting for component %s/%s to be ready (timeout: %v, expectedImages: %v, expectedAnnotations: %v)", appID, componentName, timeout, normalizedOptions.ExpectedImages, normalizedOptions.ExpectedAnnotations)

	if w.isComponentReadySnapshot(appID, componentName, desiredReplicas, normalizedOptions) {
		klog.V(4).Infof("Component %s/%s already ready from snapshot", appID, componentName)
		entry.Close()
		return nil
	}

	select {
	case <-entry.ReadyChan:
		klog.V(4).Infof("Component %s/%s is ready", appID, componentName)
		return nil
	case err := <-entry.ErrorChan:
		klog.V(4).Infof("Component %s/%s wait error: %v", appID, componentName, err)
		return err
	case <-ctx.Done():
		return NewWaitError(config.StatusCancelled, fmt.Errorf("component %s/%s cancelled: %w", appID, componentName, ctx.Err()))
	case <-time.After(timeout):
		if snapshot, ok := w.componentSnapshotForOptions(appID, componentName, normalizedOptions); ok {
			if abnormal := strings.TrimSpace(snapshot.lastAbnormal); abnormal != "" {
				return NewWaitErrorWithAbnormal(
					config.StatusFailed,
					fmt.Errorf("component %s/%s timeout after %v with abnormal pod state: %s", appID, componentName, timeout, abnormal),
					abnormal,
				)
			}
		}
		return NewWaitError(config.StatusTimeout, fmt.Errorf("component %s/%s timeout after %v", appID, componentName, timeout))
	}
}

// OnPodAdd 处理 Pod 创建事件 - 由 Informer 调用
func (w *ResourceReadyWaiter) OnPodAdd(pod *corev1.Pod) {
	if !w.lockPodSnapshotHandler(0, false) {
		return
	}
	defer w.podGenerationMu.RUnlock()
	w.onPodUpdate(nil, pod)
}

// OnPodUpdate 处理 Pod 更新事件 - 由 Informer 调用
func (w *ResourceReadyWaiter) OnPodUpdate(oldPod, newPod *corev1.Pod) {
	if !w.lockPodSnapshotHandler(0, false) {
		return
	}
	defer w.podGenerationMu.RUnlock()
	w.onPodUpdate(oldPod, newPod)
}

func (w *ResourceReadyWaiter) onPodAddForGeneration(generation uint64, pod *corev1.Pod) {
	if !w.lockPodSnapshotHandler(generation, true) {
		return
	}
	defer w.podGenerationMu.RUnlock()
	w.onPodUpdate(nil, pod)
}

func (w *ResourceReadyWaiter) onPodUpdateForGeneration(generation uint64, oldPod, newPod *corev1.Pod) {
	if !w.lockPodSnapshotHandler(generation, true) {
		return
	}
	defer w.podGenerationMu.RUnlock()
	w.onPodUpdate(oldPod, newPod)
}

func (w *ResourceReadyWaiter) onPodUpdate(oldPod, newPod *corev1.Pod) {
	if newPod == nil {
		return
	}

	podKey := buildPodKey(newPod.Namespace, newPod.Name)
	appID, componentName, componentID, ok := extractComponentMeta(newPod.Labels)
	if !ok {
		w.updatePodStatus(podKey, nil)
		w.clearPodRestartStatus(podKey)
		return
	}

	componentKey := buildComponentKey(appID, componentName)
	info := &podStatusInfo{
		componentKey:   componentKey,
		appID:          appID,
		componentName:  componentName,
		componentID:    componentID,
		ready:          isPodReady(newPod),
		images:         podImageSet(newPod),
		annotations:    podAnnotations(newPod),
		abnormalReason: kube.ExtractPodAbnormalReason(newPod),
		updatedAt:      time.Now(),
	}
	w.updatePodStatus(podKey, info)
	w.handlePodRestartUpdate(oldPod, newPod, podKey, appID, componentName, componentID)
}

// OnPodDelete 处理 Pod 删除事件
func (w *ResourceReadyWaiter) OnPodDelete(pod *corev1.Pod) {
	if !w.lockPodSnapshotHandler(0, false) {
		return
	}
	defer w.podGenerationMu.RUnlock()
	w.onPodDelete(pod)
}

func (w *ResourceReadyWaiter) onPodDeleteForGeneration(generation uint64, pod *corev1.Pod) {
	if !w.lockPodSnapshotHandler(generation, true) {
		return
	}
	defer w.podGenerationMu.RUnlock()
	w.onPodDelete(pod)
}

func (w *ResourceReadyWaiter) onPodDelete(pod *corev1.Pod) {
	if pod == nil {
		return
	}
	podKey := buildPodKey(pod.Namespace, pod.Name)
	w.updatePodStatus(podKey, nil)
	w.clearPodRestartStatus(podKey)
}

func (w *ResourceReadyWaiter) lockPodSnapshotHandler(generation uint64, scoped bool) bool {
	if w == nil {
		return false
	}
	w.podGenerationMu.RLock()
	select {
	case <-w.statusSyncStop:
		w.podGenerationMu.RUnlock()
		return false
	default:
	}
	if scoped && (!w.podGenerationLive || w.podGeneration != generation) {
		w.podGenerationMu.RUnlock()
		return false
	}
	return true
}

// GetPendingCount 获取等待中的资源数量（用于监控）
func (w *ResourceReadyWaiter) GetPendingCount() int {
	count := 0
	w.waiters.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// GetPendingKeys 获取所有等待中的资源键（用于调试）
func (w *ResourceReadyWaiter) GetPendingKeys() []string {
	var keys []string
	w.waiters.Range(func(key, _ interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys
}

func extractComponentMeta(labels map[string]string) (string, string, int, bool) {
	if len(labels) == 0 {
		return "", "", 0, false
	}
	appID := labels[config.LabelAppID]
	componentName := labels[config.LabelComponentName]
	componentIDStr := labels[config.LabelComponentID]
	if appID == "" || componentName == "" || componentIDStr == "" {
		return "", "", 0, false
	}
	componentID, err := strconv.Atoi(componentIDStr)
	if err != nil {
		return "", "", 0, false
	}
	return appID, componentName, componentID, true
}

func isPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.DeletionTimestamp != nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (w *ResourceReadyWaiter) handlePodRestartUpdate(oldPod, newPod *corev1.Pod, podKey, appID, componentName string, componentID int) {
	if w == nil || w.podRestarts == nil || newPod == nil {
		return
	}
	if !isDeploymentOwnedPod(newPod) {
		w.clearPodRestartStatus(podKey)
		return
	}

	newTotal := totalPodRestartCount(newPod)
	oldTotal, hasOld := int32(0), false
	if oldPod != nil {
		oldTotal = totalPodRestartCount(oldPod)
		hasOld = true
	}
	if !hasOld {
		w.podRestarts.observeBaseline(podKey, newTotal)
		return
	}
	if newTotal <= oldTotal {
		w.podRestarts.observeBaseline(podKey, newTotal)
		return
	}

	cfg, ok := w.loadPodRestartMonitorConfig(newPod.Namespace, newPod.Name)
	if !ok {
		w.podRestarts.observeBaseline(podKey, newTotal)
		return
	}

	meta := podRestartEventMeta{
		namespace:     newPod.Namespace,
		podName:       newPod.Name,
		appID:         appID,
		componentName: componentName,
		componentID:   componentID,
	}
	now := time.Now
	if w.now != nil {
		now = w.now
	}
	event, triggered := w.podRestarts.record(podKey, oldTotal, newTotal, hasOld, cfg, meta, now())
	if !triggered {
		return
	}
	if w.podRestartTrigger != nil {
		w.podRestartTrigger(event)
		return
	}
	klog.InfoS("deployment pod restart threshold reached",
		"namespace", event.Namespace,
		"pod", event.PodName,
		"appID", event.AppID,
		"component", event.ComponentName,
		"componentID", event.ComponentID,
		"window", event.Window.String(),
		"threshold", event.Threshold,
		"restartCount", event.RestartCount,
	)
}

func (w *ResourceReadyWaiter) loadPodRestartMonitorConfig(namespace, podName string) (PodRestartMonitorConfig, bool) {
	if w == nil || w.podRestartConfigFn == nil {
		return PodRestartMonitorConfig{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), podRestartConfigTimeout)
	defer cancel()
	cfg, err := w.podRestartConfigFn(ctx)
	if err != nil {
		klog.ErrorS(err, "load pod restart monitor config failed", "namespace", namespace, "pod", podName)
		return PodRestartMonitorConfig{}, false
	}
	if cfg.Window <= 0 {
		klog.ErrorS(fmt.Errorf("window must be greater than 0"), "invalid pod restart monitor config", "namespace", namespace, "pod", podName)
		return PodRestartMonitorConfig{}, false
	}
	if cfg.Threshold <= 0 {
		klog.ErrorS(fmt.Errorf("threshold must be greater than 0"), "invalid pod restart monitor config", "namespace", namespace, "pod", podName)
		return PodRestartMonitorConfig{}, false
	}
	return cfg, true
}

func (w *ResourceReadyWaiter) clearPodRestartStatus(podKey string) {
	if w == nil || w.podRestarts == nil {
		return
	}
	w.podRestarts.delete(podKey)
}

func isDeploymentOwnedPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == podOwnerKindReplicaSet {
			return true
		}
	}
	return false
}

func totalPodRestartCount(pod *corev1.Pod) int32 {
	if pod == nil {
		return 0
	}
	total := int32(0)
	for _, status := range pod.Status.InitContainerStatuses {
		total += status.RestartCount
	}
	for _, status := range pod.Status.ContainerStatuses {
		total += status.RestartCount
	}
	return total
}

func (t *podRestartTracker) observeBaseline(podKey string, total int32) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.pods[podKey]
	if !ok {
		t.pods[podKey] = &podRestartState{lastRestartTotal: total}
		return
	}
	if total < state.lastRestartTotal {
		state.restartTimes = nil
		state.lastTriggeredAt = time.Time{}
	}
	state.lastRestartTotal = total
}

func (t *podRestartTracker) record(podKey string, oldTotal, newTotal int32, hasOld bool, cfg PodRestartMonitorConfig, meta podRestartEventMeta, now time.Time) (DeploymentPodRestartEvent, bool) {
	if t == nil {
		return DeploymentPodRestartEvent{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	state, ok := t.pods[podKey]
	if !ok {
		baseline := newTotal
		if hasOld {
			baseline = oldTotal
		}
		state = &podRestartState{lastRestartTotal: baseline}
		t.pods[podKey] = state
	}
	if newTotal < state.lastRestartTotal {
		state.lastRestartTotal = newTotal
		state.restartTimes = nil
		state.lastTriggeredAt = time.Time{}
		return DeploymentPodRestartEvent{}, false
	}

	delta := int(newTotal - state.lastRestartTotal)
	state.lastRestartTotal = newTotal
	if delta <= 0 {
		return DeploymentPodRestartEvent{}, false
	}
	if !cfg.Enabled {
		state.restartTimes = nil
		state.lastTriggeredAt = time.Time{}
		return DeploymentPodRestartEvent{}, false
	}

	cutoff := now.Add(-cfg.Window)
	state.restartTimes = pruneRestartTimes(state.restartTimes, cutoff)
	for i := 0; i < delta; i++ {
		state.restartTimes = append(state.restartTimes, now)
	}
	restartCount := len(state.restartTimes)
	if restartCount < cfg.Threshold {
		return DeploymentPodRestartEvent{}, false
	}
	if !state.lastTriggeredAt.IsZero() && state.lastTriggeredAt.After(cutoff) {
		return DeploymentPodRestartEvent{}, false
	}
	state.lastTriggeredAt = now
	return DeploymentPodRestartEvent{
		Namespace:     meta.namespace,
		PodName:       meta.podName,
		AppID:         meta.appID,
		ComponentName: meta.componentName,
		ComponentID:   meta.componentID,
		Window:        cfg.Window,
		Threshold:     cfg.Threshold,
		RestartCount:  restartCount,
		OccurredAt:    now,
	}, true
}

func (t *podRestartTracker) delete(podKey string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pods, podKey)
}

func pruneRestartTimes(in []time.Time, cutoff time.Time) []time.Time {
	if len(in) == 0 {
		return nil
	}
	out := in[:0]
	for _, ts := range in {
		if ts.Before(cutoff) {
			continue
		}
		out = append(out, ts)
	}
	return out
}

func (w *ResourceReadyWaiter) updatePodStatus(podKey string, info *podStatusInfo) {
	if w.pods == nil {
		return
	}
	prev, prevOk, next, nextOk := w.pods.update(podKey, info)
	if nextOk {
		w.notifyComponentReady(next)
	}
	if !snapshotChanged(prevOk, prev, nextOk, next) {
		return
	}
	if nextOk {
		w.syncComponentSnapshot(next)
		return
	}
	w.syncComponentSnapshot(prev)
}

func snapshotChanged(prevOk bool, prev componentSnapshot, nextOk bool, next componentSnapshot) bool {
	if prevOk != nextOk {
		return true
	}
	if prevOk && nextOk {
		return prev.readyCount != next.readyCount || prev.totalCount != next.totalCount || prev.lastAbnormal != next.lastAbnormal
	}
	return false
}

func (t *podTracker) update(podKey string, info *podStatusInfo) (componentSnapshot, bool, componentSnapshot, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	componentKey := ""
	if info != nil {
		componentKey = info.componentKey
	} else if existing, ok := t.pods[podKey]; ok {
		componentKey = existing.componentKey
	} else {
		return componentSnapshot{}, false, componentSnapshot{}, false
	}

	prevSnapshot, prevOk := t.snapshotLocked(componentKey, ComponentReadyWaitOptions{})
	if info == nil {
		delete(t.pods, podKey)
	} else {
		t.pods[podKey] = *info
	}
	nextSnapshot, nextOk := t.snapshotLocked(componentKey, ComponentReadyWaitOptions{})
	if !nextOk && prevOk {
		nextSnapshot = componentSnapshot{
			appID:         prevSnapshot.appID,
			componentName: prevSnapshot.componentName,
			componentID:   prevSnapshot.componentID,
			readyCount:    0,
			lastAbnormal:  "",
		}
		nextOk = true
	}
	return prevSnapshot, prevOk, nextSnapshot, nextOk
}

func (t *podTracker) snapshotLocked(componentKey string, options ComponentReadyWaitOptions) (componentSnapshot, bool) {
	var snapshot componentSnapshot
	var latestTime time.Time
	found := false
	for _, info := range t.pods {
		if info.componentKey != componentKey {
			continue
		}
		if !podImagesContainAll(info.images, options.ExpectedImages) {
			continue
		}
		if !podAnnotationsContainAll(info.annotations, options.ExpectedAnnotations) {
			continue
		}
		if !found {
			snapshot.appID = info.appID
			snapshot.componentName = info.componentName
			snapshot.componentID = info.componentID
			found = true
		}
		snapshot.totalCount++
		if info.ready {
			snapshot.readyCount++
		}
		if info.abnormalReason != "" && (latestTime.IsZero() || info.updatedAt.After(latestTime)) {
			snapshot.lastAbnormal = info.abnormalReason
			latestTime = info.updatedAt
		}
	}
	return snapshot, found
}

func (t *podTracker) snapshot(componentKey string) (componentSnapshot, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotLocked(componentKey, ComponentReadyWaitOptions{})
}

func (t *podTracker) snapshotForOptions(componentKey string, options ComponentReadyWaitOptions) (componentSnapshot, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotLocked(componentKey, options)
}

func (w *ResourceReadyWaiter) componentSnapshot(appID, componentName string) (componentSnapshot, bool) {
	if w.pods == nil {
		return componentSnapshot{}, false
	}
	return w.pods.snapshot(buildComponentKey(appID, componentName))
}

func (w *ResourceReadyWaiter) componentSnapshotForOptions(appID, componentName string, options ComponentReadyWaitOptions) (componentSnapshot, bool) {
	if w.pods == nil {
		return componentSnapshot{}, false
	}
	return w.pods.snapshotForOptions(buildComponentKey(appID, componentName), options)
}

func (w *ResourceReadyWaiter) isComponentReadySnapshot(appID, componentName string, desiredReplicas int32, options ComponentReadyWaitOptions) bool {
	if desiredReplicas <= 0 {
		return false
	}
	snapshot, ok := w.componentSnapshotForOptions(appID, componentName, options)
	if !ok || snapshot.totalCount == 0 {
		return false
	}
	return snapshot.readyCount >= desiredReplicas
}

func normalizeComponentReadyWaitOptions(options ComponentReadyWaitOptions) ComponentReadyWaitOptions {
	return ComponentReadyWaitOptions{
		ExpectedImages:      normalizeExpectedImages(options.ExpectedImages),
		ExpectedAnnotations: normalizeExpectedAnnotations(options.ExpectedAnnotations),
	}
}

func normalizeExpectedImages(images []string) []string {
	if len(images) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(images))
	normalized := make([]string, 0, len(images))
	for _, image := range images {
		image = strings.TrimSpace(image)
		if image == "" {
			continue
		}
		if _, ok := seen[image]; ok {
			continue
		}
		seen[image] = struct{}{}
		normalized = append(normalized, image)
	}
	return normalized
}

func normalizeExpectedAnnotations(annotations map[string]string) map[string]string {
	if len(annotations) == 0 {
		return nil
	}
	normalized := make(map[string]string, len(annotations))
	for key, value := range annotations {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		normalized[key] = value
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func podImageSet(pod *corev1.Pod) map[string]struct{} {
	if pod == nil {
		return nil
	}
	images := make(map[string]struct{})
	addImage := func(image string) {
		image = strings.TrimSpace(image)
		if image != "" {
			images[image] = struct{}{}
		}
	}
	for _, container := range pod.Spec.InitContainers {
		addImage(container.Image)
	}
	for _, container := range pod.Spec.Containers {
		addImage(container.Image)
	}
	if len(images) == 0 {
		return nil
	}
	return images
}

func podAnnotations(pod *corev1.Pod) map[string]string {
	if pod == nil || len(pod.Annotations) == 0 {
		return nil
	}
	annotations := make(map[string]string, len(pod.Annotations))
	for key, value := range pod.Annotations {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		annotations[key] = value
	}
	if len(annotations) == 0 {
		return nil
	}
	return annotations
}

func podImagesContainAll(images map[string]struct{}, expectedImages []string) bool {
	if len(expectedImages) == 0 {
		return true
	}
	if len(images) == 0 {
		return false
	}
	for _, image := range expectedImages {
		if _, ok := images[image]; !ok {
			return false
		}
	}
	return true
}

func podAnnotationsContainAll(annotations map[string]string, expectedAnnotations map[string]string) bool {
	if len(expectedAnnotations) == 0 {
		return true
	}
	if len(annotations) == 0 {
		return false
	}
	for key, value := range expectedAnnotations {
		if annotations[key] != value {
			return false
		}
	}
	return true
}

func (w *ResourceReadyWaiter) syncComponentSnapshot(snapshot componentSnapshot) {
	if w == nil || w.statusSyncFunc == nil {
		return
	}
	update := buildStatusUpdate(snapshot)
	key, schedule := w.enqueueStatusSync(update)
	if !schedule {
		return
	}
	submitCtx, cancel := context.WithTimeout(context.Background(), statusSyncSubmitTimeout)
	defer cancel()
	if err := w.submitStatusSyncLane(submitCtx, key); err != nil {
		if errors.Is(err, async.ErrExecutorClosed) {
			w.discardStatusSyncLane(key)
			klog.V(4).Infof("skip component status sync appID=%s componentID=%d: executor closed", snapshot.appID, snapshot.componentID)
			return
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			w.deferStatusSyncLane(key)
			klog.Warningf("defer component status sync appID=%s componentID=%d: submit timeout", snapshot.appID, snapshot.componentID)
			return
		}
		w.deferStatusSyncLane(key)
		klog.Warningf("submit component status sync failed appID=%s componentID=%d: %v", snapshot.appID, snapshot.componentID, err)
	}
}

func buildStatusUpdate(snapshot componentSnapshot) *ComponentStatusUpdate {
	ready := snapshot.readyCount
	lastAbnormal := snapshot.lastAbnormal
	total := snapshot.totalCount
	status := componentStatusFromSnapshot(snapshot)
	return &ComponentStatusUpdate{
		AppID:         snapshot.appID,
		ComponentID:   snapshot.componentID,
		ComponentName: snapshot.componentName,
		Status:        &status,
		ReadyReplicas: &ready,
		Replicas:      &total,
		LastAbnormal:  &lastAbnormal,
	}
}

func (w *ResourceReadyWaiter) executeStatusSync(update *ComponentStatusUpdate) {
	if w == nil || w.statusSyncFunc == nil || update == nil {
		return
	}
	select {
	case <-w.statusSyncStop:
		return
	default:
	}
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic: %v", r)
			klog.ErrorS(err, "status sync callback panic recovered", "appID", update.AppID, "component", update.ComponentName)
		}
	}()
	w.statusSyncFunc(update)
}

func (w *ResourceReadyWaiter) enqueueStatusSync(update *ComponentStatusUpdate) (componentStatusSyncKey, bool) {
	key := componentStatusSyncKey{}
	if w == nil || update == nil {
		return key, false
	}
	key = componentStatusSyncKey{appID: update.AppID, componentID: update.ComponentID}
	w.statusSyncMu.Lock()
	defer w.statusSyncMu.Unlock()
	select {
	case <-w.statusSyncStop:
		return key, false
	default:
	}
	lane := w.statusSyncLanes[key]
	if lane == nil {
		lane = &componentStatusSyncLane{}
		w.statusSyncLanes[key] = lane
	}
	lane.latest = update
	lane.epoch = w.statusSyncEpoch
	if lane.active {
		return key, false
	}
	lane.active = true
	return key, true
}

func (w *ResourceReadyWaiter) submitStatusSyncLane(ctx context.Context, key componentStatusSyncKey) error {
	if w == nil {
		return nil
	}
	if w.statusSyncExecutor == nil {
		w.drainStatusSyncLane(key)
		return nil
	}
	return w.statusSyncExecutor.Submit(ctx, func() {
		w.drainStatusSyncLane(key)
	})
}

func (w *ResourceReadyWaiter) drainStatusSyncLane(key componentStatusSyncKey) {
	for {
		select {
		case <-w.statusSyncStop:
			w.discardStatusSyncLane(key)
			return
		default:
		}

		update, epoch, ok := w.takeLatestStatusSync(key)
		if !ok {
			return
		}
		w.executeStatusSyncIfCurrent(update, epoch)
	}
}

func (w *ResourceReadyWaiter) executeStatusSyncIfCurrent(update *ComponentStatusUpdate, epoch uint64) {
	if w == nil {
		return
	}
	w.podGenerationMu.RLock()
	defer w.podGenerationMu.RUnlock()
	if w.isCurrentStatusSyncEpoch(epoch) {
		w.executeStatusSync(update)
	}
}

func (w *ResourceReadyWaiter) takeLatestStatusSync(key componentStatusSyncKey) (*ComponentStatusUpdate, uint64, bool) {
	w.statusSyncMu.Lock()
	defer w.statusSyncMu.Unlock()
	lane := w.statusSyncLanes[key]
	if lane == nil {
		return nil, 0, false
	}
	if lane.latest == nil {
		delete(w.statusSyncLanes, key)
		return nil, 0, false
	}
	update := lane.latest
	epoch := lane.epoch
	lane.latest = nil
	return update, epoch, true
}

func (w *ResourceReadyWaiter) isCurrentStatusSyncEpoch(epoch uint64) bool {
	w.statusSyncMu.Lock()
	defer w.statusSyncMu.Unlock()
	return epoch == w.statusSyncEpoch
}

func (w *ResourceReadyWaiter) deferStatusSyncLane(key componentStatusSyncKey) {
	if w == nil {
		return
	}
	w.statusSyncMu.Lock()
	lane := w.statusSyncLanes[key]
	if lane != nil {
		lane.active = false
		if lane.latest == nil {
			delete(w.statusSyncLanes, key)
		}
	}
	w.statusSyncMu.Unlock()
	w.signalStatusSyncRetry()
}

func (w *ResourceReadyWaiter) discardStatusSyncLane(key componentStatusSyncKey) {
	if w == nil {
		return
	}
	w.statusSyncMu.Lock()
	delete(w.statusSyncLanes, key)
	w.statusSyncMu.Unlock()
}

func (w *ResourceReadyWaiter) signalStatusSyncRetry() {
	if w == nil {
		return
	}
	select {
	case <-w.statusSyncStop:
		return
	default:
	}
	select {
	case w.statusSyncSignal <- struct{}{}:
	default:
	}
}

func (w *ResourceReadyWaiter) runStatusSyncRetry() {
	defer w.statusSyncWG.Done()
	for {
		select {
		case <-w.statusSyncStop:
			return
		case <-w.statusSyncSignal:
			w.retryDeferredStatusSyncLanes()
		}
	}
}

func (w *ResourceReadyWaiter) retryDeferredStatusSyncLanes() {
	for {
		key, ok := w.activateDeferredStatusSyncLane()
		if !ok {
			return
		}
		if err := w.submitStatusSyncLane(context.Background(), key); err != nil {
			if errors.Is(err, async.ErrExecutorClosed) {
				w.discardStatusSyncLane(key)
				return
			}
			w.deferStatusSyncLane(key)
			return
		}
	}
}

func (w *ResourceReadyWaiter) activateDeferredStatusSyncLane() (componentStatusSyncKey, bool) {
	w.statusSyncMu.Lock()
	defer w.statusSyncMu.Unlock()
	select {
	case <-w.statusSyncStop:
		return componentStatusSyncKey{}, false
	default:
	}
	for key, lane := range w.statusSyncLanes {
		if lane == nil || lane.active || lane.latest == nil {
			continue
		}
		lane.active = true
		return key, true
	}
	return componentStatusSyncKey{}, false
}

func (w *ResourceReadyWaiter) resetStatusSyncGeneration() {
	w.statusSyncMu.Lock()
	defer w.statusSyncMu.Unlock()
	w.statusSyncEpoch++
	for key, lane := range w.statusSyncLanes {
		if lane == nil || !lane.active {
			delete(w.statusSyncLanes, key)
			continue
		}
		lane.latest = nil
		lane.epoch = w.statusSyncEpoch
	}
}

func componentStatusFromSnapshot(snapshot componentSnapshot) config.ComponentStatus {
	if snapshot.lastAbnormal != "" {
		return config.ComponentStatusFailed
	}
	if snapshot.totalCount == 0 {
		return config.ComponentStatusUnknown
	}
	if snapshot.readyCount >= snapshot.totalCount {
		return config.ComponentStatusRunning
	}
	return config.ComponentStatusPending
}

func (w *ResourceReadyWaiter) notifyComponentReady(snapshot componentSnapshot) {
	key := buildKey(ResourceTypeComponent, snapshot.appID, snapshot.componentName)
	entryVal, ok := w.waiters.Load(key)
	if !ok {
		return
	}
	entry := entryVal.(*WaitEntry)
	if entry.IsClosed() {
		return
	}
	options := ComponentReadyWaitOptions{
		ExpectedImages:      entry.ExpectedImages,
		ExpectedAnnotations: entry.ExpectedAnnotations,
	}
	if w.isComponentReadySnapshot(snapshot.appID, snapshot.componentName, entry.DesiredReplicas, options) {
		entry.Close()
	}
}

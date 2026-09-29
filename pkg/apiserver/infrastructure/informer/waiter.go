package informer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

const (
	podRestartConfigTimeout = 2 * time.Second
	podOwnerKindReplicaSet  = "ReplicaSet"
)

// ResourceReadyWaiter synchronizes Controller Pod snapshots and restart events.
type ResourceReadyWaiter struct {
	// statusSyncFunc 状态同步回调（更新数据库）
	statusSyncFunc StatusSyncFunc
	// statusSyncQueue serializes each component while two workers bound callbacks.
	statusSyncQueue    workqueue.TypedInterface[componentStatusSyncKey]
	statusSyncMu       sync.Mutex
	statusSyncLatest   map[componentStatusSyncKey]componentStatusSyncUpdate
	statusSyncEpoch    uint64
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

type componentStatusSyncUpdate struct {
	update *ComponentStatusUpdate
	epoch  uint64
}

type podStatusInfo struct {
	componentKey   string
	appID          string
	componentName  string
	componentID    int
	ready          bool
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

// NewResourceReadyWaiter creates the Controller Pod event handler.
func NewResourceReadyWaiter() *ResourceReadyWaiter {
	waiter := &ResourceReadyWaiter{
		pods:             newPodTracker(),
		podRestarts:      newPodRestartTracker(),
		statusSyncQueue:  workqueue.NewTyped[componentStatusSyncKey](),
		statusSyncLatest: make(map[componentStatusSyncKey]componentStatusSyncUpdate),
		statusSyncStop:   make(chan struct{}),
		now:              time.Now,
	}
	waiter.statusSyncWG.Add(2)
	for range 2 {
		go waiter.runStatusSyncWorker()
	}
	return waiter
}

// Close releases async resources created by waiter.
func (w *ResourceReadyWaiter) Close() {
	if w == nil {
		return
	}
	w.closeOnce.Do(func() {
		close(w.statusSyncStop)
		w.statusSyncQueue.ShutDown()
		w.fencePodSnapshotGenerations()
		w.statusSyncWG.Wait()
	})
}

// ResetPodSnapshots clears informer-derived pod state while preserving callbacks.
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

func buildPodKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func buildComponentKey(appID, componentName string) string {
	return fmt.Sprintf("%s/%s", appID, componentName)
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

	prevSnapshot, prevOk := t.snapshotLocked(componentKey)
	if info == nil {
		delete(t.pods, podKey)
	} else {
		t.pods[podKey] = *info
	}
	nextSnapshot, nextOk := t.snapshotLocked(componentKey)
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

func (t *podTracker) snapshotLocked(componentKey string) (componentSnapshot, bool) {
	var snapshot componentSnapshot
	var latestTime time.Time
	found := false
	for _, info := range t.pods {
		if info.componentKey != componentKey {
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
	key := componentStatusSyncKey{appID: update.AppID, componentID: update.ComponentID}
	w.statusSyncMu.Lock()
	defer w.statusSyncMu.Unlock()
	select {
	case <-w.statusSyncStop:
		return
	default:
	}
	w.statusSyncLatest[key] = componentStatusSyncUpdate{update: update, epoch: w.statusSyncEpoch}
	w.statusSyncQueue.Add(key)
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

func (w *ResourceReadyWaiter) runStatusSyncWorker() {
	defer w.statusSyncWG.Done()
	for {
		key, shutdown := w.statusSyncQueue.Get()
		if shutdown {
			return
		}
		// ShutDown still returns queued keys; Close must discard their callbacks.
		select {
		case <-w.statusSyncStop:
			w.statusSyncQueue.Done(key)
			return
		default:
		}
		update, epoch, ok := w.takeLatestStatusSync(key)
		if ok {
			w.executeStatusSyncIfCurrent(update, epoch)
		}
		w.statusSyncQueue.Done(key)
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
	latest, ok := w.statusSyncLatest[key]
	delete(w.statusSyncLatest, key)
	return latest.update, latest.epoch, ok
}

func (w *ResourceReadyWaiter) isCurrentStatusSyncEpoch(epoch uint64) bool {
	w.statusSyncMu.Lock()
	defer w.statusSyncMu.Unlock()
	return epoch == w.statusSyncEpoch
}

func (w *ResourceReadyWaiter) resetStatusSyncGeneration() {
	w.statusSyncMu.Lock()
	defer w.statusSyncMu.Unlock()
	w.statusSyncEpoch++
	clear(w.statusSyncLatest)
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

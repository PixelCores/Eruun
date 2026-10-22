package informer

import corev1 "k8s.io/api/core/v1"

// ResetPodSnapshots clears informer-derived pod state while preserving callbacks.
func (w *ComponentStatusSyncer) ResetPodSnapshots() {
	if w == nil {
		return
	}
	w.podGenerationMu.Lock()
	w.podGeneration++
	w.podGenerationLive = false
	w.resetPodSnapshotsLocked()
	w.podGenerationMu.Unlock()
}

// OnPodAdd 处理 Pod 创建事件 - 由 Informer 调用
func (w *ComponentStatusSyncer) OnPodAdd(pod *corev1.Pod) {
	if !w.lockPodSnapshotHandler(0, false) {
		return
	}
	defer w.podGenerationMu.RUnlock()
	w.onPodUpdate(nil, pod)
}

// OnPodUpdate 处理 Pod 更新事件 - 由 Informer 调用
func (w *ComponentStatusSyncer) OnPodUpdate(oldPod, newPod *corev1.Pod) {
	if !w.lockPodSnapshotHandler(0, false) {
		return
	}
	defer w.podGenerationMu.RUnlock()
	w.onPodUpdate(oldPod, newPod)
}

// OnPodDelete 处理 Pod 删除事件
func (w *ComponentStatusSyncer) OnPodDelete(pod *corev1.Pod) {
	if !w.lockPodSnapshotHandler(0, false) {
		return
	}
	defer w.podGenerationMu.RUnlock()
	w.onPodDelete(pod)
}

package apiserver

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const runtimeIdentityLabel = "eruun.io/runtime-id"
const unassignedRuntimeIdentity = "unassigned"

// Publish one Pod through the existing Service. Kubernetes retains ownership of
// EndpointSlices and removes unready Pods using its ordinary readiness probes.
func (s *restServer) publishLeaderService(ctx context.Context) error {
	cfg := s.cfg.LeaderConfig
	if cfg.ServiceName == "" {
		return nil
	}
	if s.KubeClient == nil {
		return fmt.Errorf("Kubernetes client is not initialized")
	}
	pod, err := s.KubeClient.CoreV1().Pods(cfg.Namespace).Get(ctx, cfg.PodName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get runtime Pod: %w", err)
	}
	if pod.UID == "" || pod.DeletionTimestamp != nil {
		return fmt.Errorf("runtime Pod has no live identity")
	}
	if s.leaderPodUID != "" && s.leaderPodUID != string(pod.UID) {
		return fmt.Errorf("runtime Pod was replaced; refusing another Pod incarnation")
	}
	s.leaderPodUID = string(pod.UID)
	if pod.Labels[runtimeIdentityLabel] != s.leaderPodUID {
		patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"uid": pod.UID, "resourceVersion": pod.ResourceVersion, "labels": map[string]string{runtimeIdentityLabel: s.leaderPodUID}}})
		if err != nil {
			return fmt.Errorf("encode runtime Pod identity: %w", err)
		}
		if _, err = s.KubeClient.CoreV1().Pods(cfg.Namespace).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("label runtime Pod: %w", err)
		}
	}
	service, err := s.KubeClient.CoreV1().Services(cfg.Namespace).Get(ctx, cfg.ServiceName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get runtime Service: %w", err)
	}
	if _, ok := service.Spec.Selector[runtimeIdentityLabel]; !ok {
		return fmt.Errorf("runtime Service selector must contain %s", runtimeIdentityLabel)
	}
	for key, value := range service.Spec.Selector {
		if key != runtimeIdentityLabel && pod.Labels[key] != value {
			return fmt.Errorf("runtime Pod does not match Service selector %q", key)
		}
	}
	// Read the Service version BEFORE checking the current Lease. A delayed old
	// leader cannot overwrite a successor: its ownership check or Service CAS fails.
	lease, err := s.KubeClient.CoordinationV1().Leases(cfg.Namespace).Get(ctx, cfg.LockName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("verify runtime Lease: %w", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != cfg.ID {
		return fmt.Errorf("runtime Lease is owned by another node")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if service.Spec.Selector[runtimeIdentityLabel] == s.leaderPodUID {
		return nil
	}
	service.Spec.Selector[runtimeIdentityLabel] = s.leaderPodUID
	if _, err = s.KubeClient.CoreV1().Services(cfg.Namespace).Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("select Leader Pod: %w", err)
	}
	return nil
}

func (s *restServer) withdrawLeaderService(ctx context.Context) error {
	cfg := s.cfg.LeaderConfig
	if cfg.ServiceName == "" || s.leaderPodUID == "" {
		return nil
	}
	service, err := s.KubeClient.CoreV1().Services(cfg.Namespace).Get(ctx, cfg.ServiceName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get runtime Service: %w", err)
	}
	if service.Spec.Selector[runtimeIdentityLabel] != s.leaderPodUID {
		return nil
	}
	service.Spec.Selector[runtimeIdentityLabel] = unassignedRuntimeIdentity
	_, err = s.KubeClient.CoreV1().Services(cfg.Namespace).Update(ctx, service, metav1.UpdateOptions{})
	// A successor won the Service version race. Never retry against its version.
	if apierrors.IsConflict(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deselect old Leader Pod: %w", err)
	}
	return nil
}

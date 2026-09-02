package application

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (c *applicationsServiceImpl) ListComponentContainers(ctx context.Context, appID, componentName string) (*apisv1.ComponentContainersResponse, error) {
	appID = strings.TrimSpace(appID)
	componentName = strings.TrimSpace(componentName)
	if appID == "" || componentName == "" {
		return nil, bcode.ErrComponentNotFound
	}
	if c.ComponentRepo == nil {
		return nil, fmt.Errorf("component repository is nil")
	}

	component, err := c.ComponentRepo.FindByName(ctx, appID, componentName)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrComponentNotFound
		}
		return nil, err
	}
	if component == nil {
		return nil, bcode.ErrComponentNotFound
	}

	resp := &apisv1.ComponentContainersResponse{
		AppID:         appID,
		ComponentName: component.Name,
		ComponentType: component.ComponentType,
		Pods:          make([]apisv1.ComponentPodContainers, 0),
	}
	if !componentUsesPods(component.ComponentType) {
		return resp, nil
	}
	if c.KubeClient == nil {
		return nil, fmt.Errorf("kube client is nil")
	}

	pods, err := c.listComponentPods(ctx, appID, component)
	if err != nil {
		return nil, err
	}
	if pods == nil || len(pods.Items) == 0 {
		return resp, nil
	}

	activePods := make([]corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		pod := pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		activePods = append(activePods, pod)
	}
	if len(activePods) == 0 {
		return resp, nil
	}

	sort.SliceStable(activePods, func(i, j int) bool {
		left := activePods[i].CreationTimestamp.Time
		right := activePods[j].CreationTimestamp.Time
		if left.Equal(right) {
			return activePods[i].Name < activePods[j].Name
		}
		return left.After(right)
	})

	resp.Pods = make([]apisv1.ComponentPodContainers, 0, len(activePods))
	for i := range activePods {
		pod := activePods[i]
		resp.Pods = append(resp.Pods, apisv1.ComponentPodContainers{
			PodName:    pod.Name,
			Namespace:  pod.Namespace,
			Phase:      string(pod.Status.Phase),
			Containers: mapComponentPodContainers(&pod),
		})
	}
	return resp, nil
}

func mapComponentPodContainers(pod *corev1.Pod) []apisv1.ComponentContainerInfo {
	if pod == nil {
		return nil
	}
	statusByName := make(map[string]corev1.ContainerStatus, len(pod.Status.ContainerStatuses))
	for _, status := range pod.Status.ContainerStatuses {
		name := strings.TrimSpace(status.Name)
		if name == "" {
			continue
		}
		statusByName[name] = status
	}

	containers := make([]apisv1.ComponentContainerInfo, 0, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		name := strings.TrimSpace(container.Name)
		if name == "" {
			continue
		}
		info := apisv1.ComponentContainerInfo{
			Name:  name,
			Image: container.Image,
			State: "unknown",
		}
		if status, ok := statusByName[name]; ok {
			info.Ready = status.Ready
			info.RestartCount = status.RestartCount
			info.State, info.Reason = componentContainerState(status.State)
		}
		containers = append(containers, info)
	}
	return containers
}

func componentContainerState(state corev1.ContainerState) (string, string) {
	switch {
	case state.Running != nil:
		return "running", ""
	case state.Waiting != nil:
		return "waiting", strings.TrimSpace(state.Waiting.Reason)
	case state.Terminated != nil:
		return "terminated", strings.TrimSpace(state.Terminated.Reason)
	default:
		return "unknown", ""
	}
}

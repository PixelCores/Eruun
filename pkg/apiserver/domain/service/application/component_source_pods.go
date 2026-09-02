package application

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func (c *applicationsServiceImpl) listComponentPods(
	ctx context.Context,
	appID string,
	component *model.ApplicationComponent,
) (*corev1.PodList, error) {
	if component == nil {
		return &corev1.PodList{}, nil
	}
	namespace := strings.TrimSpace(component.Namespace)
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	if !component.HasSourceWorkload() {
		return kube.ListPodsByLabels(ctx, c.KubeClient, namespace, labels.Set{
			config.LabelAppID:         appID,
			config.LabelComponentName: naming.BoundedLabelValue(component.Name),
		})
	}

	selector := labels.Set{}
	if component.SourcePodSelector != nil {
		for key, raw := range *component.SourcePodSelector {
			value, ok := raw.(string)
			if ok && strings.TrimSpace(key) != "" {
				selector[key] = value
			}
		}
	}
	candidates, err := kube.ListPodsByLabels(ctx, c.KubeClient, namespace, selector)
	if err != nil {
		return nil, err
	}
	ownerUIDs, err := c.sourcePodOwnerUIDs(ctx, namespace, component)
	if err != nil {
		return nil, err
	}
	result := &corev1.PodList{TypeMeta: candidates.TypeMeta, ListMeta: candidates.ListMeta}
	for index := range candidates.Items {
		pod := candidates.Items[index]
		if podControlledByAnyUID(&pod, ownerUIDs) {
			result.Items = append(result.Items, pod)
		}
	}
	return result, nil
}

func (c *applicationsServiceImpl) sourcePodOwnerUIDs(
	ctx context.Context,
	namespace string,
	component *model.ApplicationComponent,
) (map[types.UID]struct{}, error) {
	uid := types.UID(strings.TrimSpace(*component.SourceWorkloadUID))
	owners := map[types.UID]struct{}{uid: {}}
	switch strings.ToLower(strings.TrimSpace(component.SourceWorkloadKind)) {
	case "deployment":
		replicaSets, err := c.KubeClient.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list ReplicaSets for source Deployment %s/%s: %w", namespace, component.SourceWorkloadName, err)
		}
		for index := range replicaSets.Items {
			if controlledBySource(&replicaSets.Items[index], "Deployment", uid) {
				owners[replicaSets.Items[index].UID] = struct{}{}
			}
		}
	case "cronjob":
		jobs, err := c.KubeClient.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list Jobs for source CronJob %s/%s: %w", namespace, component.SourceWorkloadName, err)
		}
		for index := range jobs.Items {
			if controlledBySource(&jobs.Items[index], "CronJob", uid) {
				owners[jobs.Items[index].UID] = struct{}{}
			}
		}
	case "statefulset", "daemonset", "job":
	default:
		return nil, fmt.Errorf("unsupported source workload kind %q", component.SourceWorkloadKind)
	}
	return owners, nil
}

func controlledBySource(object metav1.Object, kind string, uid types.UID) bool {
	if object == nil {
		return false
	}
	for _, owner := range object.GetOwnerReferences() {
		if owner.Controller != nil && *owner.Controller && strings.EqualFold(owner.Kind, kind) && owner.UID == uid {
			return true
		}
	}
	return false
}

func podControlledByAnyUID(pod *corev1.Pod, ownerUIDs map[types.UID]struct{}) bool {
	if pod == nil {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if _, found := ownerUIDs[owner.UID]; found {
			return true
		}
	}
	return false
}

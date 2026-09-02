package job

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

func shouldForceCleanupSharedWorkload(ctx context.Context, kubeClient kubernetes.Interface, namespace string, resourceLabels map[string]string) (bool, string) {
	if kubeClient == nil {
		return false, "kube client is nil"
	}

	shareName, strategy := shareInfoFromLabels(resourceLabels)
	if shareName == "" {
		return false, "share label missing"
	}
	if strategy != config.ShareStrategyDefault && strategy != config.ShareStrategyIgnore {
		return false, fmt.Sprintf("strategy=%s", strategy)
	}

	appID := strings.TrimSpace(resourceLabels[config.LabelAppID])
	componentName := strings.TrimSpace(resourceLabels[config.LabelComponentName])
	if appID == "" || componentName == "" {
		return false, "component labels missing"
	}

	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = config.DefaultNamespace
	}
	labelSet := labels.Set{
		config.LabelAppID:         appID,
		config.LabelComponentName: componentName,
	}
	componentID := strings.TrimSpace(resourceLabels[config.LabelComponentID])
	if componentID != "" {
		labelSet[config.LabelComponentID] = componentID
	}

	baseCtx := ctx
	if baseCtx == nil || baseCtx.Err() != nil {
		baseCtx = context.Background()
	}
	opCtx, cancel := context.WithTimeout(baseCtx, config.DelTimeOut)
	defer cancel()
	pods, err := kube.ListPodsByLabels(opCtx, kubeClient, ns, labelSet)
	if err != nil {
		return false, fmt.Sprintf("inspect component pods failed: %v", err)
	}
	if pods == nil {
		return false, "component pods not found"
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		reason := kube.ExtractPodAbnormalReason(pod)
		if reason == "" {
			continue
		}
		return true, fmt.Sprintf("pod=%s reason=%s", pod.Name, reason)
	}
	return false, "no abnormal pod"
}

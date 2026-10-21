package application

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
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
	return kube.ListPodsByLabels(ctx, c.KubeClient, namespace, labels.Set{
		spec.LabelAppID:         appID,
		spec.LabelComponentName: naming.BoundedLabelValue(component.Name),
	})
}

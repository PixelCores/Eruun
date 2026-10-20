package jobtest

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func RetryTestPolicy() *workflowconfig.JobRetryPolicy {
	return &workflowconfig.JobRetryPolicy{OnOOM: "resize", MaxRetries: 1, BackoffSeconds: 1, MemoryGrowthFactor: 2,
		MaxResources: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}
}

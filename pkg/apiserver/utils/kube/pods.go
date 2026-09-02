package kube

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// ListPodsByLabels lists Pods in a namespace using the provided label set.
func ListPodsByLabels(ctx context.Context, client kubernetes.Interface, namespace string, labelSet labels.Set) (*corev1.PodList, error) {
	return client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSet.String()})
}

package job

import (
	"context"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

type deleteNamespacedFunc func(context.Context, string, string) error
type deleteClusterFunc func(context.Context, string) error

// cleanupNamespacedResources deletes created namespaced resources tracked in the cleanup context.
func cleanupNamespacedResources(ctx context.Context, kind config.ResourceKind, namespaceFallback string, timeout time.Duration, resourceLabel string, deleteFn deleteNamespacedFunc, isNotFound func(error) bool, successSuffix string) {
	if deleteFn == nil {
		return
	}
	refs := resourcesForCleanup(ctx, kind)
	if len(refs) == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, ref := range refs {
		if !ref.Created {
			continue
		}
		ns := ref.Namespace
		if ns == "" {
			ns = namespaceFallback
		}
		if err := deleteFn(cleanupCtx, ns, ref.Name); err != nil {
			if isNotFound == nil || !isNotFound(err) {
				klog.Errorf("failed to delete %s %s/%s during cleanup: %v", resourceLabel, ns, ref.Name, err)
			}
			continue
		}
		if successSuffix != "" {
			klog.Infof("deleted %s %s/%s %s", resourceLabel, ns, ref.Name, successSuffix)
		}
	}
}

// cleanupClusterResources deletes created cluster-scoped resources tracked in the cleanup context.
func cleanupClusterResources(ctx context.Context, kind config.ResourceKind, timeout time.Duration, resourceLabel string, deleteFn deleteClusterFunc, isNotFound func(error) bool) {
	if deleteFn == nil {
		return
	}
	refs := resourcesForCleanup(ctx, kind)
	if len(refs) == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, ref := range refs {
		if !ref.Created {
			continue
		}
		if err := deleteFn(cleanupCtx, ref.Name); err != nil {
			if isNotFound == nil || !isNotFound(err) {
				klog.Errorf("failed to delete %s %s during cleanup: %v", resourceLabel, ref.Name, err)
			}
		}
	}
}

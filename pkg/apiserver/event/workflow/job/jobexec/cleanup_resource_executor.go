package jobexec

import (
	"context"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"

	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

type deleteNamespacedFunc func(context.Context, string, string) error

// cleanupNamespacedResources deletes created namespaced resources tracked in the cleanup context.
func CleanupNamespacedResources(ctx context.Context, kind domainspec.ResourceKind, namespaceFallback string, timeout time.Duration, resourceLabel string, deleteFn deleteNamespacedFunc, successSuffix string) {
	if deleteFn == nil {
		return
	}
	refs := ResourcesForCleanup(ctx, kind)
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
			if !k8serrors.IsNotFound(err) {
				klog.Errorf("failed to delete %s %s/%s during cleanup: %v", resourceLabel, ns, ref.Name, err)
			}
			continue
		}
		if successSuffix != "" {
			klog.Infof("deleted %s %s/%s %s", resourceLabel, ns, ref.Name, successSuffix)
		}
	}
}

package apiserver

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

func bestEffortReleaseLeaderLock(lock resourcelock.Interface, timeout time.Duration) bool {
	if lock == nil {
		klog.V(2).InfoS("Skip leader lease release because lock is unavailable")
		return false
	}
	identity := lock.Identity()
	if identity == "" {
		klog.V(2).InfoS("Skip leader lease release because identity is empty")
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	oldRecord, _, err := lock.Get(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(2).InfoS("Skip leader lease release because lock was not found", "identity", identity, "lock", lock.Describe())
			return false
		}
		klog.ErrorS(err, "Unable to retrieve leader lease for release", "identity", identity, "lock", lock.Describe())
		return false
	}
	if oldRecord == nil || oldRecord.HolderIdentity != identity {
		holder := ""
		if oldRecord != nil {
			holder = oldRecord.HolderIdentity
		}
		klog.V(2).InfoS("Skip leader lease release because holder changed", "identity", identity, "holder", holder, "lock", lock.Describe())
		return false
	}

	now := metav1.Now()
	releaseRecord := resourcelock.LeaderElectionRecord{
		LeaseDurationSeconds: 1,
		AcquireTime:          now,
		RenewTime:            now,
		LeaderTransitions:    oldRecord.LeaderTransitions,
	}
	if err := lock.Update(ctx, releaseRecord); err != nil {
		klog.ErrorS(err, "Unable to release leader lease", "identity", identity, "lock", lock.Describe())
		return false
	}
	klog.InfoS("Released leader lease after leader-scoped work stopped", "identity", identity, "lock", lock.Describe())
	return true
}

func renewDeadlineForLeaseDuration(duration time.Duration) time.Duration {
	if duration <= time.Second {
		return duration / 2
	}
	deadline := duration * 2 / 3
	minDeadline := 10 * time.Second
	if duration > minDeadline && deadline < minDeadline {
		return minDeadline
	}
	return deadline
}

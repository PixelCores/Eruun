package informer

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/cache"
)

// JobObserver provides shared snapshots and coalesced wakeups for one Job.
// A missing snapshot is unknown, not proof of deletion. The caller validates
// execution identity and confirms a terminal candidate against the API server.
type JobObserver interface {
	WaitForJob(context.Context, string, string, func(*batchv1.Job) (bool, error)) error
}

func (o *KubernetesWorkloadObserver) notifyJobWaiters(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		return
	}
	o.jobWaitersMu.Lock()
	defer o.jobWaitersMu.Unlock()
	for wake := range o.jobWaiters[key] {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func (o *KubernetesWorkloadObserver) WaitForJob(ctx context.Context, namespace, name string, check func(*batchv1.Job) (bool, error)) error {
	if o == nil || o.jobLister == nil || !o.synced.Load() {
		return fmt.Errorf("kubernetes workload observer Job cache is not synchronized")
	}
	if namespace == "" || name == "" || check == nil {
		return fmt.Errorf("Job observation requires namespace, name and a check")
	}
	key := namespace + "/" + name
	wake := make(chan struct{}, 1)
	o.jobWaitersMu.Lock()
	if o.jobWaiters[key] == nil {
		o.jobWaiters[key] = make(map[chan struct{}]struct{})
	}
	o.jobWaiters[key][wake] = struct{}{}
	o.jobWaitersMu.Unlock()
	defer func() {
		o.jobWaitersMu.Lock()
		delete(o.jobWaiters[key], wake)
		if len(o.jobWaiters[key]) == 0 {
			delete(o.jobWaiters, key)
		}
		o.jobWaitersMu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-o.runDone:
			return fmt.Errorf("kubernetes workload observer stopped")
		default:
		}
		snapshot, err := o.jobLister.Jobs(namespace).Get(name)
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("read observed Job %s: %w", key, err)
		}
		if snapshot != nil {
			snapshot = snapshot.DeepCopy()
		}
		if done, err := check(snapshot); done || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-o.runDone:
			return fmt.Errorf("kubernetes workload observer stopped")
		case <-wake:
		}
	}
}

var _ JobObserver = (*KubernetesWorkloadObserver)(nil)

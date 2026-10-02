package job

import (
	"context"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type deployNamespacedResourceJobBase struct {
	namespace      string
	job            *model.JobTask
	client         kubernetes.Interface
	store          datastore.DataStore
	ack            func()
	shareLocker    locker.Locker
	delayQueue     msg.Queue
	resourceWaiter informer.ComponentReadyObserver
	runtime        *Runtime
}

func newDeployNamespacedResourceJobBase(controllerName string, job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) (deployNamespacedResourceJobBase, bool) {
	if job == nil {
		klog.Errorf("%s: job is nil", controllerName)
		return deployNamespacedResourceJobBase{}, false
	}
	base := deployNamespacedResourceJobBase{
		namespace:   job.Namespace,
		job:         job,
		shareLocker: shareLocker,
	}
	if runtime != nil {
		base.client = runtime.Client
		base.store = runtime.Store
		base.ack = runtime.Ack
		base.delayQueue = runtime.DelayQueue
		base.resourceWaiter = runtime.ResourceWaiter
		base.runtime = runtime
	}
	return base, true
}

func (b *deployNamespacedResourceJobBase) cleanCreated(ctx context.Context, kind domainspec.ResourceKind, resourceLabel string, deleteFn deleteNamespacedFunc, isNotFound func(error) bool, successSuffix string) {
	if b.client == nil {
		return
	}
	cleanupNamespacedResources(ctx, kind, b.namespace, config.DelTimeOut, resourceLabel, deleteFn, isNotFound, successSuffix)
}

func (b *deployNamespacedResourceJobBase) SaveInfo(ctx context.Context) error {
	return saveJobInfo(ctx, b.store, b.job)
}

func (b *deployNamespacedResourceJobBase) runWithStatus(ctx context.Context, runFn runJobFunc, logMsg string) error {
	return runJobWithStatus(ctx, b.job, b.ack, runFn, logMsg)
}

func (b *deployNamespacedResourceJobBase) runWithWait(ctx context.Context, runFn runJobFunc, waitFn waitJobFunc, runLogMsg, waitLogMsg string) error {
	return runJobWithWait(ctx, b.job, b.ack, runFn, waitFn, runLogMsg, waitLogMsg)
}

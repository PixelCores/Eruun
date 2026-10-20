package jobexec

import (
	"context"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type DeployNamespacedResourceJobBase struct {
	Namespace      string
	Job            *model.JobTask
	Client         kubernetes.Interface
	Store          datastore.DataStore
	Ack            func()
	ShareLocker    locker.Locker
	DelayQueue     msg.Queue
	ResourceWaiter informer.ComponentReadyObserver
	Runtime        *Runtime
}

func NewDeployNamespacedResourceJobBase(controllerName string, job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) (DeployNamespacedResourceJobBase, bool) {
	if job == nil {
		klog.Errorf("%s: job is nil", controllerName)
		return DeployNamespacedResourceJobBase{}, false
	}
	base := DeployNamespacedResourceJobBase{
		Namespace:   job.Namespace,
		Job:         job,
		ShareLocker: shareLocker,
	}
	if runtime != nil {
		base.Client = runtime.Client
		base.Store = runtime.Store
		base.Ack = runtime.Ack
		base.DelayQueue = runtime.DelayQueue
		base.ResourceWaiter = runtime.ResourceWaiter
		base.Runtime = runtime
	}
	return base, true
}

func (b *DeployNamespacedResourceJobBase) CleanCreated(ctx context.Context, kind domainspec.ResourceKind, resourceLabel string, deleteFn deleteNamespacedFunc, isNotFound func(error) bool, successSuffix string) {
	if b.Client == nil {
		return
	}
	CleanupNamespacedResources(ctx, kind, b.Namespace, config.DelTimeOut, resourceLabel, deleteFn, isNotFound, successSuffix)
}

func (b *DeployNamespacedResourceJobBase) SaveInfo(ctx context.Context) error {
	return jobinfo.SaveJobInfo(ctx, b.Store, b.Job)
}

func (b *DeployNamespacedResourceJobBase) RunWithStatus(ctx context.Context, runFn jobinfo.RunJobFunc, logMsg string) error {
	return jobinfo.RunJobWithStatus(ctx, b.Job, b.Ack, runFn, logMsg)
}

func (b *DeployNamespacedResourceJobBase) RunWithWait(ctx context.Context, runFn jobinfo.RunJobFunc, waitFn jobinfo.WaitJobFunc, runLogMsg, waitLogMsg string) error {
	return jobinfo.RunJobWithWait(ctx, b.Job, b.Ack, runFn, waitFn, runLogMsg, waitLogMsg)
}

func (b *DeployNamespacedResourceJobBase) CleanCreatedBatchJobs(ctx context.Context, successSuffix string) {
	b.CleanCreated(ctx, domainspec.ResourceJob, "job", func(ctx context.Context, namespace, name string) error {
		live, err := b.Client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		// Garbage collection continues server-side; waiting here would spend the
		// shared cleanup budget before later created resources are deleted.
		return deleteJobForeground(ctx, b.Client, namespace, live)
	}, k8serrors.IsNotFound, successSuffix)
}

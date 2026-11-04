package jobdeploy

import (
	"context"
	"fmt"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
)

// DeleteResourceJobCtl removes an object a component stopped declaring in a
// version update: the route of a dropped gateway trait, or a Service no longer
// rendered. An object owned by another component, or kept by a share strategy,
// stays in place.
type DeleteResourceJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
	Dynamic dynamic.Interface
}

func NewDeleteResourceJobCtl(job *model.JobTask, runtime *jobexec.Runtime) *DeleteResourceJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeleteResourceJobCtl", job, runtime, nil)
	if !ok {
		return nil
	}
	ctl := &DeleteResourceJobCtl{DeployNamespacedResourceJobBase: base}
	if runtime != nil {
		ctl.Dynamic = runtime.Dynamic
	}
	return ctl
}

func (c *DeleteResourceJobCtl) Clean(context.Context) {}

func (c *DeleteResourceJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "DeleteResourceJob run error")
}

func (c *DeleteResourceJobCtl) run(ctx context.Context) error {
	if c.Dynamic == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	ref, resource, err := jobinfo.DeletionTargetFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	namespace, name, kind := ref.GetNamespace(), ref.GetName(), ref.GetKind()
	objects := c.Dynamic.Resource(resource.GVR).Namespace(namespace)
	current, err := objects.Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get %s %s/%s: %w", kind, namespace, name, err)
	}
	owner, live := ref.GetLabels(), current.GetLabels()
	if live[domainspec.LabelAppID] != owner[domainspec.LabelAppID] || live[domainspec.LabelComponentName] != owner[domainspec.LabelComponentName] {
		klog.Infof("%s %s/%s belongs to another component; keeping it", kind, namespace, name)
		return nil
	}
	if strategy, protected := jobexec.ShareProtectedFromDeletion(live); protected {
		klog.Infof("%s %s/%s is shared with strategy %s; keeping it", kind, namespace, name, strategy)
		return nil
	}
	if err := jobexec.EnsureJobWorkflowOwnership(ctx, c.Job, c.Store, "deleting "+kind+" "+namespace+"/"+name); err != nil {
		return err
	}
	uid := current.GetUID()
	err = objects.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete %s %s/%s: %w", kind, namespace, name, err)
	}
	klog.Infof("%s %s/%s deleted", kind, namespace, name)
	return nil
}

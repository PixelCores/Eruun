package jobdeploy

import (
	"context"
	"fmt"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// DeployRouteJobCtl applies the component route rendered for the gateway
// provider. It runs after the component Services in the same execution group.
type DeployRouteJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
	Dynamic dynamic.Interface
}

func NewDeployRouteJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployRouteJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployRouteJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	ctl := &DeployRouteJobCtl{DeployNamespacedResourceJobBase: base}
	if runtime != nil {
		ctl.Dynamic = runtime.Dynamic
	}
	return ctl
}

func (c *DeployRouteJobCtl) Clean(ctx context.Context) {
	if c.Dynamic == nil {
		return
	}
	_, resource, err := jobinfo.RouteFromJobInfo(c.Job)
	if err != nil {
		klog.Errorf("skip route cleanup for job %s: %v", c.Job.Name, err)
		return
	}
	c.CleanCreated(ctx, resource.Kind, "route", func(ctx context.Context, namespace, name string) error {
		return c.Dynamic.Resource(resource.GVR).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, "after job failure")
}

func (c *DeployRouteJobCtl) Run(ctx context.Context) error {
	return c.RunWithWait(ctx, c.run, c.wait, "DeployRouteJob run error", "")
}

func (c *DeployRouteJobCtl) run(ctx context.Context) error {
	if c.Dynamic == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	route, resource, err := jobinfo.RouteFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	if route.GetNamespace() == "" {
		route.SetNamespace(c.Namespace)
	}
	namespace, name := route.GetNamespace(), route.GetName()
	routes := c.Dynamic.Resource(resource.GVR).Namespace(namespace)

	updateRoute := func(ctx context.Context, current *unstructured.Unstructured) error {
		if !routeNeedsUpdate(current, route) {
			jobexec.MarkResourceObserved(ctx, resource.Kind, namespace, name)
			klog.Infof("Route %s %s/%s is up-to-date; skipping update", route.GetKind(), namespace, name)
			return nil
		}
		if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*unstructured.Unstructured, error) {
			return routes.Get(ctx, name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *unstructured.Unstructured) error {
			_, err := routes.Update(ctx, buildUpdatedRoute(latest, route), metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("update route %s %s/%s failed: %w", route.GetKind(), namespace, name, err)
		}
		jobexec.MarkResourceObserved(ctx, resource.Kind, namespace, name)
		klog.Infof("Route %s %s/%s updated successfully", route.GetKind(), namespace, name)
		return nil
	}

	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[unstructured.Unstructured]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:          c.Job,
			Ack:          c.Ack,
			Labels:       route.GetLabels(),
			Kind:         resource.Kind,
			LockProvider: c.ShareLocker,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := routes.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					klog.Infof("Route %s %s/%s marked as shared ignore; skipping", route.GetKind(), namespace, name)
				} else {
					klog.Infof("Route %s %s/%s already exists and is shared; skipping", route.GetKind(), namespace, name)
				}
			},
		},
		Namespace: namespace,
		Name:      name,
		GetFn: func(ctx context.Context) (*unstructured.Unstructured, error) {
			return routes.Get(ctx, name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*unstructured.Unstructured, error) {
			return routes.Create(ctx, route, metav1.CreateOptions{})
		},
		OnExisting: updateRoute,
	})
	if err != nil {
		return fmt.Errorf("ensure route %s %s/%s failed: %w", route.GetKind(), namespace, name, err)
	}
	if created {
		klog.Infof("Route %s %s/%s created successfully", route.GetKind(), namespace, name)
	}
	return nil
}

// wait polls until the route is ready. Ingress has no portable acceptance
// status, so its presence is the readiness signal; an HTTPRoute must be
// accepted by the platform Gateway with resolved references.
func (c *DeployRouteJobCtl) wait(ctx context.Context) error {
	route, resource, err := jobinfo.RouteFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	namespace, name := route.GetNamespace(), route.GetName()
	if namespace == "" {
		namespace = c.Namespace
	}
	timeout := time.After(time.Duration(c.timeout()) * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return jobinfo.NewStatusError(config.StatusCancelled, fmt.Errorf("route %s cancelled: %w", name, ctx.Err()))
		case <-timeout:
			return jobinfo.NewStatusError(config.StatusTimeout, fmt.Errorf("wait route %s timeout", name))
		case <-ticker.C:
			current, err := c.Dynamic.Resource(resource.GVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
			if k8serrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return fmt.Errorf("wait route %s error: %w", name, err)
			}
			if resource.Kind != domainspec.ResourceHTTPRoute {
				return nil
			}
			ready, err := httpRouteReady(current, c.Runtime.Gateway.ParentRef)
			if err != nil {
				return fmt.Errorf("route %s/%s: %w", namespace, name, err)
			}
			if ready {
				return nil
			}
		}
	}
}

// DeleteRouteJobCtl removes the route of a component whose gateway trait a
// version update dropped. A route owned by another component, or kept by a
// share strategy, stays in place.
type DeleteRouteJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
	Dynamic dynamic.Interface
}

func NewDeleteRouteJobCtl(job *model.JobTask, runtime *jobexec.Runtime) *DeleteRouteJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeleteRouteJobCtl", job, runtime, nil)
	if !ok {
		return nil
	}
	ctl := &DeleteRouteJobCtl{DeployNamespacedResourceJobBase: base}
	if runtime != nil {
		ctl.Dynamic = runtime.Dynamic
	}
	return ctl
}

func (c *DeleteRouteJobCtl) Clean(context.Context) {}

func (c *DeleteRouteJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "DeleteRouteJob run error")
}

func (c *DeleteRouteJobCtl) run(ctx context.Context) error {
	if c.Dynamic == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	ref, resource, err := jobinfo.RouteFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	namespace, name := ref.GetNamespace(), ref.GetName()
	routes := c.Dynamic.Resource(resource.GVR).Namespace(namespace)
	current, err := routes.Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get route %s %s/%s: %w", ref.GetKind(), namespace, name, err)
	}
	owner, live := ref.GetLabels(), current.GetLabels()
	if live[domainspec.LabelAppID] != owner[domainspec.LabelAppID] || live[domainspec.LabelComponentName] != owner[domainspec.LabelComponentName] {
		klog.Infof("Route %s %s/%s belongs to another component; keeping it", ref.GetKind(), namespace, name)
		return nil
	}
	if strategy, protected := jobexec.ShareProtectedFromDeletion(live); protected {
		klog.Infof("Route %s %s/%s is shared with strategy %s; keeping it", ref.GetKind(), namespace, name, strategy)
		return nil
	}
	if err := jobexec.EnsureJobWorkflowOwnership(ctx, c.Job, c.Store, "deleting route "+namespace+"/"+name); err != nil {
		return err
	}
	uid := current.GetUID()
	err = routes.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete route %s %s/%s: %w", ref.GetKind(), namespace, name, err)
	}
	klog.Infof("Route %s %s/%s deleted", ref.GetKind(), namespace, name)
	return nil
}

// httpRouteReady reads the status the platform Gateway's listener reports for
// the route's current generation. It fails on a rejection, a partially invalid
// route, or a broken reference other than a Service the controller has not
// observed yet; anything else keeps waiting.
func httpRouteReady(route *unstructured.Unstructured, parent domainspec.GatewayParentRef) (bool, error) {
	parents, _, _ := unstructured.NestedSlice(route.Object, "status", "parents")
	for _, item := range parents {
		status, _ := item.(map[string]interface{})
		// The reference may carry non-string fields such as port; read only the
		// three that identify the platform listener.
		namespace, _, _ := unstructured.NestedString(status, "parentRef", "namespace")
		name, _, _ := unstructured.NestedString(status, "parentRef", "name")
		sectionName, _, _ := unstructured.NestedString(status, "parentRef", "sectionName")
		if namespace != parent.Namespace || name != parent.Name || sectionName != parent.SectionName {
			continue
		}
		conditions := map[string]map[string]interface{}{}
		items, _, _ := unstructured.NestedSlice(status, "conditions")
		for _, entry := range items {
			condition, _ := entry.(map[string]interface{})
			observed, _, _ := unstructured.NestedInt64(condition, "observedGeneration")
			if conditionType, _ := condition["type"].(string); observed == route.GetGeneration() {
				conditions[conditionType] = condition
			}
		}
		accepted, resolved, partial := conditions["Accepted"], conditions["ResolvedRefs"], conditions["PartiallyInvalid"]
		switch {
		case accepted["status"] == "False":
			return false, routeConditionError(accepted)
		case partial["status"] == "True":
			return false, routeConditionError(partial)
		case resolved["status"] == "False" && resolved["reason"] != "BackendNotFound":
			return false, routeConditionError(resolved)
		}
		return accepted["status"] == "True" && resolved["status"] == "True", nil
	}
	return false, nil
}

func routeConditionError(condition map[string]interface{}) error {
	return fmt.Errorf("%v=%v reason=%v: %v", condition["type"], condition["status"], condition["reason"], condition["message"])
}

func (c *DeployRouteJobCtl) timeout() int64 {
	if c.Job.Timeout == 0 {
		c.Job.Timeout = config.DeployTimeout
	}
	return c.Job.Timeout
}

func routeNeedsUpdate(current, desired *unstructured.Unstructured) bool {
	if current == nil || desired == nil {
		return false
	}
	updated := buildUpdatedRoute(current, desired)
	return !apiequality.Semantic.DeepEqual(current.Object["spec"], updated.Object["spec"]) ||
		!apiequality.Semantic.DeepEqual(current.GetLabels(), updated.GetLabels()) ||
		!apiequality.Semantic.DeepEqual(current.GetAnnotations(), updated.GetAnnotations())
}

// buildUpdatedRoute keeps server-owned metadata from current and replaces the
// spec, preserving Eruun system labels that the desired object omits.
func buildUpdatedRoute(current, desired *unstructured.Unstructured) *unstructured.Unstructured {
	updated := current.DeepCopy()
	updated.SetLabels(preserveStringMapKeys(current.GetLabels(), desired.GetLabels(), eruunSystemLabelKeys))
	updated.SetAnnotations(desired.GetAnnotations())
	updated.Object["spec"] = runtime.DeepCopyJSONValue(desired.Object["spec"])
	return updated
}

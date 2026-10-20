package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type VersionRestartJobInfo struct {
	Component *model.ApplicationComponent
}

type versionRestartWaitTarget struct {
	kind            string
	namespace       string
	name            string
	componentLabel  string
	desiredReplicas int32
	expectedImages  []string
}

type VersionRestartJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
}

func NewVersionRestartJobCtl(job *model.JobTask, runtime *jobexec.Runtime) *VersionRestartJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("VersionRestartJobCtl", job, runtime, nil)
	if !ok {
		return nil
	}
	return &VersionRestartJobCtl{DeployNamespacedResourceJobBase: base}
}

func (c *VersionRestartJobCtl) Clean(context.Context) {}

func (c *VersionRestartJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "version restart job run error")
}

func (c *VersionRestartJobCtl) run(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}
	if c.Store == nil {
		return fmt.Errorf("store is nil")
	}
	info, err := jobinfo.RequiredJobInfo[*VersionRestartJobInfo](c.Job)
	if err != nil {
		return err
	}
	component := normalizeVersionRestartComponent(info.Component)
	if component.Name == "" {
		return fmt.Errorf("restart component name is empty")
	}
	if component.Status == string(config.ComponentStatusStopped) {
		c.skip("component is stopped")
		return nil
	}
	if strategy, shared := versionRestartShareStrategyForComponent(component); shared {
		c.skip(fmt.Sprintf("shared component strategy=%s", strategy))
		return nil
	}

	restartedAt := formatVersionRestartAt(time.Now())
	patch, err := buildWorkloadRestartPatch(restartedAt)
	if err != nil {
		return err
	}
	var target *versionRestartWaitTarget
	switch component.ComponentType {
	case config.ServerJob:
		target, err = c.restartDeployment(ctx, component, restartedAt, patch)
		if err != nil {
			return err
		}
	case config.StoreJob:
		target, err = c.restartStatefulSet(ctx, component, restartedAt, patch)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("component %s with type %q cannot be restarted", component.Name, component.ComponentType)
	}
	if c.Job.Status == config.StatusSkipped {
		return nil
	}
	if err := c.markComponentRuntime(ctx, component, config.ComponentStatusRestarting, 0, ""); err != nil {
		return err
	}
	return c.waitRestartReady(ctx, component, target, restartedAt)
}

func (c *VersionRestartJobCtl) restartDeployment(
	ctx context.Context,
	component *model.ApplicationComponent,
	restartedAt string,
	patch []byte,
) (*versionRestartWaitTarget, error) {
	properties, err := jobexec.ParseProperties(component.Properties)
	if err != nil {
		return nil, err
	}
	result, err := GenerateWebService(component, &properties)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("generate webservice for component %s failed", component.Name)
	}
	deploy, ok := result.Service.(*appsv1.Deployment)
	if !ok || deploy == nil {
		return nil, fmt.Errorf("server component %s did not generate a Deployment", component.Name)
	}
	namespace := jobexec.DatabaseResetNamespaceOrDefault(deploy.Namespace, component.Namespace)
	name := strings.TrimSpace(deploy.Name)
	if name == "" {
		name = naming.WebServiceName(component.Name, component.ResourceNameKey())
	}
	if _, err := c.Client.AppsV1().Deployments(namespace).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			klog.V(4).InfoS("version restart target deployment not found, skipping", "namespace", namespace, "name", name, "component", component.Name)
			c.skip(fmt.Sprintf("deployment %s/%s not found", namespace, name))
			return nil, nil
		}
		return nil, fmt.Errorf("rollout restart deployment %s/%s: %w", namespace, name, err)
	}
	return &versionRestartWaitTarget{
		kind:            "Deployment",
		namespace:       namespace,
		name:            name,
		componentLabel:  naming.BoundedLabelValue(component.Name),
		desiredReplicas: desiredReplicasFromDeployment(deploy),
		expectedImages:  podSpecImages(deploy.Spec.Template.Spec),
	}, nil
}

func (c *VersionRestartJobCtl) restartStatefulSet(
	ctx context.Context,
	component *model.ApplicationComponent,
	restartedAt string,
	patch []byte,
) (*versionRestartWaitTarget, error) {
	result, err := GenerateStoreService(component)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("generate store service for component %s failed", component.Name)
	}
	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	if !ok || statefulSet == nil {
		return nil, fmt.Errorf("store component %s did not generate a StatefulSet", component.Name)
	}
	namespace := jobexec.DatabaseResetNamespaceOrDefault(statefulSet.Namespace, component.Namespace)
	name := strings.TrimSpace(statefulSet.Name)
	if name == "" {
		name = naming.StoreServerName(component.Name, component.ResourceNameKey())
	}
	if _, err := c.Client.AppsV1().StatefulSets(namespace).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			klog.V(4).InfoS("version restart target statefulset not found, skipping", "namespace", namespace, "name", name, "component", component.Name)
			c.skip(fmt.Sprintf("statefulset %s/%s not found", namespace, name))
			return nil, nil
		}
		return nil, fmt.Errorf("rollout restart statefulset %s/%s: %w", namespace, name, err)
	}
	return &versionRestartWaitTarget{
		kind:            "StatefulSet",
		namespace:       namespace,
		name:            name,
		componentLabel:  naming.BoundedLabelValue(component.Name),
		desiredReplicas: desiredReplicasFromStatefulSet(statefulSet),
		expectedImages:  podSpecImages(statefulSet.Spec.Template.Spec),
	}, nil
}

func (c *VersionRestartJobCtl) waitRestartReady(ctx context.Context, component *model.ApplicationComponent, target *versionRestartWaitTarget, restartedAt string) error {
	if component == nil || target == nil || target.desiredReplicas <= 0 {
		return nil
	}
	if c.ResourceWaiter == nil {
		return fmt.Errorf("resource waiter is required for version restart %s %s/%s", target.kind, target.namespace, target.name)
	}
	timeout := time.Duration(c.timeout()) * time.Second
	err := c.ResourceWaiter.WaitForComponentReadyWithOptions(ctx, component.AppID, target.componentLabel, target.desiredReplicas, informer.ComponentReadyWaitOptions{
		ExpectedImages: target.expectedImages,
		ExpectedAnnotations: map[string]string{
			config.AnnotationWorkloadRestartAt: restartedAt,
		},
	}, timeout)
	if err == nil {
		return nil
	}
	var waitErr *informer.WaitError
	if errors.As(err, &waitErr) {
		return jobinfo.NewStatusError(waitErr.Status, workloadWaitError(ctx, c.Client, c.Job, target.kind, target.namespace, target.name, target.componentLabel, waitErr.Err))
	}
	return workloadWaitError(ctx, c.Client, c.Job, target.kind, target.namespace, target.name, target.componentLabel, err)
}

func (c *VersionRestartJobCtl) timeout() int64 {
	if c == nil || c.Job == nil || c.Job.Timeout <= 0 {
		return config.DeployTimeout
	}
	return c.Job.Timeout
}

func desiredReplicasFromDeployment(deploy *appsv1.Deployment) int32 {
	if deploy != nil && deploy.Spec.Replicas != nil {
		return *deploy.Spec.Replicas
	}
	return 1
}

func desiredReplicasFromStatefulSet(statefulSet *appsv1.StatefulSet) int32 {
	if statefulSet != nil && statefulSet.Spec.Replicas != nil {
		return *statefulSet.Spec.Replicas
	}
	return 1
}

func (c *VersionRestartJobCtl) skip(reason string) {
	if c == nil || c.Job == nil {
		return
	}
	c.Job.Status = config.StatusSkipped
	c.Job.Info = strings.TrimSpace(reason)
}

func (c *VersionRestartJobCtl) markComponentRuntime(ctx context.Context, component *model.ApplicationComponent, status config.ComponentStatus, readyReplicas int32, lastAbnormal string) error {
	if component == nil {
		return nil
	}
	if err := repository.UpdateComponentRuntimeFields(ctx, c.Store, component, map[string]interface{}{
		"status":         string(status),
		"ready_replicas": readyReplicas,
		"last_abnormal":  lastAbnormal,
	}); err != nil {
		klog.ErrorS(err, "update component runtime status failed", "appID", component.AppID, "component", component.Name, "status", status)
		return err
	}
	component.Status = string(status)
	component.ReadyReplicas = readyReplicas
	component.LastAbnormal = lastAbnormal
	jobexec.InvalidateComponentsCache(ctx, c.Runtime, component.AppID, "version restart status sync")
	return nil
}

func normalizeVersionRestartComponent(component *model.ApplicationComponent) *model.ApplicationComponent {
	if component == nil {
		return &model.ApplicationComponent{Namespace: config.DefaultNamespace}
	}
	cp := *component
	if strings.TrimSpace(cp.Namespace) == "" {
		cp.Namespace = config.DefaultNamespace
	}
	return &cp
}

func versionRestartShareStrategyForComponent(component *model.ApplicationComponent) (domainspec.ShareStrategy, bool) {
	strategy, shared := component.ShareStrategy()
	if !shared || strategy == domainspec.ShareStrategyForce {
		return "", false
	}
	return strategy, true
}

func formatVersionRestartAt(now time.Time) string {
	return now.UTC().Format(time.RFC3339Nano)
}

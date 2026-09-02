package job

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainadoption "github.com/PixelCores/Eruun/pkg/apiserver/domain/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
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
	deployNamespacedResourceJobBase
	runtime *jobRuntime
}

func NewVersionRestartJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func()) *VersionRestartJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("VersionRestartJobCtl", job, client, store, ack, nil)
	if !ok {
		return nil
	}
	return &VersionRestartJobCtl{deployNamespacedResourceJobBase: base}
}

func (c *VersionRestartJobCtl) Clean(context.Context) {}

func (c *VersionRestartJobCtl) setRuntime(runtime *jobRuntime) {
	if c == nil {
		return
	}
	c.runtime = runtime
	c.deployNamespacedResourceJobBase.setRuntime(runtime)
}

func (c *VersionRestartJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "version restart job run error")
}

func (c *VersionRestartJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	if c.store == nil {
		return fmt.Errorf("store is nil")
	}
	info, err := requiredJobInfo[*VersionRestartJobInfo](c.job)
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
	if c.job.Status == config.StatusSkipped {
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
	source, adopted, sourceErr := adoptedSourceForJob(ctx, c.store, c.job, "Deployment")
	if sourceErr != nil {
		return nil, sourceErr
	}
	if adopted {
		return c.restartAdoptedDeployment(ctx, source, restartedAt)
	}
	properties := ParseProperties(component.Properties)
	result := GenerateWebService(component, &properties)
	if result == nil {
		return nil, fmt.Errorf("generate webservice for component %s failed", component.Name)
	}
	deploy, ok := result.Service.(*appsv1.Deployment)
	if !ok || deploy == nil {
		return nil, fmt.Errorf("server component %s did not generate a Deployment", component.Name)
	}
	namespace := databaseResetNamespaceOrDefault(deploy.Namespace, component.Namespace)
	name := strings.TrimSpace(deploy.Name)
	if name == "" {
		name = buildWebServiceName(component.Name, component.ResourceNameKey())
	}
	if _, err := c.client.AppsV1().Deployments(namespace).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
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
	source, adopted, sourceErr := adoptedSourceForJob(ctx, c.store, c.job, "StatefulSet")
	if sourceErr != nil {
		return nil, sourceErr
	}
	if adopted {
		return c.restartAdoptedStatefulSet(ctx, source, restartedAt)
	}
	result := GenerateStoreService(component)
	if result == nil {
		return nil, fmt.Errorf("generate store service for component %s failed", component.Name)
	}
	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	if !ok || statefulSet == nil {
		return nil, fmt.Errorf("store component %s did not generate a StatefulSet", component.Name)
	}
	namespace := databaseResetNamespaceOrDefault(statefulSet.Namespace, component.Namespace)
	name := strings.TrimSpace(statefulSet.Name)
	if name == "" {
		name = buildStoreSeverName(component.Name, component.ResourceNameKey())
	}
	if _, err := c.client.AppsV1().StatefulSets(namespace).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
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

func (c *VersionRestartJobCtl) restartAdoptedDeployment(
	ctx context.Context,
	source *model.ApplicationComponent,
	restartedAt string,
) (*versionRestartWaitTarget, error) {
	namespace := adoptedSourceNamespace(source, c.job.Namespace)
	name := strings.TrimSpace(source.SourceWorkloadName)
	var updated *appsv1.Deployment
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*appsv1.Deployment, error) {
		return c.client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *appsv1.Deployment) error {
		if err := validateAdoptedSourceUID(latest.UID, source, "Deployment", namespace, name); err != nil {
			return err
		}
		if latest.Spec.Paused {
			return fmt.Errorf("Deployment is paused")
		}
		candidate := latest.DeepCopy()
		applyAdoptedRestartMetadata(&candidate.Spec.Template, source, restartedAt)
		var err error
		updated, err = c.client.AppsV1().Deployments(namespace).Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return nil, fmt.Errorf("rollout restart adopted deployment %s/%s: %w", namespace, name, err)
	}
	return &versionRestartWaitTarget{
		kind:            "Deployment",
		namespace:       namespace,
		name:            name,
		componentLabel:  naming.BoundedLabelValue(source.Name),
		desiredReplicas: desiredReplicasFromDeployment(updated),
		expectedImages:  podSpecImages(updated.Spec.Template.Spec),
	}, nil
}

func (c *VersionRestartJobCtl) restartAdoptedStatefulSet(
	ctx context.Context,
	source *model.ApplicationComponent,
	restartedAt string,
) (*versionRestartWaitTarget, error) {
	namespace := adoptedSourceNamespace(source, c.job.Namespace)
	name := strings.TrimSpace(source.SourceWorkloadName)
	current, err := c.client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get adopted statefulset %s/%s for restart: %w", namespace, name, err)
	}
	if err := validateAdoptedSourceUID(current.UID, source, "StatefulSet", namespace, name); err != nil {
		return nil, err
	}
	if err := c.validateAdoptedStatefulSetRestart(ctx, current, source); err != nil {
		return nil, fmt.Errorf("preflight adopted statefulset %s/%s restart: %w", namespace, name, err)
	}

	var updated *appsv1.StatefulSet
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*appsv1.StatefulSet, error) {
		return c.client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *appsv1.StatefulSet) error {
		if err := validateAdoptedSourceUID(latest.UID, source, "StatefulSet", namespace, name); err != nil {
			return err
		}
		// Re-run the destructive-retention and PVC checks immediately before
		// the write; the resource may have changed since the first GET.
		if err := c.validateAdoptedStatefulSetRestart(ctx, latest, source); err != nil {
			return err
		}
		candidate := latest.DeepCopy()
		applyAdoptedRestartMetadata(&candidate.Spec.Template, source, restartedAt)
		var err error
		updated, err = c.client.AppsV1().StatefulSets(namespace).Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return nil, fmt.Errorf("rollout restart adopted statefulset %s/%s: %w", namespace, name, err)
	}
	return &versionRestartWaitTarget{
		kind:            "StatefulSet",
		namespace:       namespace,
		name:            name,
		componentLabel:  naming.BoundedLabelValue(source.Name),
		desiredReplicas: desiredReplicasFromStatefulSet(updated),
		expectedImages:  podSpecImages(updated.Spec.Template.Spec),
	}, nil
}

func applyAdoptedRestartMetadata(
	template *corev1.PodTemplateSpec,
	source *model.ApplicationComponent,
	restartedAt string,
) {
	if template == nil || source == nil {
		return
	}
	if template.Labels == nil {
		template.Labels = make(map[string]string)
	}
	template.Labels[config.LabelAppID] = source.AppID
	template.Labels[config.LabelComponentName] = naming.BoundedLabelValue(source.Name)
	template.Labels[config.LabelComponentID] = strconv.Itoa(source.ID)
	if template.Annotations == nil {
		template.Annotations = make(map[string]string)
	}
	template.Annotations[config.AnnotationWorkloadRestartAt] = restartedAt
}

func (c *VersionRestartJobCtl) validateAdoptedStatefulSetRestart(
	ctx context.Context,
	statefulSet *appsv1.StatefulSet,
	source *model.ApplicationComponent,
) error {
	if statefulSet == nil {
		return fmt.Errorf("statefulset is nil")
	}
	if err := domainadoption.ValidateStatefulSetRestartStrategy(statefulSet); err != nil {
		return err
	}
	retention := statefulSet.Spec.PersistentVolumeClaimRetentionPolicy
	if retention != nil && retention.WhenScaled == appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		return fmt.Errorf("persistentVolumeClaimRetentionPolicy.whenScaled=Delete would remove PVCs")
	}
	replicas := desiredReplicasFromStatefulSet(statefulSet)
	if replicas <= 0 && source != nil && source.ResumeReplicas != nil {
		replicas = *source.ResumeReplicas
	}
	if replicas <= 0 {
		return fmt.Errorf("replica snapshot must be greater than 0")
	}
	for _, claimName := range adoptedStatefulSetRestartPVCNames(statefulSet, replicas) {
		pvc, err := c.client.CoreV1().PersistentVolumeClaims(statefulSet.Namespace).Get(ctx, claimName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get referenced PVC %s/%s: %w", statefulSet.Namespace, claimName, err)
		}
		if pvc.DeletionTimestamp != nil {
			return fmt.Errorf("referenced PVC %s/%s is terminating", statefulSet.Namespace, claimName)
		}
		if pvc.Status.Phase != corev1.ClaimBound {
			return fmt.Errorf(
				"referenced PVC %s/%s must be Bound, got %s",
				statefulSet.Namespace,
				claimName,
				pvc.Status.Phase,
			)
		}
	}
	return nil
}

func adoptedStatefulSetRestartPVCNames(statefulSet *appsv1.StatefulSet, replicas int32) []string {
	if statefulSet == nil {
		return nil
	}
	names := make(map[string]struct{})
	for _, volume := range statefulSet.Spec.Template.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if name := strings.TrimSpace(volume.PersistentVolumeClaim.ClaimName); name != "" {
			names[name] = struct{}{}
		}
	}
	startOrdinal := int32(0)
	if statefulSet.Spec.Ordinals != nil {
		startOrdinal = statefulSet.Spec.Ordinals.Start
	}
	for _, template := range statefulSet.Spec.VolumeClaimTemplates {
		templateName := strings.TrimSpace(template.Name)
		if templateName == "" {
			continue
		}
		for ordinal := startOrdinal; ordinal < startOrdinal+replicas; ordinal++ {
			names[fmt.Sprintf("%s-%s-%d", templateName, statefulSet.Name, ordinal)] = struct{}{}
		}
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func (c *VersionRestartJobCtl) waitRestartReady(ctx context.Context, component *model.ApplicationComponent, target *versionRestartWaitTarget, restartedAt string) error {
	if component == nil || target == nil || target.desiredReplicas <= 0 {
		return nil
	}
	if c.resourceWaiter == nil {
		return fmt.Errorf("resource waiter is required for version restart %s %s/%s", target.kind, target.namespace, target.name)
	}
	timeout := time.Duration(c.timeout()) * time.Second
	err := c.resourceWaiter.WaitForComponentReadyWithOptions(ctx, component.AppID, target.componentLabel, target.desiredReplicas, informer.ComponentReadyWaitOptions{
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
		return NewStatusError(waitErr.Status, workloadWaitError(ctx, c.client, c.job, target.kind, target.namespace, target.name, target.componentLabel, waitErr.Err))
	}
	return workloadWaitError(ctx, c.client, c.job, target.kind, target.namespace, target.name, target.componentLabel, err)
}

func (c *VersionRestartJobCtl) timeout() int64 {
	if c == nil || c.job == nil || c.job.Timeout <= 0 {
		return config.DeployTimeout
	}
	return c.job.Timeout
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
	if c == nil || c.job == nil {
		return
	}
	c.job.Status = config.StatusSkipped
	c.job.Info = strings.TrimSpace(reason)
}

func (c *VersionRestartJobCtl) markComponentRuntime(ctx context.Context, component *model.ApplicationComponent, status config.ComponentStatus, readyReplicas int32, lastAbnormal string) error {
	if component == nil {
		return nil
	}
	if err := repository.UpdateComponentRuntimeFields(ctx, c.store, component, map[string]interface{}{
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
	invalidateComponentsCache(c.runtime, component.AppID, "version restart status sync")
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

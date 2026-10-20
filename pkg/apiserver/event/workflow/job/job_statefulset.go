package job

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type DeployStatefulSetJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
	desiredReplicas                int32
	expectedPodTemplateAnnotations map[string]string
}

func NewDeployStatefulSetJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployStatefulSetJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployStatefulSetJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployStatefulSetJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployStatefulSetJobCtl) Clean(ctx context.Context) {
	if c.Client == nil {
		return
	}
	namespace := c.Namespace
	name := naming.StoreServerName(c.Job.Name, c.Job.ResourceAppNameOrID())
	var labels map[string]string
	if statefulSet, ok := jobinfo.OptionalJobInfo[*appsv1.StatefulSet](c.Job); ok {
		if statefulSet.Namespace != "" {
			namespace = statefulSet.Namespace
		}
		if statefulSet.Name != "" {
			name = statefulSet.Name
		}
		labels = statefulSet.Labels
	}
	if namespace == "" {
		namespace = config.DefaultNamespace
	}

	jobexec.CleanupNamespacedResources(ctx, spec.ResourceStatefulSet, c.Namespace, config.DelTimeOut, "statefulset", func(ctx context.Context, namespace, name string) error {
		return c.Client.AppsV1().StatefulSets(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, errors.IsNotFound, "after job failure")

	forceDelete, reason := jobexec.ShouldForceCleanupSharedWorkload(ctx, c.Client, namespace, labels)
	if !forceDelete {
		klog.V(4).Infof("cleanup: keep shared statefulset %s/%s (%s)", namespace, name, reason)
		return
	}

	deleteCtx, cancel := context.WithTimeout(context.Background(), config.DelTimeOut)
	defer cancel()
	if err := c.Client.AppsV1().StatefulSets(namespace).Delete(deleteCtx, name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		klog.Errorf("cleanup: delete shared statefulset %s/%s failed: %v", namespace, name, err)
		return
	}
	klog.Infof("cleanup: deleted shared statefulset %s/%s due to abnormal pod (%s)", namespace, name, reason)
}

func (c *DeployStatefulSetJobCtl) Run(ctx context.Context) error {
	return c.RunWithWait(ctx, c.run, c.wait, "DeployServiceJob run error", "")
}

func (c *DeployStatefulSetJobCtl) run(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}
	c.expectedPodTemplateAnnotations = nil

	//During execution, it is possible to determine which resources need to be created,
	//but these resources are limited to those closely related to the components, such as PVC.
	statefulSet, err := jobinfo.StatefulSetFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	if statefulSet.Spec.Replicas != nil {
		c.desiredReplicas = *statefulSet.Spec.Replicas
	} else {
		c.desiredReplicas = 1
	}

	statefulSetName := naming.StoreServerName(c.Job.Name, c.Job.ResourceAppNameOrID())
	statefulSet.Name = statefulSetName
	if statefulSet.Namespace == "" {
		statefulSet.Namespace = c.Namespace
	}

	updateStatefulSet := func(ctx context.Context, current *appsv1.StatefulSet) error {
		if !statefulSetNeedsUpdate(current, statefulSet) {
			c.expectedPodTemplateAnnotations = podTemplateReadyWaitAnnotationsFromTemplate(c.Job, &current.Spec.Template)
			jobexec.MarkResourceObserved(ctx, spec.ResourceStatefulSet, statefulSet.Namespace, statefulSet.Name)
			klog.Infof("StatefulSet %s/%s is up-to-date; skipping update", statefulSet.Namespace, statefulSet.Name)
			return nil
		}
		podTemplateChanged := statefulSetPodTemplateChanged(current, statefulSet)
		var expectedAnnotations map[string]string
		if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*appsv1.StatefulSet, error) {
			return c.Client.AppsV1().StatefulSets(statefulSet.Namespace).Get(ctx, statefulSet.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *appsv1.StatefulSet) error {
			updated := buildUpdatedStatefulSet(latest, statefulSet)
			if podTemplateChanged {
				expectedAnnotations = applyPodTemplateReadyWaitAnnotations(c.Job, &updated.Spec.Template)
			}
			_, err := c.Client.AppsV1().StatefulSets(statefulSet.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("update statefulset %s/%s failed: %w", statefulSet.Namespace, statefulSet.Name, err)
		}
		c.expectedPodTemplateAnnotations = expectedAnnotations
		jobexec.MarkResourceObserved(ctx, spec.ResourceStatefulSet, statefulSet.Namespace, statefulSet.Name)
		klog.Infof("StatefulSet %s/%s updated successfully", statefulSet.Namespace, statefulSet.Name)
		return nil
	}

	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[appsv1.StatefulSet]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:          c.Job,
			Ack:          c.Ack,
			Labels:       statefulSet.Labels,
			Kind:         spec.ResourceStatefulSet,
			LockProvider: c.ShareLocker,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := c.Client.AppsV1().StatefulSets(statefulSet.Namespace).List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy spec.ShareStrategy) {
				if strategy == spec.ShareStrategyIgnore {
					klog.Infof("StatefulSet %q marked as shared ignore; skipping", statefulSet.Name)
				} else {
					klog.Infof("StatefulSet %q already exists and is shared; skipping", statefulSet.Name)
				}
			},
		},
		Namespace: statefulSet.Namespace,
		Name:      statefulSet.Name,
		GetFn: func(ctx context.Context) (*appsv1.StatefulSet, error) {
			return c.Client.AppsV1().StatefulSets(statefulSet.Namespace).Get(ctx, statefulSet.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*appsv1.StatefulSet, error) {
			createCandidate := statefulSet.DeepCopy()
			expectedAnnotations := applyPodTemplateReadyWaitAnnotations(c.Job, &createCandidate.Spec.Template)
			created, err := c.Client.AppsV1().StatefulSets(statefulSet.Namespace).Create(ctx, createCandidate, metav1.CreateOptions{})
			if err == nil {
				c.expectedPodTemplateAnnotations = expectedAnnotations
			}
			return created, err
		},
		OnExisting:      updateStatefulSet,
		IsNotFound:      errors.IsNotFound,
		IsAlreadyExists: errors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure statefulset %s/%s: %w", statefulSet.Namespace, statefulSet.Name, err)
	}
	if created {
		klog.Infof("JobTask Deploy Successfully %q.\n", statefulSet.Name)
		return nil
	}
	return nil
}

func (c *DeployStatefulSetJobCtl) wait(ctx context.Context) error {
	targetName := naming.StoreServerName(c.Job.Name, c.Job.ResourceAppNameOrID())
	namespace := c.Job.Namespace
	var expectedImages []string
	if statefulSet, ok := jobinfo.OptionalJobInfo[*appsv1.StatefulSet](c.Job); ok {
		if statefulSet.Namespace != "" {
			namespace = statefulSet.Namespace
		}
		if statefulSet.Name != "" {
			targetName = statefulSet.Name
		}
		expectedImages = podSpecImages(statefulSet.Spec.Template.Spec)
	}
	timeout := time.Duration(c.timeout()) * time.Second

	waiter := c.ResourceWaiter
	if waiter == nil {
		return fmt.Errorf("resource waiter is required for statefulset %s/%s", c.Job.Namespace, targetName)
	}

	klog.V(4).Infof("Using informer-based wait for component %s/%s", c.Job.AppID, c.Job.Name)
	err := waiter.WaitForComponentReadyWithOptions(ctx, c.Job.AppID, naming.BoundedLabelValue(c.Job.Name), c.desiredReplicas, informer.ComponentReadyWaitOptions{
		ExpectedImages:      expectedImages,
		ExpectedAnnotations: c.expectedPodTemplateAnnotations,
	}, timeout)
	if err != nil {
		if we, ok := err.(*informer.WaitError); ok {
			return jobinfo.NewStatusError(we.Status, workloadWaitError(ctx, c.Client, c.Job, "StatefulSet", namespace, targetName, naming.BoundedLabelValue(c.Job.Name), we.Err))
		}
		return workloadWaitError(ctx, c.Client, c.Job, "StatefulSet", namespace, targetName, naming.BoundedLabelValue(c.Job.Name), err)
	}
	return nil
}

func (c *DeployStatefulSetJobCtl) timeout() int64 {
	if c.Job.Timeout == 0 {
		c.Job.Timeout = config.DeployTimeout
	}
	return c.Job.Timeout
}

func GenerateStoreService(component *model.ApplicationComponent) (*jobexec.GenerateServiceResult, error) {
	// 如果命名空间为空，则使用默认的命名空间
	if component.Namespace == "" {
		component.Namespace = config.DefaultNamespace
	}
	statefulSetName := naming.StoreServerName(component.Name, component.ResourceNameKey())
	containerName := utils.NormalizeLowerStrip(component.Name)

	properties, err := jobexec.ParseProperties(component.Properties)
	if err != nil {
		return nil, fmt.Errorf("parse component %s properties: %w", component.Name, err)
	}

	// 构建标签
	labels := jobexec.BuildLabels(component, &properties)

	// 构建需要开放的端口
	var ContainerPort []corev1.ContainerPort
	for _, v := range properties.Ports {
		ContainerPort = append(ContainerPort, corev1.ContainerPort{
			ContainerPort: v.Port,
		})
	}

	// 构建环境变量
	var envs []corev1.EnvVar
	for _, name := range slices.Sorted(maps.Keys(properties.Env)) {
		envs = append(envs, corev1.EnvVar{Name: name, Value: properties.Env[name]})
	}

	serviceName, err := statefulSetServiceName(component)
	if err != nil {
		return nil, fmt.Errorf("generate component %s: %w", component.Name, err)
	}

	container := corev1.Container{
		Name:            containerName,
		Image:           component.Image,
		Ports:           ContainerPort,
		Env:             envs,
		ImagePullPolicy: workflowconfig.DefaultWorkflowImagePullPolicy,
	}
	if len(properties.Command) > 0 {
		container.Command = properties.Command
	}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        statefulSetName,
			Namespace:   component.Namespace,
			Labels:      labels, // 设置 StatefulSet 自身的 labels，供 Informer 过滤和状态同步
			Annotations: jobexec.BuildAnnotations(component),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &component.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: defaultServiceSelector(component),
			},
			ServiceName: serviceName,
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: jobexec.BuildAnnotations(component),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						container,
					},
					TerminationGracePeriodSeconds: utils.Int64Ptr(30),
				},
			},
		},
	}

	additionalObjects, err := traitsPlu.ApplyTraits(component, statefulSet)
	if err != nil {
		return nil, fmt.Errorf("generate component %s: %w", component.Name, err)
	}
	return &jobexec.GenerateServiceResult{
		Service:           statefulSet,
		AdditionalObjects: additionalObjects,
	}, nil
}

func statefulSetServiceName(component *model.ApplicationComponent) (string, error) {
	if component == nil {
		return "", fmt.Errorf("component is nil")
	}
	defaultName := naming.ServiceName(component.Name, component.ResourceNameKey())
	traits, err := statefulSetComponentTraits(component)
	if err != nil {
		return "", err
	}
	for _, service := range traits.Service {
		if !service.Headless {
			continue
		}
		serviceType := serviceTypeFromTrait(service.Type)
		if serviceType == corev1.ServiceTypeExternalName {
			continue
		}
		if name := strings.TrimSpace(service.Name); name != "" {
			return name, nil
		}
		return defaultName, nil
	}
	return defaultName, nil
}

func statefulSetComponentTraits(component *model.ApplicationComponent) (*spec.Traits, error) {
	if component == nil || component.Traits == nil {
		return &spec.Traits{}, nil
	}
	traitBytes, err := json.Marshal(component.Traits)
	if err != nil {
		return nil, fmt.Errorf("marshal component traits: %w", err)
	}
	if string(traitBytes) == "{}" || string(traitBytes) == "null" {
		return &spec.Traits{}, nil
	}
	var traits spec.Traits
	if err := json.Unmarshal(traitBytes, &traits); err != nil {
		return nil, fmt.Errorf("unmarshal component traits: %w", err)
	}
	return &traits, nil
}

func statefulSetNeedsUpdate(current, desired *appsv1.StatefulSet) bool {
	if current == nil || desired == nil {
		return false
	}
	updated := buildUpdatedStatefulSet(current, desired)
	currentSpec := normalizeStatefulSetSpecForCompare(current.Spec)
	updatedSpec := normalizeStatefulSetSpecForCompare(updated.Spec)
	if !apiequality.Semantic.DeepEqual(currentSpec, updatedSpec) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Labels, updated.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Annotations, updated.Annotations) {
		return true
	}
	return false
}

func statefulSetPodTemplateChanged(current, desired *appsv1.StatefulSet) bool {
	if current == nil || desired == nil {
		return false
	}
	updated := buildUpdatedStatefulSet(current, desired)
	if !apiequality.Semantic.DeepEqual(current.Spec.Template.Labels, updated.Spec.Template.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Spec.Template.Annotations, updated.Spec.Template.Annotations) {
		return true
	}
	currentPodSpec := normalizePodSpecForCompare(current.Spec.Template.Spec)
	updatedPodSpec := normalizePodSpecForCompare(updated.Spec.Template.Spec)
	return !apiequality.Semantic.DeepEqual(currentPodSpec, updatedPodSpec)
}

func buildUpdatedStatefulSet(current, desired *appsv1.StatefulSet) *appsv1.StatefulSet {
	updated := current.DeepCopy()
	if desired == nil {
		return updated
	}
	updated.Labels = mergeStringMap(updated.Labels, desired.Labels)
	updated.Annotations = mergeStringMap(updated.Annotations, desired.Annotations)
	if desired.Spec.Replicas != nil {
		updated.Spec.Replicas = desired.Spec.Replicas
	}
	if desired.Spec.UpdateStrategy.Type != "" || desired.Spec.UpdateStrategy.RollingUpdate != nil {
		updated.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
	}
	if desired.Spec.PersistentVolumeClaimRetentionPolicy != nil {
		policy := *desired.Spec.PersistentVolumeClaimRetentionPolicy
		updated.Spec.PersistentVolumeClaimRetentionPolicy = &policy
	}
	if len(desired.Spec.Template.Spec.Containers) > 0 ||
		len(desired.Spec.Template.Spec.InitContainers) > 0 ||
		len(desired.Spec.Template.Spec.Volumes) > 0 {
		updated.Spec.Template.Spec = desired.Spec.Template.Spec
	}
	updated.Spec.Template.Labels = mergeStringMap(updated.Spec.Template.Labels, desired.Spec.Template.Labels)
	updated.Spec.Template.Annotations = mergeStringMap(updated.Spec.Template.Annotations, desired.Spec.Template.Annotations)
	return updated
}

func normalizeStatefulSetSpecForCompare(spec appsv1.StatefulSetSpec) appsv1.StatefulSetSpec {
	normalized := spec.DeepCopy()
	if normalized == nil {
		return appsv1.StatefulSetSpec{}
	}
	normalized.Template.Spec = normalizePodSpecForCompare(normalized.Template.Spec)
	return *normalized
}

func mergeStringMap(base, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	merged := utils.CopyStringMap(base)
	if merged == nil {
		merged = make(map[string]string, len(overlay))
	}
	for k, v := range overlay {
		merged[k] = v
	}
	return merged
}

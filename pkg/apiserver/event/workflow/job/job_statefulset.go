package job

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/fatih/color"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type DeployStatefulSetJobCtl struct {
	deployNamespacedResourceJobBase
	desiredReplicas                int32
	expectedPodTemplateAnnotations map[string]string
}

func NewDeployStatefulSetJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployStatefulSetJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployStatefulSetJobCtl", job, client, store, ack, shareLocker)
	if !ok {
		return nil
	}
	return &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployStatefulSetJobCtl) Clean(ctx context.Context) {
	if c.client == nil {
		return
	}
	ownershipCtx, ownershipCancel := adoptedRecoveryContext(ctx)
	_, _, adopted, err := adoptedApplicationForJob(ownershipCtx, c.store, c.job)
	ownershipCancel()
	if err != nil {
		// Failure cleanup is intentionally fail-closed. A datastore outage or a
		// detached application must never turn an adopted source workload into a
		// deletion candidate.
		klog.Warningf("cleanup: keep statefulset because application ownership could not be resolved: %v", err)
		return
	} else if adopted {
		klog.V(4).Infof("cleanup: keep adopted statefulset for application %s", c.job.AppID)
		return
	}
	namespace := c.namespace
	name := buildStoreSeverName(c.job.Name, c.job.ResourceAppNameOrID())
	var labels map[string]string
	if statefulSet, ok := optionalJobInfo[*appsv1.StatefulSet](c.job); ok {
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

	cleanupNamespacedResources(ctx, spec.ResourceStatefulSet, c.namespace, config.DelTimeOut, "statefulset", func(ctx context.Context, namespace, name string) error {
		return c.client.AppsV1().StatefulSets(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, errors.IsNotFound, "after job failure")

	forceDelete, reason := shouldForceCleanupSharedWorkload(ctx, c.client, namespace, labels)
	if !forceDelete {
		klog.V(4).Infof("cleanup: keep shared statefulset %s/%s (%s)", namespace, name, reason)
		return
	}

	deleteCtx, cancel := context.WithTimeout(context.Background(), config.DelTimeOut)
	defer cancel()
	if err := c.client.AppsV1().StatefulSets(namespace).Delete(deleteCtx, name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		klog.Errorf("cleanup: delete shared statefulset %s/%s failed: %v", namespace, name, err)
		return
	}
	klog.Infof("cleanup: deleted shared statefulset %s/%s due to abnormal pod (%s)", namespace, name, reason)
}

func (c *DeployStatefulSetJobCtl) Run(ctx context.Context) error {
	return c.runWithWait(ctx, c.run, c.wait, "DeployServiceJob run error", "")
}

func (c *DeployStatefulSetJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	c.expectedPodTemplateAnnotations = nil

	//During execution, it is possible to determine which resources need to be created,
	//but these resources are limited to those closely related to the components, such as PVC.
	statefulSet, err := statefulSetFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if statefulSet.Spec.Replicas != nil {
		c.desiredReplicas = *statefulSet.Spec.Replicas
	} else {
		c.desiredReplicas = 1
	}

	statefulSetName := buildStoreSeverName(c.job.Name, c.job.ResourceAppNameOrID())
	statefulSet.Name = statefulSetName
	if statefulSet.Namespace == "" {
		statefulSet.Namespace = c.namespace
	}
	if source, adopted, sourceErr := adoptedSourceForJob(ctx, c.store, c.job, "StatefulSet"); sourceErr != nil {
		return sourceErr
	} else if adopted {
		statefulSet.Name = source.SourceWorkloadName
		statefulSet.Namespace = adoptedSourceNamespace(source, statefulSet.Namespace)
		if statefulSet.Namespace == "" {
			statefulSet.Namespace = c.namespace
		}
		return c.reconcileAdoptedStatefulSet(ctx, statefulSet, source)
	}

	updateStatefulSet := func(ctx context.Context, current *appsv1.StatefulSet) error {
		if !statefulSetNeedsUpdate(current, statefulSet) {
			c.expectedPodTemplateAnnotations = podTemplateReadyWaitAnnotationsFromTemplate(c.job, &current.Spec.Template)
			markResourceObserved(ctx, spec.ResourceStatefulSet, statefulSet.Namespace, statefulSet.Name)
			klog.Infof("StatefulSet %s/%s is up-to-date; skipping update", statefulSet.Namespace, statefulSet.Name)
			return nil
		}
		podTemplateChanged := statefulSetPodTemplateChanged(current, statefulSet)
		var expectedAnnotations map[string]string
		if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*appsv1.StatefulSet, error) {
			return c.client.AppsV1().StatefulSets(statefulSet.Namespace).Get(ctx, statefulSet.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *appsv1.StatefulSet) error {
			updated := buildUpdatedStatefulSet(latest, statefulSet)
			if podTemplateChanged {
				expectedAnnotations = applyPodTemplateReadyWaitAnnotations(c.job, &updated.Spec.Template)
			}
			_, err := c.client.AppsV1().StatefulSets(statefulSet.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("update statefulset %s/%s failed: %w", statefulSet.Namespace, statefulSet.Name, err)
		}
		c.expectedPodTemplateAnnotations = expectedAnnotations
		markResourceObserved(ctx, spec.ResourceStatefulSet, statefulSet.Namespace, statefulSet.Name)
		klog.Infof("StatefulSet %s/%s updated successfully", statefulSet.Namespace, statefulSet.Name)
		return nil
	}

	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[appsv1.StatefulSet]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:          c.job,
			ack:          c.ack,
			labels:       statefulSet.Labels,
			kind:         spec.ResourceStatefulSet,
			lockProvider: c.shareLocker,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := c.client.AppsV1().StatefulSets(statefulSet.Namespace).List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy spec.ShareStrategy) {
				if strategy == spec.ShareStrategyIgnore {
					klog.Infof("StatefulSet %q marked as shared ignore; skipping", statefulSet.Name)
				} else {
					klog.Infof("StatefulSet %q already exists and is shared; skipping", statefulSet.Name)
				}
			},
		},
		namespace: statefulSet.Namespace,
		name:      statefulSet.Name,
		getFn: func(ctx context.Context) (*appsv1.StatefulSet, error) {
			return c.client.AppsV1().StatefulSets(statefulSet.Namespace).Get(ctx, statefulSet.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*appsv1.StatefulSet, error) {
			createCandidate := statefulSet.DeepCopy()
			expectedAnnotations := applyPodTemplateReadyWaitAnnotations(c.job, &createCandidate.Spec.Template)
			created, err := c.client.AppsV1().StatefulSets(statefulSet.Namespace).Create(ctx, createCandidate, metav1.CreateOptions{})
			if err == nil {
				c.expectedPodTemplateAnnotations = expectedAnnotations
			}
			return created, err
		},
		onExisting:      updateStatefulSet,
		isNotFound:      errors.IsNotFound,
		isAlreadyExists: errors.IsAlreadyExists,
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
	targetName := buildStoreSeverName(c.job.Name, c.job.ResourceAppNameOrID())
	namespace := c.job.Namespace
	var expectedImages []string
	if statefulSet, ok := optionalJobInfo[*appsv1.StatefulSet](c.job); ok {
		if statefulSet.Namespace != "" {
			namespace = statefulSet.Namespace
		}
		if statefulSet.Name != "" {
			targetName = statefulSet.Name
		}
		expectedImages = podSpecImages(statefulSet.Spec.Template.Spec)
	}
	timeout := time.Duration(c.timeout()) * time.Second

	waiter := c.resourceWaiter
	if waiter == nil {
		return fmt.Errorf("resource waiter is required for statefulset %s/%s", c.job.Namespace, targetName)
	}

	klog.V(4).Infof("Using informer-based wait for component %s/%s", c.job.AppID, c.job.Name)
	err := waiter.WaitForComponentReadyWithOptions(ctx, c.job.AppID, naming.BoundedLabelValue(c.job.Name), c.desiredReplicas, informer.ComponentReadyWaitOptions{
		ExpectedImages:      expectedImages,
		ExpectedAnnotations: c.expectedPodTemplateAnnotations,
	}, timeout)
	if err != nil {
		if we, ok := err.(*informer.WaitError); ok {
			return NewStatusError(we.Status, workloadWaitError(ctx, c.client, c.job, "StatefulSet", namespace, targetName, naming.BoundedLabelValue(c.job.Name), we.Err))
		}
		return workloadWaitError(ctx, c.client, c.job, "StatefulSet", namespace, targetName, naming.BoundedLabelValue(c.job.Name), err)
	}
	return nil
}

func (c *DeployStatefulSetJobCtl) timeout() int64 {
	if c.job.Timeout == 0 {
		c.job.Timeout = config.DeployTimeout
	}
	return c.job.Timeout
}

func (c *DeployStatefulSetJobCtl) reconcileAdoptedStatefulSet(ctx context.Context, desired *appsv1.StatefulSet, source *model.ApplicationComponent) error {
	if desired == nil || source == nil {
		return fmt.Errorf("adopted statefulset desired resource and source binding are required")
	}
	current, err := c.client.AppsV1().StatefulSets(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return c.recreateAdoptedStatefulSet(ctx, desired, source)
		}
		return fmt.Errorf("get adopted statefulset %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSourceUID(current.UID, source, "StatefulSet", desired.Namespace, desired.Name); err != nil {
		snapshotObject, snapshotErr := adoptedStatefulSetSnapshotForPersistence(current)
		if snapshotErr != nil {
			return fmt.Errorf("prepare recreated adopted statefulset snapshot: %w", snapshotErr)
		}
		recovered, recoverErr := recoverPendingAdoptedWorkload(
			ctx,
			c.store,
			c.job,
			source,
			"StatefulSet",
			desired.Namespace,
			desired.Name,
			snapshotObject,
			current,
			c.runtime,
			c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted statefulset binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	current, err = c.restoreAdoptedStatefulSetRetentionPolicy(ctx, current, source)
	if err != nil {
		return fmt.Errorf("restore adopted statefulset PVC retention policy: %w", err)
	}
	needsUpdate, err := adoptedStatefulSetNeedsUpdate(current, desired)
	if err != nil {
		return fmt.Errorf("adopted statefulset %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if !needsUpdate {
		c.expectedPodTemplateAnnotations = podTemplateReadyWaitAnnotationsFromTemplate(c.job, &current.Spec.Template)
		markResourceObserved(ctx, spec.ResourceStatefulSet, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*appsv1.StatefulSet, error) {
		return c.client.AppsV1().StatefulSets(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *appsv1.StatefulSet) error {
		if err := validateAdoptedSourceUID(latest.UID, source, "StatefulSet", desired.Namespace, desired.Name); err != nil {
			return err
		}
		changed, err := adoptedStatefulSetNeedsUpdate(latest, desired)
		if err != nil || !changed {
			return err
		}
		candidate, err := adoptedStatefulSetForExistingUpdate(latest, desired)
		if err != nil {
			return err
		}
		if !apiequality.Semantic.DeepEqual(latest.Spec.Template, candidate.Spec.Template) {
			applyPodTemplateReadyWaitAnnotations(c.job, &candidate.Spec.Template)
		}
		_, err = c.client.AppsV1().StatefulSets(desired.Namespace).Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("update adopted statefulset %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	updated, err := c.client.AppsV1().StatefulSets(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get updated adopted statefulset %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	c.expectedPodTemplateAnnotations = podTemplateReadyWaitAnnotationsFromTemplate(c.job, &updated.Spec.Template)
	markResourceObserved(ctx, spec.ResourceStatefulSet, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployStatefulSetJobCtl) recreateAdoptedStatefulSet(
	ctx context.Context,
	desired *appsv1.StatefulSet,
	source *model.ApplicationComponent,
) error {
	recreation, err := prepareAdoptedWorkloadRecreation(
		ctx,
		c.store,
		c.job,
		source,
		"StatefulSet",
		desired.Namespace,
		desired.Name,
	)
	if err != nil {
		return fmt.Errorf("prepare adopted statefulset recreation: %w", err)
	}
	var baseline appsv1.StatefulSet
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted statefulset recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted statefulset recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	candidate, err := adoptedStatefulSetForExistingUpdate(&baseline, desired)
	if err != nil {
		return fmt.Errorf("build adopted statefulset recreation candidate: %w", err)
	}
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"}
	expectedAnnotations := applyPodTemplateReadyWaitAnnotations(c.job, &candidate.Spec.Template)
	var desiredRetentionPolicy *appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy
	if retention := candidate.Spec.PersistentVolumeClaimRetentionPolicy; retention != nil {
		desiredPolicy := *retention
		desiredRetentionPolicy = &desiredPolicy
		if desiredPolicy.WhenDeleted == appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
			safePolicy := desiredPolicy
			safePolicy.WhenDeleted = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
			candidate.Spec.PersistentVolumeClaimRetentionPolicy = &safePolicy
			if candidate.Annotations == nil {
				candidate.Annotations = make(map[string]string)
			}
			candidate.Annotations[config.AnnotationAdoptedStatefulSetRetentionRestore] = string(desiredPolicy.WhenDeleted)
		}
	}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted statefulset recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	created, err := c.client.AppsV1().StatefulSets(candidate.Namespace).Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			replacement, getErr := c.client.AppsV1().StatefulSets(candidate.Namespace).Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted statefulset %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
			}
			snapshotObject, snapshotErr := adoptedStatefulSetSnapshotForPersistence(replacement)
			if snapshotErr != nil {
				return fmt.Errorf("prepare concurrent recreated adopted statefulset snapshot: %w", snapshotErr)
			}
			recovered, recoverErr := recoverPendingAdoptedWorkloadLocked(
				recreationCtx,
				c.store,
				c.job,
				source,
				"StatefulSet",
				candidate.Namespace,
				candidate.Name,
				snapshotObject,
				replacement,
				c.runtime,
			)
			if recoverErr != nil {
				return fmt.Errorf("recover concurrent adopted statefulset recreation: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedStatefulSet(ctx, desired, source)
			}
			return fmt.Errorf(
				"adopted statefulset ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf("recreate adopted statefulset %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	snapshotObject := created.DeepCopy()
	delete(snapshotObject.Annotations, config.AnnotationAdoptedStatefulSetRetentionRestore)
	if desiredRetentionPolicy != nil {
		policy := *desiredRetentionPolicy
		snapshotObject.Spec.PersistentVolumeClaimRetentionPolicy = &policy
	}
	if err := recreation.persistCreated(recreationCtx, snapshotObject, created, c.runtime); err != nil {
		return fmt.Errorf("persist recreated adopted statefulset binding; pending claim retained: %w", err)
	}
	created, err = c.restoreAdoptedStatefulSetRetentionPolicy(ctx, created, source)
	if err != nil {
		return fmt.Errorf("restore recreated adopted statefulset PVC retention policy: %w", err)
	}
	c.expectedPodTemplateAnnotations = expectedAnnotations
	markResourceObserved(ctx, spec.ResourceStatefulSet, created.Namespace, created.Name)
	return nil
}

func (c *DeployStatefulSetJobCtl) restoreAdoptedStatefulSetRetentionPolicy(
	ctx context.Context,
	current *appsv1.StatefulSet,
	source *model.ApplicationComponent,
) (*appsv1.StatefulSet, error) {
	if current == nil {
		return nil, fmt.Errorf("statefulset is nil")
	}
	pending := strings.TrimSpace(current.Annotations[config.AnnotationAdoptedStatefulSetRetentionRestore])
	if pending == "" {
		return current, nil
	}
	if pending != string(appsv1.DeletePersistentVolumeClaimRetentionPolicyType) {
		return nil, fmt.Errorf("unsupported pending retention policy %q", pending)
	}
	var restored *appsv1.StatefulSet
	err := updateResourceWithRetry(ctx, func(ctx context.Context) (*appsv1.StatefulSet, error) {
		return c.client.AppsV1().StatefulSets(current.Namespace).Get(ctx, current.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *appsv1.StatefulSet) error {
		if err := validateAdoptedSourceUID(latest.UID, source, "StatefulSet", current.Namespace, current.Name); err != nil {
			return err
		}
		marker := strings.TrimSpace(latest.Annotations[config.AnnotationAdoptedStatefulSetRetentionRestore])
		if marker == "" {
			restored = latest
			return nil
		}
		if marker != pending {
			return fmt.Errorf("pending retention policy changed from %q to %q", pending, marker)
		}
		update := latest.DeepCopy()
		policy := appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		}
		if update.Spec.PersistentVolumeClaimRetentionPolicy != nil {
			policy = *update.Spec.PersistentVolumeClaimRetentionPolicy
			policy.WhenDeleted = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
		}
		update.Spec.PersistentVolumeClaimRetentionPolicy = &policy
		delete(update.Annotations, config.AnnotationAdoptedStatefulSetRetentionRestore)
		updated, err := c.client.AppsV1().StatefulSets(update.Namespace).Update(ctx, update, metav1.UpdateOptions{})
		if err == nil {
			restored = updated
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	if restored == nil {
		return c.client.AppsV1().StatefulSets(current.Namespace).Get(ctx, current.Name, metav1.GetOptions{})
	}
	return restored, nil
}

func adoptedStatefulSetSnapshotForPersistence(current *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	if current == nil {
		return nil, fmt.Errorf("statefulset is nil")
	}
	snapshotObject := current.DeepCopy()
	pending := strings.TrimSpace(snapshotObject.Annotations[config.AnnotationAdoptedStatefulSetRetentionRestore])
	if pending == "" {
		return snapshotObject, nil
	}
	if pending != string(appsv1.DeletePersistentVolumeClaimRetentionPolicyType) {
		return nil, fmt.Errorf("unsupported pending retention policy %q", pending)
	}
	delete(snapshotObject.Annotations, config.AnnotationAdoptedStatefulSetRetentionRestore)
	policy := appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
	}
	if snapshotObject.Spec.PersistentVolumeClaimRetentionPolicy != nil {
		policy = *snapshotObject.Spec.PersistentVolumeClaimRetentionPolicy
		policy.WhenDeleted = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	}
	snapshotObject.Spec.PersistentVolumeClaimRetentionPolicy = &policy
	return snapshotObject, nil
}

func GenerateStoreService(component *model.ApplicationComponent) *GenerateServiceResult {
	// 如果命名空间为空，则使用默认的命名空间
	if component.Namespace == "" {
		component.Namespace = config.DefaultNamespace
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	containerName := utils.NormalizeLowerStrip(component.Name)

	properties := ParseProperties(component.Properties)

	// 构建标签
	labels := BuildLabels(component, &properties)

	// 构建需要开放的端口
	var ContainerPort []corev1.ContainerPort
	for _, v := range properties.Ports {
		ContainerPort = append(ContainerPort, corev1.ContainerPort{
			ContainerPort: v.Port,
		})
	}

	// 构建环境变量
	var envs []corev1.EnvVar
	for k, v := range properties.Env {
		envs = append(envs, corev1.EnvVar{Name: k, Value: v})
	}

	serviceName, err := statefulSetServiceName(component)
	if err != nil {
		klog.Errorf("Service Info %s StatefulSet ServiceName Error:%s", color.WhiteString(component.Namespace+"/"+component.Name), err)
		return nil
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
			Annotations: BuildAnnotations(component),
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
					Annotations: BuildAnnotations(component),
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
		klog.Errorf("Service Info %s Traits Error:%s", color.WhiteString(component.Namespace+"/"+component.Name), err)
		return nil
	}
	return &GenerateServiceResult{
		Service:           statefulSet,
		AdditionalObjects: additionalObjects,
	}
}

func statefulSetServiceName(component *model.ApplicationComponent) (string, error) {
	if component == nil {
		return "", fmt.Errorf("component is nil")
	}
	defaultName := buildServiceName(component.Name, component.ResourceNameKey())
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

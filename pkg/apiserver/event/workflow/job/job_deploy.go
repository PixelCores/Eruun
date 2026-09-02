package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fatih/color"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type DeployJobCtl struct {
	deployNamespacedResourceJobBase
	desiredReplicas                int32
	expectedPodTemplateAnnotations map[string]string
}

const defaultDeploymentRollingUpdatePercent = "25%"

func NewDeployJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployJobCtl {
	if client == nil || store == nil {
		return nil
	}
	base, ok := newDeployNamespacedResourceJobBase("DeployJobCtl", job, client, store, ack, shareLocker)
	if !ok {
		return nil
	}
	return &DeployJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployJobCtl) Clean(ctx context.Context) {
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
		klog.Warningf("cleanup: keep deployment because application ownership could not be resolved: %v", err)
		return
	} else if adopted {
		klog.V(4).Infof("cleanup: keep adopted deployment for application %s", c.job.AppID)
		return
	}
	namespace := c.job.Namespace
	name := buildWebServiceName(c.job.Name, c.job.ResourceAppNameOrID())
	var labels map[string]string
	if deploy, ok := optionalJobInfo[*appsv1.Deployment](c.job); ok {
		if deploy.Namespace != "" {
			namespace = deploy.Namespace
		}
		if deploy.Name != "" {
			name = deploy.Name
		}
		labels = deploy.Labels
	}
	if namespace == "" {
		namespace = config.DefaultNamespace
	}

	cleanupNamespacedResources(ctx, config.ResourceDeployment, c.job.Namespace, config.DelTimeOut, "deployment", func(ctx context.Context, namespace, name string) error {
		return c.client.AppsV1().Deployments(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after job failure")

	forceDelete, reason := shouldForceCleanupSharedWorkload(ctx, c.client, namespace, labels)
	if !forceDelete {
		klog.V(4).Infof("cleanup: keep shared deployment %s/%s (%s)", namespace, name, reason)
		return
	}

	deleteCtx, cancel := context.WithTimeout(context.Background(), config.DelTimeOut)
	defer cancel()
	if err := c.client.AppsV1().Deployments(namespace).Delete(deleteCtx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		klog.Errorf("cleanup: delete shared deployment %s/%s failed: %v", namespace, name, err)
		return
	}
	klog.Infof("cleanup: deleted shared deployment %s/%s due to abnormal pod (%s)", namespace, name, reason)
}

func (c *DeployJobCtl) Run(ctx context.Context) error {
	return c.runWithWait(ctx, c.run, c.wait, "", "")
}

func (c *DeployJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	c.expectedPodTemplateAnnotations = nil
	var (
		deploy     *appsv1.Deployment
		deployName string
	)
	deploy, err := deploymentFromJobInfo(c.job)
	if err != nil {
		return err
	}
	deployName = buildWebServiceName(c.job.Name, c.job.ResourceAppNameOrID())
	deploy.Name = deployName
	if source, adopted, sourceErr := adoptedSourceForJob(ctx, c.store, c.job, "Deployment"); sourceErr != nil {
		return sourceErr
	} else if adopted {
		deploy.Name = source.SourceWorkloadName
		deploy.Namespace = adoptedSourceNamespace(source, deploy.Namespace)
		if deploy.Namespace == "" {
			deploy.Namespace = c.job.Namespace
		}
		if deploy.Spec.Replicas != nil {
			c.desiredReplicas = *deploy.Spec.Replicas
		} else {
			c.desiredReplicas = 1
		}
		return c.reconcileAdoptedDeployment(ctx, deploy, source)
	}
	if deploy.Spec.Replicas != nil {
		c.desiredReplicas = *deploy.Spec.Replicas
	} else {
		c.desiredReplicas = 1
	}

	updateDeployment := func(ctx context.Context, current *appsv1.Deployment) error {
		if !isDeploymentChanged(current, deploy) {
			c.expectedPodTemplateAnnotations = podTemplateReadyWaitAnnotationsFromTemplate(c.job, &current.Spec.Template)
			klog.Infof("Deployment %q is up-to-date, skip update.", deploy.Name)
			markResourceObserved(ctx, config.ResourceDeployment, deploy.Namespace, deploy.Name)
			return nil
		}
		podTemplateChanged := deploymentPodTemplateChanged(current, deploy)
		var updated *appsv1.Deployment
		var expectedAnnotations map[string]string
		if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*appsv1.Deployment, error) {
			return c.client.AppsV1().Deployments(deploy.Namespace).Get(ctx, deploy.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *appsv1.Deployment) error {
			updateCandidate := deploymentForExistingUpdate(latest, deploy)
			if podTemplateChanged {
				expectedAnnotations = applyPodTemplateReadyWaitAnnotations(c.job, &updateCandidate.Spec.Template)
			}
			var err error
			updated, err = c.updateDeployment(ctx, updateCandidate)
			return err
		}); err != nil {
			klog.Errorf("failed to update deployment %q: %v", deploy.Name, err)
			return err
		}
		c.expectedPodTemplateAnnotations = expectedAnnotations
		klog.Infof("Deployment %q updated successfully.", updated.Name)
		markResourceObserved(ctx, config.ResourceDeployment, deploy.Namespace, deploy.Name)
		return nil
	}

	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[appsv1.Deployment]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:          c.job,
			ack:          c.ack,
			labels:       deploy.Labels,
			kind:         config.ResourceDeployment,
			lockProvider: c.shareLocker,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := c.client.AppsV1().Deployments(deploy.Namespace).List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy config.ShareStrategy) {
				if strategy == config.ShareStrategyIgnore {
					klog.Infof("Deployment %q marked as shared ignore; skipping", deploy.Name)
				} else {
					klog.Infof("Deployment %q already exists and is shared; skipping", deploy.Name)
				}
			},
		},
		namespace: deploy.Namespace,
		name:      deploy.Name,
		getFn: func(ctx context.Context) (*appsv1.Deployment, error) {
			return c.client.AppsV1().Deployments(deploy.Namespace).Get(ctx, deploy.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*appsv1.Deployment, error) {
			createCandidate := deploy.DeepCopy()
			expectedAnnotations := applyPodTemplateReadyWaitAnnotations(c.job, &createCandidate.Spec.Template)
			created, err := c.client.AppsV1().Deployments(deploy.Namespace).Create(ctx, createCandidate, metav1.CreateOptions{})
			if err == nil {
				c.expectedPodTemplateAnnotations = expectedAnnotations
			}
			return created, err
		},
		onExisting:      updateDeployment,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure deployment %q failed: %w", deploy.Name, err)
	}
	if created {
		klog.Infof("JobTask Deploy Successfully %q.\n", deploy.Name)
	}

	return nil
}

func (c *DeployJobCtl) wait(ctx context.Context) error {
	targetName := buildWebServiceName(c.job.Name, c.job.ResourceAppNameOrID())
	namespace := c.job.Namespace
	var expectedImages []string
	if deploy, ok := optionalJobInfo[*appsv1.Deployment](c.job); ok {
		if deploy.Namespace != "" {
			namespace = deploy.Namespace
		}
		if deploy.Name != "" {
			targetName = deploy.Name
		}
		expectedImages = podSpecImages(deploy.Spec.Template.Spec)
	}
	timeout := time.Duration(c.timeout()) * time.Second

	waiter := c.resourceWaiter
	if waiter == nil {
		return fmt.Errorf("resource waiter is required for deployment %s/%s", c.job.Namespace, targetName)
	}

	klog.V(4).Infof("Using informer-based wait for component %s/%s", c.job.AppID, c.job.Name)
	err := waiter.WaitForComponentReadyWithOptions(ctx, c.job.AppID, naming.BoundedLabelValue(c.job.Name), c.desiredReplicas, informer.ComponentReadyWaitOptions{
		ExpectedImages:      expectedImages,
		ExpectedAnnotations: c.expectedPodTemplateAnnotations,
	}, timeout)
	if err != nil {
		var we *informer.WaitError
		if errors.As(err, &we) {
			return NewStatusError(we.Status, workloadWaitError(ctx, c.client, c.job, "Deployment", namespace, targetName, naming.BoundedLabelValue(c.job.Name), we.Err))
		}
		return workloadWaitError(ctx, c.client, c.job, "Deployment", namespace, targetName, naming.BoundedLabelValue(c.job.Name), err)
	}
	return nil
}

func (c *DeployJobCtl) timeout() int64 {
	if c.job.Timeout == 0 {
		c.job.Timeout = config.DeployTimeout
	}
	return c.job.Timeout
}

func GenerateWebService(component *model.ApplicationComponent, properties *model.Properties) *GenerateServiceResult {
	deploymentName := buildWebServiceName(component.Name, component.ResourceNameKey())
	containerName := utils.NormalizeLowerStrip(component.Name)
	var ContainerPort []corev1.ContainerPort
	for _, v := range properties.Ports {
		ContainerPort = append(ContainerPort, corev1.ContainerPort{
			ContainerPort: v.Port,
		})
	}

	var envs []corev1.EnvVar
	for k, v := range properties.Env {
		envs = append(envs, corev1.EnvVar{Name: k, Value: v})
	}

	labels := BuildLabels(component, properties)

	container := corev1.Container{
		Name:            containerName,
		Image:           component.Image,
		Ports:           ContainerPort,
		Env:             envs,
		ImagePullPolicy: config.DefaultWorkflowImagePullPolicy,
	}
	if properties != nil && len(properties.Command) > 0 {
		container.Command = properties.Command
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deploymentName,
			Namespace:   component.Namespace,
			Labels:      labels, // 设置 Deployment 自身的 labels，供 Informer 过滤和状态同步
			Annotations: BuildAnnotations(component),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &component.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: defaultServiceSelector(component),
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
				},
			},
		},
	}

	additionalObjects, err := traitsPlu.ApplyTraits(component, deployment)
	if err != nil {
		klog.Errorf("Service Info %s Traits Error:%s", color.WhiteString(component.Namespace+"/"+component.Name), err)
		return nil
	}
	return &GenerateServiceResult{
		Service:           deployment,
		AdditionalObjects: additionalObjects,
	}
}

func (c *DeployJobCtl) updateDeployment(ctx context.Context, deploy *appsv1.Deployment) (*appsv1.Deployment, error) {
	result, err := c.client.AppsV1().Deployments(deploy.Namespace).Update(ctx, deploy, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update deployment failed: %w", err)
	}
	return result, nil
}

func (c *DeployJobCtl) reconcileAdoptedDeployment(ctx context.Context, desired *appsv1.Deployment, source *model.ApplicationComponent) error {
	if desired == nil || source == nil {
		return fmt.Errorf("adopted deployment desired resource and source binding are required")
	}
	current, err := c.client.AppsV1().Deployments(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedDeployment(ctx, desired, source)
		}
		return fmt.Errorf("get adopted deployment %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSourceUID(current.UID, source, "Deployment", desired.Namespace, desired.Name); err != nil {
		recovered, recoverErr := recoverPendingAdoptedWorkload(
			ctx,
			c.store,
			c.job,
			source,
			"Deployment",
			desired.Namespace,
			desired.Name,
			current,
			current,
			c.runtime,
			c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted deployment binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	if !adoptedDeploymentNeedsUpdate(current, desired) {
		c.expectedPodTemplateAnnotations = podTemplateReadyWaitAnnotationsFromTemplate(c.job, &current.Spec.Template)
		markResourceObserved(ctx, config.ResourceDeployment, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*appsv1.Deployment, error) {
		return c.client.AppsV1().Deployments(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *appsv1.Deployment) error {
		if err := validateAdoptedSourceUID(latest.UID, source, "Deployment", desired.Namespace, desired.Name); err != nil {
			return err
		}
		candidate := adoptedDeploymentForExistingUpdate(latest, desired)
		if !adoptedDeploymentNeedsUpdate(latest, desired) {
			return nil
		}
		if !apiequality.Semantic.DeepEqual(latest.Spec.Template, candidate.Spec.Template) {
			applyPodTemplateReadyWaitAnnotations(c.job, &candidate.Spec.Template)
		}
		_, err := c.updateDeployment(ctx, candidate)
		return err
	}); err != nil {
		return fmt.Errorf("update adopted deployment %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	updated, err := c.client.AppsV1().Deployments(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get updated adopted deployment %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	c.expectedPodTemplateAnnotations = podTemplateReadyWaitAnnotationsFromTemplate(c.job, &updated.Spec.Template)
	markResourceObserved(ctx, config.ResourceDeployment, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployJobCtl) recreateAdoptedDeployment(
	ctx context.Context,
	desired *appsv1.Deployment,
	source *model.ApplicationComponent,
) error {
	recreation, err := prepareAdoptedWorkloadRecreation(
		ctx,
		c.store,
		c.job,
		source,
		"Deployment",
		desired.Namespace,
		desired.Name,
	)
	if err != nil {
		return fmt.Errorf("prepare adopted deployment recreation: %w", err)
	}
	var baseline appsv1.Deployment
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted deployment recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted deployment recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	candidate := adoptedDeploymentForExistingUpdate(&baseline, desired)
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
	expectedAnnotations := applyPodTemplateReadyWaitAnnotations(c.job, &candidate.Spec.Template)
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted deployment recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	created, err := c.client.AppsV1().Deployments(candidate.Namespace).Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := c.client.AppsV1().Deployments(candidate.Namespace).Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted deployment %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
			}
			recovered, recoverErr := recoverPendingAdoptedWorkloadLocked(
				recreationCtx,
				c.store,
				c.job,
				source,
				"Deployment",
				candidate.Namespace,
				candidate.Name,
				replacement,
				replacement,
				c.runtime,
			)
			if recoverErr != nil {
				return fmt.Errorf("recover concurrent adopted deployment recreation: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedDeployment(ctx, desired, source)
			}
			return fmt.Errorf(
				"adopted deployment ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf("recreate adopted deployment %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	if err := recreation.persistCreated(recreationCtx, created, created, c.runtime); err != nil {
		return fmt.Errorf("persist recreated adopted deployment binding; pending claim retained: %w", err)
	}
	c.expectedPodTemplateAnnotations = expectedAnnotations
	markResourceObserved(ctx, config.ResourceDeployment, created.Namespace, created.Name)
	return nil
}

func deploymentForExistingUpdate(current, desired *appsv1.Deployment) *appsv1.Deployment {
	update := current.DeepCopy()
	update.Labels = preserveStringMapKeys(current.Labels, desired.Labels, eruunSystemLabelKeys)
	update.Annotations = preserveStringMapKeys(current.Annotations, desired.Annotations, nil)
	update.Spec = *desired.Spec.DeepCopy()
	if current.Spec.Selector != nil {
		update.Spec.Selector = current.Spec.Selector.DeepCopy()
	}
	update.Spec.Template.Labels = preserveStringMapKeys(current.Spec.Template.Labels, desired.Spec.Template.Labels, eruunSystemLabelKeys)
	if current.Spec.Selector != nil && len(current.Spec.Selector.MatchLabels) > 0 {
		update.Spec.Template.Labels = mergeStringMap(update.Spec.Template.Labels, current.Spec.Selector.MatchLabels)
	}
	update.Spec.Template.Annotations = preserveStringMapKeys(current.Spec.Template.Annotations, desired.Spec.Template.Annotations, eruunWorkloadTemplateAnnotationKeys)
	return update
}

func isDeploymentChanged(current, desired *appsv1.Deployment) bool {
	update := deploymentForExistingUpdate(current, desired)
	if !apiequality.Semantic.DeepEqual(current.Labels, update.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Annotations, update.Annotations) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Spec.Selector, update.Spec.Selector) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Spec.Replicas, update.Spec.Replicas) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Spec.Template.Labels, update.Spec.Template.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Spec.Template.Annotations, update.Spec.Template.Annotations) {
		return true
	}
	if deploymentStrategyChanged(current.Spec.Strategy, desired.Spec.Strategy) {
		return true
	}

	currentPodSpec := normalizePodSpecForCompare(current.Spec.Template.Spec)
	updatePodSpec := normalizePodSpecForCompare(update.Spec.Template.Spec)
	if !apiequality.Semantic.DeepEqual(currentPodSpec, updatePodSpec) {
		return true
	}

	return false
}

func deploymentPodTemplateChanged(current, desired *appsv1.Deployment) bool {
	if current == nil || desired == nil {
		return false
	}
	update := deploymentForExistingUpdate(current, desired)
	if !apiequality.Semantic.DeepEqual(current.Spec.Template.Labels, update.Spec.Template.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Spec.Template.Annotations, update.Spec.Template.Annotations) {
		return true
	}
	currentPodSpec := normalizePodSpecForCompare(current.Spec.Template.Spec)
	updatePodSpec := normalizePodSpecForCompare(update.Spec.Template.Spec)
	return !apiequality.Semantic.DeepEqual(currentPodSpec, updatePodSpec)
}

func deploymentStrategyChanged(current, desired appsv1.DeploymentStrategy) bool {
	if deploymentStrategyConfigured(desired) {
		return !apiequality.Semantic.DeepEqual(current, desired)
	}
	// Empty desired strategy means Eruun no longer declares this field.
	return deploymentStrategyRequiresReset(current)
}

func deploymentStrategyConfigured(strategy appsv1.DeploymentStrategy) bool {
	return strategy.Type != "" || strategy.RollingUpdate != nil
}

func deploymentStrategyRequiresReset(strategy appsv1.DeploymentStrategy) bool {
	if !deploymentStrategyConfigured(strategy) {
		return false
	}
	if strategy.Type != "" && strategy.Type != appsv1.RollingUpdateDeploymentStrategyType {
		return true
	}
	return rollingUpdateDeploymentOverridesDefault(strategy.RollingUpdate)
}

func rollingUpdateDeploymentOverridesDefault(rollingUpdate *appsv1.RollingUpdateDeployment) bool {
	if rollingUpdate == nil {
		return false
	}
	return !intOrStringIsNilOrDefaultDeploymentPercent(rollingUpdate.MaxSurge) ||
		!intOrStringIsNilOrDefaultDeploymentPercent(rollingUpdate.MaxUnavailable)
}

func intOrStringIsNilOrDefaultDeploymentPercent(value *intstr.IntOrString) bool {
	if value == nil {
		return true
	}
	defaultValue := intstr.FromString(defaultDeploymentRollingUpdatePercent)
	return apiequality.Semantic.DeepEqual(*value, defaultValue)
}

func cleanObjectMeta(meta *metav1.ObjectMeta) {
	meta.ResourceVersion = ""
	meta.UID = ""
	meta.CreationTimestamp = metav1.Time{}
	meta.ManagedFields = nil
}

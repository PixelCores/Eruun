package jobdeploy

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type DeployJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
	desiredReplicas                int32
	expectedPodTemplateAnnotations map[string]string
}

const defaultDeploymentRollingUpdatePercent = "25%"

func NewDeployJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployJobCtl {
	if runtime == nil || runtime.Client == nil || runtime.Store == nil {
		return nil
	}
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployJobCtl) Clean(ctx context.Context) {
	if c.Client == nil {
		return
	}
	namespace := c.Job.Namespace
	name := naming.WebServiceName(c.Job.Name, c.Job.ResourceAppNameOrID())
	var labels map[string]string
	if deploy, ok := jobinfo.OptionalJobInfo[*appsv1.Deployment](c.Job); ok {
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

	jobexec.CleanupNamespacedResources(ctx, domainspec.ResourceDeployment, c.Job.Namespace, config.DelTimeOut, "deployment", func(ctx context.Context, namespace, name string) error {
		return c.Client.AppsV1().Deployments(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after job failure")

	forceDelete, reason := jobexec.ShouldForceCleanupSharedWorkload(ctx, c.Client, namespace, labels)
	if !forceDelete {
		klog.V(4).Infof("cleanup: keep shared deployment %s/%s (%s)", namespace, name, reason)
		return
	}

	deleteCtx, cancel := context.WithTimeout(context.Background(), config.DelTimeOut)
	defer cancel()
	if err := c.Client.AppsV1().Deployments(namespace).Delete(deleteCtx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		klog.Errorf("cleanup: delete shared deployment %s/%s failed: %v", namespace, name, err)
		return
	}
	klog.Infof("cleanup: deleted shared deployment %s/%s due to abnormal pod (%s)", namespace, name, reason)
}

func (c *DeployJobCtl) Run(ctx context.Context) error {
	return c.RunWithWait(ctx, c.run, c.wait, "", "")
}

func (c *DeployJobCtl) run(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}
	c.expectedPodTemplateAnnotations = nil
	var (
		deploy     *appsv1.Deployment
		deployName string
	)
	deploy, err := jobinfo.DeploymentFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	deployName = naming.WebServiceName(c.Job.Name, c.Job.ResourceAppNameOrID())
	deploy.Name = deployName
	if deploy.Spec.Replicas != nil {
		c.desiredReplicas = *deploy.Spec.Replicas
	} else {
		c.desiredReplicas = 1
	}

	updateDeployment := func(ctx context.Context, current *appsv1.Deployment) error {
		if !isDeploymentChanged(current, deploy) {
			c.expectedPodTemplateAnnotations = podTemplateReadyWaitAnnotationsFromTemplate(c.Job, &current.Spec.Template)
			klog.Infof("Deployment %q is up-to-date, skip update.", deploy.Name)
			jobexec.MarkResourceObserved(ctx, domainspec.ResourceDeployment, deploy.Namespace, deploy.Name)
			return nil
		}
		podTemplateChanged := deploymentPodTemplateChanged(current, deploy)
		var updated *appsv1.Deployment
		var expectedAnnotations map[string]string
		if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*appsv1.Deployment, error) {
			return c.Client.AppsV1().Deployments(deploy.Namespace).Get(ctx, deploy.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *appsv1.Deployment) error {
			updateCandidate := deploymentForExistingUpdate(latest, deploy)
			if podTemplateChanged {
				expectedAnnotations = applyPodTemplateReadyWaitAnnotations(c.Job, &updateCandidate.Spec.Template)
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
		jobexec.MarkResourceObserved(ctx, domainspec.ResourceDeployment, deploy.Namespace, deploy.Name)
		return nil
	}

	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[appsv1.Deployment]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:          c.Job,
			Ack:          c.Ack,
			Labels:       deploy.Labels,
			Kind:         domainspec.ResourceDeployment,
			LockProvider: c.ShareLocker,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := c.Client.AppsV1().Deployments(deploy.Namespace).List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					klog.Infof("Deployment %q marked as shared ignore; skipping", deploy.Name)
				} else {
					klog.Infof("Deployment %q already exists and is shared; skipping", deploy.Name)
				}
			},
		},
		Namespace: deploy.Namespace,
		Name:      deploy.Name,
		GetFn: func(ctx context.Context) (*appsv1.Deployment, error) {
			return c.Client.AppsV1().Deployments(deploy.Namespace).Get(ctx, deploy.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*appsv1.Deployment, error) {
			createCandidate := deploy.DeepCopy()
			expectedAnnotations := applyPodTemplateReadyWaitAnnotations(c.Job, &createCandidate.Spec.Template)
			created, err := c.Client.AppsV1().Deployments(deploy.Namespace).Create(ctx, createCandidate, metav1.CreateOptions{})
			if err == nil {
				c.expectedPodTemplateAnnotations = expectedAnnotations
			}
			return created, err
		},
		OnExisting:      updateDeployment,
		IsNotFound:      k8serrors.IsNotFound,
		IsAlreadyExists: k8serrors.IsAlreadyExists,
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
	targetName := naming.WebServiceName(c.Job.Name, c.Job.ResourceAppNameOrID())
	namespace := c.Job.Namespace
	var expectedImages []string
	if deploy, ok := jobinfo.OptionalJobInfo[*appsv1.Deployment](c.Job); ok {
		if deploy.Namespace != "" {
			namespace = deploy.Namespace
		}
		if deploy.Name != "" {
			targetName = deploy.Name
		}
		expectedImages = podSpecImages(deploy.Spec.Template.Spec)
	}
	timeout := time.Duration(c.timeout()) * time.Second

	waiter := c.ResourceWaiter
	if waiter == nil {
		return fmt.Errorf("resource waiter is required for deployment %s/%s", c.Job.Namespace, targetName)
	}

	klog.V(4).Infof("Using informer-based wait for component %s/%s", c.Job.AppID, c.Job.Name)
	err := waiter.WaitForComponentReadyWithOptions(ctx, c.Job.AppID, naming.BoundedLabelValue(c.Job.Name), c.desiredReplicas, informer.ComponentReadyWaitOptions{
		ExpectedImages:      expectedImages,
		ExpectedAnnotations: c.expectedPodTemplateAnnotations,
	}, timeout)
	if err != nil {
		var we *informer.WaitError
		if errors.As(err, &we) {
			return jobinfo.NewStatusError(we.Status, workloadWaitError(ctx, c.Client, c.Job, "Deployment", namespace, targetName, naming.BoundedLabelValue(c.Job.Name), we.Err))
		}
		return workloadWaitError(ctx, c.Client, c.Job, "Deployment", namespace, targetName, naming.BoundedLabelValue(c.Job.Name), err)
	}
	return nil
}

func (c *DeployJobCtl) timeout() int64 {
	if c.Job.Timeout == 0 {
		c.Job.Timeout = config.DeployTimeout
	}
	return c.Job.Timeout
}

func GenerateWebService(component *model.ApplicationComponent, properties *model.Properties, profiles traitsPlu.Profiles) (*jobexec.GenerateServiceResult, error) {
	deploymentName := naming.WebServiceName(component.Name, component.ResourceNameKey())
	containerName := utils.NormalizeLowerStrip(component.Name)
	var ContainerPort []corev1.ContainerPort
	for _, v := range properties.Ports {
		ContainerPort = append(ContainerPort, corev1.ContainerPort{
			ContainerPort: v.Port,
		})
	}

	var envs []corev1.EnvVar
	for _, name := range slices.Sorted(maps.Keys(properties.Env)) {
		envs = append(envs, corev1.EnvVar{Name: name, Value: properties.Env[name]})
	}

	labels := jobexec.BuildLabels(component, properties)

	container := corev1.Container{
		Name:            containerName,
		Image:           component.Image,
		Ports:           ContainerPort,
		Env:             envs,
		ImagePullPolicy: workflowconfig.DefaultWorkflowImagePullPolicy,
	}
	if properties != nil && len(properties.Command) > 0 {
		container.Command = properties.Command
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deploymentName,
			Namespace:   component.Namespace,
			Labels:      labels, // 设置 Deployment 自身的 labels，供 Informer 过滤和状态同步
			Annotations: jobexec.BuildAnnotations(component),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &component.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: defaultServiceSelector(component),
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
				},
			},
		},
	}

	additionalObjects, err := traitsPlu.ApplyTraits(component, deployment, profiles)
	if err != nil {
		return nil, fmt.Errorf("generate component %s: %w", component.Name, err)
	}
	return &jobexec.GenerateServiceResult{
		Service:           deployment,
		AdditionalObjects: additionalObjects,
	}, nil
}

func (c *DeployJobCtl) updateDeployment(ctx context.Context, deploy *appsv1.Deployment) (*appsv1.Deployment, error) {
	result, err := c.Client.AppsV1().Deployments(deploy.Namespace).Update(ctx, deploy, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update deployment failed: %w", err)
	}
	return result, nil
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
	return !apiequality.Semantic.DeepEqual(currentPodSpec, updatePodSpec)
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

func CleanObjectMeta(meta *metav1.ObjectMeta) {
	meta.ResourceVersion = ""
	meta.UID = ""
	meta.CreationTimestamp = metav1.Time{}
	meta.ManagedFields = nil
}

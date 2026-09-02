package application

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func (c *applicationsServiceImpl) CleanupApplicationResources(ctx context.Context, appID string) (*apisv1.CleanupApplicationResourcesResponse, error) {
	var response *apisv1.CleanupApplicationResourcesResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "cleanup-application-resources", func(lockCtx context.Context, app *model.Applications) error {
		var cleanupErr error
		response, cleanupErr = c.cleanupApplicationResourcesUnlocked(lockCtx, app.ID, false)
		return cleanupErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) cleanupApplicationResourcesUnlocked(
	ctx context.Context,
	appID string,
	bypassWorkflowGuards bool,
) (*apisv1.CleanupApplicationResourcesResponse, error) {
	app, err := c.AppRepo.FindByID(ctx, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	if app.EffectiveManagementMode() != config.ManagementModeNative {
		return nil, fmt.Errorf("%w: resource cleanup is disabled for %s applications",
			bcode.ErrApplicationManagementMode, app.EffectiveManagementMode())
	}
	if !bypassWorkflowGuards {
		if err := EnsureAppWorkflowIdle(ctx, c.Store, app.ID); err != nil {
			return nil, err
		}
		if err := EnsureNoPendingStatefulSetCleanup(ctx, c.Store, app.ID); err != nil {
			return nil, err
		}
	}
	defer func() {
		c.invalidateApplicationListCaches()
		c.invalidateApplicationComponentsCache(app.ID)
	}()
	startTime := time.Now().Unix()
	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(components, applicationResourceNameKey(app))
	if len(components) == 0 {
		resp := &apisv1.CleanupApplicationResourcesResponse{AppID: app.ID}
		resp.TaskID = c.attachOperationTask(ctx, app, config.WorkflowTaskTypeCleanup, operationTaskNameCleanup, startTime, startTime, nil, nil)
		return resp, nil
	}

	reporter := newCleanupReporter()
	for _, component := range components {
		if component == nil {
			continue
		}
		if skip, reason := c.shouldSkipSharedComponentCleanup(ctx, component); skip {
			klog.Infof("cleanup: skip shared component %s/%s (%s)", component.Namespace, component.Name, reason)
			c.updateComponentCleanupStatus(ctx, component, config.ComponentStatusNotDeploy)
			continue
		} else if reason != "" {
			klog.Infof("cleanup: continue shared component cleanup %s/%s (%s)", component.Namespace, component.Name, reason)
		}
		c.markComponentCleaning(ctx, component)
		if err := c.deleteComponentResources(ctx, component, reporter); err != nil {
			klog.Errorf("cleanup component %s/%s failed: %v", component.Name, component.AppID, err)
		}
		c.finalizeComponentCleanupStatus(ctx, component)
	}
	endTime := time.Now().Unix()

	resp := &apisv1.CleanupApplicationResourcesResponse{
		AppID:            app.ID,
		DeletedResources: reporter.deletedResources,
	}
	resp.TaskID = c.attachOperationTask(ctx, app, config.WorkflowTaskTypeCleanup, operationTaskNameCleanup, startTime, endTime, buildCleanupJobRecords(reporter), reporter.failedResources)
	if len(reporter.failedResources) > 0 {
		resp.FailedResources = reporter.failedResources
		return resp, reporter.err()
	}
	return resp, nil
}

func (c *applicationsServiceImpl) markComponentCleaning(ctx context.Context, component *model.ApplicationComponent) {
	if component == nil {
		return
	}
	if !componentUsesPods(component.ComponentType) {
		return
	}
	c.updateComponentCleanupStatus(ctx, component, config.ComponentStatusCleaning)
}

func (c *applicationsServiceImpl) finalizeComponentCleanupStatus(ctx context.Context, component *model.ApplicationComponent) {
	if component == nil {
		return
	}
	status := config.ComponentStatusNotDeploy
	if componentUsesPods(component.ComponentType) && c.shouldKeepCleaning(ctx, component) {
		status = config.ComponentStatusCleaning
	}
	c.updateComponentCleanupStatus(ctx, component, status)
}

func componentUsesPods(componentType config.JobType) bool {
	return config.ComponentTypeUsesPods(componentType)
}

func (c *applicationsServiceImpl) shouldKeepCleaning(ctx context.Context, component *model.ApplicationComponent) bool {
	if component == nil {
		return false
	}
	appID := strings.TrimSpace(component.AppID)
	name := strings.TrimSpace(component.Name)
	if appID == "" || name == "" {
		return false
	}
	if c.KubeClient == nil {
		return false
	}
	namespace := strings.TrimSpace(component.Namespace)
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	labelSet := labels.Set{
		config.LabelAppID:         appID,
		config.LabelComponentName: naming.BoundedLabelValue(name),
	}
	if component.ID > 0 {
		labelSet[config.LabelComponentID] = strconv.Itoa(component.ID)
	}
	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationCleanupTimeout)
	defer cancel()
	pods, err := kube.ListPodsByLabels(opCtx, c.KubeClient, namespace, labelSet)
	if err != nil {
		klog.Errorf("list pods for cleanup check failed appID=%s component=%s: %v", appID, name, err)
		return true
	}
	return pods != nil && len(pods.Items) > 0
}

func (c *applicationsServiceImpl) updateComponentCleanupStatus(ctx context.Context, component *model.ApplicationComponent, status config.ComponentStatus) {
	if component == nil {
		return
	}
	readyReplicas := int32(0)
	lastAbnormal := ""
	component.Status = string(status)
	component.ReadyReplicas = readyReplicas
	component.LastAbnormal = lastAbnormal
	if err := repository.UpdateComponentRuntimeFields(ctx, c.Store, component, map[string]interface{}{
		"status":         string(status),
		"ready_replicas": readyReplicas,
		"last_abnormal":  lastAbnormal,
	}); err != nil {
		klog.Errorf("update component %s status to %s failed: %v", component.Name, status, err)
		return
	}
	klog.Infof("component %s (id=%d) status updated to %s after resource cleanup", component.Name, component.ID, status)
}

// SharedLifecycleStrategyForComponent returns the normalized share strategy
// when default/ignore protects the component from per-application lifecycle
// actions. Missing share traits and share=force remain application-managed.
func SharedLifecycleStrategyForComponent(component *model.ApplicationComponent) (config.ShareStrategy, bool) {
	strategy, shared := component.ShareStrategy()
	if !shared || strategy == config.ShareStrategyForce {
		return "", false
	}
	return strategy, true
}

func (c *applicationsServiceImpl) shouldSkipSharedComponentCleanup(ctx context.Context, component *model.ApplicationComponent) (bool, string) {
	strategy, shared := SharedLifecycleStrategyForComponent(component)
	if !shared {
		return false, ""
	}
	if !componentUsesPods(component.ComponentType) {
		return true, fmt.Sprintf("strategy=%s non-pod component", strategy)
	}

	abnormal, reason, err := c.sharedComponentHasAbnormalPods(ctx, component)
	if err != nil {
		return true, fmt.Sprintf("strategy=%s inspect pods failed: %v", strategy, err)
	}
	if abnormal {
		return false, fmt.Sprintf("strategy=%s abnormal pod: %s", strategy, reason)
	}
	return true, fmt.Sprintf("strategy=%s no abnormal pod", strategy)
}

func (c *applicationsServiceImpl) sharedComponentHasAbnormalPods(ctx context.Context, component *model.ApplicationComponent) (bool, string, error) {
	if component == nil {
		return false, "", nil
	}
	if c.KubeClient == nil {
		return false, "", fmt.Errorf("kube client is nil")
	}
	appID := strings.TrimSpace(component.AppID)
	componentName := strings.TrimSpace(component.Name)
	if appID == "" || componentName == "" {
		return false, "", fmt.Errorf("component identity is incomplete")
	}
	namespace := pickNamespace(strings.TrimSpace(component.Namespace), config.DefaultNamespace)
	labelSet := labels.Set{
		config.LabelAppID:         appID,
		config.LabelComponentName: naming.BoundedLabelValue(componentName),
	}
	if component.ID > 0 {
		labelSet[config.LabelComponentID] = strconv.Itoa(component.ID)
	}

	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationCleanupTimeout)
	defer cancel()
	pods, err := kube.ListPodsByLabels(opCtx, c.KubeClient, namespace, labelSet)
	if err != nil {
		return false, "", err
	}
	if pods == nil {
		return false, "", nil
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		reason := kube.ExtractPodAbnormalReason(pod)
		if reason == "" {
			continue
		}
		return true, fmt.Sprintf("%s/%s %s", namespace, pod.Name, reason), nil
	}
	return false, "", nil
}

func (c *applicationsServiceImpl) deleteComponentResources(ctx context.Context, component *model.ApplicationComponent, reporter *cleanupReporter) error {
	props := job.ParseProperties(component.Properties)
	componentCopy := *component
	if componentCopy.Namespace == "" {
		componentCopy.Namespace = config.DefaultNamespace
	}
	componentPtr := &componentCopy

	switch component.ComponentType {
	case config.ServerJob:
		result := job.GenerateWebService(componentPtr, &props)
		deployNS := componentPtr.Namespace
		deployName := naming.WebServiceName(component.Name, component.ResourceNameKey())
		if result != nil {
			if deploy, ok := result.Service.(*appsv1.Deployment); ok && deploy != nil {
				if deploy.Namespace != "" {
					deployNS = deploy.Namespace
				}
				if deploy.Name != "" {
					deployName = deploy.Name
				}
			}
			c.deleteAdditionalObjects(ctx, componentPtr.Namespace, result.AdditionalObjects, reporter)
		}
		reporter.record("Deployment", deployNS, deployName, c.deleteDeployment(ctx, deployNS, deployName))
	case config.StoreJob:
		result := job.GenerateStoreService(componentPtr)
		statefulNS := componentPtr.Namespace
		statefulName := naming.StoreServerName(component.Name, component.ResourceNameKey())
		if result != nil {
			if sts, ok := result.Service.(*appsv1.StatefulSet); ok && sts != nil {
				if sts.Namespace != "" {
					statefulNS = sts.Namespace
				}
				if sts.Name != "" {
					statefulName = sts.Name
				}
			}
			c.deleteAdditionalObjects(ctx, componentPtr.Namespace, result.AdditionalObjects, reporter)
		}
		reporter.record("StatefulSet", statefulNS, statefulName, c.deleteStatefulSet(ctx, statefulNS, statefulName))
	case config.ConfJob:
		c.deleteConfigMapForComponent(ctx, componentPtr, &props, reporter)
	case config.SecretJob:
		c.deleteSecretForComponent(ctx, componentPtr, &props, reporter)
	case config.InstantJob:
		result := job.GenerateInstantJob(componentPtr, &props, props.RunPolicy)
		jobNS := componentPtr.Namespace
		jobName := naming.JobName(component.Name, component.ResourceNameKey())
		if result != nil {
			if jobObj, ok := result.Service.(*batchv1.Job); ok && jobObj != nil {
				if jobObj.Namespace != "" {
					jobNS = jobObj.Namespace
				}
				if jobObj.Name != "" {
					jobName = jobObj.Name
				}
			}
			c.deleteAdditionalObjects(ctx, componentPtr.Namespace, result.AdditionalObjects, reporter)
		}
		reporter.record("Job", jobNS, jobName, c.deleteJob(ctx, jobNS, jobName))
	case config.ScheduledJob:
		schedule := strings.TrimSpace(props.Schedule)
		if schedule != "" {
			normalized, err := utils.NormalizeCronSchedule(schedule)
			if err != nil {
				klog.Errorf("cleanup scheduled job cron normalize failed: %v", err)
			}
			result := job.GenerateScheduledCronJob(componentPtr, &props, normalized)
			cronNS := componentPtr.Namespace
			cronName := naming.CronJobName(component.Name, component.ResourceNameKey())
			if result != nil {
				if cronObj, ok := result.Service.(*batchv1.CronJob); ok && cronObj != nil {
					if cronObj.Namespace != "" {
						cronNS = cronObj.Namespace
					}
					if cronObj.Name != "" {
						cronName = cronObj.Name
					}
				}
				c.deleteAdditionalObjects(ctx, componentPtr.Namespace, result.AdditionalObjects, reporter)
			}
			reporter.record("CronJob", cronNS, cronName, c.deleteCronJob(ctx, cronNS, cronName))
		}
	case config.CloudJob:
		// cloudjob is API-invocation only in v1 skeleton, no Kubernetes resources to delete.
		return nil
	}

	c.deleteServiceForComponent(ctx, componentPtr, &props, reporter)
	c.deleteIngressForComponent(ctx, componentPtr, reporter)
	return nil
}

func (c *applicationsServiceImpl) deleteServiceForComponent(ctx context.Context, component *model.ApplicationComponent, props *model.Properties, reporter *cleanupReporter) {
	if component == nil || reporter == nil {
		return
	}
	ns := pickNamespace(component.Namespace, config.DefaultNamespace)

	if c.KubeClient != nil {
		appID := strings.TrimSpace(component.AppID)
		componentName := strings.TrimSpace(component.Name)
		if appID != "" && componentName != "" {
			labelSet := labels.Set{
				config.LabelAppID:         appID,
				config.LabelComponentName: naming.BoundedLabelValue(componentName),
			}
			opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationCleanupTimeout)
			serviceList, err := c.KubeClient.CoreV1().Services(ns).List(opCtx, metav1.ListOptions{
				LabelSelector: labelSet.AsSelector().String(),
			})
			cancel()
			if err != nil {
				klog.Errorf("list services by labels failed appID=%s component=%s namespace=%s: %v", appID, componentName, ns, err)
			} else {
				if len(serviceList.Items) > 0 {
					for i := range serviceList.Items {
						svcName := strings.TrimSpace(serviceList.Items[i].Name)
						if svcName == "" {
							continue
						}
						reporter.record("Service", ns, svcName, c.deleteService(ctx, ns, svcName))
					}
					return
				}
				klog.V(3).Infof("no labeled services found for cleanup appID=%s component=%s namespace=%s", appID, componentName, ns)
			}
		}
	}

	// Backward compatibility: fall back to legacy default service naming cleanup.
	if props == nil || len(props.Ports) == 0 {
		return
	}
	svc := job.GenerateService(component, props)
	name := naming.ServiceName(component.Name, component.ResourceNameKey())
	if svc != nil && svc.Name != nil && *svc.Name != "" {
		name = *svc.Name
		if svc.Namespace != nil && *svc.Namespace != "" {
			ns = *svc.Namespace
		}
	}
	reporter.record("Service", ns, name, c.deleteService(ctx, ns, name))
}

func (c *applicationsServiceImpl) deleteIngressForComponent(ctx context.Context, component *model.ApplicationComponent, reporter *cleanupReporter) {
	if component == nil || reporter == nil || c.KubeClient == nil {
		return
	}
	ns := pickNamespace(component.Namespace, config.DefaultNamespace)
	appID := strings.TrimSpace(component.AppID)
	componentName := strings.TrimSpace(component.Name)
	if appID == "" || componentName == "" {
		return
	}

	labelSet := labels.Set{
		config.LabelAppID:         appID,
		config.LabelComponentName: naming.BoundedLabelValue(componentName),
	}
	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationCleanupTimeout)
	ingressList, err := c.KubeClient.NetworkingV1().Ingresses(ns).List(opCtx, metav1.ListOptions{
		LabelSelector: labelSet.AsSelector().String(),
	})
	cancel()
	if err != nil {
		klog.Errorf("list ingresses by labels failed appID=%s component=%s namespace=%s: %v", appID, componentName, ns, err)
		return
	}
	if len(ingressList.Items) == 0 {
		klog.V(3).Infof("no labeled ingresses found for cleanup appID=%s component=%s namespace=%s", appID, componentName, ns)
		return
	}
	for i := range ingressList.Items {
		ingressName := strings.TrimSpace(ingressList.Items[i].Name)
		if ingressName == "" {
			continue
		}
		reporter.record("Ingress", ns, ingressName, c.deleteIngress(ctx, ns, ingressName))
	}
}

func (c *applicationsServiceImpl) deleteConfigMapForComponent(ctx context.Context, component *model.ApplicationComponent, props *model.Properties, reporter *cleanupReporter) {
	obj := job.GenerateConfigMap(component, props)
	switch cm := obj.(type) {
	case *model.ConfigMapInput:
		ns := pickNamespace(cm.Namespace, component.Namespace)
		name := cm.Name
		if name == "" {
			name = component.Name
		}
		reporter.record("ConfigMap", ns, name, c.deleteConfigMap(ctx, ns, name))
	case *corev1.ConfigMap:
		ns := pickNamespace(cm.Namespace, component.Namespace)
		name := cm.Name
		if name == "" {
			name = component.Name
		}
		reporter.record("ConfigMap", ns, name, c.deleteConfigMap(ctx, ns, name))
	default:
		// nothing to delete
	}
}

func (c *applicationsServiceImpl) deleteSecretForComponent(ctx context.Context, component *model.ApplicationComponent, props *model.Properties, reporter *cleanupReporter) {
	obj := job.GenerateSecret(component, props)
	switch sec := obj.(type) {
	case *model.SecretInput:
		ns := pickNamespace(sec.Namespace, component.Namespace)
		name := sec.Name
		if name == "" {
			name = component.Name
		}
		reporter.record("Secret", ns, name, c.deleteSecret(ctx, ns, name))
	case *corev1.Secret:
		ns := pickNamespace(sec.Namespace, component.Namespace)
		name := sec.Name
		if name == "" {
			name = component.Name
		}
		reporter.record("Secret", ns, name, c.deleteSecret(ctx, ns, name))
	default:
		// nothing
	}
}

func (c *applicationsServiceImpl) deleteAdditionalObjects(ctx context.Context, fallbackNamespace string, objs []client.Object, reporter *cleanupReporter) {
	for _, obj := range objs {
		switch resource := obj.(type) {
		case *corev1.PersistentVolumeClaim:
			ns := pickNamespace(resource.Namespace, fallbackNamespace)
			klog.V(4).Infof("cleanup: preserving pvc %s/%s", ns, resource.Name)
		case *networkingv1.Ingress:
			ns := pickNamespace(resource.Namespace, fallbackNamespace)
			reporter.record("Ingress", ns, resource.Name, c.deleteIngress(ctx, ns, resource.Name))
		case *corev1.ServiceAccount:
			ns := pickNamespace(resource.Namespace, fallbackNamespace)
			klog.V(4).Infof("cleanup: preserving serviceaccount %s/%s", ns, resource.Name)
		case *rbacv1.Role:
			ns := pickNamespace(resource.Namespace, fallbackNamespace)
			klog.V(4).Infof("cleanup: preserving role %s/%s", ns, resource.Name)
		case *rbacv1.RoleBinding:
			ns := pickNamespace(resource.Namespace, fallbackNamespace)
			klog.V(4).Infof("cleanup: preserving rolebinding %s/%s", ns, resource.Name)
		case *rbacv1.ClusterRole:
			klog.V(4).Infof("cleanup: preserving clusterrole %s", resource.Name)
		case *rbacv1.ClusterRoleBinding:
			klog.V(4).Infof("cleanup: preserving clusterrolebinding %s", resource.Name)
		default:
			klog.V(4).Infof("cleanup: unsupported additional object type %T", obj)
		}
	}
}

func (c *applicationsServiceImpl) deleteDeployment(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	return c.deleteNamespaced(ctx, namespace, func(opCtx context.Context, ns string) error {
		return c.KubeClient.AppsV1().Deployments(ns).Delete(opCtx, name, metav1.DeleteOptions{})
	})
}

func (c *applicationsServiceImpl) deleteStatefulSet(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	return c.deleteNamespaced(ctx, namespace, func(opCtx context.Context, ns string) error {
		return c.KubeClient.AppsV1().StatefulSets(ns).Delete(opCtx, name, metav1.DeleteOptions{})
	})
}

func (c *applicationsServiceImpl) deleteJob(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	return c.deleteNamespaced(ctx, namespace, func(opCtx context.Context, ns string) error {
		return c.KubeClient.BatchV1().Jobs(ns).Delete(opCtx, name, metav1.DeleteOptions{})
	})
}

func (c *applicationsServiceImpl) deleteCronJob(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	return c.deleteNamespaced(ctx, namespace, func(opCtx context.Context, ns string) error {
		return c.KubeClient.BatchV1().CronJobs(ns).Delete(opCtx, name, metav1.DeleteOptions{})
	})
}

func (c *applicationsServiceImpl) deleteService(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	return c.deleteNamespaced(ctx, namespace, func(opCtx context.Context, ns string) error {
		return c.KubeClient.CoreV1().Services(ns).Delete(opCtx, name, metav1.DeleteOptions{})
	})
}

func (c *applicationsServiceImpl) deleteConfigMap(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	return c.deleteNamespaced(ctx, namespace, func(opCtx context.Context, ns string) error {
		return c.KubeClient.CoreV1().ConfigMaps(ns).Delete(opCtx, name, metav1.DeleteOptions{})
	})
}

func (c *applicationsServiceImpl) deleteSecret(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	return c.deleteNamespaced(ctx, namespace, func(opCtx context.Context, ns string) error {
		return c.KubeClient.CoreV1().Secrets(ns).Delete(opCtx, name, metav1.DeleteOptions{})
	})
}

func (c *applicationsServiceImpl) deletePVC(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	return c.deleteNamespaced(ctx, namespace, func(opCtx context.Context, ns string) error {
		return c.KubeClient.CoreV1().PersistentVolumeClaims(ns).Delete(opCtx, name, metav1.DeleteOptions{})
	})
}

func (c *applicationsServiceImpl) deleteIngress(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	return c.deleteNamespaced(ctx, namespace, func(opCtx context.Context, ns string) error {
		return c.KubeClient.NetworkingV1().Ingresses(ns).Delete(opCtx, name, metav1.DeleteOptions{})
	})
}

func (c *applicationsServiceImpl) deleteNamespaced(ctx context.Context, namespace string, fn func(context.Context, string) error) error {
	ns := namespace
	if ns == "" {
		ns = config.DefaultNamespace
	}
	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationCleanupTimeout)
	defer cancel()
	err := fn(opCtx, ns)
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (c *applicationsServiceImpl) deleteCluster(ctx context.Context, fn func(context.Context) error) error {
	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationCleanupTimeout)
	defer cancel()
	err := fn(opCtx)
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	return nil
}

type cleanupReporter struct {
	deletedResources []string
	failedResources  []string
	errs             []error
}

func newCleanupReporter() *cleanupReporter {
	return &cleanupReporter{
		deletedResources: []string{},
		failedResources:  []string{},
	}
}

func (r *cleanupReporter) record(kind, namespace, name string, err error) {
	if name == "" {
		return
	}
	target := formatResource(kind, namespace, name)
	if err != nil {
		r.failedResources = append(r.failedResources, fmt.Sprintf("%s (%v)", target, err))
		r.errs = append(r.errs, err)
	} else {
		r.deletedResources = append(r.deletedResources, target)
	}
}

func (r *cleanupReporter) err() error {
	if len(r.errs) == 0 {
		return nil
	}
	if len(r.errs) == 1 {
		return r.errs[0]
	}
	return fmt.Errorf("%d cleanup operations failed; first error: %w", len(r.errs), r.errs[0])
}

func formatResource(kind, namespace, name string) string {
	if namespace == "" {
		return fmt.Sprintf("%s:%s", kind, name)
	}
	return fmt.Sprintf("%s:%s/%s", kind, namespace, name)
}

func pickNamespace(candidate, fallback string) string {
	if candidate != "" {
		return candidate
	}
	if fallback != "" {
		return fallback
	}
	return config.DefaultNamespace
}

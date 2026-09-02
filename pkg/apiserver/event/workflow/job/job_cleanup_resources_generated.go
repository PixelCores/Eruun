package job

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func (c *CleanupResourcesJobCtl) deleteGeneratedResources(ctx context.Context, component *model.ApplicationComponent, props *model.Properties, deleted *cleanupResourceSet) {
	switch component.ComponentType {
	case config.ServerJob:
		result := GenerateWebService(component, props)
		ns := component.Namespace
		name := buildWebServiceName(component.Name, component.ResourceNameKey())
		if result != nil {
			if deploy, ok := result.Service.(*appsv1.Deployment); ok && deploy != nil {
				ns = pickNonEmpty(deploy.Namespace, ns)
				name = pickNonEmpty(deploy.Name, name)
			}
			c.deleteAdditionalObjects(ctx, component.Namespace, result.AdditionalObjects, deleted)
		}
		c.deleteTrackedResource(ctx, deleted, spec.ResourceDeployment, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteDeployment(deleteCtx, ns, name)
		})
	case config.StoreJob:
		result := GenerateStoreService(component)
		ns := component.Namespace
		name := buildStoreSeverName(component.Name, component.ResourceNameKey())
		if result != nil {
			if sts, ok := result.Service.(*appsv1.StatefulSet); ok && sts != nil {
				ns = pickNonEmpty(sts.Namespace, ns)
				name = pickNonEmpty(sts.Name, name)
			}
			c.deleteAdditionalObjects(ctx, component.Namespace, result.AdditionalObjects, deleted)
		}
		c.deleteTrackedResource(ctx, deleted, spec.ResourceStatefulSet, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteStatefulSet(deleteCtx, ns, name)
		})
	case config.ConfJob:
		c.deleteConfigMapForComponent(ctx, component, props, deleted)
	case config.SecretJob:
		c.deleteSecretForComponent(ctx, component, props, deleted)
	case config.InstantJob:
		result := GenerateInstantJob(component, props, props.RunPolicy)
		ns := component.Namespace
		name := buildJobName(component.Name, component.ResourceNameKey())
		if result != nil {
			if jobObj, ok := result.Service.(*batchv1.Job); ok && jobObj != nil {
				ns = pickNonEmpty(jobObj.Namespace, ns)
				name = pickNonEmpty(jobObj.Name, name)
			}
			c.deleteAdditionalObjects(ctx, component.Namespace, result.AdditionalObjects, deleted)
		}
		c.deleteTrackedResource(ctx, deleted, spec.ResourceJob, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteJob(deleteCtx, ns, name)
		})
	case config.ScheduledJob:
		if strings.TrimSpace(props.Schedule) == "" {
			break
		}
		result := GenerateScheduledCronJob(component, props, props.Schedule)
		ns := component.Namespace
		name := buildCronJobName(component.Name, component.ResourceNameKey())
		if result != nil {
			if cronObj, ok := result.Service.(*batchv1.CronJob); ok && cronObj != nil {
				ns = pickNonEmpty(cronObj.Namespace, ns)
				name = pickNonEmpty(cronObj.Name, name)
			}
			c.deleteAdditionalObjects(ctx, component.Namespace, result.AdditionalObjects, deleted)
		}
		c.deleteTrackedResource(ctx, deleted, spec.ResourceCronJob, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteCronJob(deleteCtx, ns, name)
		})
	}

	c.deleteServicesForComponent(ctx, component, props, deleted)
}

func (c *CleanupResourcesJobCtl) deleteServicesForComponent(ctx context.Context, component *model.ApplicationComponent, props *model.Properties, deleted *cleanupResourceSet) {
	if component.ComponentType == config.InstantJob || component.ComponentType == config.ScheduledJob || component.ComponentType == config.CloudJob {
		return
	}

	serviceTraits, err := cleanupServiceTraits(component)
	if err != nil {
		deleted.errs = append(deleted.errs, err)
		return
	}
	for _, trait := range serviceTraits {
		svc := GenerateServiceFromTrait(component, props, trait)
		ns := component.Namespace
		name := strings.TrimSpace(trait.Name)
		if name == "" {
			name = buildServiceName(component.Name, component.ResourceNameKey())
		}
		if svc != nil {
			name = pickNonEmpty(valueOrEmpty(svc.Name), name)
			ns = pickNonEmpty(valueOrEmpty(svc.Namespace), ns)
		}
		c.deleteTrackedResource(ctx, deleted, spec.ResourceService, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteService(deleteCtx, ns, name)
		})
	}

	if len(props.Ports) > 0 {
		svc := GenerateService(component, props)
		ns := component.Namespace
		name := buildServiceName(component.Name, component.ResourceNameKey())
		if svc != nil {
			name = pickNonEmpty(valueOrEmpty(svc.Name), name)
			ns = pickNonEmpty(valueOrEmpty(svc.Namespace), ns)
		}
		c.deleteTrackedResource(ctx, deleted, spec.ResourceService, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteService(deleteCtx, ns, name)
		})
	}
}

func cleanupServiceTraits(component *model.ApplicationComponent) ([]spec.ServiceTraitSpec, error) {
	if component == nil || component.Traits == nil {
		return nil, nil
	}
	raw, err := json.Marshal(component.Traits)
	if err != nil {
		return nil, fmt.Errorf("marshal component traits: %w", err)
	}
	if string(raw) == "{}" || string(raw) == "null" {
		return nil, nil
	}
	var traits spec.Traits
	if err := json.Unmarshal(raw, &traits); err != nil {
		return nil, fmt.Errorf("unmarshal component traits: %w", err)
	}
	return traits.Service, nil
}

func (c *CleanupResourcesJobCtl) deleteAdditionalObjects(ctx context.Context, fallbackNamespace string, objs []client.Object, deleted *cleanupResourceSet) {
	for _, obj := range objs {
		switch resource := obj.(type) {
		case *corev1.PersistentVolumeClaim:
			ns := pickNonEmpty(resource.Namespace, fallbackNamespace)
			klog.V(4).Infof("cleanup resources: preserving pvc %s/%s", ns, resource.Name)
		case *networkingv1.Ingress:
			ns := pickNonEmpty(resource.Namespace, fallbackNamespace)
			c.deleteTrackedResource(ctx, deleted, spec.ResourceIngress, ns, resource.Name, false, func(deleteCtx context.Context) error {
				return c.deleteIngress(deleteCtx, ns, resource.Name)
			})
		case *corev1.ServiceAccount:
			ns := pickNonEmpty(resource.Namespace, fallbackNamespace)
			klog.V(4).Infof("cleanup resources: preserving serviceaccount %s/%s", ns, resource.Name)
		case *rbacv1.Role:
			ns := pickNonEmpty(resource.Namespace, fallbackNamespace)
			klog.V(4).Infof("cleanup resources: preserving role %s/%s", ns, resource.Name)
		case *rbacv1.RoleBinding:
			ns := pickNonEmpty(resource.Namespace, fallbackNamespace)
			klog.V(4).Infof("cleanup resources: preserving rolebinding %s/%s", ns, resource.Name)
		case *rbacv1.ClusterRole:
			klog.V(4).Infof("cleanup resources: preserving clusterrole %s", resource.Name)
		case *rbacv1.ClusterRoleBinding:
			klog.V(4).Infof("cleanup resources: preserving clusterrolebinding %s", resource.Name)
		default:
			klog.V(4).Infof("cleanup resources: unsupported additional object type %T", obj)
		}
	}
}

func (c *CleanupResourcesJobCtl) deleteConfigMapForComponent(ctx context.Context, component *model.ApplicationComponent, props *model.Properties, deleted *cleanupResourceSet) {
	obj := GenerateConfigMap(component, props)
	switch cm := obj.(type) {
	case *model.ConfigMapInput:
		ns := pickNonEmpty(cm.Namespace, component.Namespace)
		name := pickNonEmpty(cm.Name, component.Name)
		c.deleteTrackedResource(ctx, deleted, spec.ResourceConfigMap, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteConfigMap(deleteCtx, ns, name)
		})
	case *corev1.ConfigMap:
		ns := pickNonEmpty(cm.Namespace, component.Namespace)
		name := pickNonEmpty(cm.Name, component.Name)
		c.deleteTrackedResource(ctx, deleted, spec.ResourceConfigMap, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteConfigMap(deleteCtx, ns, name)
		})
	}
}

func (c *CleanupResourcesJobCtl) deleteSecretForComponent(ctx context.Context, component *model.ApplicationComponent, props *model.Properties, deleted *cleanupResourceSet) {
	obj := GenerateSecret(component, props)
	switch sec := obj.(type) {
	case *model.SecretInput:
		ns := pickNonEmpty(sec.Namespace, component.Namespace)
		name := pickNonEmpty(sec.Name, component.Name)
		c.deleteTrackedResource(ctx, deleted, spec.ResourceSecret, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteSecret(deleteCtx, ns, name)
		})
	case *corev1.Secret:
		ns := pickNonEmpty(sec.Namespace, component.Namespace)
		name := pickNonEmpty(sec.Name, component.Name)
		c.deleteTrackedResource(ctx, deleted, spec.ResourceSecret, ns, name, false, func(deleteCtx context.Context) error {
			return c.deleteSecret(deleteCtx, ns, name)
		})
	}
}

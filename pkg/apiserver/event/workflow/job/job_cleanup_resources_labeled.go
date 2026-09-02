package job

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func (c *CleanupResourcesJobCtl) deleteLabeledResources(ctx context.Context, component *model.ApplicationComponent, deleted *cleanupResourceSet) {
	selector := cleanupLabelSelector(component)
	if selector == "" {
		return
	}
	ns := component.Namespace
	listOptions := metav1.ListOptions{LabelSelector: selector}

	c.deleteLabeledWorkloads(ctx, ns, listOptions, deleted)
	c.deleteLabeledCoreResources(ctx, ns, listOptions, deleted)
	c.deleteLabeledNetworkingResources(ctx, ns, listOptions, deleted)
}

func (c *CleanupResourcesJobCtl) deleteLabeledWorkloads(ctx context.Context, ns string, listOptions metav1.ListOptions, deleted *cleanupResourceSet) {
	if list, err := c.client.AppsV1().Deployments(ns).List(ctx, listOptions); err != nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("list labeled deployments: %w", err))
	} else {
		for i := range list.Items {
			item := &list.Items[i]
			c.deleteTrackedResource(ctx, deleted, config.ResourceDeployment, item.Namespace, item.Name, false, func(deleteCtx context.Context) error {
				return c.deleteDeployment(deleteCtx, item.Namespace, item.Name)
			})
		}
	}
	if list, err := c.client.AppsV1().StatefulSets(ns).List(ctx, listOptions); err != nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("list labeled statefulsets: %w", err))
	} else {
		for i := range list.Items {
			item := &list.Items[i]
			c.deleteTrackedResource(ctx, deleted, config.ResourceStatefulSet, item.Namespace, item.Name, false, func(deleteCtx context.Context) error {
				return c.deleteStatefulSet(deleteCtx, item.Namespace, item.Name)
			})
		}
	}
	if list, err := c.client.BatchV1().Jobs(ns).List(ctx, listOptions); err != nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("list labeled jobs: %w", err))
	} else {
		for i := range list.Items {
			item := &list.Items[i]
			c.deleteTrackedResource(ctx, deleted, config.ResourceJob, item.Namespace, item.Name, false, func(deleteCtx context.Context) error {
				return c.deleteJob(deleteCtx, item.Namespace, item.Name)
			})
		}
	}
	if list, err := c.client.BatchV1().CronJobs(ns).List(ctx, listOptions); err != nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("list labeled cronjobs: %w", err))
	} else {
		for i := range list.Items {
			item := &list.Items[i]
			c.deleteTrackedResource(ctx, deleted, config.ResourceCronJob, item.Namespace, item.Name, false, func(deleteCtx context.Context) error {
				return c.deleteCronJob(deleteCtx, item.Namespace, item.Name)
			})
		}
	}
}

func (c *CleanupResourcesJobCtl) deleteLabeledCoreResources(ctx context.Context, ns string, listOptions metav1.ListOptions, deleted *cleanupResourceSet) {
	if list, err := c.client.CoreV1().Services(ns).List(ctx, listOptions); err != nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("list labeled services: %w", err))
	} else {
		for i := range list.Items {
			item := &list.Items[i]
			c.deleteTrackedResource(ctx, deleted, config.ResourceService, item.Namespace, item.Name, false, func(deleteCtx context.Context) error {
				return c.deleteService(deleteCtx, item.Namespace, item.Name)
			})
		}
	}
	if list, err := c.client.CoreV1().ConfigMaps(ns).List(ctx, listOptions); err != nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("list labeled configmaps: %w", err))
	} else {
		for i := range list.Items {
			item := &list.Items[i]
			c.deleteTrackedResource(ctx, deleted, config.ResourceConfigMap, item.Namespace, item.Name, false, func(deleteCtx context.Context) error {
				return c.deleteConfigMap(deleteCtx, item.Namespace, item.Name)
			})
		}
	}
	if list, err := c.client.CoreV1().Secrets(ns).List(ctx, listOptions); err != nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("list labeled secrets: %w", err))
	} else {
		for i := range list.Items {
			item := &list.Items[i]
			c.deleteTrackedResource(ctx, deleted, config.ResourceSecret, item.Namespace, item.Name, false, func(deleteCtx context.Context) error {
				return c.deleteSecret(deleteCtx, item.Namespace, item.Name)
			})
		}
	}
	klog.V(4).Infof("cleanup resources: preserving labeled pvcs in namespace %s selector=%s", ns, listOptions.LabelSelector)
	klog.V(4).Infof("cleanup resources: preserving labeled serviceaccounts in namespace %s selector=%s", ns, listOptions.LabelSelector)
}

func (c *CleanupResourcesJobCtl) deleteLabeledNetworkingResources(ctx context.Context, ns string, listOptions metav1.ListOptions, deleted *cleanupResourceSet) {
	if list, err := c.client.NetworkingV1().Ingresses(ns).List(ctx, listOptions); err != nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("list labeled ingresses: %w", err))
	} else {
		for i := range list.Items {
			item := &list.Items[i]
			c.deleteTrackedResource(ctx, deleted, config.ResourceIngress, item.Namespace, item.Name, false, func(deleteCtx context.Context) error {
				return c.deleteIngress(deleteCtx, item.Namespace, item.Name)
			})
		}
	}
}

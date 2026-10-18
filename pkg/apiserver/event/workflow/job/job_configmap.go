package job

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type DeployConfigMapJobCtl struct {
	deployNamespacedResourceJobBase
	urlSecurityPolicy *spec.URLSecurityPolicySpec
}

func NewDeployConfigMapJobCtl(job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) *DeployConfigMapJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployConfigMapJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	ctl := &DeployConfigMapJobCtl{
		deployNamespacedResourceJobBase: base,
	}
	if runtime != nil {
		ctl.urlSecurityPolicy = runtime.URLSecurityPolicy
	}
	return ctl
}

func (c *DeployConfigMapJobCtl) Clean(ctx context.Context) {
	c.cleanCreated(ctx, spec.ResourceConfigMap, "configmap", func(ctx context.Context, namespace, name string) error {
		return c.client.CoreV1().ConfigMaps(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after job failure")
}

func (c *DeployConfigMapJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeployConfigMapJob run error")
}

func (c *DeployConfigMapJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	cm, err := configMapFromJobInfo(ctx, c.job, c.urlSecurityPolicy)
	if err != nil {
		return err
	}
	if cm.Namespace == "" {
		cm.Namespace = c.job.Namespace
	}
	return c.deployConfigMap(ctx, cm)
}

func (c *DeployConfigMapJobCtl) deployConfigMap(ctx context.Context, cm *corev1.ConfigMap) error {
	logger := klog.FromContext(ctx)
	cli := c.client.CoreV1().ConfigMaps(cm.Namespace)
	updateConfigMap := func(ctx context.Context, _ *corev1.ConfigMap) error {
		if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.ConfigMap, error) {
			return cli.Get(ctx, cm.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *corev1.ConfigMap) error {
			cm.ResourceVersion = latest.ResourceVersion
			_, err := cli.Update(ctx, cm, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("update configmap %q failed: %w", cm.Name, err)
		}
		logger.Info("ConfigMap updated", "namespace", cm.Namespace, "name", cm.Name)
		markResourceObserved(ctx, spec.ResourceConfigMap, cm.Namespace, cm.Name)
		return nil
	}

	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[corev1.ConfigMap]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:          c.job,
			ack:          c.ack,
			labels:       cm.Labels,
			kind:         spec.ResourceConfigMap,
			lockProvider: c.shareLocker,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy spec.ShareStrategy) {
				if strategy == spec.ShareStrategyIgnore {
					logger.Info("ConfigMap marked as shared ignore; skipping", "namespace", cm.Namespace, "name", cm.Name)
				} else {
					logger.Info("ConfigMap already exists and is shared; skipping", "namespace", cm.Namespace, "name", cm.Name)
				}
			},
		},
		namespace: cm.Namespace,
		name:      cm.Name,
		getFn: func(ctx context.Context) (*corev1.ConfigMap, error) {
			return cli.Get(ctx, cm.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*corev1.ConfigMap, error) {
			return cli.Create(ctx, cm, metav1.CreateOptions{})
		},
		onExisting:      updateConfigMap,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure configmap %q failed: %w", cm.Name, err)
	}
	if created {
		logger.Info("ConfigMap created", "namespace", cm.Namespace, "name", cm.Name)
	}

	if c.job.Status != config.StatusSkipped {
		c.job.Status = config.StatusCompleted
		c.ack()
	}
	return nil
}

// GenerateConfigMap Generate a simplified ConfigMap input based on components and attributes.
// First, read the external file URL from Conf["config.url"]; otherwise, directly use the content in Conf as the content of ConfigMap.
func GenerateConfigMap(component *model.ApplicationComponent, properties *model.Properties) *ConfigMapInput {
	name, namespace := generatedResourceIdentity(component)

	if url, fileName, ok := externalConfigFileInput(properties, true); ok {
		return &ConfigMapInput{
			Name:      name,
			Namespace: namespace,
			URL:       url,
			FileName:  fileName,
			Labels:    BuildLabels(component, properties),
		}
	}

	labels := BuildLabels(component, properties)
	var data map[string]string
	if properties != nil {
		data = properties.Conf
	}

	return &ConfigMapInput{
		Name:      name,
		Namespace: namespace,
		Labels:    labels,
		Data:      data,
	}
}

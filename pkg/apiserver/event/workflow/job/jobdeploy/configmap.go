package jobdeploy

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type DeployConfigMapJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
	urlSecurityPolicy *spec.URLSecurityPolicySpec
}

func NewDeployConfigMapJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployConfigMapJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployConfigMapJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	ctl := &DeployConfigMapJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
	if runtime != nil {
		ctl.urlSecurityPolicy = runtime.URLSecurityPolicy
	}
	return ctl
}

func (c *DeployConfigMapJobCtl) Clean(ctx context.Context) {
	c.CleanCreated(ctx, spec.ResourceConfigMap, "configmap", func(ctx context.Context, namespace, name string) error {
		return c.Client.CoreV1().ConfigMaps(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, "after job failure")
}

func (c *DeployConfigMapJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "DeployConfigMapJob run error")
}

func (c *DeployConfigMapJobCtl) run(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	cm, err := jobinfo.ConfigMapFromJobInfo(ctx, c.Job, c.urlSecurityPolicy)
	if err != nil {
		return err
	}
	if cm.Namespace == "" {
		cm.Namespace = c.Job.Namespace
	}
	return c.deployConfigMap(ctx, cm)
}

func (c *DeployConfigMapJobCtl) deployConfigMap(ctx context.Context, cm *corev1.ConfigMap) error {
	logger := klog.FromContext(ctx)
	cli := c.Client.CoreV1().ConfigMaps(cm.Namespace)
	updateConfigMap := func(ctx context.Context, _ *corev1.ConfigMap) error {
		if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.ConfigMap, error) {
			return cli.Get(ctx, cm.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *corev1.ConfigMap) error {
			cm.ResourceVersion = latest.ResourceVersion
			_, err := cli.Update(ctx, cm, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("update configmap %q failed: %w", cm.Name, err)
		}
		logger.Info("ConfigMap updated", "namespace", cm.Namespace, "name", cm.Name)
		jobexec.MarkResourceObserved(ctx, spec.ResourceConfigMap, cm.Namespace, cm.Name)
		return nil
	}

	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[corev1.ConfigMap]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:          c.Job,
			Ack:          c.Ack,
			Labels:       cm.Labels,
			Kind:         spec.ResourceConfigMap,
			LockProvider: c.ShareLocker,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy spec.ShareStrategy) {
				if strategy == spec.ShareStrategyIgnore {
					logger.Info("ConfigMap marked as shared ignore; skipping", "namespace", cm.Namespace, "name", cm.Name)
				} else {
					logger.Info("ConfigMap already exists and is shared; skipping", "namespace", cm.Namespace, "name", cm.Name)
				}
			},
		},
		Namespace: cm.Namespace,
		Name:      cm.Name,
		GetFn: func(ctx context.Context) (*corev1.ConfigMap, error) {
			return cli.Get(ctx, cm.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*corev1.ConfigMap, error) {
			return cli.Create(ctx, cm, metav1.CreateOptions{})
		},
		OnExisting: updateConfigMap,
	})
	if err != nil {
		return fmt.Errorf("ensure configmap %q failed: %w", cm.Name, err)
	}
	if created {
		logger.Info("ConfigMap created", "namespace", cm.Namespace, "name", cm.Name)
	}

	if c.Job.Status != config.StatusSkipped {
		c.Job.Status = config.StatusCompleted
		c.Ack()
	}
	return nil
}

// GenerateConfigMap Generate a simplified ConfigMap input based on components and attributes.
// First, read the external file URL from Conf["config.url"]; otherwise, directly use the content in Conf as the content of ConfigMap.
func GenerateConfigMap(component *model.ApplicationComponent, properties *model.Properties) *jobinfo.ConfigMapInput {
	name, namespace := generatedResourceIdentity(component)

	if url, fileName, ok := externalConfigFileInput(properties, true); ok {
		return &jobinfo.ConfigMapInput{
			Name:      name,
			Namespace: namespace,
			URL:       url,
			FileName:  fileName,
			Labels:    jobexec.BuildLabels(component, properties),
		}
	}

	labels := jobexec.BuildLabels(component, properties)
	var data map[string]string
	if properties != nil {
		data = properties.Conf
	}

	return &jobinfo.ConfigMapInput{
		Name:      name,
		Namespace: namespace,
		Labels:    labels,
		Data:      data,
	}
}

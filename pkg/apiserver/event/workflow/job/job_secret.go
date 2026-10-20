package job

import (
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// DeploySecretJobCtl creates or updates a Secret resource in the target namespace.
// It assumes JobInfo carries a fully-formed *corev1.Secret (creation intent). Pure references
// should not be routed to this JobCtl.
type DeploySecretJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
	urlSecurityPolicy *spec.URLSecurityPolicySpec
}

func NewDeploySecretJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeploySecretJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("NewDeploySecretJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	ctl := &DeploySecretJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
	if runtime != nil {
		ctl.urlSecurityPolicy = runtime.URLSecurityPolicy
	}
	return ctl
}

func (c *DeploySecretJobCtl) Clean(ctx context.Context) {
	c.CleanCreated(ctx, spec.ResourceSecret, "secret", func(ctx context.Context, namespace, name string) error {
		return c.Client.CoreV1().Secrets(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after job failure")
}

func (c *DeploySecretJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "DeploySecretJob run error")
}

func (c *DeploySecretJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	secret, err := jobinfo.SecretFromJobInfo(ctx, c.Job, c.urlSecurityPolicy)
	if err != nil {
		return err
	}
	if secret.Namespace == "" {
		secret.Namespace = c.Job.Namespace
	}

	// Default to Opaque if not set
	if string(secret.Type) == "" {
		secret.Type = corev1.SecretTypeOpaque
	}

	cli := c.Client.CoreV1().Secrets(secret.Namespace)

	updateSecret := func(ctx context.Context, current *corev1.Secret) error {
		if current.Immutable != nil && *current.Immutable {
			if !equalSecretPayload(current, secret) {
				return fmt.Errorf("secret %s/%s is immutable; content differs and cannot be updated", secret.Namespace, secret.Name)
			}
			logger.Info("Secret is immutable and unchanged; skipping update", "secretName", secret.Name, "namespace", secret.Namespace)
			jobexec.MarkResourceObserved(ctx, spec.ResourceSecret, secret.Namespace, secret.Name)
			return nil
		}
		if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.Secret, error) {
			return cli.Get(ctx, secret.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *corev1.Secret) error {
			if latest.Immutable != nil && *latest.Immutable {
				if !equalSecretPayload(latest, secret) {
					return fmt.Errorf("secret %s/%s is immutable; content differs and cannot be updated", secret.Namespace, secret.Name)
				}
				return nil
			}
			secret.ResourceVersion = latest.ResourceVersion
			_, err := cli.Update(ctx, secret, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("update secret %q failed: %w", secret.Name, err)
		}
		logger.Info("Secret updated", "secretName", secret.Name, "namespace", secret.Namespace)
		jobexec.MarkResourceObserved(ctx, spec.ResourceSecret, secret.Namespace, secret.Name)
		return nil
	}

	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[corev1.Secret]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:          c.Job,
			Ack:          c.Ack,
			Labels:       secret.Labels,
			Kind:         spec.ResourceSecret,
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
					logger.Info("Secret marked as shared ignore; skipping", "secretName", secret.Name, "namespace", secret.Namespace)
				} else {
					logger.Info("Secret already exists and is shared; skipping", "secretName", secret.Name, "namespace", secret.Namespace)
				}
			},
		},
		Namespace: secret.Namespace,
		Name:      secret.Name,
		GetFn: func(ctx context.Context) (*corev1.Secret, error) {
			return cli.Get(ctx, secret.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*corev1.Secret, error) {
			return cli.Create(ctx, secret, metav1.CreateOptions{})
		},
		OnExisting:      updateSecret,
		IsNotFound:      k8serrors.IsNotFound,
		IsAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure secret %q failed: %w", secret.Name, err)
	}
	if created {
		logger.Info("Secret created", "secretName", secret.Name, "namespace", secret.Namespace)
	}

	if c.Job.Status != config.StatusSkipped {
		c.Job.Status = config.StatusCompleted
		c.Ack()
	}
	return nil
}

// equalSecretPayload compares update-relevant fields of two Secret objects without exposing data.
func equalSecretPayload(a, b *corev1.Secret) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Type != b.Type {
		return false
	}
	currentData := materializeSecretData(a)
	desiredData := materializeSecretData(b)
	if len(currentData) != len(desiredData) {
		return false
	}
	for k, v := range currentData {
		bv, ok := desiredData[k]
		if !ok {
			return false
		}
		if !bytes.Equal(v, bv) {
			return false
		}
	}
	return true
}

func materializeSecretData(secret *corev1.Secret) map[string][]byte {
	if secret == nil {
		return nil
	}
	merged := make(map[string][]byte, len(secret.Data)+len(secret.StringData))
	for key, value := range secret.Data {
		merged[key] = append([]byte(nil), value...)
	}
	for key, value := range secret.StringData {
		merged[key] = []byte(value)
	}
	return merged
}

func GenerateSecret(component *model.ApplicationComponent, properties *model.Properties) *jobinfo.SecretInput {
	name, namespace := generatedResourceIdentity(component)

	if url, fileName, ok := externalConfigFileInput(properties, properties != nil && properties.Secret != nil); ok {
		return &jobinfo.SecretInput{
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
		data = properties.Secret
	}

	return &jobinfo.SecretInput{
		Name:      name,
		Namespace: namespace,
		Labels:    labels,
		Data:      data,
	}
}

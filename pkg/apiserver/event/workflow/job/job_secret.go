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
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// DeploySecretJobCtl creates or updates a Secret resource in the target namespace.
// It assumes JobInfo carries a fully-formed *corev1.Secret (creation intent). Pure references
// should not be routed to this JobCtl.
type DeploySecretJobCtl struct {
	deployNamespacedResourceJobBase
	urlSecurityPolicy *spec.URLSecurityPolicySpec
}

func NewDeploySecretJobCtl(job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) *DeploySecretJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("NewDeploySecretJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	ctl := &DeploySecretJobCtl{
		deployNamespacedResourceJobBase: base,
	}
	if runtime != nil {
		ctl.urlSecurityPolicy = runtime.URLSecurityPolicy
	}
	return ctl
}

func (c *DeploySecretJobCtl) Clean(ctx context.Context) {
	c.cleanCreated(ctx, spec.ResourceSecret, "secret", func(ctx context.Context, namespace, name string) error {
		return c.client.CoreV1().Secrets(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after job failure")
}

func (c *DeploySecretJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeploySecretJob run error")
}

func (c *DeploySecretJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	secret, err := secretFromJobInfo(ctx, c.job, c.urlSecurityPolicy)
	if err != nil {
		return err
	}
	if secret.Namespace == "" {
		secret.Namespace = c.job.Namespace
	}

	// Default to Opaque if not set
	if string(secret.Type) == "" {
		secret.Type = corev1.SecretTypeOpaque
	}

	cli := c.client.CoreV1().Secrets(secret.Namespace)

	updateSecret := func(ctx context.Context, current *corev1.Secret) error {
		if current.Immutable != nil && *current.Immutable {
			if !equalSecretPayload(current, secret) {
				return fmt.Errorf("secret %s/%s is immutable; content differs and cannot be updated", secret.Namespace, secret.Name)
			}
			logger.Info("Secret is immutable and unchanged; skipping update", "secretName", secret.Name, "namespace", secret.Namespace)
			markResourceObserved(ctx, spec.ResourceSecret, secret.Namespace, secret.Name)
			return nil
		}
		if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.Secret, error) {
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
		markResourceObserved(ctx, spec.ResourceSecret, secret.Namespace, secret.Name)
		return nil
	}

	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[corev1.Secret]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:          c.job,
			ack:          c.ack,
			labels:       secret.Labels,
			kind:         spec.ResourceSecret,
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
					logger.Info("Secret marked as shared ignore; skipping", "secretName", secret.Name, "namespace", secret.Namespace)
				} else {
					logger.Info("Secret already exists and is shared; skipping", "secretName", secret.Name, "namespace", secret.Namespace)
				}
			},
		},
		namespace: secret.Namespace,
		name:      secret.Name,
		getFn: func(ctx context.Context) (*corev1.Secret, error) {
			return cli.Get(ctx, secret.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*corev1.Secret, error) {
			return cli.Create(ctx, secret, metav1.CreateOptions{})
		},
		onExisting:      updateSecret,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure secret %q failed: %w", secret.Name, err)
	}
	if created {
		logger.Info("Secret created", "secretName", secret.Name, "namespace", secret.Namespace)
	}

	if c.job.Status != config.StatusSkipped {
		c.job.Status = config.StatusCompleted
		c.ack()
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

func GenerateSecret(component *model.ApplicationComponent, properties *model.Properties) *SecretInput {
	name, namespace := generatedResourceIdentity(component)

	if url, fileName, ok := externalConfigFileInput(properties, properties != nil && properties.Secret != nil); ok {
		return &SecretInput{
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
		data = properties.Secret
	}

	return &SecretInput{
		Name:      name,
		Namespace: namespace,
		Labels:    labels,
		Data:      data,
	}
}

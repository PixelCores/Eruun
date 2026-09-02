package job

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type DeployConfigMapJobCtl struct {
	deployNamespacedResourceJobBase
	urlSecurityPolicy *spec.URLSecurityPolicySpec
}

func NewDeployConfigMapJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker, urlSecurityPolicy *spec.URLSecurityPolicySpec) *DeployConfigMapJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployConfigMapJobCtl", job, client, store, ack, shareLocker)
	if !ok {
		return nil
	}
	return &DeployConfigMapJobCtl{
		deployNamespacedResourceJobBase: base,
		urlSecurityPolicy:               urlSecurityPolicy,
	}
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
	if binding, adopted, sourceErr := adoptedResourceForJob(
		ctx,
		c.store,
		c.job,
		"ConfigMap",
		cm.Namespace,
		cm.Name,
	); sourceErr != nil {
		return sourceErr
	} else if adopted {
		if err := c.reconcileAdoptedConfigMap(ctx, cm, binding); err != nil {
			return err
		}
		if c.job.Status != config.StatusSkipped {
			c.job.Status = config.StatusCompleted
			if c.ack != nil {
				c.ack()
			}
		}
		return nil
	}
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

func (c *DeployConfigMapJobCtl) reconcileAdoptedConfigMap(
	ctx context.Context,
	desired *corev1.ConfigMap,
	binding *adoptedResourceBinding,
) error {
	if desired == nil {
		return fmt.Errorf("adopted configmap desired resource is required")
	}
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return err
	}
	if !writable {
		return nil
	}
	cli := c.client.CoreV1().ConfigMaps(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedConfigMap(ctx, desired, binding)
		}
		return fmt.Errorf("get adopted configmap %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		recovered, recoverErr := recoverPendingAdoptedDependency(
			ctx, c.store, binding, current, current, c.runtime, c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted configmap binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	candidate, err := adoptedConfigMapForExistingUpdate(current, desired)
	if err != nil {
		return err
	}
	if adoptedConfigMapEqual(current, candidate) {
		markResourceObserved(ctx, spec.ResourceConfigMap, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.ConfigMap, error) {
		return cli.Get(ctx, desired.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *corev1.ConfigMap) error {
		if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
			return err
		}
		candidate, err := adoptedConfigMapForExistingUpdate(latest, desired)
		if err != nil || adoptedConfigMapEqual(latest, candidate) {
			return err
		}
		_, err = cli.Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("update adopted configmap %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	markResourceObserved(ctx, spec.ResourceConfigMap, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployConfigMapJobCtl) recreateAdoptedConfigMap(
	ctx context.Context,
	desired *corev1.ConfigMap,
	binding *adoptedResourceBinding,
) error {
	recreation, err := prepareAdoptedDependencyRecreation(c.store, binding)
	if err != nil {
		return fmt.Errorf("prepare adopted configmap recreation: %w", err)
	}
	var baseline corev1.ConfigMap
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted configmap recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted configmap recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	if desired.Immutable != nil && !apiequality.Semantic.DeepEqual(baseline.Immutable, desired.Immutable) {
		return fmt.Errorf("build adopted configmap recreation candidate: immutable flag changes are forbidden")
	}
	immutable := baseline.Immutable
	baseline.Immutable = nil
	candidate, err := adoptedConfigMapForExistingUpdate(&baseline, desired)
	if err != nil {
		return fmt.Errorf("build adopted configmap recreation candidate: %w", err)
	}
	candidate.Immutable = immutable
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted configmap recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	created, err := c.client.CoreV1().ConfigMaps(candidate.Namespace).Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := c.client.CoreV1().ConfigMaps(candidate.Namespace).Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted configmap %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
			}
			recovered, recoverErr := recoverPendingAdoptedDependencyLocked(
				recreationCtx,
				c.store,
				&recreation.adoptedResourceBinding,
				replacement,
				replacement,
				c.runtime,
			)
			if recoverErr != nil {
				return fmt.Errorf("recover concurrent adopted configmap recreation: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedConfigMap(ctx, desired, &recreation.adoptedResourceBinding)
			}
			return fmt.Errorf(
				"adopted configmap ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf("recreate adopted configmap %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	if err := recreation.persistCreated(recreationCtx, created, created, c.runtime); err != nil {
		return fmt.Errorf("persist recreated adopted configmap binding; pending claim retained: %w", err)
	}
	markResourceObserved(ctx, spec.ResourceConfigMap, created.Namespace, created.Name)
	return nil
}

func adoptedConfigMapForExistingUpdate(current, desired *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	updated := current.DeepCopy()
	if desired == nil {
		return updated, nil
	}
	if desired.Immutable != nil && !apiequality.Semantic.DeepEqual(current.Immutable, desired.Immutable) {
		return nil, fmt.Errorf("configmap %s/%s immutable flag changes are forbidden", current.Namespace, current.Name)
	}
	updated.Labels = adoptedOverlayStringMap(current.Labels, desired.Labels)
	updated.Annotations = adoptedOverlayStringMap(current.Annotations, desired.Annotations)
	if desired.Data != nil {
		updated.Data = make(map[string]string, len(current.Data)+len(desired.Data))
		for key, value := range current.Data {
			updated.Data[key] = value
		}
		for key, value := range desired.Data {
			updated.Data[key] = value
		}
	}
	if desired.BinaryData != nil {
		updated.BinaryData = make(map[string][]byte, len(current.BinaryData)+len(desired.BinaryData))
		for key, value := range current.BinaryData {
			updated.BinaryData[key] = append([]byte(nil), value...)
		}
		for key, value := range desired.BinaryData {
			updated.BinaryData[key] = append([]byte(nil), value...)
		}
	}
	if current.Immutable != nil && *current.Immutable &&
		(!apiequality.Semantic.DeepEqual(current.Data, updated.Data) ||
			!apiequality.Semantic.DeepEqual(current.BinaryData, updated.BinaryData)) {
		return nil, fmt.Errorf("configmap %s/%s is immutable; content differs", current.Namespace, current.Name)
	}
	return updated, nil
}

func adoptedConfigMapEqual(current, updated *corev1.ConfigMap) bool {
	if current == nil || updated == nil {
		return current == updated
	}
	return apiequality.Semantic.DeepEqual(current.Labels, updated.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, updated.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Data, updated.Data) &&
		apiequality.Semantic.DeepEqual(current.BinaryData, updated.BinaryData) &&
		apiequality.Semantic.DeepEqual(current.Immutable, updated.Immutable)
}

func (c *DeployConfigMapJobCtl) wait(ctx context.Context) {}

// GenerateConfigMap Generate a simplified ConfigMap input based on components and attributes.
// First, read the external file URL from Conf["config.url"]; otherwise, directly use the content in Conf as the content of ConfigMap.
func GenerateConfigMap(component *model.ApplicationComponent, properties *model.Properties) interface{} {
	name, namespace := generatedResourceIdentity(component)

	if url, fileName, ok := externalConfigFileInput(properties, true); ok {
		return &model.ConfigMapInput{
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
		data = keyValueDataOrNil(properties.Conf)
	}

	return &model.ConfigMapInput{
		Name:      name,
		Namespace: namespace,
		Labels:    labels,
		Data:      data,
	}
}

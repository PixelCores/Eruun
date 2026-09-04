package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

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
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/importsecret"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// DeploySecretJobCtl creates or updates a Secret resource in the target namespace.
// It assumes JobInfo carries a fully-formed *corev1.Secret (creation intent). Pure references
// should not be routed to this JobCtl.
type DeploySecretJobCtl struct {
	deployNamespacedResourceJobBase
	urlSecurityPolicy   *spec.URLSecurityPolicySpec
	importSecretKeyring *importsecret.Keyring
}

func NewDeploySecretJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker, urlSecurityPolicy *spec.URLSecurityPolicySpec) *DeploySecretJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("NewDeploySecretJobCtl", job, client, store, ack, shareLocker)
	if !ok {
		return nil
	}
	return &DeploySecretJobCtl{
		deployNamespacedResourceJobBase: base,
		urlSecurityPolicy:               urlSecurityPolicy,
	}
}

func (c *DeploySecretJobCtl) Clean(ctx context.Context) {
	c.cleanCreated(ctx, spec.ResourceSecret, "secret", func(ctx context.Context, namespace, name string) error {
		return c.client.CoreV1().Secrets(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after job failure")
}

func (c *DeploySecretJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeploySecretJob run error")
}

func (c *DeploySecretJobCtl) setRuntime(runtime *jobRuntime) {
	c.deployNamespacedResourceJobBase.setRuntime(runtime)
	if runtime == nil {
		c.importSecretKeyring = nil
		return
	}
	c.importSecretKeyring = runtime.importSecretKeyring
}

func (c *DeploySecretJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	app, snapshot, adopted, err := adoptedApplicationForJob(ctx, c.store, c.job)
	if err != nil {
		return err
	}
	if adopted {
		secret, err := adoptedSecretIntentFromJobInfo(c.job)
		if err != nil {
			return err
		}
		if secret.Namespace == "" {
			secret.Namespace = c.job.Namespace
		}
		resource, err := findAdoptedSnapshotResource(snapshot, c.job.Name, "Secret", secret.Namespace, secret.Name)
		if err != nil {
			return err
		}
		binding := &adoptedResourceBinding{application: app, snapshot: snapshot, resource: resource}
		if err := c.reconcileAdoptedSecret(ctx, secret, binding); err != nil {
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

func (c *DeploySecretJobCtl) reconcileAdoptedSecret(
	ctx context.Context,
	desired *corev1.Secret,
	binding *adoptedResourceBinding,
) error {
	if desired == nil {
		return fmt.Errorf("adopted secret desired resource is required")
	}
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return err
	}
	if !writable {
		return nil
	}
	baseline, material, err := c.decryptAdoptedSecretMaterial(ctx, binding)
	if err != nil {
		return err
	}
	defer clearSecretData(material.data)

	cli := c.client.CoreV1().Secrets(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedSecret(ctx, desired, baseline, material, binding)
		}
		return fmt.Errorf("get adopted secret %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		recovered, recoverErr := recoverPendingAdoptedSecret(
			ctx,
			c.store,
			binding,
			current,
			current,
			material.ciphertextUpdates,
			c.runtime,
			c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted secret binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	candidate, err := adoptedSecretForExistingUpdate(current, desired, baseline, binding.resource.SecretKeys, material.data)
	if err != nil {
		return err
	}
	if !adoptedSecretEqual(current, candidate) {
		if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.Secret, error) {
			return cli.Get(ctx, desired.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *corev1.Secret) error {
			if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
				return err
			}
			candidate, err := adoptedSecretForExistingUpdate(latest, desired, baseline, binding.resource.SecretKeys, material.data)
			if err != nil || adoptedSecretEqual(latest, candidate) {
				return err
			}
			_, err = cli.Update(ctx, candidate, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("update adopted secret %s/%s: %w", desired.Namespace, desired.Name, err)
		}
	}
	if material.rotationNeeded {
		if err := persistRotatedAdoptedSecretData(
			ctx,
			c.store,
			binding.application.ID,
			binding.resource.Source.Name,
			material.rotationUpdates,
			c.runtime,
		); err != nil {
			return err
		}
	}
	markResourceObserved(ctx, spec.ResourceSecret, desired.Namespace, desired.Name)
	return nil
}

type adoptedSecretMaterial struct {
	data              map[string][]byte
	ciphertextUpdates []adoptedSecretCiphertextUpdate
	rotationUpdates   []adoptedSecretCiphertextUpdate
	rotationNeeded    bool
}

func adoptedSecretIntentFromJobInfo(job *model.JobTask) (*corev1.Secret, error) {
	if job == nil {
		return nil, fmt.Errorf("job task is nil")
	}
	switch info := job.JobInfo.(type) {
	case *corev1.Secret:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*corev1.Secret]())
		}
		if len(info.Data) > 0 || len(info.StringData) > 0 {
			return nil, fmt.Errorf("adopted Secret JobInfo must not contain plaintext data or stringData")
		}
		intent := &corev1.Secret{
			TypeMeta:   info.TypeMeta,
			ObjectMeta: *info.ObjectMeta.DeepCopy(),
			Type:       info.Type,
		}
		if info.Immutable != nil {
			value := *info.Immutable
			intent.Immutable = &value
		}
		return intent, nil
	case *model.SecretInput:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*model.SecretInput]())
		}
		if len(info.Data) > 0 || strings.TrimSpace(info.URL) != "" {
			return nil, fmt.Errorf("adopted Secret JobInfo must not contain plaintext data or URL input")
		}
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:        info.Name,
				Namespace:   info.Namespace,
				Labels:      adoptedOverlayStringMap(nil, info.Labels),
				Annotations: adoptedOverlayStringMap(nil, info.Annotations),
			},
			Type: corev1.SecretType(info.Type),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported adopted secret job info type: %T", job.JobInfo)
	}
}

func (c *DeploySecretJobCtl) decryptAdoptedSecretMaterial(
	ctx context.Context,
	binding *adoptedResourceBinding,
) (*corev1.Secret, *adoptedSecretMaterial, error) {
	if binding == nil || binding.application == nil || binding.snapshot == nil || binding.resource == nil {
		return nil, nil, fmt.Errorf("adopted secret binding is incomplete")
	}
	if c.importSecretKeyring == nil {
		return nil, nil, importsecret.ErrKeyringNotConfigured
	}
	var baseline corev1.Secret
	if err := json.Unmarshal(binding.resource.Manifest, &baseline); err != nil {
		return nil, nil, fmt.Errorf("decode adopted secret snapshot manifest: %w", err)
	}
	source := binding.resource.Source
	namespace := strings.TrimSpace(source.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(binding.snapshot.Namespace)
	}
	if namespace != strings.TrimSpace(binding.snapshot.Namespace) {
		return nil, nil, fmt.Errorf(
			"adopted secret source namespace %s differs from application namespace %s",
			namespace,
			binding.snapshot.Namespace,
		)
	}
	if baseline.Name != source.Name || baseline.Namespace != namespace {
		return nil, nil, fmt.Errorf(
			"adopted secret snapshot manifest identity %s/%s does not match source %s/%s",
			baseline.Namespace,
			baseline.Name,
			namespace,
			source.Name,
		)
	}
	if baseline.APIVersion != source.APIVersion || !strings.EqualFold(baseline.Kind, source.Kind) {
		return nil, nil, fmt.Errorf(
			"adopted secret snapshot manifest type %s/%s does not match source %s/%s",
			baseline.APIVersion,
			baseline.Kind,
			source.APIVersion,
			source.Kind,
		)
	}
	if len(baseline.Data) > 0 || len(baseline.StringData) > 0 {
		return nil, nil, fmt.Errorf("adopted secret snapshot manifest must not contain plaintext payload")
	}
	holders, err := loadAdoptedSecretCiphertextHolders(ctx, c.store, binding)
	if err != nil {
		return nil, nil, err
	}
	managedKeys := make(map[string]struct{}, len(binding.resource.SecretKeys))
	for _, key := range binding.resource.SecretKeys {
		if _, duplicate := managedKeys[key]; duplicate {
			return nil, nil, fmt.Errorf("adopted secret snapshot contains duplicate managed key %s", key)
		}
		managedKeys[key] = struct{}{}
	}
	var managedData map[string][]byte
	ciphertextUpdates := make([]adoptedSecretCiphertextUpdate, 0, len(holders))
	rotationUpdates := make([]adoptedSecretCiphertextUpdate, 0, len(holders))
	for _, holder := range holders {
		envelopes := holder.payload[source.Name]
		if err := validateAdoptedSecretEnvelopeKeys(envelopes, managedKeys, namespace, source.Name); err != nil {
			clearSecretData(managedData)
			return nil, nil, err
		}
		holderData := make(map[string][]byte, len(binding.resource.SecretKeys))
		holderNeedsRotation := false
		for _, key := range binding.resource.SecretKeys {
			envelope := envelopes[key]
			aad := importsecret.ResourceAAD(
				binding.application.ID,
				namespace,
				source.APIVersion,
				source.Kind,
				source.Name,
				key,
			)
			plaintext, err := c.importSecretKeyring.Decrypt(envelope, aad)
			if err != nil {
				clearSecretData(holderData)
				clearSecretData(managedData)
				return nil, nil, fmt.Errorf("decrypt adopted secret key %s for %s/%s: %w", key, namespace, source.Name, err)
			}
			holderData[key] = plaintext
			if !c.importSecretKeyring.NeedsRotation(envelope) {
				continue
			}
			rotated, err := c.importSecretKeyring.Encrypt(plaintext, aad)
			if err != nil {
				clearSecretData(holderData)
				clearSecretData(managedData)
				return nil, nil, fmt.Errorf("rotate adopted secret key %s for %s/%s: %w", key, namespace, source.Name, err)
			}
			envelopes[key] = rotated
			holderNeedsRotation = true
		}
		if managedData == nil {
			managedData = holderData
		} else {
			if !secretDataEqual(managedData, holderData) {
				clearSecretData(holderData)
				clearSecretData(managedData)
				return nil, nil, fmt.Errorf(
					"adopted secret ciphertext holders for %s/%s decrypt to inconsistent values",
					namespace,
					source.Name,
				)
			}
			clearSecretData(holderData)
		}
		encryptedData := holder.component.AdoptedSecretData
		if holderNeedsRotation {
			encryptedData, err = model.NewJSONStructByStruct(holder.payload)
			if err != nil {
				clearSecretData(managedData)
				return nil, nil, fmt.Errorf("encode rotated adopted secret ciphertext: %w", err)
			}
		}
		update := adoptedSecretCiphertextUpdate{
			component:     holder.component,
			encryptedData: encryptedData,
		}
		ciphertextUpdates = append(ciphertextUpdates, update)
		if holderNeedsRotation {
			rotationUpdates = append(rotationUpdates, update)
		}
	}
	return &baseline, &adoptedSecretMaterial{
		data:              managedData,
		ciphertextUpdates: ciphertextUpdates,
		rotationUpdates:   rotationUpdates,
		rotationNeeded:    len(rotationUpdates) > 0,
	}, nil
}

type adoptedSecretCiphertextHolder struct {
	component *model.ApplicationComponent
	payload   map[string]map[string]importsecret.Envelope
}

func loadAdoptedSecretCiphertextHolders(
	ctx context.Context,
	store datastore.DataStore,
	binding *adoptedResourceBinding,
) ([]adoptedSecretCiphertextHolder, error) {
	if store == nil || binding == nil || binding.application == nil || binding.resource == nil {
		return nil, fmt.Errorf("adopted secret component lookup state is incomplete")
	}
	componentName := strings.TrimSpace(binding.resource.ComponentName)
	entities, err := store.List(
		ctx,
		&model.ApplicationComponent{AppID: binding.application.ID},
		&datastore.ListOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("load adopted secret components: %w", err)
	}
	holders := make([]adoptedSecretCiphertextHolder, 0)
	namedComponentFound := componentName == ""
	seenComponents := make(map[string]struct{}, len(entities))
	for _, entity := range entities {
		component, ok := entity.(*model.ApplicationComponent)
		if !ok || component == nil || component.AppID != binding.application.ID {
			continue
		}
		if _, duplicate := seenComponents[component.Name]; duplicate {
			return nil, fmt.Errorf("adopted secret component %s is duplicated", component.Name)
		}
		seenComponents[component.Name] = struct{}{}
		if component.Name == componentName {
			namedComponentFound = true
		}
		if component.AdoptedSecretData == nil {
			continue
		}
		payload, err := decodeAdoptedSecretEnvelopes(component.AdoptedSecretData)
		if err != nil {
			return nil, fmt.Errorf("decode adopted secret ciphertext for component %s: %w", component.Name, err)
		}
		if _, containsSource := payload[binding.resource.Source.Name]; !containsSource {
			continue
		}
		holders = append(holders, adoptedSecretCiphertextHolder{component: component, payload: payload})
	}
	if !namedComponentFound {
		return nil, fmt.Errorf("adopted secret component %s does not exist", componentName)
	}
	if len(holders) == 0 {
		return nil, fmt.Errorf(
			"adopted secret ciphertext for %s is not held by any application component",
			binding.resource.Source.Name,
		)
	}
	if componentName != "" {
		namedHolderFound := false
		for _, holder := range holders {
			if holder.component.Name == componentName {
				namedHolderFound = true
				break
			}
		}
		if !namedHolderFound {
			return nil, fmt.Errorf("adopted secret component %s has no encrypted data for %s", componentName, binding.resource.Source.Name)
		}
	}
	sort.SliceStable(holders, func(i, j int) bool {
		return holders[i].component.Name < holders[j].component.Name
	})
	return holders, nil
}

func validateAdoptedSecretEnvelopeKeys(
	envelopes map[string]importsecret.Envelope,
	managedKeys map[string]struct{},
	namespace, name string,
) error {
	if len(envelopes) != len(managedKeys) {
		return fmt.Errorf(
			"adopted secret ciphertext keys for %s/%s differ from the adoption snapshot",
			namespace,
			name,
		)
	}
	for key := range envelopes {
		if _, managed := managedKeys[key]; !managed {
			return fmt.Errorf(
				"adopted secret ciphertext key %s for %s/%s is not present in the adoption snapshot",
				key,
				namespace,
				name,
			)
		}
	}
	return nil
}

func decodeAdoptedSecretEnvelopes(
	encryptedData *model.JSONStruct,
) (map[string]map[string]importsecret.Envelope, error) {
	if encryptedData == nil {
		return nil, fmt.Errorf("adopted secret encrypted data is required")
	}
	data, err := json.Marshal(encryptedData)
	if err != nil {
		return nil, fmt.Errorf("encode adopted secret ciphertext: %w", err)
	}
	var payload map[string]map[string]importsecret.Envelope
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decode adopted secret ciphertext: %w", err)
	}
	if payload == nil {
		return nil, fmt.Errorf("adopted secret ciphertext is empty")
	}
	return payload, nil
}

func (c *DeploySecretJobCtl) recreateAdoptedSecret(
	ctx context.Context,
	desired, baseline *corev1.Secret,
	material *adoptedSecretMaterial,
	binding *adoptedResourceBinding,
) error {
	if material == nil || len(material.ciphertextUpdates) == 0 {
		return fmt.Errorf("adopted secret recreation material is incomplete")
	}
	recreation, err := prepareAdoptedDependencyRecreation(c.store, binding)
	if err != nil {
		return fmt.Errorf("prepare adopted secret recreation: %w", err)
	}
	candidate, err := adoptedSecretForRecreation(desired, baseline, binding.resource.SecretKeys, material.data)
	if err != nil {
		return err
	}
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted secret recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	cli := c.client.CoreV1().Secrets(candidate.Namespace)
	created, err := cli.Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := cli.Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted secret %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
			}
			recovered, recoverErr := recoverPendingAdoptedSecretLocked(
				recreationCtx,
				c.store,
				&recreation.adoptedResourceBinding,
				replacement,
				replacement,
				material.ciphertextUpdates,
				c.runtime,
			)
			if recoverErr != nil {
				return fmt.Errorf("recover concurrent adopted secret recreation: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedSecret(ctx, desired, &recreation.adoptedResourceBinding)
			}
			return fmt.Errorf(
				"adopted secret ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf("recreate adopted secret %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	if err := recreation.persistCreatedSecret(recreationCtx, created, created, material.ciphertextUpdates, c.runtime); err != nil {
		return fmt.Errorf("persist recreated adopted secret binding; pending claim retained: %w", err)
	}
	markResourceObserved(ctx, spec.ResourceSecret, created.Namespace, created.Name)
	return nil
}

func adoptedSecretForExistingUpdate(
	current, desired, baseline *corev1.Secret,
	managedKeys []string,
	managedData map[string][]byte,
) (*corev1.Secret, error) {
	if current == nil || desired == nil || baseline == nil {
		return nil, fmt.Errorf("adopted secret live update state is incomplete")
	}
	if desired.Immutable != nil && secretImmutableValue(current.Immutable) != *desired.Immutable {
		return nil, fmt.Errorf("secret %s/%s immutable flag changes are forbidden", current.Namespace, current.Name)
	}
	if secretImmutableValue(current.Immutable) != secretImmutableValue(baseline.Immutable) {
		return nil, fmt.Errorf("secret %s/%s immutable state differs from the adoption snapshot", current.Namespace, current.Name)
	}
	updated := current.DeepCopy()
	updated.Labels = adoptedOverlayStringMap(current.Labels, desired.Labels)
	updated.Annotations = adoptedOverlayStringMap(current.Annotations, desired.Annotations)
	if desired.Type != "" {
		updated.Type = desired.Type
	} else if baseline.Type != "" {
		updated.Type = baseline.Type
	}
	updated.Data = copySecretData(current.Data)
	for _, key := range managedKeys {
		value, ok := managedData[key]
		if !ok {
			return nil, fmt.Errorf("adopted secret managed key %s is not decrypted", key)
		}
		if updated.Data == nil {
			updated.Data = make(map[string][]byte, len(managedKeys))
		}
		updated.Data[key] = append([]byte(nil), value...)
	}
	updated.StringData = nil
	if current.Immutable != nil && *current.Immutable &&
		(!secretDataEqual(current.Data, updated.Data) || current.Type != updated.Type) {
		return nil, fmt.Errorf("secret %s/%s is immutable; managed payload differs", current.Namespace, current.Name)
	}
	return updated, nil
}

func adoptedSecretForRecreation(
	desired, baseline *corev1.Secret,
	managedKeys []string,
	managedData map[string][]byte,
) (*corev1.Secret, error) {
	if desired == nil || baseline == nil {
		return nil, fmt.Errorf("adopted secret recreation state is incomplete")
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return nil, fmt.Errorf(
			"adopted secret recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	if desired.Immutable != nil && secretImmutableValue(baseline.Immutable) != *desired.Immutable {
		return nil, fmt.Errorf("secret %s/%s immutable flag changes are forbidden", desired.Namespace, desired.Name)
	}
	candidate := baseline.DeepCopy()
	candidate.Labels = adoptedOverlayStringMap(baseline.Labels, desired.Labels)
	candidate.Annotations = adoptedOverlayStringMap(baseline.Annotations, desired.Annotations)
	if desired.Type != "" {
		candidate.Type = desired.Type
	}
	candidate.Data = make(map[string][]byte, len(managedKeys))
	for _, key := range managedKeys {
		value, ok := managedData[key]
		if !ok {
			return nil, fmt.Errorf("adopted secret managed key %s is not decrypted", key)
		}
		candidate.Data[key] = append([]byte(nil), value...)
	}
	candidate.StringData = nil
	return candidate, nil
}

func adoptedSecretEqual(current, candidate *corev1.Secret) bool {
	if current == nil || candidate == nil {
		return current == candidate
	}
	return apiequality.Semantic.DeepEqual(current.Labels, candidate.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, candidate.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Immutable, candidate.Immutable) &&
		current.Type == candidate.Type &&
		secretDataEqual(current.Data, candidate.Data)
}

func secretImmutableValue(value *bool) bool {
	return value != nil && *value
}

func secretDataEqual(left, right map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		rightValue, ok := right[key]
		if !ok || !bytes.Equal(leftValue, rightValue) {
			return false
		}
	}
	return true
}

func copySecretData(source map[string][]byte) map[string][]byte {
	if source == nil {
		return nil
	}
	copied := make(map[string][]byte, len(source))
	for key, value := range source {
		copied[key] = append([]byte(nil), value...)
	}
	return copied
}

func clearSecretData(data map[string][]byte) {
	for key := range data {
		clear(data[key])
		delete(data, key)
	}
}

// wait is a no-op for Secret objects.
func (c *DeploySecretJobCtl) wait(ctx context.Context) {}

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

func GenerateSecret(component *model.ApplicationComponent, properties *model.Properties) interface{} {
	name, namespace := generatedResourceIdentity(component)

	if url, fileName, ok := externalConfigFileInput(properties, properties != nil && properties.Secret != nil); ok {
		return &model.SecretInput{
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
		data = keyValueDataOrNil(properties.Secret)
	}

	return &model.SecretInput{
		Name:      name,
		Namespace: namespace,
		Labels:    labels,
		Data:      data,
	}
}

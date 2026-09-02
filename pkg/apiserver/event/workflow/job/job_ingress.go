package job

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"

	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type DeployIngressJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployIngressJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployIngressJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployIngressJobCtl", job, client, store, ack, shareLocker)
	if !ok {
		return nil
	}
	return &DeployIngressJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployIngressJobCtl) Clean(ctx context.Context) {
	c.cleanCreated(ctx, config.ResourceIngress, "ingress", func(ctx context.Context, namespace, name string) error {
		return c.client.NetworkingV1().Ingresses(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after job failure")
}

func (c *DeployIngressJobCtl) Run(ctx context.Context) error {
	return c.runWithWait(ctx, c.run, c.wait, "DeployIngressJob run error", "")
}

func (c *DeployIngressJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	ingress, err := ingressFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if ingress.Namespace == "" {
		ingress.Namespace = c.namespace
	}
	if binding, adopted, sourceErr := adoptedResourceForJob(
		ctx,
		c.store,
		c.job,
		"Ingress",
		ingress.Namespace,
		ingress.Name,
	); sourceErr != nil {
		return sourceErr
	} else if adopted {
		ingress.Name = binding.resource.Source.Name
		ingress.Namespace = binding.resource.Source.Namespace
		if ingress.Namespace == "" {
			ingress.Namespace = c.namespace
		}
		return c.reconcileAdoptedIngress(ctx, ingress, binding)
	}

	if reusableName, err := c.findReusableIngressName(ctx, ingress); err != nil {
		klog.Warningf("list reusable ingress candidate failed for %s/%s: %v", ingress.Namespace, ingress.Name, err)
	} else if reusableName != "" && reusableName != ingress.Name {
		klog.Infof("Ingress %s/%s overlaps existing route, reusing ingress name %s for update", ingress.Namespace, ingress.Name, reusableName)
		ingress.Name = reusableName
		c.job.Name = reusableName
	}

	updateIngress := func(ctx context.Context, current *networkingv1.Ingress) error {
		if !ingressNeedsUpdate(current, ingress) {
			markResourceObserved(ctx, config.ResourceIngress, ingress.Namespace, ingress.Name)
			klog.Infof("Ingress %s/%s is up-to-date; skipping update", ingress.Namespace, ingress.Name)
			return nil
		}
		if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*networkingv1.Ingress, error) {
			return c.client.NetworkingV1().Ingresses(ingress.Namespace).Get(ctx, ingress.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *networkingv1.Ingress) error {
			updated := buildUpdatedIngress(latest, ingress)
			_, err := c.client.NetworkingV1().Ingresses(ingress.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("update ingress %s/%s failed: %w", ingress.Namespace, ingress.Name, err)
		}
		markResourceObserved(ctx, config.ResourceIngress, ingress.Namespace, ingress.Name)
		klog.Infof("Ingress %s/%s updated successfully", ingress.Namespace, ingress.Name)
		return nil
	}

	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[networkingv1.Ingress]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:          c.job,
			ack:          c.ack,
			labels:       ingress.Labels,
			kind:         config.ResourceIngress,
			lockProvider: c.shareLocker,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := c.client.NetworkingV1().Ingresses(ingress.Namespace).List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy config.ShareStrategy) {
				if strategy == config.ShareStrategyIgnore {
					klog.Infof("Ingress %s/%s marked as shared ignore; skipping", ingress.Namespace, ingress.Name)
				} else {
					klog.Infof("Ingress %s/%s already exists and is shared; skipping", ingress.Namespace, ingress.Name)
				}
			},
		},
		namespace: ingress.Namespace,
		name:      ingress.Name,
		getFn: func(ctx context.Context) (*networkingv1.Ingress, error) {
			return c.client.NetworkingV1().Ingresses(ingress.Namespace).Get(ctx, ingress.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*networkingv1.Ingress, error) {
			return c.client.NetworkingV1().Ingresses(ingress.Namespace).Create(ctx, ingress, metav1.CreateOptions{})
		},
		onExisting:      updateIngress,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure ingress %s/%s failed: %w", ingress.Namespace, ingress.Name, err)
	}
	if created {
		klog.Infof("Ingress %q created successfully", ingress.Name)
	}

	return nil
}

func (c *DeployIngressJobCtl) reconcileAdoptedIngress(
	ctx context.Context,
	desired *networkingv1.Ingress,
	binding *adoptedResourceBinding,
) error {
	if desired == nil {
		return fmt.Errorf("adopted ingress desired resource is required")
	}
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return err
	}
	if !writable {
		return nil
	}
	cli := c.client.NetworkingV1().Ingresses(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedIngress(ctx, desired, binding)
		}
		return fmt.Errorf("get adopted ingress %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		recovered, recoverErr := recoverPendingAdoptedDependency(
			ctx, c.store, binding, current, current, c.runtime, c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted ingress binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	if !adoptedIngressNeedsUpdate(current, desired) {
		markResourceObserved(ctx, config.ResourceIngress, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*networkingv1.Ingress, error) {
		return cli.Get(ctx, desired.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *networkingv1.Ingress) error {
		if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
			return err
		}
		candidate := adoptedIngressForExistingUpdate(latest, desired)
		if apiequality.Semantic.DeepEqual(latest.Labels, candidate.Labels) &&
			apiequality.Semantic.DeepEqual(latest.Annotations, candidate.Annotations) &&
			apiequality.Semantic.DeepEqual(latest.Spec, candidate.Spec) {
			return nil
		}
		_, err := cli.Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("update adopted ingress %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	markResourceObserved(ctx, config.ResourceIngress, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployIngressJobCtl) recreateAdoptedIngress(
	ctx context.Context,
	desired *networkingv1.Ingress,
	binding *adoptedResourceBinding,
) error {
	recreation, err := prepareAdoptedDependencyRecreation(c.store, binding)
	if err != nil {
		return fmt.Errorf("prepare adopted ingress recreation: %w", err)
	}
	var baseline networkingv1.Ingress
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted ingress recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted ingress recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	candidate := adoptedIngressForExistingUpdate(&baseline, desired)
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "Ingress"}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted ingress recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	created, err := c.client.NetworkingV1().Ingresses(candidate.Namespace).Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := c.client.NetworkingV1().Ingresses(candidate.Namespace).Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted ingress %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
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
				return fmt.Errorf("recover concurrent adopted ingress recreation: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedIngress(ctx, desired, &recreation.adoptedResourceBinding)
			}
			return fmt.Errorf(
				"adopted ingress ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf("recreate adopted ingress %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	if err := recreation.persistCreated(recreationCtx, created, created, c.runtime); err != nil {
		return fmt.Errorf("persist recreated adopted ingress binding; pending claim retained: %w", err)
	}
	markResourceObserved(ctx, config.ResourceIngress, created.Namespace, created.Name)
	return nil
}

func adoptedIngressForExistingUpdate(current, desired *networkingv1.Ingress) *networkingv1.Ingress {
	updated := current.DeepCopy()
	if desired == nil {
		return updated
	}
	updated.Labels = adoptedOverlayStringMap(current.Labels, desired.Labels)
	updated.Annotations = adoptedOverlayStringMap(current.Annotations, desired.Annotations)
	if desired.Spec.DefaultBackend != nil {
		updated.Spec.DefaultBackend = desired.Spec.DefaultBackend.DeepCopy()
	}
	if desired.Spec.IngressClassName != nil {
		className := *desired.Spec.IngressClassName
		updated.Spec.IngressClassName = &className
	}
	if desired.Spec.TLS != nil {
		updated.Spec.TLS = append([]networkingv1.IngressTLS(nil), desired.Spec.TLS...)
	}
	if desired.Spec.Rules != nil {
		updated.Spec.Rules = append([]networkingv1.IngressRule(nil), desired.Spec.Rules...)
	}
	return updated
}

func adoptedIngressNeedsUpdate(current, desired *networkingv1.Ingress) bool {
	if current == nil || desired == nil {
		return false
	}
	updated := adoptedIngressForExistingUpdate(current, desired)
	return !apiequality.Semantic.DeepEqual(current.Labels, updated.Labels) ||
		!apiequality.Semantic.DeepEqual(current.Annotations, updated.Annotations) ||
		!apiequality.Semantic.DeepEqual(current.Spec, updated.Spec)
}

func (c *DeployIngressJobCtl) findReusableIngressName(ctx context.Context, desired *networkingv1.Ingress) (string, error) {
	if c == nil || c.client == nil || desired == nil {
		return "", nil
	}
	ns := strings.TrimSpace(desired.Namespace)
	if ns == "" {
		ns = config.DefaultNamespace
	}
	list, err := c.client.NetworkingV1().Ingresses(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	bestScore := 0
	bestName := ""
	for i := range list.Items {
		existing := &list.Items[i]
		if existing == nil || existing.Name == desired.Name {
			continue
		}
		if !ingressSpecsOverlap(existing.Spec, desired.Spec) {
			continue
		}
		score := reusableIngressScore(existing, desired)
		if score > bestScore {
			bestScore = score
			bestName = existing.Name
		}
	}
	if bestScore == 0 {
		return "", nil
	}
	return bestName, nil
}

func ingressSpecsOverlap(existing, desired networkingv1.IngressSpec) bool {
	existingKeys := ingressRouteKeys(existing)
	if len(existingKeys) == 0 {
		return false
	}
	for key := range ingressRouteKeys(desired) {
		if _, ok := existingKeys[key]; ok {
			return true
		}
	}
	return false
}

func ingressRouteKeys(spec networkingv1.IngressSpec) map[string]struct{} {
	keys := make(map[string]struct{})
	for _, rule := range spec.Rules {
		if rule.HTTP == nil {
			continue
		}
		host := strings.TrimSpace(rule.Host)
		for _, path := range rule.HTTP.Paths {
			routePath := strings.TrimSpace(path.Path)
			if routePath == "" {
				routePath = "/"
			}
			keys[host+"|"+routePath] = struct{}{}
		}
	}
	return keys
}

func reusableIngressScore(existing, desired *networkingv1.Ingress) int {
	if existing == nil || desired == nil {
		return 0
	}
	desiredAppID := ingressAppIdentity(desired)
	existingAppID := ingressAppIdentity(existing)
	if desiredAppID == "" || existingAppID == "" || desiredAppID != existingAppID {
		return 0
	}

	desiredAppLabel := strings.TrimSpace(desired.Labels[config.LabelAppID])
	existingAppLabel := strings.TrimSpace(existing.Labels[config.LabelAppID])
	if desiredAppLabel != "" && desiredAppLabel == existingAppLabel {
		return 3
	}

	desiredComponent := strings.TrimSpace(desired.Labels[config.LabelComponentName])
	existingComponent := strings.TrimSpace(existing.Labels[config.LabelComponentName])
	if desiredComponent != "" && existingComponent != "" && desiredComponent == existingComponent {
		return 2
	}

	desiredBase := ingressLogicalBaseName(desired.Name)
	existingBase := ingressLogicalBaseName(existing.Name)
	if desiredBase != "" && desiredBase == existingBase &&
		strings.HasPrefix(desired.Name, "ing-") && strings.HasPrefix(existing.Name, "ing-") {
		return 1
	}
	return 0
}

func ingressAppIdentity(ing *networkingv1.Ingress) string {
	if ing == nil {
		return ""
	}
	if appID := strings.TrimSpace(ing.Labels[config.LabelAppID]); appID != "" {
		return appID
	}
	return ingressAppSegment(ing.Name)
}

func ingressAppSegment(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	idx := strings.LastIndex(trimmed, "-")
	if idx < 0 || idx == len(trimmed)-1 {
		return ""
	}
	return trimmed[idx+1:]
}

func ingressLogicalBaseName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	idx := strings.LastIndex(trimmed, "-")
	if idx <= 0 {
		return trimmed
	}
	return trimmed[:idx]
}

func (c *DeployIngressJobCtl) wait(ctx context.Context) error {
	timeout := time.After(time.Duration(c.timeout()) * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	getIngressName := func() string {
		if ingressObj, ok := optionalJobInfo[*networkingv1.Ingress](c.job); ok && ingressObj.Name != "" {
			return ingressObj.Name
		}
		return c.job.Name
	}

	for {
		select {
		case <-ctx.Done():
			name := getIngressName()
			return NewStatusError(config.StatusCancelled, fmt.Errorf("ingress %s cancelled: %w", name, ctx.Err()))
		case <-timeout:
			name := getIngressName()
			return NewStatusError(config.StatusTimeout, fmt.Errorf("wait ingress %s timeout", name))
		case <-ticker.C:
			ingressName := getIngressName()
			ing, err := c.client.NetworkingV1().Ingresses(c.job.Namespace).Get(ctx, ingressName, metav1.GetOptions{})
			if err != nil {
				if k8serrors.IsNotFound(err) {
					continue
				}
				return fmt.Errorf("wait ingress %s error: %w", ingressName, err)
			}
			if ingressReady(ing) {
				return nil
			}
		}
	}
}

func (c *DeployIngressJobCtl) timeout() int64 {
	if c.job.Timeout == 0 {
		c.job.Timeout = config.DeployTimeout
	}
	return c.job.Timeout
}

func ingressReady(ing *networkingv1.Ingress) bool {
	if ing == nil {
		return false
	}
	return true
}

func ingressNeedsUpdate(current, desired *networkingv1.Ingress) bool {
	if current == nil || desired == nil {
		return false
	}
	updated := buildUpdatedIngress(current, desired)
	if !apiequality.Semantic.DeepEqual(current.Spec, updated.Spec) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Labels, updated.Labels) {
		return true
	}
	return !apiequality.Semantic.DeepEqual(current.Annotations, updated.Annotations)
}

func buildUpdatedIngress(current, desired *networkingv1.Ingress) *networkingv1.Ingress {
	updated := current.DeepCopy()
	if desired == nil {
		return updated
	}
	updated.Labels = preserveIngressSystemLabels(current.Labels, desired.Labels)
	updated.Annotations = preserveIngressSystemAnnotations(current.Annotations, desired.Annotations)
	updated.Spec = desired.Spec
	return updated
}

func preserveIngressSystemLabels(current, desired map[string]string) map[string]string {
	return preserveStringMapKeys(current, desired, eruunSystemLabelKeys)
}

func preserveIngressSystemAnnotations(current, desired map[string]string) map[string]string {
	return preserveStringMapKeys(current, desired, ingressSystemAnnotationKeys)
}

var ingressSystemAnnotationKeys = []string{}

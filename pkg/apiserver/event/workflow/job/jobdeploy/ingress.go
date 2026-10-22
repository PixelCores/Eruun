package jobdeploy

import (
	"context"
	"fmt"
	"strings"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type DeployIngressJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
}

func NewDeployIngressJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployIngressJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployIngressJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployIngressJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployIngressJobCtl) Clean(ctx context.Context) {
	c.CleanCreated(ctx, domainspec.ResourceIngress, "ingress", func(ctx context.Context, namespace, name string) error {
		return c.Client.NetworkingV1().Ingresses(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after job failure")
}

func (c *DeployIngressJobCtl) Run(ctx context.Context) error {
	return c.RunWithWait(ctx, c.run, c.wait, "DeployIngressJob run error", "")
}

func (c *DeployIngressJobCtl) run(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	ingress, err := jobinfo.IngressFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	if ingress.Namespace == "" {
		ingress.Namespace = c.Namespace
	}

	if reusableName, err := c.findReusableIngressName(ctx, ingress); err != nil {
		klog.Warningf("list reusable ingress candidate failed for %s/%s: %v", ingress.Namespace, ingress.Name, err)
	} else if reusableName != "" && reusableName != ingress.Name {
		klog.Infof("Ingress %s/%s overlaps existing route, reusing ingress name %s for update", ingress.Namespace, ingress.Name, reusableName)
		ingress.Name = reusableName
		c.Job.Name = reusableName
	}

	updateIngress := func(ctx context.Context, current *networkingv1.Ingress) error {
		if !ingressNeedsUpdate(current, ingress) {
			jobexec.MarkResourceObserved(ctx, domainspec.ResourceIngress, ingress.Namespace, ingress.Name)
			klog.Infof("Ingress %s/%s is up-to-date; skipping update", ingress.Namespace, ingress.Name)
			return nil
		}
		if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*networkingv1.Ingress, error) {
			return c.Client.NetworkingV1().Ingresses(ingress.Namespace).Get(ctx, ingress.Name, metav1.GetOptions{})
		}, func(ctx context.Context, latest *networkingv1.Ingress) error {
			updated := buildUpdatedIngress(latest, ingress)
			_, err := c.Client.NetworkingV1().Ingresses(ingress.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return fmt.Errorf("update ingress %s/%s failed: %w", ingress.Namespace, ingress.Name, err)
		}
		jobexec.MarkResourceObserved(ctx, domainspec.ResourceIngress, ingress.Namespace, ingress.Name)
		klog.Infof("Ingress %s/%s updated successfully", ingress.Namespace, ingress.Name)
		return nil
	}

	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[networkingv1.Ingress]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:          c.Job,
			Ack:          c.Ack,
			Labels:       ingress.Labels,
			Kind:         domainspec.ResourceIngress,
			LockProvider: c.ShareLocker,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := c.Client.NetworkingV1().Ingresses(ingress.Namespace).List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					klog.Infof("Ingress %s/%s marked as shared ignore; skipping", ingress.Namespace, ingress.Name)
				} else {
					klog.Infof("Ingress %s/%s already exists and is shared; skipping", ingress.Namespace, ingress.Name)
				}
			},
		},
		Namespace: ingress.Namespace,
		Name:      ingress.Name,
		GetFn: func(ctx context.Context) (*networkingv1.Ingress, error) {
			return c.Client.NetworkingV1().Ingresses(ingress.Namespace).Get(ctx, ingress.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*networkingv1.Ingress, error) {
			return c.Client.NetworkingV1().Ingresses(ingress.Namespace).Create(ctx, ingress, metav1.CreateOptions{})
		},
		OnExisting: updateIngress,
	})
	if err != nil {
		return fmt.Errorf("ensure ingress %s/%s failed: %w", ingress.Namespace, ingress.Name, err)
	}
	if created {
		klog.Infof("Ingress %q created successfully", ingress.Name)
	}

	return nil
}

func (c *DeployIngressJobCtl) findReusableIngressName(ctx context.Context, desired *networkingv1.Ingress) (string, error) {
	if c.Client == nil || desired == nil {
		return "", fmt.Errorf("find reusable ingress: Kubernetes client and desired ingress are required")
	}
	ns := strings.TrimSpace(desired.Namespace)
	list, err := c.Client.NetworkingV1().Ingresses(ns).List(ctx, metav1.ListOptions{})
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

	desiredAppLabel := strings.TrimSpace(desired.Labels[domainspec.LabelAppID])
	existingAppLabel := strings.TrimSpace(existing.Labels[domainspec.LabelAppID])
	if desiredAppLabel != "" && desiredAppLabel == existingAppLabel {
		return 3
	}

	desiredComponent := strings.TrimSpace(desired.Labels[domainspec.LabelComponentName])
	existingComponent := strings.TrimSpace(existing.Labels[domainspec.LabelComponentName])
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
	if appID := strings.TrimSpace(ing.Labels[domainspec.LabelAppID]); appID != "" {
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
		if ingressObj, ok := jobinfo.OptionalJobInfo[*networkingv1.Ingress](c.Job); ok && ingressObj.Name != "" {
			return ingressObj.Name
		}
		return c.Job.Name
	}

	for {
		select {
		case <-ctx.Done():
			name := getIngressName()
			return jobinfo.NewStatusError(config.StatusCancelled, fmt.Errorf("ingress %s cancelled: %w", name, ctx.Err()))
		case <-timeout:
			name := getIngressName()
			return jobinfo.NewStatusError(config.StatusTimeout, fmt.Errorf("wait ingress %s timeout", name))
		case <-ticker.C:
			ingressName := getIngressName()
			ing, err := c.Client.NetworkingV1().Ingresses(c.Job.Namespace).Get(ctx, ingressName, metav1.GetOptions{})
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
	if c.Job.Timeout == 0 {
		c.Job.Timeout = config.DeployTimeout
	}
	return c.Job.Timeout
}

func ingressReady(ing *networkingv1.Ingress) bool {
	return ing != nil
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

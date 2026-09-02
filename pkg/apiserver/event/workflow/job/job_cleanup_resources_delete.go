package job

import (
	"context"
	"fmt"
	"strings"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type cleanupResourceRef struct {
	kind      config.ResourceKind
	namespace string
	name      string
	cluster   bool
}

type cleanupResourceSet struct {
	refs []cleanupResourceRef
	seen map[string]struct{}
	errs []error
}

type cleanupResourceDeleteFunc func(context.Context) error

func (c *CleanupResourcesJobCtl) deleteTrackedResource(ctx context.Context, deleted *cleanupResourceSet, kind config.ResourceKind, namespace, name string, cluster bool, deleteFn cleanupResourceDeleteFunc) {
	ref, ok := newCleanupResourceRef(kind, namespace, name, cluster)
	if !ok {
		return
	}
	if c.deferRequiredStatefulSetJobDelete(ref) {
		klog.V(4).Infof("cleanup resources: defer Job %s to required StatefulSet reconciliation", cleanupResourceDisplayName(ref))
		return
	}
	protected, exists, err := c.resourceDeleteProtected(ctx, ref)
	if err != nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("inspect %s %s before delete: %w", kind, cleanupResourceDisplayName(ref), err))
		return
	}
	if !exists {
		return
	}
	if protected {
		if err := c.requiredStatefulSetDeletionProtectedError(ref); err != nil {
			deleted.errs = append(deleted.errs, err)
		}
		return
	}
	c.recordResourceDelete(ctx, deleted, ref, deleteFn)
}

func (c *CleanupResourcesJobCtl) recordResourceDelete(ctx context.Context, deleted *cleanupResourceSet, ref cleanupResourceRef, deleteFn cleanupResourceDeleteFunc) {
	if deleteFn == nil {
		deleted.errs = append(deleted.errs, fmt.Errorf("delete %s %s: delete function is nil", ref.kind, cleanupResourceDisplayName(ref)))
		return
	}
	operation := fmt.Sprintf("deleting %s %s", ref.kind, cleanupResourceDisplayName(ref))
	if err := ensureJobWorkflowOwnership(ctx, c.job, c.store, operation); err != nil {
		deleted.errs = append(deleted.errs, err)
		return
	}
	if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, operation); err != nil {
		deleted.errs = append(deleted.errs, err)
		return
	}
	deleted.record(ref.kind, ref.namespace, ref.name, ref.cluster, deleteFn(ctx))
}

func (c *CleanupResourcesJobCtl) resourceDeleteProtected(ctx context.Context, ref cleanupResourceRef) (bool, bool, error) {
	resourceLabels, exists, err := c.resourceLabels(ctx, ref)
	if err != nil || !exists {
		return false, exists, err
	}
	strategy, protected := cleanupResourceShareProtected(resourceLabels)
	if protected {
		klog.Infof("cleanup resources: keep shared %s %s strategy=%s", ref.kind, cleanupResourceDisplayName(ref), strategy)
	}
	return protected, true, nil
}

func (c *CleanupResourcesJobCtl) resourceLabels(ctx context.Context, ref cleanupResourceRef) (map[string]string, bool, error) {
	var (
		obj metav1.Object
		err error
	)
	switch ref.kind {
	case config.ResourceDeployment:
		obj, err = c.client.AppsV1().Deployments(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceStatefulSet:
		obj, err = c.client.AppsV1().StatefulSets(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceJob:
		obj, err = c.client.BatchV1().Jobs(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceCronJob:
		obj, err = c.client.BatchV1().CronJobs(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceService:
		obj, err = c.client.CoreV1().Services(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceConfigMap:
		obj, err = c.client.CoreV1().ConfigMaps(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceSecret:
		obj, err = c.client.CoreV1().Secrets(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourcePVC:
		obj, err = c.client.CoreV1().PersistentVolumeClaims(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceIngress:
		obj, err = c.client.NetworkingV1().Ingresses(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceServiceAccount:
		obj, err = c.client.CoreV1().ServiceAccounts(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceRole:
		obj, err = c.client.RbacV1().Roles(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceRoleBinding:
		obj, err = c.client.RbacV1().RoleBindings(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceClusterRole:
		obj, err = c.client.RbacV1().ClusterRoles().Get(ctx, ref.name, metav1.GetOptions{})
	case config.ResourceClusterRoleBinding:
		obj, err = c.client.RbacV1().ClusterRoleBindings().Get(ctx, ref.name, metav1.GetOptions{})
	default:
		return nil, false, fmt.Errorf("unsupported cleanup resource kind %q", ref.kind)
	}
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if obj == nil {
		return nil, false, nil
	}
	return obj.GetLabels(), true, nil
}

func (c *CleanupResourcesJobCtl) deleteDeployment(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	return c.client.AppsV1().Deployments(namespaceOrDefault(namespace)).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *CleanupResourcesJobCtl) deleteStatefulSet(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	ref, ok := newCleanupResourceRef(config.ResourceStatefulSet, namespace, name, false)
	if !ok {
		return nil
	}
	if c != nil && c.job != nil && versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo) {
		if err := c.ensureStatefulSetPVCRetention(ctx, ref); err != nil {
			return err
		}
		var lastDeleteError error
		for attempt := 1; ; attempt++ {
			statefulSet, err := c.client.AppsV1().StatefulSets(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("get required StatefulSet %s before delete: %w", cleanupResourceDisplayName(ref), err)
			}
			if _, protected := cleanupResourceShareProtected(statefulSet.Labels); protected {
				return c.requiredStatefulSetDeletionProtectedError(ref)
			}
			retentionTarget, remembered := c.statefulSetRetentionTargets[ref.namespace+"/"+ref.name]
			if !remembered || retentionTarget.uid != statefulSet.UID {
				return k8serrors.NewConflict(
					schema.GroupResource{Group: "apps", Resource: "statefulsets"},
					ref.name,
					fmt.Errorf("StatefulSet %s changed after PVC retention convergence", cleanupResourceDisplayName(ref)),
				)
			}
			if !statefulSetPVCRetentionIsRetain(statefulSet) ||
				(statefulSet.Generation > 0 && statefulSet.Status.ObservedGeneration < statefulSet.Generation) {
				return k8serrors.NewConflict(
					schema.GroupResource{Group: "apps", Resource: "statefulsets"},
					ref.name,
					fmt.Errorf("StatefulSet %s PVC retention is not observed as Retain/Retain", cleanupResourceDisplayName(ref)),
				)
			}
			if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, fmt.Sprintf("deleting required StatefulSet %s", cleanupResourceDisplayName(ref))); err != nil {
				return err
			}
			if attempt > requiredStatefulSetSafetyRefreshMaxAttempts {
				return k8serrors.NewConflict(
					schema.GroupResource{Group: "apps", Resource: "statefulsets"},
					ref.name,
					fmt.Errorf(
						"delete StatefulSet %s did not converge after %d attempts: %w",
						cleanupResourceDisplayName(ref), requiredStatefulSetSafetyRefreshMaxAttempts, lastDeleteError,
					),
				)
			}

			propagation := metav1.DeletePropagationOrphan
			uid := statefulSet.UID
			resourceVersion := statefulSet.ResourceVersion
			err = c.client.AppsV1().StatefulSets(ref.namespace).Delete(ctx, ref.name, metav1.DeleteOptions{
				PropagationPolicy: &propagation,
				Preconditions: &metav1.Preconditions{
					UID:             &uid,
					ResourceVersion: &resourceVersion,
				},
			})
			if err == nil {
				return nil
			}
			if !k8serrors.IsConflict(err) && !k8serrors.IsNotFound(err) {
				return fmt.Errorf("delete required StatefulSet %s: %w", cleanupResourceDisplayName(ref), err)
			}
			// Re-read before retrying so a benign resourceVersion drift can
			// converge while a replacement UID or newly protected object still
			// fails closed. A NotFound response is only considered successful
			// once the next GET confirms that the pinned object is gone.
			lastDeleteError = err
		}
	}
	return c.client.AppsV1().StatefulSets(ref.namespace).Delete(ctx, ref.name, metav1.DeleteOptions{})
}

func (c *CleanupResourcesJobCtl) deleteJob(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	return c.client.BatchV1().Jobs(namespaceOrDefault(namespace)).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *CleanupResourcesJobCtl) deleteCronJob(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	return c.client.BatchV1().CronJobs(namespaceOrDefault(namespace)).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *CleanupResourcesJobCtl) deleteService(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	return c.client.CoreV1().Services(namespaceOrDefault(namespace)).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *CleanupResourcesJobCtl) deleteConfigMap(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	return c.client.CoreV1().ConfigMaps(namespaceOrDefault(namespace)).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *CleanupResourcesJobCtl) deleteSecret(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	return c.client.CoreV1().Secrets(namespaceOrDefault(namespace)).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *CleanupResourcesJobCtl) deletePVC(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	return c.client.CoreV1().PersistentVolumeClaims(namespaceOrDefault(namespace)).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *CleanupResourcesJobCtl) deleteIngress(ctx context.Context, namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	return c.client.NetworkingV1().Ingresses(namespaceOrDefault(namespace)).Delete(ctx, name, metav1.DeleteOptions{})
}

func (s *cleanupResourceSet) record(kind config.ResourceKind, namespace, name string, cluster bool, err error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	ns := namespaceOrDefault(namespace)
	if cluster {
		ns = ""
	}
	key := strings.Join([]string{string(kind), ns, name}, "/")
	if _, exists := s.seen[key]; !exists {
		s.seen[key] = struct{}{}
		s.refs = append(s.refs, cleanupResourceRef{kind: kind, namespace: ns, name: name, cluster: cluster})
	}
	if err != nil && !k8serrors.IsNotFound(err) {
		s.errs = append(s.errs, fmt.Errorf("delete %s %s/%s: %w", kind, ns, name, err))
	}
}

func newCleanupResourceRef(kind config.ResourceKind, namespace, name string, cluster bool) (cleanupResourceRef, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return cleanupResourceRef{}, false
	}
	ns := namespaceOrDefault(namespace)
	if cluster {
		ns = ""
	}
	return cleanupResourceRef{kind: kind, namespace: ns, name: name, cluster: cluster}, true
}

func cleanupResourceShareProtected(resourceLabels map[string]string) (config.ShareStrategy, bool) {
	shareName, strategy := shareInfoFromLabels(resourceLabels)
	if strings.TrimSpace(shareName) == "" {
		return "", false
	}
	return strategy, strategy == config.ShareStrategyDefault || strategy == config.ShareStrategyIgnore
}

func cleanupResourceDisplayName(ref cleanupResourceRef) string {
	if ref.cluster {
		return ref.name
	}
	return fmt.Sprintf("%s/%s", ref.namespace, ref.name)
}

func cleanupLabelSelector(component *model.ApplicationComponent) string {
	if component == nil {
		return ""
	}
	appID := strings.TrimSpace(component.AppID)
	componentName := strings.TrimSpace(component.Name)
	if appID == "" || componentName == "" {
		return ""
	}
	return labels.Set{
		config.LabelAppID:         appID,
		config.LabelComponentName: naming.BoundedLabelValue(componentName),
	}.AsSelector().String()
}

func cleanupComponentUsesPods(componentType config.JobType) bool {
	switch componentType {
	case config.ServerJob, config.StoreJob, config.InstantJob, config.ScheduledJob:
		return true
	default:
		return false
	}
}

func namespaceOrDefault(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return config.DefaultNamespace
	}
	return namespace
}

func pickNonEmpty(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value != "" {
		return value
	}
	return strings.TrimSpace(fallback)
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

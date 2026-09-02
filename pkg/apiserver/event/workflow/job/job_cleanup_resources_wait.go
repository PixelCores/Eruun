package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

const cleanupPollInterval = 500 * time.Millisecond

func (c *CleanupResourcesJobCtl) waitForCleanup(ctx context.Context, component *model.ApplicationComponent, refs []cleanupResourceRef) error {
	timeout := time.Duration(c.timeout()) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(config.DeployTimeout) * time.Second
	}
	err := wait.PollUntilContextTimeout(ctx, cleanupPollInterval, timeout, true, func(checkCtx context.Context) (bool, error) {
		for _, ref := range refs {
			gone, err := c.resourceGone(checkCtx, ref)
			if err != nil {
				return false, err
			}
			if !gone {
				return false, nil
			}
		}
		if cleanupComponentUsesPods(component.ComponentType) {
			gone, err := c.componentPodsGone(checkCtx, component)
			if err != nil {
				return false, err
			}
			if !gone {
				return false, nil
			}
		}
		gone, err := c.requiredStatefulSetPVCsGone(checkCtx, component)
		if err != nil {
			return false, err
		}
		if !gone {
			return false, nil
		}
		return true, nil
	})
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, wait.ErrWaitTimeout) {
		return NewStatusError(config.StatusTimeout, fmt.Errorf("cleanup resources for component %s timeout", component.Name))
	}
	return err
}

func (c *CleanupResourcesJobCtl) resourceGone(ctx context.Context, ref cleanupResourceRef) (bool, error) {
	var err error
	switch ref.kind {
	case domainspec.ResourceDeployment:
		_, err = c.client.AppsV1().Deployments(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceStatefulSet:
		_, err = c.client.AppsV1().StatefulSets(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceJob:
		_, err = c.client.BatchV1().Jobs(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceCronJob:
		_, err = c.client.BatchV1().CronJobs(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceService:
		_, err = c.client.CoreV1().Services(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceConfigMap:
		_, err = c.client.CoreV1().ConfigMaps(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceSecret:
		_, err = c.client.CoreV1().Secrets(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourcePVC:
		_, err = c.client.CoreV1().PersistentVolumeClaims(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceIngress:
		_, err = c.client.NetworkingV1().Ingresses(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceServiceAccount:
		_, err = c.client.CoreV1().ServiceAccounts(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceRole:
		_, err = c.client.RbacV1().Roles(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceRoleBinding:
		_, err = c.client.RbacV1().RoleBindings(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceClusterRole:
		_, err = c.client.RbacV1().ClusterRoles().Get(ctx, ref.name, metav1.GetOptions{})
	case domainspec.ResourceClusterRoleBinding:
		_, err = c.client.RbacV1().ClusterRoleBindings().Get(ctx, ref.name, metav1.GetOptions{})
	default:
		return false, fmt.Errorf("unsupported cleanup resource kind %q", ref.kind)
	}
	if err == nil {
		return false, nil
	}
	if k8serrors.IsNotFound(err) {
		return true, nil
	}
	return false, err
}

func (c *CleanupResourcesJobCtl) componentPodsGone(ctx context.Context, component *model.ApplicationComponent) (bool, error) {
	requireStatefulSetDeletion := c != nil && c.job != nil && versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo)
	if requireStatefulSetDeletion {
		return c.requiredStatefulSetPodsGone(ctx, component)
	}
	selector := cleanupLabelSelector(component)
	if selector == "" {
		return true, nil
	}
	namespace := namespaceOrDefault(component.Namespace)
	list, err := c.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	if len(list.Items) == 0 {
		return true, nil
	}
	pendingDelete := false
	for i := range list.Items {
		pod := &list.Items[i]
		if strings.TrimSpace(pod.Name) == "" {
			continue
		}
		if strategy, protected := cleanupResourceShareProtected(pod.Labels); protected {
			klog.Infof("cleanup resources: keep shared pod %s/%s strategy=%s", namespace, pod.Name, strategy)
			continue
		}
		ownerJobs, protectedOwnerJob, err := c.componentPodOwnerJobs(ctx, namespace, pod)
		if err != nil {
			return false, err
		}
		if protectedOwnerJob != "" {
			klog.Infof("cleanup resources: keep pod %s/%s because owner job %s is shared", namespace, pod.Name, protectedOwnerJob)
			continue
		}
		if pod.DeletionTimestamp != nil {
			pendingDelete = true
			continue
		}
		for _, ownerName := range ownerJobs {
			if err := c.deleteJob(ctx, namespace, ownerName); err != nil && !k8serrors.IsNotFound(err) {
				return false, fmt.Errorf("delete component pod owner job %s/%s: %w", namespace, ownerName, err)
			}
		}
		if err := c.client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			return false, fmt.Errorf("delete component pod %s/%s: %w", namespace, pod.Name, err)
		}
		pendingDelete = true
	}
	return !pendingDelete, nil
}

func (c *CleanupResourcesJobCtl) componentPodOwnerJobs(ctx context.Context, namespace string, pod metav1.Object) ([]string, string, error) {
	if pod == nil {
		return nil, "", nil
	}
	ownerJobs := make([]string, 0, len(pod.GetOwnerReferences()))
	for _, owner := range pod.GetOwnerReferences() {
		if !strings.EqualFold(owner.Kind, string(domainspec.KubeKindJob)) || strings.TrimSpace(owner.Name) == "" {
			continue
		}
		ref, ok := newCleanupResourceRef(domainspec.ResourceJob, namespace, owner.Name, false)
		if !ok {
			continue
		}
		protected, exists, err := c.resourceDeleteProtected(ctx, ref)
		if err != nil {
			return nil, "", fmt.Errorf("inspect component pod owner job %s/%s: %w", namespace, owner.Name, err)
		}
		if protected {
			return nil, owner.Name, nil
		}
		if exists {
			ownerJobs = append(ownerJobs, owner.Name)
		}
	}
	return ownerJobs, "", nil
}

func (c *CleanupResourcesJobCtl) timeout() int64 {
	if c.job.Timeout <= 0 {
		return int64(config.DeployTimeout)
	}
	return c.job.Timeout
}

package job

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainadoption "github.com/PixelCores/Eruun/pkg/apiserver/domain/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type DeployPVCJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployPVCJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployPVCJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("NewDeployPVCJobCtl", job, client, store, ack, shareLocker)
	if !ok {
		return nil
	}
	return &DeployPVCJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployPVCJobCtl) Clean(ctx context.Context) {
	if c == nil || c.job == nil {
		return
	}
	klog.V(4).Infof("cleanup: preserving pvc resources for failed pvc deploy job %s", c.job.Name)
}

func (c *DeployPVCJobCtl) Run(ctx context.Context) error {
	return c.runWithWait(ctx, c.run, c.wait, "deploy pvc job run error", "deploy pvc wait error")
}

func (c *DeployPVCJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	pvc, err := pvcFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if pvc.Namespace == "" {
		pvc.Namespace = c.job.Namespace
	}
	if binding, adopted, sourceErr := adoptedResourceForJob(
		ctx,
		c.store,
		c.job,
		"PersistentVolumeClaim",
		pvc.Namespace,
		pvc.Name,
	); sourceErr != nil {
		return sourceErr
	} else if adopted {
		return c.reconcileAdoptedPVC(ctx, pvc, binding)
	}

	observeExistingPVC := func(ctx context.Context, current *corev1.PersistentVolumeClaim) error {
		logger.Info("pvc already exists; skipping spec update", "pvcName", current.Name)
		markResourceObserved(ctx, domainspec.ResourcePVC, pvc.Namespace, pvc.Name)
		return nil
	}

	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[corev1.PersistentVolumeClaim]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:          c.job,
			ack:          c.ack,
			labels:       pvc.Labels,
			kind:         domainspec.ResourcePVC,
			lockProvider: c.shareLocker,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("pvc marked as shared ignore; skipping", "pvcName", pvc.Name)
				} else {
					logger.Info("pvc already exists and is shared; skipping", "pvcName", pvc.Name)
				}
			},
		},
		namespace: pvc.Namespace,
		name:      pvc.Name,
		getFn: func(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
			return c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
			return c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
		},
		onExisting:      observeExistingPVC,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure pvc %q failed: %w", pvc.Name, err)
	}
	if created {
		logger.Info("PVC created successfully", "pvcName", pvc.Name)
	}

	return nil
}

func (c *DeployPVCJobCtl) reconcileAdoptedPVC(
	ctx context.Context,
	desired *corev1.PersistentVolumeClaim,
	binding *adoptedResourceBinding,
) error {
	if desired == nil || binding == nil || binding.resource == nil {
		return fmt.Errorf("adopted pvc desired resource and source binding are required")
	}
	resource := binding.resource
	if resource.Disposition == domainadoption.DispositionDataProtected {
		if resource.Ownership != domainadoption.OwnershipDataProtected &&
			resource.Ownership != domainadoption.OwnershipExclusive {
			return fmt.Errorf(
				"adopted pvc %s/%s has unsafe ownership %q",
				desired.Namespace,
				desired.Name,
				resource.Ownership,
			)
		}
	} else {
		writable, err := adoptedResourceAllowsWrite(binding)
		if err != nil {
			return err
		}
		if !writable {
			return nil
		}
	}
	cli := c.client.CoreV1().PersistentVolumeClaims(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return fmt.Errorf(
				"adopted pvc %s/%s is missing; protected data resources are never recreated",
				desired.Namespace,
				desired.Name,
			)
		}
		return fmt.Errorf("get adopted pvc %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		return err
	}
	changed, err := c.validateAdoptedPVCResize(ctx, current, desired, binding.snapshot)
	if err != nil {
		return fmt.Errorf("adopted pvc %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if !changed {
		markResourceObserved(ctx, domainspec.ResourcePVC, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
		return cli.Get(ctx, desired.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *corev1.PersistentVolumeClaim) error {
		if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
			return err
		}
		changed, err := c.validateAdoptedPVCResize(ctx, latest, desired, binding.snapshot)
		if err != nil || !changed {
			return err
		}
		requested := desired.Spec.Resources.Requests[corev1.ResourceStorage]
		candidate := latest.DeepCopy()
		if candidate.Spec.Resources.Requests == nil {
			candidate.Spec.Resources.Requests = corev1.ResourceList{}
		}
		candidate.Spec.Resources.Requests[corev1.ResourceStorage] = requested.DeepCopy()
		_, err = cli.Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("resize adopted pvc %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	markResourceObserved(ctx, domainspec.ResourcePVC, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployPVCJobCtl) validateAdoptedPVCResize(
	ctx context.Context,
	current, desired *corev1.PersistentVolumeClaim,
	snapshot *domainadoption.Snapshot,
) (bool, error) {
	if current == nil || desired == nil {
		return false, fmt.Errorf("live and desired pvc are required")
	}
	if desired.Spec.StorageClassName != nil &&
		!apiequality.Semantic.DeepEqual(current.Spec.StorageClassName, desired.Spec.StorageClassName) {
		return false, fmt.Errorf("storageClassName changes are forbidden")
	}
	if len(desired.Spec.AccessModes) > 0 &&
		!apiequality.Semantic.DeepEqual(current.Spec.AccessModes, desired.Spec.AccessModes) {
		return false, fmt.Errorf("accessModes changes are forbidden")
	}
	if desired.Spec.VolumeMode != nil &&
		!apiequality.Semantic.DeepEqual(current.Spec.VolumeMode, desired.Spec.VolumeMode) {
		return false, fmt.Errorf("volumeMode changes are forbidden")
	}
	if desired.Spec.VolumeName != "" && desired.Spec.VolumeName != current.Spec.VolumeName {
		return false, fmt.Errorf("bound volumeName changes are forbidden")
	}
	if desired.Spec.Selector != nil && !apiequality.Semantic.DeepEqual(current.Spec.Selector, desired.Spec.Selector) {
		return false, fmt.Errorf("selector changes are forbidden")
	}
	if desired.Spec.DataSource != nil && !apiequality.Semantic.DeepEqual(current.Spec.DataSource, desired.Spec.DataSource) {
		return false, fmt.Errorf("dataSource changes are forbidden")
	}
	if desired.Spec.DataSourceRef != nil && !apiequality.Semantic.DeepEqual(current.Spec.DataSourceRef, desired.Spec.DataSourceRef) {
		return false, fmt.Errorf("dataSourceRef changes are forbidden")
	}

	requested, hasRequested := desired.Spec.Resources.Requests[corev1.ResourceStorage]
	if !hasRequested {
		return false, nil
	}
	currentRequest, hasCurrentRequest := current.Spec.Resources.Requests[corev1.ResourceStorage]
	if !hasCurrentRequest {
		return false, fmt.Errorf("live pvc has no storage request")
	}
	switch requested.Cmp(currentRequest) {
	case -1:
		return false, fmt.Errorf("storage shrink from %s to %s is forbidden", currentRequest.String(), requested.String())
	case 0:
		return false, nil
	}
	if pvcBelongsToStatefulSetVCT(current, snapshot) {
		return false, fmt.Errorf("volumeClaimTemplate pvc resize is forbidden")
	}
	if current.Status.Phase != corev1.ClaimBound {
		return false, fmt.Errorf("online expansion requires a Bound pvc")
	}
	if current.Spec.StorageClassName == nil || strings.TrimSpace(*current.Spec.StorageClassName) == "" {
		return false, fmt.Errorf("online expansion requires an explicit storageClassName")
	}
	storageClassName := strings.TrimSpace(*current.Spec.StorageClassName)
	storageClass, err := c.client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("get storageclass %q: %w", storageClassName, err)
	}
	if storageClass.AllowVolumeExpansion == nil || !*storageClass.AllowVolumeExpansion {
		return false, fmt.Errorf("storageclass %q does not allow volume expansion", storageClassName)
	}
	return true, nil
}

func pvcBelongsToStatefulSetVCT(
	pvc *corev1.PersistentVolumeClaim,
	snapshot *domainadoption.Snapshot,
) bool {
	if pvc == nil {
		return false
	}
	for _, owner := range pvc.OwnerReferences {
		if strings.EqualFold(owner.Kind, "StatefulSet") {
			return true
		}
	}
	if snapshot == nil {
		return false
	}
	for _, resource := range snapshot.Resources {
		sourceNamespace := resource.Source.Namespace
		if sourceNamespace == "" {
			sourceNamespace = snapshot.Namespace
		}
		if !strings.EqualFold(resource.Source.Kind, "StatefulSet") ||
			sourceNamespace != pvc.Namespace ||
			len(resource.Manifest) == 0 {
			continue
		}
		var statefulSet appsv1.StatefulSet
		if err := json.Unmarshal(resource.Manifest, &statefulSet); err != nil {
			continue
		}
		for _, template := range statefulSet.Spec.VolumeClaimTemplates {
			prefix := template.Name + "-" + statefulSet.Name + "-"
			if !strings.HasPrefix(pvc.Name, prefix) {
				continue
			}
			ordinal := strings.TrimPrefix(pvc.Name, prefix)
			if value, err := strconv.Atoi(ordinal); err == nil && value >= 0 {
				return true
			}
		}
	}
	return false
}

func (c *DeployPVCJobCtl) wait(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	var pvcName string
	if p, ok := optionalJobInfo[*corev1.PersistentVolumeClaim](c.job); ok {
		pvcName = p.Name
	} else {
		pvcName = c.job.Name
	}

	return waitForPolledResource(ctx, pollWaitOptions{
		timeout:  time.Duration(c.timeout()) * time.Second,
		interval: 2 * time.Second,
		poll: func(ctx context.Context) (bool, error) {
			isReady, err := c.getPVCStatus(ctx)
			if err == nil && isReady {
				logger.Info("PVC is ready", "pvcName", pvcName)
			}
			return isReady, err
		},
		onCancel: func(err error) error {
			return NewStatusError(config.StatusCancelled, fmt.Errorf("pvc %s cancelled: %w", pvcName, err))
		},
		onTimeout: func() error {
			logger.Info("Timed out waiting for PVC", "pvcName", pvcName)
			return NewStatusError(config.StatusTimeout, fmt.Errorf("wait pvc %s timeout", pvcName))
		},
		onError: func(err error) error {
			logger.Error(err, "Error checking PVC status", "pvcName", pvcName)
			return fmt.Errorf("wait pvc %s error: %w", pvcName, err)
		},
	})
}

func (c *DeployPVCJobCtl) getPVCStatus(ctx context.Context) (bool, error) {
	pvcInfo, err := pvcFromJobInfo(c.job)
	if err != nil {
		return false, fmt.Errorf("failed to get PVC info from job %s: %w", c.job.Name, err)
	}
	pvcName := pvcInfo.Name

	pvc, err := c.client.CoreV1().PersistentVolumeClaims(c.job.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	// PVC创建成功且状态为Bound或Pending都认为是就绪
	return pvc.Status.Phase == corev1.ClaimBound || pvc.Status.Phase == corev1.ClaimPending, nil
}

func (c *DeployPVCJobCtl) timeout() int64 {
	if c.job.Timeout == 0 {
		c.job.Timeout = config.DeployTimeout
	}
	return c.job.Timeout
}

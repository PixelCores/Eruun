package job

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type DeployPVCJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployPVCJobCtl(job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) *DeployPVCJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("NewDeployPVCJobCtl", job, runtime, shareLocker)
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

	pvc, err := jobinfo.PvcFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if pvc.Namespace == "" {
		pvc.Namespace = c.job.Namespace
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

func (c *DeployPVCJobCtl) wait(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	var pvcName string
	if p, ok := jobinfo.OptionalJobInfo[*corev1.PersistentVolumeClaim](c.job); ok {
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
			return jobinfo.NewStatusError(config.StatusCancelled, fmt.Errorf("pvc %s cancelled: %w", pvcName, err))
		},
		onTimeout: func() error {
			logger.Info("Timed out waiting for PVC", "pvcName", pvcName)
			return jobinfo.NewStatusError(config.StatusTimeout, fmt.Errorf("wait pvc %s timeout", pvcName))
		},
		onError: func(err error) error {
			logger.Error(err, "Error checking PVC status", "pvcName", pvcName)
			return fmt.Errorf("wait pvc %s error: %w", pvcName, err)
		},
	})
}

func (c *DeployPVCJobCtl) getPVCStatus(ctx context.Context) (bool, error) {
	pvcInfo, err := jobinfo.PvcFromJobInfo(c.job)
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

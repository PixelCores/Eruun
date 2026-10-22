package jobdeploy

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
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type DeployPVCJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
}

func NewDeployPVCJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployPVCJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("NewDeployPVCJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployPVCJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployPVCJobCtl) Clean(ctx context.Context) {
	if c == nil || c.Job == nil {
		return
	}
	klog.V(4).Infof("cleanup: preserving pvc resources for failed pvc deploy job %s", c.Job.Name)
}

func (c *DeployPVCJobCtl) Run(ctx context.Context) error {
	return c.RunWithWait(ctx, c.run, c.wait, "deploy pvc job run error", "deploy pvc wait error")
}

func (c *DeployPVCJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	pvc, err := jobinfo.PvcFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	if pvc.Namespace == "" {
		pvc.Namespace = c.Job.Namespace
	}

	observeExistingPVC := func(ctx context.Context, current *corev1.PersistentVolumeClaim) error {
		logger.Info("pvc already exists; skipping spec update", "pvcName", current.Name)
		jobexec.MarkResourceObserved(ctx, domainspec.ResourcePVC, pvc.Namespace, pvc.Name)
		return nil
	}

	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[corev1.PersistentVolumeClaim]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:          c.Job,
			Ack:          c.Ack,
			Labels:       pvc.Labels,
			Kind:         domainspec.ResourcePVC,
			LockProvider: c.ShareLocker,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := c.Client.CoreV1().PersistentVolumeClaims(pvc.Namespace).List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("pvc marked as shared ignore; skipping", "pvcName", pvc.Name)
				} else {
					logger.Info("pvc already exists and is shared; skipping", "pvcName", pvc.Name)
				}
			},
		},
		Namespace: pvc.Namespace,
		Name:      pvc.Name,
		GetFn: func(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
			return c.Client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
			return c.Client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
		},
		OnExisting: observeExistingPVC,
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
	if p, ok := jobinfo.OptionalJobInfo[*corev1.PersistentVolumeClaim](c.Job); ok {
		pvcName = p.Name
	} else {
		pvcName = c.Job.Name
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
	pvcInfo, err := jobinfo.PvcFromJobInfo(c.Job)
	if err != nil {
		return false, fmt.Errorf("failed to get PVC info from job %s: %w", c.Job.Name, err)
	}
	pvcName := pvcInfo.Name

	pvc, err := c.Client.CoreV1().PersistentVolumeClaims(c.Job.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
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
	if c.Job.Timeout == 0 {
		c.Job.Timeout = config.DeployTimeout
	}
	return c.Job.Timeout
}

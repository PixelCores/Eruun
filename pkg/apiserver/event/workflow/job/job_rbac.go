package job

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// DeployServiceAccountJobCtl creates or updates a ServiceAccount resource.
type DeployServiceAccountJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployServiceAccountJobCtl(job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) *DeployServiceAccountJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployServiceAccountJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployServiceAccountJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployServiceAccountJobCtl) Clean(context.Context) {}

func (c *DeployServiceAccountJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeployServiceAccountJob run error")
}

func (c *DeployServiceAccountJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	sa, err := jobinfo.ServiceAccountFromJobInfo(c.job)
	if err != nil {
		return err
	}

	if sa.Namespace == "" {
		sa.Namespace = c.job.Namespace
	}
	ensureManagedLabel(sa, c.job.AppID)

	cli := c.client.CoreV1().ServiceAccounts(sa.Namespace)
	updateServiceAccount := func(ctx context.Context, current *corev1.ServiceAccount) error {
		if !isManagedResource(current, c.job.AppID) {
			logger.Info("serviceAccount exists but is not managed; skipping update", "namespace", sa.Namespace, "name", sa.Name)
			markResourceObserved(ctx, domainspec.ResourceServiceAccount, sa.Namespace, sa.Name)
			return nil
		}
		if shouldUpdateServiceAccount(current, sa) {
			if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.ServiceAccount, error) {
				return cli.Get(ctx, sa.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *corev1.ServiceAccount) error {
				if !isManagedResource(latest, c.job.AppID) {
					return nil
				}
				sa.ResourceVersion = latest.ResourceVersion
				_, err := cli.Update(ctx, sa, metav1.UpdateOptions{})
				return err
			}); err != nil {
				return fmt.Errorf("update serviceAccount %q failed: %w", sa.Name, err)
			}
			logger.Info("serviceAccount updated", "namespace", sa.Namespace, "name", sa.Name)
		} else {
			logger.Info("serviceAccount is up-to-date, skipping update", "namespace", sa.Namespace, "name", sa.Name)
		}
		markResourceObserved(ctx, domainspec.ResourceServiceAccount, sa.Namespace, sa.Name)
		return nil
	}
	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[corev1.ServiceAccount]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:             c.job,
			ack:             c.ack,
			labels:          sa.Labels,
			kind:            domainspec.ResourceServiceAccount,
			lockProvider:    c.shareLocker,
			reconcileShared: true,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("serviceAccount marked as shared ignore; skipping", "namespace", sa.Namespace, "name", sa.Name)
				} else {
					logger.Info("serviceAccount already exists and is shared; skipping", "namespace", sa.Namespace, "name", sa.Name)
				}
			},
		},
		namespace: sa.Namespace,
		name:      sa.Name,
		getFn: func(ctx context.Context) (*corev1.ServiceAccount, error) {
			return cli.Get(ctx, sa.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*corev1.ServiceAccount, error) {
			return cli.Create(ctx, sa, metav1.CreateOptions{})
		},
		onExisting:      updateServiceAccount,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure serviceAccount %q failed: %w", sa.Name, err)
	}
	if created {
		logger.Info("serviceAccount created", "namespace", sa.Namespace, "name", sa.Name)
	}
	return nil
}

// DeployRoleJobCtl reconciles namespace-scoped Role objects.
type DeployRoleJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployRoleJobCtl(job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) *DeployRoleJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployRoleJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployRoleJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployRoleJobCtl) Clean(context.Context) {}

func (c *DeployRoleJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeployRoleJob run error")
}

func (c *DeployRoleJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	role, err := jobinfo.RoleFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if role.Namespace == "" {
		role.Namespace = c.job.Namespace
	}
	ensureManagedLabel(role, c.job.AppID)

	cli := c.client.RbacV1().Roles(role.Namespace)
	updateRole := func(ctx context.Context, current *rbacv1.Role) error {
		if !isManagedResource(current, c.job.AppID) {
			logger.Info("role exists but is not managed; skipping update", "namespace", role.Namespace, "name", role.Name)
			markResourceObserved(ctx, domainspec.ResourceRole, role.Namespace, role.Name)
			return nil
		}
		if shouldUpdateRole(current, role) {
			if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.Role, error) {
				return cli.Get(ctx, role.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *rbacv1.Role) error {
				if !isManagedResource(latest, c.job.AppID) {
					return nil
				}
				role.ResourceVersion = latest.ResourceVersion
				_, err := cli.Update(ctx, role, metav1.UpdateOptions{})
				return err
			}); err != nil {
				return fmt.Errorf("update role %q failed: %w", role.Name, err)
			}
			logger.Info("role updated", "namespace", role.Namespace, "name", role.Name)
		} else {
			logger.Info("role is up-to-date, skipping update", "namespace", role.Namespace, "name", role.Name)
		}
		markResourceObserved(ctx, domainspec.ResourceRole, role.Namespace, role.Name)
		return nil
	}
	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[rbacv1.Role]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:             c.job,
			ack:             c.ack,
			labels:          role.Labels,
			kind:            domainspec.ResourceRole,
			lockProvider:    c.shareLocker,
			reconcileShared: true,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("role marked as shared ignore; skipping", "namespace", role.Namespace, "name", role.Name)
				} else {
					logger.Info("role already exists and is shared; skipping", "namespace", role.Namespace, "name", role.Name)
				}
			},
		},
		namespace:       role.Namespace,
		name:            role.Name,
		getFn:           func(ctx context.Context) (*rbacv1.Role, error) { return cli.Get(ctx, role.Name, metav1.GetOptions{}) },
		createFn:        func(ctx context.Context) (*rbacv1.Role, error) { return cli.Create(ctx, role, metav1.CreateOptions{}) },
		onExisting:      updateRole,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure role %q failed: %w", role.Name, err)
	}
	if created {
		logger.Info("role created", "namespace", role.Namespace, "name", role.Name)
	}
	return nil
}

// DeployRoleBindingJobCtl reconciles RoleBinding objects.
type DeployRoleBindingJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployRoleBindingJobCtl(job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) *DeployRoleBindingJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployRoleBindingJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployRoleBindingJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployRoleBindingJobCtl) Clean(context.Context) {}

func (c *DeployRoleBindingJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeployRoleBindingJob run error")
}

func (c *DeployRoleBindingJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	binding, err := jobinfo.RoleBindingFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if binding.Namespace == "" {
		binding.Namespace = c.job.Namespace
	}
	ensureManagedLabel(binding, c.job.AppID)

	cli := c.client.RbacV1().RoleBindings(binding.Namespace)
	updateBinding := func(ctx context.Context, current *rbacv1.RoleBinding) error {
		if !isManagedResource(current, c.job.AppID) {
			logger.Info("roleBinding exists but is not managed; skipping update", "namespace", binding.Namespace, "name", binding.Name)
			markResourceObserved(ctx, domainspec.ResourceRoleBinding, binding.Namespace, binding.Name)
			return nil
		}
		if shouldUpdateRoleBinding(current, binding) {
			if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.RoleBinding, error) {
				return cli.Get(ctx, binding.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *rbacv1.RoleBinding) error {
				if !isManagedResource(latest, c.job.AppID) {
					return nil
				}
				binding.ResourceVersion = latest.ResourceVersion
				_, err := cli.Update(ctx, binding, metav1.UpdateOptions{})
				return err
			}); err != nil {
				return fmt.Errorf("update roleBinding %q failed: %w", binding.Name, err)
			}
			logger.Info("roleBinding updated", "namespace", binding.Namespace, "name", binding.Name)
		} else {
			logger.Info("roleBinding is up-to-date, skipping update", "namespace", binding.Namespace, "name", binding.Name)
		}
		markResourceObserved(ctx, domainspec.ResourceRoleBinding, binding.Namespace, binding.Name)
		return nil
	}
	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[rbacv1.RoleBinding]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:             c.job,
			ack:             c.ack,
			labels:          binding.Labels,
			kind:            domainspec.ResourceRoleBinding,
			lockProvider:    c.shareLocker,
			reconcileShared: true,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("roleBinding marked as shared ignore; skipping", "namespace", binding.Namespace, "name", binding.Name)
				} else {
					logger.Info("roleBinding already exists and is shared; skipping", "namespace", binding.Namespace, "name", binding.Name)
				}
			},
		},
		namespace: binding.Namespace,
		name:      binding.Name,
		getFn: func(ctx context.Context) (*rbacv1.RoleBinding, error) {
			return cli.Get(ctx, binding.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*rbacv1.RoleBinding, error) {
			return cli.Create(ctx, binding, metav1.CreateOptions{})
		},
		onExisting:      updateBinding,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure roleBinding %q failed: %w", binding.Name, err)
	}
	if created {
		logger.Info("roleBinding created", "namespace", binding.Namespace, "name", binding.Name)
	}
	return nil
}

// DeployClusterRoleJobCtl reconciles ClusterRole objects.
type DeployClusterRoleJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployClusterRoleJobCtl(job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) *DeployClusterRoleJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployClusterRoleJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployClusterRoleJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployClusterRoleJobCtl) Clean(context.Context) {}

func (c *DeployClusterRoleJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeployClusterRoleJob run error")
}

func (c *DeployClusterRoleJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	role, err := jobinfo.ClusterRoleFromJobInfo(c.job)
	if err != nil {
		return err
	}
	ensureManagedLabel(role, c.job.AppID)

	cli := c.client.RbacV1().ClusterRoles()
	updateRole := func(ctx context.Context, current *rbacv1.ClusterRole) error {
		if !isManagedResource(current, c.job.AppID) {
			logger.Info("clusterRole exists but is not managed; skipping update", "name", role.Name)
			markResourceObserved(ctx, domainspec.ResourceClusterRole, "", role.Name)
			return nil
		}
		if shouldUpdateClusterRole(current, role) {
			if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.ClusterRole, error) {
				return cli.Get(ctx, role.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *rbacv1.ClusterRole) error {
				if !isManagedResource(latest, c.job.AppID) {
					return nil
				}
				role.ResourceVersion = latest.ResourceVersion
				_, err := cli.Update(ctx, role, metav1.UpdateOptions{})
				return err
			}); err != nil {
				return fmt.Errorf("update clusterRole %q failed: %w", role.Name, err)
			}
			logger.Info("clusterRole updated", "name", role.Name)
		} else {
			logger.Info("clusterRole is up-to-date, skipping update", "name", role.Name)
		}
		markResourceObserved(ctx, domainspec.ResourceClusterRole, "", role.Name)
		return nil
	}
	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[rbacv1.ClusterRole]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:             c.job,
			ack:             c.ack,
			labels:          role.Labels,
			kind:            domainspec.ResourceClusterRole,
			lockProvider:    c.shareLocker,
			reconcileShared: true,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("clusterRole marked as shared ignore; skipping", "name", role.Name)
				} else {
					logger.Info("clusterRole already exists and is shared; skipping", "name", role.Name)
				}
			},
		},
		namespace: "",
		name:      role.Name,
		getFn: func(ctx context.Context) (*rbacv1.ClusterRole, error) {
			return cli.Get(ctx, role.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*rbacv1.ClusterRole, error) {
			return cli.Create(ctx, role, metav1.CreateOptions{})
		},
		onExisting:      updateRole,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure clusterRole %q failed: %w", role.Name, err)
	}
	if created {
		logger.Info("clusterRole created", "name", role.Name)
	}
	return nil
}

// DeployClusterRoleBindingJobCtl reconciles ClusterRoleBinding objects.
type DeployClusterRoleBindingJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployClusterRoleBindingJobCtl(job *model.JobTask, runtime *Runtime, shareLocker locker.Locker) *DeployClusterRoleBindingJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployClusterRoleBindingJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployClusterRoleBindingJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployClusterRoleBindingJobCtl) Clean(context.Context) {}

func (c *DeployClusterRoleBindingJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeployClusterRoleBindingJob run error")
}

func (c *DeployClusterRoleBindingJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	binding, err := jobinfo.ClusterRoleBindingFromJobInfo(c.job)
	if err != nil {
		return err
	}
	ensureManagedLabel(binding, c.job.AppID)

	cli := c.client.RbacV1().ClusterRoleBindings()
	updateBinding := func(ctx context.Context, current *rbacv1.ClusterRoleBinding) error {
		if !isManagedResource(current, c.job.AppID) {
			logger.Info("clusterRoleBinding exists but is not managed; skipping update", "name", binding.Name)
			markResourceObserved(ctx, domainspec.ResourceClusterRoleBinding, "", binding.Name)
			return nil
		}
		if shouldUpdateClusterRoleBinding(current, binding) {
			if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.ClusterRoleBinding, error) {
				return cli.Get(ctx, binding.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *rbacv1.ClusterRoleBinding) error {
				if !isManagedResource(latest, c.job.AppID) {
					return nil
				}
				binding.ResourceVersion = latest.ResourceVersion
				_, err := cli.Update(ctx, binding, metav1.UpdateOptions{})
				return err
			}); err != nil {
				return fmt.Errorf("update clusterRoleBinding %q failed: %w", binding.Name, err)
			}
			logger.Info("clusterRoleBinding updated", "name", binding.Name)
		} else {
			logger.Info("clusterRoleBinding is up-to-date, skipping update", "name", binding.Name)
		}
		markResourceObserved(ctx, domainspec.ResourceClusterRoleBinding, "", binding.Name)
		return nil
	}
	_, created, err := reconcileTrackedResource(ctx, trackedResourceReconcileOptions[rbacv1.ClusterRoleBinding]{
		sharedResourceAccessOptions: sharedResourceAccessOptions{
			job:             c.job,
			ack:             c.ack,
			labels:          binding.Labels,
			kind:            domainspec.ResourceClusterRoleBinding,
			lockProvider:    c.shareLocker,
			reconcileShared: true,
			listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			logSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("clusterRoleBinding marked as shared ignore; skipping", "name", binding.Name)
				} else {
					logger.Info("clusterRoleBinding already exists and is shared; skipping", "name", binding.Name)
				}
			},
		},
		namespace: "",
		name:      binding.Name,
		getFn: func(ctx context.Context) (*rbacv1.ClusterRoleBinding, error) {
			return cli.Get(ctx, binding.Name, metav1.GetOptions{})
		},
		createFn: func(ctx context.Context) (*rbacv1.ClusterRoleBinding, error) {
			return cli.Create(ctx, binding, metav1.CreateOptions{})
		},
		onExisting:      updateBinding,
		isNotFound:      k8serrors.IsNotFound,
		isAlreadyExists: k8serrors.IsAlreadyExists,
	})
	if err != nil {
		return fmt.Errorf("ensure clusterRoleBinding %q failed: %w", binding.Name, err)
	}
	if created {
		logger.Info("clusterRoleBinding created", "name", binding.Name)
	}
	return nil
}

func shouldUpdateServiceAccount(existing, desired *corev1.ServiceAccount) bool {
	if !apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(existing.Annotations, desired.Annotations) {
		return true
	}
	return !apiequality.Semantic.DeepEqual(existing.AutomountServiceAccountToken, desired.AutomountServiceAccountToken)
}

func shouldUpdateRole(existing, desired *rbacv1.Role) bool {
	if !apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(existing.Annotations, desired.Annotations) {
		return true
	}
	return !apiequality.Semantic.DeepEqual(existing.Rules, desired.Rules)
}

func shouldUpdateRoleBinding(existing, desired *rbacv1.RoleBinding) bool {
	if !apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(existing.Annotations, desired.Annotations) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(existing.Subjects, desired.Subjects) {
		return true
	}
	return !apiequality.Semantic.DeepEqual(existing.RoleRef, desired.RoleRef)
}

func shouldUpdateClusterRole(existing, desired *rbacv1.ClusterRole) bool {
	if !apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(existing.Annotations, desired.Annotations) {
		return true
	}
	return !apiequality.Semantic.DeepEqual(existing.Rules, desired.Rules)
}

func shouldUpdateClusterRoleBinding(existing, desired *rbacv1.ClusterRoleBinding) bool {
	if !apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(existing.Annotations, desired.Annotations) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(existing.Subjects, desired.Subjects) {
		return true
	}
	return !apiequality.Semantic.DeepEqual(existing.RoleRef, desired.RoleRef)
}

func ensureManagedLabel(obj metav1.Object, appID string) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string, 2)
	}
	labels[config.LabelManagedBy] = config.ManagedByEruun
	labels[config.LabelAppID] = strings.TrimSpace(appID)
	obj.SetLabels(labels)
}

func isManagedResource(obj metav1.Object, expected string) bool {
	labels := obj.GetLabels()
	if labels == nil {
		return false
	}
	return labels[config.LabelManagedBy] == config.ManagedByEruun &&
		labels[config.LabelAppID] == strings.TrimSpace(expected)
}

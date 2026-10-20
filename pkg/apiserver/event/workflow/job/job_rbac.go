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
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// DeployServiceAccountJobCtl creates or updates a ServiceAccount resource.
type DeployServiceAccountJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
}

func NewDeployServiceAccountJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployServiceAccountJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployServiceAccountJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployServiceAccountJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployServiceAccountJobCtl) Clean(context.Context) {}

func (c *DeployServiceAccountJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "DeployServiceAccountJob run error")
}

func (c *DeployServiceAccountJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	sa, err := jobinfo.ServiceAccountFromJobInfo(c.Job)
	if err != nil {
		return err
	}

	if sa.Namespace == "" {
		sa.Namespace = c.Job.Namespace
	}
	ensureManagedLabel(sa, c.Job.AppID)

	cli := c.Client.CoreV1().ServiceAccounts(sa.Namespace)
	updateServiceAccount := func(ctx context.Context, current *corev1.ServiceAccount) error {
		if !isManagedResource(current, c.Job.AppID) {
			logger.Info("serviceAccount exists but is not managed; skipping update", "namespace", sa.Namespace, "name", sa.Name)
			jobexec.MarkResourceObserved(ctx, domainspec.ResourceServiceAccount, sa.Namespace, sa.Name)
			return nil
		}
		if shouldUpdateServiceAccount(current, sa) {
			if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.ServiceAccount, error) {
				return cli.Get(ctx, sa.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *corev1.ServiceAccount) error {
				if !isManagedResource(latest, c.Job.AppID) {
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
		jobexec.MarkResourceObserved(ctx, domainspec.ResourceServiceAccount, sa.Namespace, sa.Name)
		return nil
	}
	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[corev1.ServiceAccount]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:             c.Job,
			Ack:             c.Ack,
			Labels:          sa.Labels,
			Kind:            domainspec.ResourceServiceAccount,
			LockProvider:    c.ShareLocker,
			ReconcileShared: true,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("serviceAccount marked as shared ignore; skipping", "namespace", sa.Namespace, "name", sa.Name)
				} else {
					logger.Info("serviceAccount already exists and is shared; skipping", "namespace", sa.Namespace, "name", sa.Name)
				}
			},
		},
		Namespace: sa.Namespace,
		Name:      sa.Name,
		GetFn: func(ctx context.Context) (*corev1.ServiceAccount, error) {
			return cli.Get(ctx, sa.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*corev1.ServiceAccount, error) {
			return cli.Create(ctx, sa, metav1.CreateOptions{})
		},
		OnExisting:      updateServiceAccount,
		IsNotFound:      k8serrors.IsNotFound,
		IsAlreadyExists: k8serrors.IsAlreadyExists,
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
	jobexec.DeployNamespacedResourceJobBase
}

func NewDeployRoleJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployRoleJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployRoleJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployRoleJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployRoleJobCtl) Clean(context.Context) {}

func (c *DeployRoleJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "DeployRoleJob run error")
}

func (c *DeployRoleJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	role, err := jobinfo.RoleFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	if role.Namespace == "" {
		role.Namespace = c.Job.Namespace
	}
	ensureManagedLabel(role, c.Job.AppID)

	cli := c.Client.RbacV1().Roles(role.Namespace)
	updateRole := func(ctx context.Context, current *rbacv1.Role) error {
		if !isManagedResource(current, c.Job.AppID) {
			logger.Info("role exists but is not managed; skipping update", "namespace", role.Namespace, "name", role.Name)
			jobexec.MarkResourceObserved(ctx, domainspec.ResourceRole, role.Namespace, role.Name)
			return nil
		}
		if shouldUpdateRole(current, role) {
			if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.Role, error) {
				return cli.Get(ctx, role.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *rbacv1.Role) error {
				if !isManagedResource(latest, c.Job.AppID) {
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
		jobexec.MarkResourceObserved(ctx, domainspec.ResourceRole, role.Namespace, role.Name)
		return nil
	}
	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[rbacv1.Role]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:             c.Job,
			Ack:             c.Ack,
			Labels:          role.Labels,
			Kind:            domainspec.ResourceRole,
			LockProvider:    c.ShareLocker,
			ReconcileShared: true,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("role marked as shared ignore; skipping", "namespace", role.Namespace, "name", role.Name)
				} else {
					logger.Info("role already exists and is shared; skipping", "namespace", role.Namespace, "name", role.Name)
				}
			},
		},
		Namespace:       role.Namespace,
		Name:            role.Name,
		GetFn:           func(ctx context.Context) (*rbacv1.Role, error) { return cli.Get(ctx, role.Name, metav1.GetOptions{}) },
		CreateFn:        func(ctx context.Context) (*rbacv1.Role, error) { return cli.Create(ctx, role, metav1.CreateOptions{}) },
		OnExisting:      updateRole,
		IsNotFound:      k8serrors.IsNotFound,
		IsAlreadyExists: k8serrors.IsAlreadyExists,
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
	jobexec.DeployNamespacedResourceJobBase
}

func NewDeployRoleBindingJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployRoleBindingJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployRoleBindingJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployRoleBindingJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployRoleBindingJobCtl) Clean(context.Context) {}

func (c *DeployRoleBindingJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "DeployRoleBindingJob run error")
}

func (c *DeployRoleBindingJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	binding, err := jobinfo.RoleBindingFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	if binding.Namespace == "" {
		binding.Namespace = c.Job.Namespace
	}
	ensureManagedLabel(binding, c.Job.AppID)

	cli := c.Client.RbacV1().RoleBindings(binding.Namespace)
	updateBinding := func(ctx context.Context, current *rbacv1.RoleBinding) error {
		if !isManagedResource(current, c.Job.AppID) {
			logger.Info("roleBinding exists but is not managed; skipping update", "namespace", binding.Namespace, "name", binding.Name)
			jobexec.MarkResourceObserved(ctx, domainspec.ResourceRoleBinding, binding.Namespace, binding.Name)
			return nil
		}
		if shouldUpdateRoleBinding(current, binding) {
			if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.RoleBinding, error) {
				return cli.Get(ctx, binding.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *rbacv1.RoleBinding) error {
				if !isManagedResource(latest, c.Job.AppID) {
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
		jobexec.MarkResourceObserved(ctx, domainspec.ResourceRoleBinding, binding.Namespace, binding.Name)
		return nil
	}
	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[rbacv1.RoleBinding]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:             c.Job,
			Ack:             c.Ack,
			Labels:          binding.Labels,
			Kind:            domainspec.ResourceRoleBinding,
			LockProvider:    c.ShareLocker,
			ReconcileShared: true,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("roleBinding marked as shared ignore; skipping", "namespace", binding.Namespace, "name", binding.Name)
				} else {
					logger.Info("roleBinding already exists and is shared; skipping", "namespace", binding.Namespace, "name", binding.Name)
				}
			},
		},
		Namespace: binding.Namespace,
		Name:      binding.Name,
		GetFn: func(ctx context.Context) (*rbacv1.RoleBinding, error) {
			return cli.Get(ctx, binding.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*rbacv1.RoleBinding, error) {
			return cli.Create(ctx, binding, metav1.CreateOptions{})
		},
		OnExisting:      updateBinding,
		IsNotFound:      k8serrors.IsNotFound,
		IsAlreadyExists: k8serrors.IsAlreadyExists,
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
	jobexec.DeployNamespacedResourceJobBase
}

func NewDeployClusterRoleJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployClusterRoleJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployClusterRoleJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployClusterRoleJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployClusterRoleJobCtl) Clean(context.Context) {}

func (c *DeployClusterRoleJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "DeployClusterRoleJob run error")
}

func (c *DeployClusterRoleJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	role, err := jobinfo.ClusterRoleFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	ensureManagedLabel(role, c.Job.AppID)

	cli := c.Client.RbacV1().ClusterRoles()
	updateRole := func(ctx context.Context, current *rbacv1.ClusterRole) error {
		if !isManagedResource(current, c.Job.AppID) {
			logger.Info("clusterRole exists but is not managed; skipping update", "name", role.Name)
			jobexec.MarkResourceObserved(ctx, domainspec.ResourceClusterRole, "", role.Name)
			return nil
		}
		if shouldUpdateClusterRole(current, role) {
			if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.ClusterRole, error) {
				return cli.Get(ctx, role.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *rbacv1.ClusterRole) error {
				if !isManagedResource(latest, c.Job.AppID) {
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
		jobexec.MarkResourceObserved(ctx, domainspec.ResourceClusterRole, "", role.Name)
		return nil
	}
	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[rbacv1.ClusterRole]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:             c.Job,
			Ack:             c.Ack,
			Labels:          role.Labels,
			Kind:            domainspec.ResourceClusterRole,
			LockProvider:    c.ShareLocker,
			ReconcileShared: true,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("clusterRole marked as shared ignore; skipping", "name", role.Name)
				} else {
					logger.Info("clusterRole already exists and is shared; skipping", "name", role.Name)
				}
			},
		},
		Namespace: "",
		Name:      role.Name,
		GetFn: func(ctx context.Context) (*rbacv1.ClusterRole, error) {
			return cli.Get(ctx, role.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*rbacv1.ClusterRole, error) {
			return cli.Create(ctx, role, metav1.CreateOptions{})
		},
		OnExisting:      updateRole,
		IsNotFound:      k8serrors.IsNotFound,
		IsAlreadyExists: k8serrors.IsAlreadyExists,
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
	jobexec.DeployNamespacedResourceJobBase
}

func NewDeployClusterRoleBindingJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployClusterRoleBindingJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DeployClusterRoleBindingJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployClusterRoleBindingJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployClusterRoleBindingJobCtl) Clean(context.Context) {}

func (c *DeployClusterRoleBindingJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "DeployClusterRoleBindingJob run error")
}

func (c *DeployClusterRoleBindingJobCtl) run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	binding, err := jobinfo.ClusterRoleBindingFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	ensureManagedLabel(binding, c.Job.AppID)

	cli := c.Client.RbacV1().ClusterRoleBindings()
	updateBinding := func(ctx context.Context, current *rbacv1.ClusterRoleBinding) error {
		if !isManagedResource(current, c.Job.AppID) {
			logger.Info("clusterRoleBinding exists but is not managed; skipping update", "name", binding.Name)
			jobexec.MarkResourceObserved(ctx, domainspec.ResourceClusterRoleBinding, "", binding.Name)
			return nil
		}
		if shouldUpdateClusterRoleBinding(current, binding) {
			if err := jobexec.UpdateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.ClusterRoleBinding, error) {
				return cli.Get(ctx, binding.Name, metav1.GetOptions{})
			}, func(ctx context.Context, latest *rbacv1.ClusterRoleBinding) error {
				if !isManagedResource(latest, c.Job.AppID) {
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
		jobexec.MarkResourceObserved(ctx, domainspec.ResourceClusterRoleBinding, "", binding.Name)
		return nil
	}
	_, created, err := jobexec.ReconcileTrackedResource(ctx, jobexec.TrackedResourceReconcileOptions[rbacv1.ClusterRoleBinding]{
		SharedResourceAccessOptions: jobexec.SharedResourceAccessOptions{
			Job:             c.Job,
			Ack:             c.Ack,
			Labels:          binding.Labels,
			Kind:            domainspec.ResourceClusterRoleBinding,
			LockProvider:    c.ShareLocker,
			ReconcileShared: true,
			ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
				list, err := cli.List(ctx, opts)
				if err != nil {
					return 0, err
				}
				return len(list.Items), nil
			},
			LogSkip: func(strategy domainspec.ShareStrategy) {
				if strategy == domainspec.ShareStrategyIgnore {
					logger.Info("clusterRoleBinding marked as shared ignore; skipping", "name", binding.Name)
				} else {
					logger.Info("clusterRoleBinding already exists and is shared; skipping", "name", binding.Name)
				}
			},
		},
		Namespace: "",
		Name:      binding.Name,
		GetFn: func(ctx context.Context) (*rbacv1.ClusterRoleBinding, error) {
			return cli.Get(ctx, binding.Name, metav1.GetOptions{})
		},
		CreateFn: func(ctx context.Context) (*rbacv1.ClusterRoleBinding, error) {
			return cli.Create(ctx, binding, metav1.CreateOptions{})
		},
		OnExisting:      updateBinding,
		IsNotFound:      k8serrors.IsNotFound,
		IsAlreadyExists: k8serrors.IsAlreadyExists,
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

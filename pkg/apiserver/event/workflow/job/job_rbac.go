package job

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// DeployServiceAccountJobCtl creates or updates a ServiceAccount resource.
type DeployServiceAccountJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployServiceAccountJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployServiceAccountJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployServiceAccountJobCtl", job, client, store, ack, shareLocker)
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

	sa, err := serviceAccountFromJobInfo(c.job)
	if err != nil {
		return err
	}

	if sa.Namespace == "" {
		sa.Namespace = c.job.Namespace
	}
	binding, adopted, err := adoptedResourceForJob(ctx, c.store, c.job, "ServiceAccount", sa.Namespace, sa.Name)
	if err != nil {
		return err
	}
	if adopted {
		return c.reconcileAdoptedServiceAccount(ctx, sa, binding)
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

func (c *DeployServiceAccountJobCtl) wait(ctx context.Context) {}

// DeployRoleJobCtl reconciles namespace-scoped Role objects.
type DeployRoleJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployRoleJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployRoleJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployRoleJobCtl", job, client, store, ack, shareLocker)
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

	role, err := roleFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if role.Namespace == "" {
		role.Namespace = c.job.Namespace
	}
	binding, adopted, err := adoptedResourceForJob(ctx, c.store, c.job, "Role", role.Namespace, role.Name)
	if err != nil {
		return err
	}
	if adopted {
		return c.reconcileAdoptedRole(ctx, role, binding)
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

func NewDeployRoleBindingJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployRoleBindingJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployRoleBindingJobCtl", job, client, store, ack, shareLocker)
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

	binding, err := roleBindingFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if binding.Namespace == "" {
		binding.Namespace = c.job.Namespace
	}
	sourceBinding, adopted, err := adoptedResourceForJob(ctx, c.store, c.job, "RoleBinding", binding.Namespace, binding.Name)
	if err != nil {
		return err
	}
	if adopted {
		return c.reconcileAdoptedRoleBinding(ctx, binding, sourceBinding)
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

func NewDeployClusterRoleJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployClusterRoleJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployClusterRoleJobCtl", job, client, store, ack, shareLocker)
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

	role, err := clusterRoleFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if _, _, adopted, err := adoptedApplicationForJob(ctx, c.store, c.job); err != nil {
		return err
	} else if adopted {
		return nil
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

func NewDeployClusterRoleBindingJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployClusterRoleBindingJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DeployClusterRoleBindingJobCtl", job, client, store, ack, shareLocker)
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

	binding, err := clusterRoleBindingFromJobInfo(c.job)
	if err != nil {
		return err
	}
	if _, _, adopted, err := adoptedApplicationForJob(ctx, c.store, c.job); err != nil {
		return err
	} else if adopted {
		return nil
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

func (c *DeployServiceAccountJobCtl) reconcileAdoptedServiceAccount(
	ctx context.Context,
	desired *corev1.ServiceAccount,
	binding *adoptedResourceBinding,
) error {
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return err
	}
	if !writable {
		return nil
	}
	cli := c.client.CoreV1().ServiceAccounts(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedServiceAccount(ctx, desired, binding)
		}
		return fmt.Errorf("get adopted serviceAccount %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		recovered, recoverErr := recoverPendingAdoptedDependency(
			ctx,
			c.store,
			binding,
			current,
			current,
			c.runtime,
			c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted serviceAccount binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	candidate := adoptedServiceAccountForExistingUpdate(current, desired)
	if adoptedServiceAccountEqual(current, candidate) {
		markResourceObserved(ctx, domainspec.ResourceServiceAccount, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.ServiceAccount, error) {
		return cli.Get(ctx, desired.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *corev1.ServiceAccount) error {
		if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
			return err
		}
		candidate := adoptedServiceAccountForExistingUpdate(latest, desired)
		if adoptedServiceAccountEqual(latest, candidate) {
			return nil
		}
		_, err := cli.Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("update adopted serviceAccount %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	markResourceObserved(ctx, domainspec.ResourceServiceAccount, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployServiceAccountJobCtl) recreateAdoptedServiceAccount(
	ctx context.Context,
	desired *corev1.ServiceAccount,
	binding *adoptedResourceBinding,
) error {
	recreation, err := prepareAdoptedDependencyRecreation(c.store, binding)
	if err != nil {
		return fmt.Errorf("prepare adopted serviceAccount recreation: %w", err)
	}
	var baseline corev1.ServiceAccount
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted serviceAccount recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted serviceAccount recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	candidate := adoptedServiceAccountForExistingUpdate(&baseline, desired)
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted serviceAccount recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	cli := c.client.CoreV1().ServiceAccounts(candidate.Namespace)
	created, err := cli.Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := cli.Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted serviceAccount %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
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
				return fmt.Errorf("recover recreated adopted serviceAccount binding: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedServiceAccount(ctx, desired, &recreation.adoptedResourceBinding)
			}
			return fmt.Errorf(
				"adopted serviceAccount ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf("recreate adopted serviceAccount %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	if err := persistCreatedAdoptedDependency(
		recreationCtx,
		recreation,
		created,
		created,
		c.runtime,
	); err != nil {
		return err
	}
	markResourceObserved(ctx, domainspec.ResourceServiceAccount, created.Namespace, created.Name)
	return nil
}

func adoptedServiceAccountForExistingUpdate(current, desired *corev1.ServiceAccount) *corev1.ServiceAccount {
	updated := current.DeepCopy()
	if desired == nil {
		return updated
	}
	updated.Labels = adoptedOverlayStringMap(current.Labels, desired.Labels)
	updated.Annotations = adoptedOverlayStringMap(current.Annotations, desired.Annotations)
	if desired.AutomountServiceAccountToken != nil {
		value := *desired.AutomountServiceAccountToken
		updated.AutomountServiceAccountToken = &value
	}
	if desired.ImagePullSecrets != nil {
		updated.ImagePullSecrets = mergeAdoptedLocalObjectReferences(current.ImagePullSecrets, desired.ImagePullSecrets)
	}
	return updated
}

func mergeAdoptedLocalObjectReferences(
	current, desired []corev1.LocalObjectReference,
) []corev1.LocalObjectReference {
	merged := append([]corev1.LocalObjectReference(nil), current...)
	for _, desiredReference := range desired {
		index := -1
		for currentIndex := range merged {
			if merged[currentIndex].Name == desiredReference.Name {
				index = currentIndex
				break
			}
		}
		if index < 0 {
			merged = append(merged, desiredReference)
			continue
		}
		merged[index] = desiredReference
	}
	return merged
}

func adoptedServiceAccountEqual(current, candidate *corev1.ServiceAccount) bool {
	if current == nil || candidate == nil {
		return current == candidate
	}
	return apiequality.Semantic.DeepEqual(current.Labels, candidate.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, candidate.Annotations) &&
		apiequality.Semantic.DeepEqual(current.AutomountServiceAccountToken, candidate.AutomountServiceAccountToken) &&
		apiequality.Semantic.DeepEqual(current.ImagePullSecrets, candidate.ImagePullSecrets)
}

func (c *DeployRoleJobCtl) reconcileAdoptedRole(
	ctx context.Context,
	desired *rbacv1.Role,
	binding *adoptedResourceBinding,
) error {
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return err
	}
	if !writable {
		return nil
	}
	cli := c.client.RbacV1().Roles(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedRole(ctx, desired, binding)
		}
		return fmt.Errorf("get adopted role %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		recovered, recoverErr := recoverPendingAdoptedDependency(
			ctx,
			c.store,
			binding,
			current,
			current,
			c.runtime,
			c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted role binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	candidate := adoptedRoleForExistingUpdate(current, desired)
	if adoptedRoleEqual(current, candidate) {
		markResourceObserved(ctx, domainspec.ResourceRole, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.Role, error) {
		return cli.Get(ctx, desired.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *rbacv1.Role) error {
		if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
			return err
		}
		candidate := adoptedRoleForExistingUpdate(latest, desired)
		if adoptedRoleEqual(latest, candidate) {
			return nil
		}
		_, err := cli.Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("update adopted role %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	markResourceObserved(ctx, domainspec.ResourceRole, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployRoleJobCtl) recreateAdoptedRole(
	ctx context.Context,
	desired *rbacv1.Role,
	binding *adoptedResourceBinding,
) error {
	recreation, err := prepareAdoptedDependencyRecreation(c.store, binding)
	if err != nil {
		return fmt.Errorf("prepare adopted role recreation: %w", err)
	}
	var baseline rbacv1.Role
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted role recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted role recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	candidate := adoptedRoleForExistingUpdate(&baseline, desired)
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted role recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	cli := c.client.RbacV1().Roles(candidate.Namespace)
	created, err := cli.Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := cli.Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted role %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
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
				return fmt.Errorf("recover recreated adopted role binding: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedRole(ctx, desired, &recreation.adoptedResourceBinding)
			}
			return fmt.Errorf(
				"adopted role ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf("recreate adopted role %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	if err := persistCreatedAdoptedDependency(
		recreationCtx,
		recreation,
		created,
		created,
		c.runtime,
	); err != nil {
		return err
	}
	markResourceObserved(ctx, domainspec.ResourceRole, created.Namespace, created.Name)
	return nil
}

func adoptedRoleForExistingUpdate(current, desired *rbacv1.Role) *rbacv1.Role {
	updated := current.DeepCopy()
	if desired == nil {
		return updated
	}
	updated.Labels = adoptedOverlayStringMap(current.Labels, desired.Labels)
	updated.Annotations = adoptedOverlayStringMap(current.Annotations, desired.Annotations)
	if desired.Rules != nil {
		updated.Rules = append([]rbacv1.PolicyRule(nil), desired.Rules...)
	}
	return updated
}

func adoptedRoleEqual(current, candidate *rbacv1.Role) bool {
	if current == nil || candidate == nil {
		return current == candidate
	}
	return apiequality.Semantic.DeepEqual(current.Labels, candidate.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, candidate.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Rules, candidate.Rules)
}

func (c *DeployRoleBindingJobCtl) reconcileAdoptedRoleBinding(
	ctx context.Context,
	desired *rbacv1.RoleBinding,
	binding *adoptedResourceBinding,
) error {
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return err
	}
	if !writable {
		return nil
	}
	cli := c.client.RbacV1().RoleBindings(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedRoleBinding(ctx, desired, binding)
		}
		return fmt.Errorf("get adopted roleBinding %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		recovered, recoverErr := recoverPendingAdoptedDependency(
			ctx,
			c.store,
			binding,
			current,
			current,
			c.runtime,
			c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted roleBinding binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	candidate, err := adoptedRoleBindingForExistingUpdate(current, desired)
	if err != nil {
		return err
	}
	if adoptedRoleBindingEqual(current, candidate) {
		markResourceObserved(ctx, domainspec.ResourceRoleBinding, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*rbacv1.RoleBinding, error) {
		return cli.Get(ctx, desired.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *rbacv1.RoleBinding) error {
		if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
			return err
		}
		candidate, err := adoptedRoleBindingForExistingUpdate(latest, desired)
		if err != nil || adoptedRoleBindingEqual(latest, candidate) {
			return err
		}
		_, err = cli.Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("update adopted roleBinding %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	markResourceObserved(ctx, domainspec.ResourceRoleBinding, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployRoleBindingJobCtl) recreateAdoptedRoleBinding(
	ctx context.Context,
	desired *rbacv1.RoleBinding,
	binding *adoptedResourceBinding,
) error {
	recreation, err := prepareAdoptedDependencyRecreation(c.store, binding)
	if err != nil {
		return fmt.Errorf("prepare adopted roleBinding recreation: %w", err)
	}
	var baseline rbacv1.RoleBinding
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted roleBinding recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted roleBinding recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	candidate, err := adoptedRoleBindingForExistingUpdate(&baseline, desired)
	if err != nil {
		return fmt.Errorf("build adopted roleBinding recreation candidate: %w", err)
	}
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted roleBinding recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	cli := c.client.RbacV1().RoleBindings(candidate.Namespace)
	created, err := cli.Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := cli.Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted roleBinding %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
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
				return fmt.Errorf("recover recreated adopted roleBinding binding: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedRoleBinding(ctx, desired, &recreation.adoptedResourceBinding)
			}
			return fmt.Errorf(
				"adopted roleBinding ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf("recreate adopted roleBinding %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	if err := persistCreatedAdoptedDependency(
		recreationCtx,
		recreation,
		created,
		created,
		c.runtime,
	); err != nil {
		return err
	}
	markResourceObserved(ctx, domainspec.ResourceRoleBinding, created.Namespace, created.Name)
	return nil
}

func adoptedRoleBindingForExistingUpdate(
	current, desired *rbacv1.RoleBinding,
) (*rbacv1.RoleBinding, error) {
	updated := current.DeepCopy()
	if desired == nil {
		return updated, nil
	}
	updated.Labels = adoptedOverlayStringMap(current.Labels, desired.Labels)
	updated.Annotations = adoptedOverlayStringMap(current.Annotations, desired.Annotations)
	if desired.Subjects != nil {
		updated.Subjects = append([]rbacv1.Subject(nil), desired.Subjects...)
	}
	if adoptedRoleRefSpecified(desired.RoleRef) {
		if adoptedRoleRefSpecified(current.RoleRef) &&
			!apiequality.Semantic.DeepEqual(current.RoleRef, desired.RoleRef) {
			return nil, fmt.Errorf(
				"roleBinding %s/%s roleRef is immutable: live=%s/%s desired=%s/%s",
				current.Namespace,
				current.Name,
				current.RoleRef.Kind,
				current.RoleRef.Name,
				desired.RoleRef.Kind,
				desired.RoleRef.Name,
			)
		}
		updated.RoleRef = desired.RoleRef
	}
	return updated, nil
}

func adoptedRoleRefSpecified(roleRef rbacv1.RoleRef) bool {
	return roleRef.APIGroup != "" || roleRef.Kind != "" || roleRef.Name != ""
}

func adoptedRoleBindingEqual(current, candidate *rbacv1.RoleBinding) bool {
	if current == nil || candidate == nil {
		return current == candidate
	}
	return apiequality.Semantic.DeepEqual(current.Labels, candidate.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, candidate.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Subjects, candidate.Subjects) &&
		apiequality.Semantic.DeepEqual(current.RoleRef, candidate.RoleRef)
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

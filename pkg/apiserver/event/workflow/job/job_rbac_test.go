package job

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestDeployServiceAccountJobCtl_Create(t *testing.T) {
	client := fake.NewSimpleClientset()
	jobTask := &model.JobTask{
		Name:      "pod-labeler-sa",
		Namespace: "ops",
		AppID:     "app-1",
		JobType:   string(config.JobDeployServiceAccount),
		JobInfo: &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-labeler-sa",
			},
		},
	}
	ctl := NewDeployServiceAccountJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	ctx := WithCleanupTracker(context.Background())

	if err := ctl.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	created, err := client.CoreV1().ServiceAccounts("ops").Get(context.Background(), "pod-labeler-sa", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected service account to be created: %v", err)
	}
	if created.Labels[config.LabelManagedBy] != config.ManagedByEruun {
		t.Fatalf("expected label %s=eruun, got %v", config.LabelManagedBy, created.Labels)
	}
}

func TestDeployServiceAccountJobCtl_CreateAlreadyExists(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "serviceaccounts", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		obj, ok := createAction.GetObject().(*corev1.ServiceAccount)
		if !ok {
			return false, nil, nil
		}
		_ = client.Tracker().Add(obj)
		return true, obj, k8serrors.NewAlreadyExists(corev1.Resource("serviceaccounts"), obj.Name)
	})
	jobTask := &model.JobTask{
		Name:      "shared-sa",
		Namespace: "ops",
		AppID:     "app-1",
		JobType:   string(config.JobDeployServiceAccount),
		JobInfo: &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "shared-sa",
				Namespace: "ops",
			},
		},
	}
	ctl := NewDeployServiceAccountJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	ctx := WithCleanupTracker(context.Background())

	if err := ctl.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	created, err := client.CoreV1().ServiceAccounts("ops").Get(context.Background(), "shared-sa", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected service account to exist: %v", err)
	}
	if created.Labels[config.LabelManagedBy] != config.ManagedByEruun {
		t.Fatalf("expected label %s=eruun, got %v", config.LabelManagedBy, created.Labels)
	}
}

func TestDeployServiceAccountJobCtl_SkipUnmanaged(t *testing.T) {
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-sa",
			Namespace: "ops",
			Labels: map[string]string{
				"owner": "platform",
			},
		},
	}
	client := fake.NewSimpleClientset(existing)
	jobTask := &model.JobTask{
		Name:      "shared-sa",
		Namespace: "ops",
		AppID:     "app-1",
		JobType:   string(config.JobDeployServiceAccount),
		JobInfo: &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: "shared-sa",
			},
		},
	}
	ctl := NewDeployServiceAccountJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	ctx := WithCleanupTracker(context.Background())

	if err := ctl.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	after, err := client.CoreV1().ServiceAccounts("ops").Get(context.Background(), "shared-sa", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected service account to exist: %v", err)
	}
	if _, ok := after.Labels[config.LabelManagedBy]; ok {
		t.Fatalf("expected unmanaged service account to remain unchanged, got labels %v", after.Labels)
	}
}

func TestDeployServiceAccountJobCtl_ShareDefaultStillReconcilesWithoutLocker(t *testing.T) {
	shareName := "proxy-webservice"
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-proxy",
			Namespace: "ops",
			Labels: map[string]string{
				config.LabelShareName: shareName,
			},
		},
	}
	client := fake.NewSimpleClientset(existing)
	jobTask := &model.JobTask{
		Name:      "proxy-sa",
		Namespace: "ops",
		AppID:     "app-1",
		JobType:   string(config.JobDeployServiceAccount),
		JobInfo: &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "proxy-sa",
				Namespace: "ops",
				Labels: map[string]string{
					config.LabelShareName:     shareName,
					config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
				},
			},
		},
	}
	ctl := NewDeployServiceAccountJobCtl(jobTask, client, &noopStore{}, func() {}, nil)
	ctx := WithCleanupTracker(context.Background())

	if err := ctl.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if jobTask.Status != config.StatusCompleted {
		t.Fatalf("expected job status completed, got %s", jobTask.Status)
	}
	if _, err := client.CoreV1().ServiceAccounts("ops").Get(context.Background(), "proxy-sa", metav1.GetOptions{}); err != nil {
		t.Fatalf("expected shared service account job to reconcile, got err=%v", err)
	}
}

func TestDeployRBACJobControllersCleanPreservesResources(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	client := fake.NewSimpleClientset(
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "shared-sa", Namespace: "ops"}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "shared-role", Namespace: "ops"}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "shared-binding", Namespace: "ops"}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "shared-cluster-role"}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "shared-cluster-binding"}},
	)
	store := &noopStore{}
	shareLocker := locker.NewNoopLocker(shareLockerPrefix)

	NewDeployServiceAccountJobCtl(&model.JobTask{Name: "shared-sa", Namespace: "ops"}, client, store, nil, shareLocker).Clean(ctx)
	NewDeployRoleJobCtl(&model.JobTask{Name: "shared-role", Namespace: "ops"}, client, store, nil, shareLocker).Clean(ctx)
	NewDeployRoleBindingJobCtl(&model.JobTask{Name: "shared-binding", Namespace: "ops"}, client, store, nil, shareLocker).Clean(ctx)
	NewDeployClusterRoleJobCtl(&model.JobTask{Name: "shared-cluster-role"}, client, store, nil, shareLocker).Clean(ctx)
	NewDeployClusterRoleBindingJobCtl(&model.JobTask{Name: "shared-cluster-binding"}, client, store, nil, shareLocker).Clean(ctx)

	_, err := client.CoreV1().ServiceAccounts("ops").Get(ctx, "shared-sa", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected service account to be preserved: %v", err)
	}
	_, err = client.RbacV1().Roles("ops").Get(ctx, "shared-role", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected role to be preserved: %v", err)
	}
	_, err = client.RbacV1().RoleBindings("ops").Get(ctx, "shared-binding", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected role binding to be preserved: %v", err)
	}
	_, err = client.RbacV1().ClusterRoles().Get(ctx, "shared-cluster-role", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected cluster role to be preserved: %v", err)
	}
	_, err = client.RbacV1().ClusterRoleBindings().Get(ctx, "shared-cluster-binding", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected cluster role binding to be preserved: %v", err)
	}
}

func TestDeployRoleJobCtl_UpdateManaged(t *testing.T) {
	existing := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-labeler-role",
			Namespace: "ops",
			Labels: map[string]string{
				config.LabelManagedBy:     config.ManagedByEruun,
				config.LabelAppID:         "app-1",
				config.LabelShareName:     "ops",
				config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
			},
		},
		Rules: []rbacv1.PolicyRule{{
			Verbs:     []string{"get"},
			APIGroups: []string{""},
			Resources: []string{"pods"},
		}},
	}
	client := fake.NewSimpleClientset(existing)
	jobTask := &model.JobTask{
		Name:      "pod-labeler-role",
		Namespace: "ops",
		AppID:     "app-1",
		JobType:   string(config.JobDeployRole),
		JobInfo: &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-labeler-role",
				Labels: map[string]string{
					config.LabelShareName:     "ops",
					config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
				},
			},
			Rules: []rbacv1.PolicyRule{{
				Verbs:     []string{"get", "patch"},
				APIGroups: []string{""},
				Resources: []string{"pods"},
			}},
		},
	}
	ctl := NewDeployRoleJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	ctx := WithCleanupTracker(context.Background())

	if err := ctl.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	updated, err := client.RbacV1().Roles("ops").Get(context.Background(), "pod-labeler-role", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected role to exist: %v", err)
	}
	if len(updated.Rules) != 1 || len(updated.Rules[0].Verbs) != 2 {
		t.Fatalf("expected role rules to be updated, got %+v", updated.Rules)
	}
}

// noopStore is a minimal datastore implementation for tests that do not persist.
type noopStore struct{}

func (*noopStore) Add(context.Context, datastore.Entity) error        { return nil }
func (*noopStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (*noopStore) Put(context.Context, datastore.Entity) error        { return nil }
func (*noopStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (*noopStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (*noopStore) Get(context.Context, datastore.Entity) error { return nil }
func (*noopStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}
func (*noopStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (*noopStore) IsExist(context.Context, datastore.Entity) (bool, error) { return false, nil }
func (*noopStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (*noopStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return true, nil
}

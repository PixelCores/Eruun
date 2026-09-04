package job

import (
	"context"

	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestAdoptedRoleRecreationPersistsClaimBeforeCreate(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("role-old")
	newUID := types.UID("role-new")
	source := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: oldUID},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"role",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "roles", func(action k8stesting.Action) (bool, runtime.Object, error) {
		candidate := action.(k8stesting.CreateAction).GetObject().(*rbacv1.Role)
		persisted := decodeTestAdoptionSnapshot(t, store.app)
		require.NotNil(t, persisted.Resources[0].PendingRecreation)
		token := candidate.Annotations[config.AnnotationAdoptedRecreationToken]
		require.NotEmpty(t, token)
		require.Equal(t, persisted.Resources[0].PendingRecreation.Token, token)
		candidate.UID = newUID
		candidate.ResourceVersion = "2"
		return false, nil, nil
	})
	ctl := NewDeployRoleJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobType:   string(config.JobDeployRole),
			JobInfo:   source.DeepCopy(),
		},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(newUID), persisted.Resources[0].Source.UID)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
	require.Equal(t, 2, store.applicationCASCount)
}

func TestAdoptedRoleRecreationAlreadyExistsWithSameTokenFinalizesClaim(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	newUID := types.UID("role-new")
	source := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: types.UID("role-old")},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"role",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	client := fake.NewSimpleClientset()
	var replacement *rbacv1.Role
	client.Fake.PrependReactor("get", "roles", func(k8stesting.Action) (bool, runtime.Object, error) {
		if replacement == nil {
			return false, nil, nil
		}
		return true, replacement.DeepCopy(), nil
	})
	client.Fake.PrependReactor("create", "roles", func(action k8stesting.Action) (bool, runtime.Object, error) {
		candidate := action.(k8stesting.CreateAction).GetObject().(*rbacv1.Role)
		persisted := decodeTestAdoptionSnapshot(t, store.app)
		require.NotNil(t, persisted.Resources[0].PendingRecreation)
		require.Equal(
			t,
			persisted.Resources[0].PendingRecreation.Token,
			candidate.Annotations[config.AnnotationAdoptedRecreationToken],
		)
		replacement = candidate.DeepCopy()
		replacement.UID = newUID
		replacement.ResourceVersion = "3"
		return true, nil, k8serrors.NewAlreadyExists(
			schema.GroupResource{Group: rbacv1.GroupName, Resource: "roles"},
			candidate.Name,
		)
	})
	ctl := NewDeployRoleJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobInfo: source.DeepCopy()},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(newUID), persisted.Resources[0].Source.UID)
	require.Equal(t, "3", persisted.Resources[0].Source.ResourceVersion)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
	require.Equal(t, 2, store.applicationCASCount)
	require.Equal(t, 1, countClientActions(client, "create", "roles"))
	require.Equal(t, 0, countClientActions(client, "delete", "roles"))
}

func TestAdoptedRoleRecreationClaimCASFailureDoesNotCreate(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	source := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: types.UID("role-old")},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"role",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	store := &adoptedSourceStore{
		app:    adoptedApplication(t, "app-1", "ops", saved),
		putErr: errors.New("database unavailable"),
	}
	client := fake.NewSimpleClientset()
	ctl := NewDeployRoleJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobType:   string(config.JobDeployRole),
			JobInfo:   source.DeepCopy(),
		},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.ErrorContains(t, err, "persist adopted role recreation claim")
	require.Equal(t, 0, countClientActions(client, "create", "roles"))
	require.Equal(t, 0, countClientActions(client, "delete", "roles"))
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(source.UID), persisted.Resources[0].Source.UID)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
}

func TestPendingAdoptedRecreationRecoversNamespacedRBACAndPolicyResources(t *testing.T) {
	const token = "persisted-recreation-token"
	type testCase struct {
		name     string
		kind     string
		role     string
		resource string
		newUID   types.UID
		source   runtime.Object
	}
	testCases := []testCase{
		{
			name: "service account", kind: "ServiceAccount", role: "service-account",
			resource: "serviceaccounts", newUID: types.UID("new-sa"),
			source: &corev1.ServiceAccount{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-sa", Namespace: "ops", UID: types.UID("old-sa")},
			},
		},
		{
			name: "role", kind: "Role", role: "role", resource: "roles", newUID: types.UID("new-role"),
			source: &rbacv1.Role{
				TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "Role"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: types.UID("old-role")},
			},
		},
		{
			name: "role binding", kind: "RoleBinding", role: "role-binding",
			resource: "rolebindings", newUID: types.UID("new-binding"),
			source: &rbacv1.RoleBinding{
				TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "RoleBinding"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-binding", Namespace: "ops", UID: types.UID("old-binding")},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "legacy-role"},
			},
		},
		{
			name: "pod disruption budget", kind: "PodDisruptionBudget", role: "pdb",
			resource: "poddisruptionbudgets", newUID: types.UID("new-pdb"),
			source: &policyv1.PodDisruptionBudget{
				TypeMeta:   metav1.TypeMeta{APIVersion: policyv1.SchemeGroupVersion.String(), Kind: "PodDisruptionBudget"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-pdb", Namespace: "ops", UID: types.UID("old-pdb")},
			},
		},
		{
			name: "network policy", kind: "NetworkPolicy", role: "network-policy",
			resource: "networkpolicies", newUID: types.UID("new-policy"),
			source: &networkingv1.NetworkPolicy{
				TypeMeta:   metav1.TypeMeta{APIVersion: networkingv1.SchemeGroupVersion.String(), Kind: "NetworkPolicy"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-policy", Namespace: "ops", UID: types.UID("old-policy")},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := WithCleanupTracker(context.Background())
			source := testCase.source.DeepCopyObject()
			replacement := testCase.source.DeepCopyObject()
			replacementMeta, ok := replacement.(metav1.Object)
			require.True(t, ok)
			replacementMeta.SetUID(testCase.newUID)
			replacementMeta.SetResourceVersion("2")
			replacementMeta.SetAnnotations(map[string]string{config.AnnotationAdoptedRecreationToken: token})
			saved := adoptedSnapshotResource(
				t,
				source,
				"backend",
				testCase.role,
				importcontract.OwnershipExclusive,
				importcontract.DispositionManaged,
			)
			saved.PendingRecreation = &importcontract.RecreationClaim{Token: token}
			store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
			client := fake.NewSimpleClientset(replacement)
			job := &model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobInfo: source}
			var runErr error
			switch testCase.kind {
			case "ServiceAccount":
				runErr = NewDeployServiceAccountJobCtl(
					job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix),
				).run(ctx)
			case "Role":
				runErr = NewDeployRoleJobCtl(
					job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix),
				).run(ctx)
			case "RoleBinding":
				runErr = NewDeployRoleBindingJobCtl(
					job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix),
				).run(ctx)
			case "PodDisruptionBudget":
				runErr = NewDeployAdoptedPodDisruptionBudgetJobCtl(
					job, client, store, func() {}, locker.NewMemoryLocker(shareLockerPrefix),
				).run(ctx)
			case "NetworkPolicy":
				runErr = NewDeployAdoptedNetworkPolicyJobCtl(
					job, client, store, func() {}, locker.NewMemoryLocker(shareLockerPrefix),
				).run(ctx)
			default:
				require.FailNow(t, "unsupported recovery test kind", testCase.kind)
			}
			require.NoError(t, runErr)
			persisted := decodeTestAdoptionSnapshot(t, store.app)
			require.Equal(t, string(replacementMeta.GetUID()), persisted.Resources[0].Source.UID)
			require.Equal(t, replacementMeta.GetResourceVersion(), persisted.Resources[0].Source.ResourceVersion)
			require.Nil(t, persisted.Resources[0].PendingRecreation)
			require.Equal(t, 1, store.applicationCASCount)
			require.Equal(t, 0, countClientActions(client, "create", testCase.resource))
			require.Equal(t, 0, countClientActions(client, "update", testCase.resource))
			require.Equal(t, 0, countClientActions(client, "delete", testCase.resource))
		})
	}
}

func TestDeployServiceAccountJobCtlRunAdoptedPreservesUnknownFieldsAndSkipsNoop(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("service-account-uid")
	automount := true
	live := &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "legacy-runtime",
			Namespace:   "ops",
			UID:         uid,
			Labels:      map[string]string{"platform.example/owner": "team-a"},
			Annotations: map[string]string{"platform.example/revision": "preserve"},
		},
		Secrets:                      []corev1.ObjectReference{{Name: "controller-managed-token"}},
		ImagePullSecrets:             []corev1.LocalObjectReference{{Name: "registry-existing"}},
		AutomountServiceAccountToken: &automount,
	}
	saved := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"service-account",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	desired := &corev1.ServiceAccount{
		ObjectMeta:                   metav1.ObjectMeta{Name: live.Name, Namespace: live.Namespace},
		AutomountServiceAccountToken: &automount,
	}
	client := fake.NewSimpleClientset(live)
	ctl := NewDeployServiceAccountJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployServiceAccount), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 0, countClientActions(client, "update", "serviceaccounts"))
	preserved, err := client.CoreV1().ServiceAccounts("ops").Get(ctx, live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "team-a", preserved.Labels["platform.example/owner"])
	require.Equal(t, "preserve", preserved.Annotations["platform.example/revision"])
	require.Equal(t, "controller-managed-token", preserved.Secrets[0].Name)
	require.Equal(t, "registry-existing", preserved.ImagePullSecrets[0].Name)
}

func TestDeployRoleJobCtlRunAdoptedUsesSourceUIDAndLiveBaseline(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("role-uid")
	live := &rbacv1.Role{
		TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "legacy-pod-reader",
			Namespace:   "ops",
			UID:         uid,
			Labels:      map[string]string{"platform.example/owner": "team-a"},
			Annotations: map[string]string{"platform.example/revision": "preserve"},
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"get"},
		}},
	}
	saved := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"role",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	desired := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      live.Name,
			Namespace: live.Namespace,
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun},
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"get", "patch"},
		}},
	}
	client := fake.NewSimpleClientset(live)
	ctl := NewDeployRoleJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployRole), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 1, countClientActions(client, "update", "roles"))
	updated, err := client.RbacV1().Roles("ops").Get(ctx, live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, uid, updated.UID)
	require.Equal(t, "team-a", updated.Labels["platform.example/owner"])
	require.Equal(t, config.ManagedByEruun, updated.Labels[config.LabelManagedBy])
	require.Equal(t, "preserve", updated.Annotations["platform.example/revision"])
	require.Equal(t, []string{"get", "patch"}, updated.Rules[0].Verbs)
}

func TestDeployNamespacedRBACJobCtlRunAdoptedPreservedDispositionNeverTouchesKubernetes(t *testing.T) {
	testCases := []struct {
		name        string
		ownership   string
		disposition string
		wantError   bool
	}{
		{
			name:        "shared",
			ownership:   importcontract.OwnershipShared,
			disposition: importcontract.DispositionSharedPreserved,
		},
		{
			name:        "external excluded",
			ownership:   importcontract.OwnershipExternal,
			disposition: importcontract.DispositionExcluded,
		},
		{
			name:        "data protected",
			ownership:   importcontract.OwnershipDataProtected,
			disposition: importcontract.DispositionDataProtected,
		},
		{
			name:        "blocked",
			ownership:   importcontract.OwnershipExclusive,
			disposition: importcontract.DispositionBlocked,
			wantError:   true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := WithCleanupTracker(context.Background())
			source := &rbacv1.Role{
				TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: types.UID("role-uid")},
			}
			saved := adoptedSnapshotResource(t, source, "backend", "role", testCase.ownership, testCase.disposition)
			store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
			client := fake.NewSimpleClientset(source)
			ctl := NewDeployRoleJobCtl(
				&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployRole), JobInfo: source.DeepCopy()},
				client,
				store,
				func() {},
				locker.NewNoopLocker(shareLockerPrefix),
			)

			err := ctl.run(ctx)
			if testCase.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Empty(t, client.Actions())
		})
	}
}

func TestDeployRoleJobCtlRunAdoptedRejectsReplacementUID(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	source := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: types.UID("source-uid")},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"role",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	replacement := source.DeepCopy()
	replacement.UID = types.UID("replacement-uid")
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	client := fake.NewSimpleClientset(replacement)
	ctl := NewDeployRoleJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployRole), JobInfo: source.DeepCopy()},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ownership conflict")
	require.Equal(t, 0, countClientActions(client, "update", "roles"))
	require.Equal(t, 0, countClientActions(client, "create", "roles"))
	require.Equal(t, 0, countClientActions(client, "delete", "roles"))
}

func TestDeployRoleJobCtlRunAdoptedNeverFallsBackToGeneratedName(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	source := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: types.UID("source-uid")},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"role",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	client := fake.NewSimpleClientset(source)
	desired := source.DeepCopy()
	desired.Name = "generated-role"
	ctl := NewDeployRoleJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployRole), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not present in the adoption snapshot")
	require.Empty(t, client.Actions())
}

func TestDeployRoleBindingJobCtlRunAdoptedRejectsImmutableRoleRefChange(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	source := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-binding", Namespace: "ops", UID: types.UID("binding-uid")},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     "legacy-role",
		},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "legacy-sa", Namespace: "ops"}},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"role-binding",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	desired := source.DeepCopy()
	desired.RoleRef.Name = "generated-role"
	client := fake.NewSimpleClientset(source)
	ctl := NewDeployRoleBindingJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployRoleBinding), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "roleRef is immutable")
	require.Equal(t, 0, countClientActions(client, "update", "rolebindings"))
	require.Equal(t, 0, countClientActions(client, "create", "rolebindings"))
	require.Equal(t, 0, countClientActions(client, "delete", "rolebindings"))
}

func TestDeployRoleBindingJobCtlRunAdoptedUsesLiveBaselineAndSkipsSecondUpdate(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("binding-uid")
	source := &rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "legacy-binding",
			Namespace:   "ops",
			UID:         uid,
			Labels:      map[string]string{"platform.example/owner": "team-a"},
			Annotations: map[string]string{"platform.example/revision": "preserve"},
		},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "legacy-role"},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "legacy-sa", Namespace: "ops"}},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"role-binding",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	desired := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      source.Name,
			Namespace: source.Namespace,
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun},
		},
		RoleRef:  source.RoleRef,
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "replacement-sa", Namespace: "ops"}},
	}
	client := fake.NewSimpleClientset(source)
	ctl := NewDeployRoleBindingJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployRoleBinding), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 1, countClientActions(client, "update", "rolebindings"))
	updated, err := client.RbacV1().RoleBindings("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, uid, updated.UID)
	require.Equal(t, "team-a", updated.Labels["platform.example/owner"])
	require.Equal(t, config.ManagedByEruun, updated.Labels[config.LabelManagedBy])
	require.Equal(t, "preserve", updated.Annotations["platform.example/revision"])
	require.Equal(t, "replacement-sa", updated.Subjects[0].Name)
	require.Equal(t, source.RoleRef, updated.RoleRef)
}

func TestAdoptedNamespacedRBACRecreateFromSnapshotAndRotateUID(t *testing.T) {
	t.Run("service account", func(t *testing.T) {
		ctx := WithCleanupTracker(context.Background())
		oldUID := types.UID("service-account-old")
		newUID := types.UID("service-account-new")
		source := &corev1.ServiceAccount{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
			ObjectMeta: metav1.ObjectMeta{Name: "legacy-sa", Namespace: "ops", UID: oldUID},
			Secrets:    []corev1.ObjectReference{{Name: "preserved-reference"}},
		}
		saved := adoptedSnapshotResource(t, source, "backend", "service-account", importcontract.OwnershipExclusive, importcontract.DispositionManaged)
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
		client := fake.NewSimpleClientset()
		client.Fake.PrependReactor("create", "serviceaccounts", func(action k8stesting.Action) (bool, runtime.Object, error) {
			object := action.(k8stesting.CreateAction).GetObject().(*corev1.ServiceAccount)
			object.UID = newUID
			object.ResourceVersion = "21"
			return false, nil, nil
		})
		ctl := NewDeployServiceAccountJobCtl(
			&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployServiceAccount), JobInfo: source.DeepCopy()},
			client,
			store,
			func() {},
			locker.NewNoopLocker(shareLockerPrefix),
		)

		require.NoError(t, ctl.run(ctx))
		created, err := client.CoreV1().ServiceAccounts("ops").Get(ctx, source.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, newUID, created.UID)
		require.Equal(t, "preserved-reference", created.Secrets[0].Name)
		require.Equal(t, string(newUID), decodeTestAdoptionSnapshot(t, store.app).Resources[0].Source.UID)
	})

	t.Run("role binding", func(t *testing.T) {
		ctx := WithCleanupTracker(context.Background())
		oldUID := types.UID("binding-old")
		newUID := types.UID("binding-new")
		source := &rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "legacy-binding", Namespace: "ops", UID: oldUID},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "legacy-role"},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "legacy-sa", Namespace: "ops"}},
		}
		saved := adoptedSnapshotResource(t, source, "backend", "role-binding", importcontract.OwnershipExclusive, importcontract.DispositionManaged)
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
		client := fake.NewSimpleClientset()
		client.Fake.PrependReactor("create", "rolebindings", func(action k8stesting.Action) (bool, runtime.Object, error) {
			object := action.(k8stesting.CreateAction).GetObject().(*rbacv1.RoleBinding)
			object.UID = newUID
			object.ResourceVersion = "22"
			return false, nil, nil
		})
		ctl := NewDeployRoleBindingJobCtl(
			&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployRoleBinding), JobInfo: source.DeepCopy()},
			client,
			store,
			func() {},
			locker.NewNoopLocker(shareLockerPrefix),
		)

		require.NoError(t, ctl.run(ctx))
		created, err := client.RbacV1().RoleBindings("ops").Get(ctx, source.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, newUID, created.UID)
		require.Equal(t, "legacy-role", created.RoleRef.Name)
		require.Equal(t, string(newUID), decodeTestAdoptionSnapshot(t, store.app).Resources[0].Source.UID)
	})
}

func TestAdoptedRoleRecreationPersistenceFailureRetainsLiveObjectAndPendingClaim(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("role-old")
	newUID := types.UID("role-new")
	source := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: oldUID},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"get"},
		}},
	}
	saved := adoptedSnapshotResource(t, source, "backend", "role", importcontract.OwnershipExclusive, importcontract.DispositionManaged)
	store := &adoptedSourceStore{
		app:                        adoptedApplication(t, "app-1", "ops", saved),
		applicationCASErrOnAttempt: 2,
		applicationCASErr:          errors.New("database unavailable"),
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "roles", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*rbacv1.Role)
		object.UID = newUID
		return false, nil, nil
	})
	ctl := NewDeployRoleJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployRole), JobInfo: source.DeepCopy()},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "persist recreated adopted Role binding")
	require.Contains(t, err.Error(), "pending claim retained")
	live, getErr := client.RbacV1().Roles("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, newUID, live.UID)
	require.Equal(t, 1, countClientActions(client, "create", "roles"))
	require.Equal(t, 0, countClientActions(client, "delete", "roles"))
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(oldUID), persisted.Resources[0].Source.UID)
	require.NotNil(t, persisted.Resources[0].PendingRecreation)
	require.Equal(
		t,
		persisted.Resources[0].PendingRecreation.Token,
		live.Annotations[config.AnnotationAdoptedRecreationToken],
	)
}

func TestAdoptedClusterScopedRBACJobsAreExternalZeroWrite(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	source := &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-sa", Namespace: "ops", UID: types.UID("service-account-uid")},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"service-account",
		importcontract.OwnershipExclusive,
		importcontract.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}

	t.Run("cluster role", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		desired := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "generated-cluster-role"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"patch"}}},
		}
		ctl := NewDeployClusterRoleJobCtl(
			&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployClusterRole), JobInfo: desired},
			client,
			store,
			func() {},
			locker.NewNoopLocker(shareLockerPrefix),
		)

		require.NoError(t, ctl.run(ctx))
		require.Empty(t, client.Actions())
		require.NotContains(t, desired.Labels, config.LabelManagedBy)
	})

	t.Run("cluster role binding", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		desired := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "generated-cluster-binding"},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "generated-cluster-role"},
		}
		ctl := NewDeployClusterRoleBindingJobCtl(
			&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployClusterRoleBinding), JobInfo: desired},
			client,
			store,
			func() {},
			locker.NewNoopLocker(shareLockerPrefix),
		)

		require.NoError(t, ctl.run(ctx))
		require.Empty(t, client.Actions())
		require.NotContains(t, desired.Labels, config.LabelManagedBy)
	})
}

package job

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainadoption "github.com/PixelCores/Eruun/pkg/apiserver/domain/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestDeployAdoptedPodDisruptionBudgetUsesLiveBaselineAndSkipsSecondUpdate(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("pdb-uid")
	minAvailable := intstr.FromInt32(1)
	live := &policyv1.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{
			APIVersion: policyv1.SchemeGroupVersion.String(),
			Kind:       "PodDisruptionBudget",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "legacy-backend-pdb",
			Namespace:   "ops",
			UID:         uid,
			Generation:  7,
			Labels:      map[string]string{"platform.example/owner": "team-a"},
			Annotations: map[string]string{"platform.example/revision": "preserve"},
			Finalizers:  []string{"platform.example/protect"},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "legacy"}},
		},
		Status: policyv1.PodDisruptionBudgetStatus{CurrentHealthy: 3},
	}
	saved := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"pdb",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	maxUnavailable := intstr.FromInt32(1)
	desired := live.DeepCopy()
	desired.Labels = map[string]string{config.LabelManagedBy: config.ManagedByEruun}
	desired.Annotations = nil
	desired.Spec.MinAvailable = nil
	desired.Spec.MaxUnavailable = &maxUnavailable
	client := fake.NewSimpleClientset(live)
	ctl := NewDeployAdoptedPodDisruptionBudgetJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobType:   "deploy_adopted_pod_disruption_budget",
			JobInfo:   desired,
		},
		client,
		store,
		func() {},
		nil,
	)

	require.NoError(t, ctl.run(ctx))
	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 1, countClientActions(client, "update", "poddisruptionbudgets"))

	updated, err := client.PolicyV1().PodDisruptionBudgets("ops").Get(
		ctx,
		live.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, uid, updated.UID)
	require.Equal(t, int64(7), updated.Generation)
	require.Equal(t, "team-a", updated.Labels["platform.example/owner"])
	require.Equal(t, config.ManagedByEruun, updated.Labels[config.LabelManagedBy])
	require.Equal(t, "preserve", updated.Annotations["platform.example/revision"])
	require.Equal(t, []string{"platform.example/protect"}, updated.Finalizers)
	require.Nil(t, updated.Spec.MinAvailable)
	require.Equal(t, maxUnavailable, *updated.Spec.MaxUnavailable)
	require.Equal(t, int32(3), updated.Status.CurrentHealthy)
}

func TestDeployAdoptedPodDisruptionBudgetEnforcesSnapshotWriteGateAndUID(t *testing.T) {
	t.Run("shared preserved performs zero Kubernetes requests", func(t *testing.T) {
		ctx := WithCleanupTracker(context.Background())
		uid := types.UID("pdb-shared")
		source := &policyv1.PodDisruptionBudget{
			TypeMeta: metav1.TypeMeta{
				APIVersion: policyv1.SchemeGroupVersion.String(),
				Kind:       "PodDisruptionBudget",
			},
			ObjectMeta: metav1.ObjectMeta{Name: "shared-pdb", Namespace: "ops", UID: uid},
			Spec: policyv1.PodDisruptionBudgetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "shared"}},
			},
		}
		saved := adoptedSnapshotResource(
			t,
			source,
			"backend",
			"pdb",
			domainadoption.OwnershipShared,
			domainadoption.DispositionSharedPreserved,
		)
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
		desired := source.DeepCopy()
		desired.Spec.Selector.MatchLabels["app"] = "changed"
		client := fake.NewSimpleClientset(source)
		ctl := NewDeployAdoptedPodDisruptionBudgetJobCtl(
			&model.JobTask{
				Name:      "backend",
				AppID:     "app-1",
				Namespace: "ops",
				JobInfo:   desired,
			},
			client,
			store,
			func() {},
			nil,
		)

		require.NoError(t, ctl.run(ctx))
		require.Empty(t, client.Actions())
	})

	t.Run("replacement UID is rejected without a write", func(t *testing.T) {
		ctx := WithCleanupTracker(context.Background())
		source := &policyv1.PodDisruptionBudget{
			TypeMeta: metav1.TypeMeta{
				APIVersion: policyv1.SchemeGroupVersion.String(),
				Kind:       "PodDisruptionBudget",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backend-pdb",
				Namespace: "ops",
				UID:       types.UID("original"),
			},
		}
		saved := adoptedSnapshotResource(
			t,
			source,
			"backend",
			"pdb",
			domainadoption.OwnershipExclusive,
			domainadoption.DispositionManaged,
		)
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
		replacement := source.DeepCopy()
		replacement.UID = types.UID("replacement")
		client := fake.NewSimpleClientset(replacement)
		ctl := NewDeployAdoptedPodDisruptionBudgetJobCtl(
			&model.JobTask{
				Name:      "backend",
				AppID:     "app-1",
				Namespace: "ops",
				JobInfo:   source.DeepCopy(),
			},
			client,
			store,
			func() {},
			locker.NewMemoryLocker(shareLockerPrefix),
		)

		err := ctl.run(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ownership conflict")
		require.Equal(t, 0, countClientActions(client, "update", "poddisruptionbudgets"))
		require.Equal(t, 0, countClientActions(client, "create", "poddisruptionbudgets"))
		require.Equal(t, 0, countClientActions(client, "delete", "poddisruptionbudgets"))
	})

	t.Run("generated name cannot replace source identity", func(t *testing.T) {
		ctx := WithCleanupTracker(context.Background())
		source := &policyv1.PodDisruptionBudget{
			TypeMeta: metav1.TypeMeta{
				APIVersion: policyv1.SchemeGroupVersion.String(),
				Kind:       "PodDisruptionBudget",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-backend-pdb",
				Namespace: "ops",
				UID:       types.UID("pdb-uid"),
			},
		}
		saved := adoptedSnapshotResource(
			t,
			source,
			"backend",
			"pdb",
			domainadoption.OwnershipExclusive,
			domainadoption.DispositionManaged,
		)
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
		desired := source.DeepCopy()
		desired.Name = "generated-backend-pdb"
		client := fake.NewSimpleClientset(source)
		ctl := NewDeployAdoptedPodDisruptionBudgetJobCtl(
			&model.JobTask{
				Name:      "backend",
				AppID:     "app-1",
				Namespace: "ops",
				JobInfo:   desired,
			},
			client,
			store,
			func() {},
			nil,
		)

		err := ctl.run(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not present in the adoption snapshot")
		require.Empty(t, client.Actions())
	})
}

func TestDeployAdoptedPodDisruptionBudgetRecreatesOriginalNameAndRotatesSnapshotUID(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("pdb-old")
	newUID := types.UID("pdb-new")
	minAvailable := intstr.FromInt32(1)
	source := &policyv1.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{
			APIVersion: policyv1.SchemeGroupVersion.String(),
			Kind:       "PodDisruptionBudget",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-backend-pdb",
			Namespace: "ops",
			UID:       oldUID,
			Labels:    map[string]string{"platform.example/owner": "team-a"},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "legacy"}},
		},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"pdb",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	maxUnavailable := intstr.FromInt32(1)
	desired := source.DeepCopy()
	desired.UID = ""
	desired.Spec.MinAvailable = nil
	desired.Spec.MaxUnavailable = &maxUnavailable
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor(
		"create",
		"poddisruptionbudgets",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			object := action.(k8stesting.CreateAction).GetObject().(*policyv1.PodDisruptionBudget)
			object.UID = newUID
			object.ResourceVersion = "41"
			return false, nil, nil
		},
	)
	ctl := NewDeployAdoptedPodDisruptionBudgetJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobInfo:   desired,
		},
		client,
		store,
		func() {},
		locker.NewMemoryLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	created, err := client.PolicyV1().PodDisruptionBudgets("ops").Get(
		ctx,
		source.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, newUID, created.UID)
	require.Equal(t, "team-a", created.Labels["platform.example/owner"])
	require.Nil(t, created.Spec.MinAvailable)
	require.Equal(t, maxUnavailable, *created.Spec.MaxUnavailable)
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(newUID), persisted.Resources[0].Source.UID)
	require.Equal(t, "41", persisted.Resources[0].Source.ResourceVersion)
	require.Equal(t, 2, store.applicationCASCount)
	require.Equal(t, 1, countClientActions(client, "create", "poddisruptionbudgets"))
}

func TestDeployAdoptedNetworkPolicyUsesLiveBaselineAndSkipsSecondUpdate(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("network-policy-uid")
	live := &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: networkingv1.SchemeGroupVersion.String(),
			Kind:       "NetworkPolicy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "legacy-backend-policy",
			Namespace:   "ops",
			UID:         uid,
			Labels:      map[string]string{"platform.example/owner": "network-team"},
			Annotations: map[string]string{"platform.example/revision": "preserve"},
			Finalizers:  []string{"platform.example/protect"},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "legacy"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress:     []networkingv1.NetworkPolicyIngressRule{},
		},
	}
	saved := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"network-policy",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	desired := live.DeepCopy()
	desired.Labels = map[string]string{config.LabelManagedBy: config.ManagedByEruun}
	desired.Annotations = nil
	desired.Spec.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}
	desired.Spec.Ingress = nil
	desired.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{}
	client := fake.NewSimpleClientset(live)
	ctl := NewDeployAdoptedNetworkPolicyJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobType:   "deploy_adopted_network_policy",
			JobInfo:   desired,
		},
		client,
		store,
		func() {},
		nil,
	)

	require.NoError(t, ctl.run(ctx))
	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 1, countClientActions(client, "update", "networkpolicies"))
	updated, err := client.NetworkingV1().NetworkPolicies("ops").Get(
		ctx,
		live.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, uid, updated.UID)
	require.Equal(t, "network-team", updated.Labels["platform.example/owner"])
	require.Equal(t, config.ManagedByEruun, updated.Labels[config.LabelManagedBy])
	require.Equal(t, "preserve", updated.Annotations["platform.example/revision"])
	require.Equal(t, []string{"platform.example/protect"}, updated.Finalizers)
	require.Equal(
		t,
		[]networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
		updated.Spec.PolicyTypes,
	)
	require.Nil(t, updated.Spec.Ingress)
	require.NotNil(t, updated.Spec.Egress)
}

func TestDeployAdoptedNetworkPolicyBlockedDispositionRejectsWithoutKubernetesRequest(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	source := &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: networkingv1.SchemeGroupVersion.String(),
			Kind:       "NetworkPolicy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "blocked-policy",
			Namespace: "ops",
			UID:       types.UID("network-policy-uid"),
		},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"network-policy",
		domainadoption.OwnershipExternal,
		domainadoption.DispositionBlocked,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	client := fake.NewSimpleClientset(source)
	ctl := NewDeployAdoptedNetworkPolicyJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobInfo:   source.DeepCopy(),
		},
		client,
		store,
		func() {},
		nil,
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not writable")
	require.Empty(t, client.Actions())
}

func TestDeployAdoptedNetworkPolicyRecreationPersistenceFailureRetainsLiveObjectAndPendingClaim(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("network-policy-old")
	newUID := types.UID("network-policy-new")
	source := &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: networkingv1.SchemeGroupVersion.String(),
			Kind:       "NetworkPolicy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-backend-policy",
			Namespace: "ops",
			UID:       oldUID,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "legacy"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"network-policy",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{
		app:                        adoptedApplication(t, "app-1", "ops", saved),
		applicationCASErrOnAttempt: 2,
		applicationCASErr:          errors.New("database unavailable"),
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor(
		"create",
		"networkpolicies",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			object := action.(k8stesting.CreateAction).GetObject().(*networkingv1.NetworkPolicy)
			object.UID = newUID
			object.ResourceVersion = "52"
			return false, nil, nil
		},
	)
	ctl := NewDeployAdoptedNetworkPolicyJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobInfo:   source.DeepCopy(),
		},
		client,
		store,
		func() {},
		locker.NewMemoryLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "persist recreated adopted NetworkPolicy binding")
	require.Contains(t, err.Error(), "pending claim retained")
	require.Equal(t, 1, countClientActions(client, "create", "networkpolicies"))
	require.Equal(t, 0, countClientActions(client, "delete", "networkpolicies"))
	live, getErr := client.NetworkingV1().NetworkPolicies("ops").Get(
		ctx,
		source.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, getErr)
	require.Equal(t, newUID, live.UID)
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(oldUID), persisted.Resources[0].Source.UID)
	require.NotNil(t, persisted.Resources[0].PendingRecreation)
	require.Equal(
		t,
		persisted.Resources[0].PendingRecreation.Token,
		live.Annotations[config.AnnotationAdoptedRecreationToken],
	)
}

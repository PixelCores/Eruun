package job

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/importsecret"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
)

func TestAdoptedRecreationAlreadyExistsPreservesFollowupGetError(t *testing.T) {
	type runRecreation func(
		context.Context,
		*fake.Clientset,
		*adoptedSourceStore,
		*model.JobTask,
		*adoptedResourceBinding,
		*model.ApplicationComponent,
		runtime.Object,
	) error
	testCases := []struct {
		name         string
		kind         string
		role         string
		resource     string
		source       runtime.Object
		workloadKind string
		run          runRecreation
	}{
		{
			name: "deployment", kind: "Deployment", role: "workload", resource: "deployments", workloadKind: "Deployment",
			source: &appsv1.Deployment{
				TypeMeta:   metav1.TypeMeta{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "Deployment"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-backend", Namespace: "ops", UID: types.UID("deployment-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, _ *adoptedResourceBinding, component *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix)).recreateAdoptedDeployment(ctx, source.(*appsv1.Deployment).DeepCopy(), component)
			},
		},
		{
			name: "statefulset", kind: "StatefulSet", role: "workload", resource: "statefulsets", workloadKind: "StatefulSet",
			source: &appsv1.StatefulSet{
				TypeMeta:   metav1.TypeMeta{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "StatefulSet"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-database", Namespace: "ops", UID: types.UID("statefulset-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, _ *adoptedResourceBinding, component *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployStatefulSetJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix)).recreateAdoptedStatefulSet(ctx, source.(*appsv1.StatefulSet).DeepCopy(), component)
			},
		},
		{
			name: "service", kind: "Service", role: "service", resource: "services",
			source: &corev1.Service{
				TypeMeta:   metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Service"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-service", Namespace: "ops", UID: types.UID("service-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, binding *adoptedResourceBinding, _ *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployServiceJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix)).recreateAdoptedService(ctx, source.(*corev1.Service).DeepCopy(), binding)
			},
		},
		{
			name: "configmap", kind: "ConfigMap", role: "configmap", resource: "configmaps",
			source: &corev1.ConfigMap{
				TypeMeta:   metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "ConfigMap"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-config", Namespace: "ops", UID: types.UID("configmap-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, binding *adoptedResourceBinding, _ *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployConfigMapJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix), nil).recreateAdoptedConfigMap(ctx, source.(*corev1.ConfigMap).DeepCopy(), binding)
			},
		},
		{
			name: "ingress", kind: "Ingress", role: "ingress", resource: "ingresses",
			source: &networkingv1.Ingress{
				TypeMeta:   metav1.TypeMeta{APIVersion: networkingv1.SchemeGroupVersion.String(), Kind: "Ingress"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-ingress", Namespace: "ops", UID: types.UID("ingress-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, binding *adoptedResourceBinding, _ *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployIngressJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix)).recreateAdoptedIngress(ctx, source.(*networkingv1.Ingress).DeepCopy(), binding)
			},
		},
		{
			name: "secret", kind: "Secret", role: "secret", resource: "secrets",
			source: &corev1.Secret{
				TypeMeta:   metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Secret"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-secret", Namespace: "ops", UID: types.UID("secret-old")},
				Data:       map[string][]byte{"password": []byte("source")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, binding *adoptedResourceBinding, _ *model.ApplicationComponent, source runtime.Object) error {
				baseline := source.(*corev1.Secret).DeepCopy()
				desired := baseline.DeepCopy()
				desired.Data = nil
				material := &adoptedSecretMaterial{
					data: map[string][]byte{"password": []byte("source")},
					ciphertextUpdates: []adoptedSecretCiphertextUpdate{{
						component:     &model.ApplicationComponent{Name: "backend"},
						encryptedData: &model.JSONStruct{},
					}},
				}
				return NewDeploySecretJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix), nil).recreateAdoptedSecret(ctx, desired, baseline, material, binding)
			},
		},
		{
			name: "service account", kind: "ServiceAccount", role: "service-account", resource: "serviceaccounts",
			source: &corev1.ServiceAccount{
				TypeMeta:   metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "ServiceAccount"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-runtime", Namespace: "ops", UID: types.UID("service-account-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, binding *adoptedResourceBinding, _ *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployServiceAccountJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix)).recreateAdoptedServiceAccount(ctx, source.(*corev1.ServiceAccount).DeepCopy(), binding)
			},
		},
		{
			name: "role", kind: "Role", role: "role", resource: "roles",
			source: &rbacv1.Role{
				TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "Role"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: types.UID("role-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, binding *adoptedResourceBinding, _ *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployRoleJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix)).recreateAdoptedRole(ctx, source.(*rbacv1.Role).DeepCopy(), binding)
			},
		},
		{
			name: "role binding", kind: "RoleBinding", role: "role-binding", resource: "rolebindings",
			source: &rbacv1.RoleBinding{
				TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "RoleBinding"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-role-binding", Namespace: "ops", UID: types.UID("role-binding-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, binding *adoptedResourceBinding, _ *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployRoleBindingJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix)).recreateAdoptedRoleBinding(ctx, source.(*rbacv1.RoleBinding).DeepCopy(), binding)
			},
		},
		{
			name: "pod disruption budget", kind: "PodDisruptionBudget", role: "pod-disruption-budget", resource: "poddisruptionbudgets",
			source: &policyv1.PodDisruptionBudget{
				TypeMeta:   metav1.TypeMeta{APIVersion: policyv1.SchemeGroupVersion.String(), Kind: "PodDisruptionBudget"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-pdb", Namespace: "ops", UID: types.UID("pdb-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, binding *adoptedResourceBinding, _ *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployAdoptedPodDisruptionBudgetJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix)).recreateAdoptedPodDisruptionBudget(ctx, source.(*policyv1.PodDisruptionBudget).DeepCopy(), binding)
			},
		},
		{
			name: "network policy", kind: "NetworkPolicy", role: "network-policy", resource: "networkpolicies",
			source: &networkingv1.NetworkPolicy{
				TypeMeta:   metav1.TypeMeta{APIVersion: networkingv1.SchemeGroupVersion.String(), Kind: "NetworkPolicy"},
				ObjectMeta: metav1.ObjectMeta{Name: "legacy-network-policy", Namespace: "ops", UID: types.UID("network-policy-old")},
			},
			run: func(ctx context.Context, client *fake.Clientset, store *adoptedSourceStore, job *model.JobTask, binding *adoptedResourceBinding, _ *model.ApplicationComponent, source runtime.Object) error {
				return NewDeployAdoptedNetworkPolicyJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix)).recreateAdoptedNetworkPolicy(ctx, source.(*networkingv1.NetworkPolicy).DeepCopy(), binding)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			meta := testCase.source.(metav1.Object)
			snapshot := adoptedSnapshotResource(
				t,
				testCase.source,
				"backend",
				testCase.role,
				importcontract.OwnershipExclusive,
				importcontract.DispositionManaged,
			)
			store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", snapshot)}
			var component *model.ApplicationComponent
			if testCase.workloadKind != "" {
				component = sourceComponent("app-1", "backend", testCase.workloadKind, meta.GetName(), meta.GetUID())
				store.component = component
			}
			job := &model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops"}
			binding, adopted, err := adoptedResourceForJob(ctx, store, job, testCase.kind, meta.GetNamespace(), meta.GetName())
			require.NoError(t, err)
			require.True(t, adopted)

			followupGetErr := errors.New("follow-up get failed")
			client := fake.NewSimpleClientset()
			client.Fake.PrependReactor("get", testCase.resource, func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, followupGetErr
			})
			client.Fake.PrependReactor("create", testCase.resource, func(action k8stesting.Action) (bool, runtime.Object, error) {
				created := action.(k8stesting.CreateAction).GetObject().(metav1.Object)
				return true, nil, k8serrors.NewAlreadyExists(
					schema.GroupResource{Resource: testCase.resource},
					created.GetName(),
				)
			})

			err = testCase.run(ctx, client, store, job, binding, component, testCase.source)
			require.Error(t, err)
			require.ErrorIs(t, err, followupGetErr)
			require.False(t, k8serrors.IsAlreadyExists(err))
			require.ErrorContains(t, err, "get concurrent recreated adopted")
		})
	}
}

func TestAdoptedDeploymentRecreationAlreadyExistsReconcilesCurrentDesiredAndReadiness(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("deployment-old")
	newUID := types.UID("deployment-new")
	source := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-backend", Namespace: "ops", UID: oldUID},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "backend"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "backend"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "backend", Image: "api:v1"}}},
			},
		},
	}
	desired := source.DeepCopy()
	desired.Spec.Template.Spec.Containers[0].Image = "api:v2"
	saved := adoptedSnapshotResource(t, source, "backend", "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged)
	component := sourceComponent("app-1", "backend", "Deployment", source.Name, oldUID)
	store := &adoptedSourceStore{
		app:       adoptedApplication(t, "app-1", "ops", saved),
		component: component,
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		candidate := action.(k8stesting.CreateAction).GetObject().(*appsv1.Deployment)
		replacement := source.DeepCopy()
		replacement.UID = newUID
		replacement.ResourceVersion = "2"
		replacement.Annotations = map[string]string{
			config.AnnotationAdoptedRecreationToken: candidate.Annotations[config.AnnotationAdoptedRecreationToken],
		}
		replacement.Spec.Template.Annotations = map[string]string{config.AnnotationJobTaskID: "stale-task"}
		require.NoError(t, client.Tracker().Add(replacement))
		return true, nil, k8serrors.NewAlreadyExists(schema.GroupResource{Resource: "deployments"}, candidate.Name)
	})
	job := &model.JobTask{
		Name: "backend", AppID: "app-1", Namespace: "ops", TaskID: "current-task", JobInfo: desired,
	}
	controller := NewDeployJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	require.NoError(t, controller.run(ctx))
	live, err := client.AppsV1().Deployments("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "api:v2", live.Spec.Template.Spec.Containers[0].Image)
	require.Equal(t, "current-task", live.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, map[string]string{config.AnnotationJobTaskID: "current-task"}, controller.expectedPodTemplateAnnotations)
	require.Equal(t, 1, countClientActions(client, "update", "deployments"))
	require.Equal(t, string(newUID), *component.SourceWorkloadUID)
}

func TestAdoptedStatefulSetRecreationAlreadyExistsRestoresRetentionAndReconcilesCurrentDesired(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("statefulset-old")
	newUID := types.UID("statefulset-new")
	deletePolicy := appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	source := &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-database", Namespace: "ops", UID: oldUID},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "database",
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": "database"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "database"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "database", Image: "db:v1"}}},
			},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: deletePolicy,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
	desired := source.DeepCopy()
	desired.Spec.Template.Spec.Containers[0].Image = "db:v2"
	saved := adoptedSnapshotResource(t, source, "database", "workload", importcontract.OwnershipExclusive, importcontract.DispositionManaged)
	component := sourceComponent("app-1", "database", "StatefulSet", source.Name, oldUID)
	store := &adoptedSourceStore{
		app:       adoptedApplication(t, "app-1", "ops", saved),
		component: component,
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "statefulsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		candidate := action.(k8stesting.CreateAction).GetObject().(*appsv1.StatefulSet)
		replacement := source.DeepCopy()
		replacement.UID = newUID
		replacement.ResourceVersion = "2"
		replacement.Annotations = map[string]string{
			config.AnnotationAdoptedRecreationToken:             candidate.Annotations[config.AnnotationAdoptedRecreationToken],
			config.AnnotationAdoptedStatefulSetRetentionRestore: string(deletePolicy),
		}
		replacement.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
		require.NoError(t, client.Tracker().Add(replacement))
		return true, nil, k8serrors.NewAlreadyExists(schema.GroupResource{Resource: "statefulsets"}, candidate.Name)
	})
	job := &model.JobTask{
		Name: "database", AppID: "app-1", Namespace: "ops", TaskID: "current-task", JobInfo: desired,
	}
	controller := NewDeployStatefulSetJobCtl(job, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	require.NoError(t, controller.run(ctx))
	live, err := client.AppsV1().StatefulSets("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "db:v2", live.Spec.Template.Spec.Containers[0].Image)
	require.Equal(t, deletePolicy, live.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	require.NotContains(t, live.Annotations, config.AnnotationAdoptedStatefulSetRetentionRestore)
	require.Equal(t, "current-task", live.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, map[string]string{config.AnnotationJobTaskID: "current-task"}, controller.expectedPodTemplateAnnotations)
	require.Equal(t, 2, countClientActions(client, "update", "statefulsets"))
	require.Equal(t, string(newUID), *component.SourceWorkloadUID)
}

func TestAdoptedSecretRecreationAlreadyExistsRepairsCiphertextManagedData(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	const appID = "app-1"
	keyring := testImportSecretKeyring(t, "active", map[string][]byte{"active": bytes.Repeat([]byte{9}, 32)})
	oldUID := types.UID("secret-old")
	newUID := types.UID("secret-new")
	source := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: oldUID},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("source-password")},
	}
	saved := adoptedSnapshotResource(t, source, "database", "secret", importcontract.OwnershipExclusive, importcontract.DispositionManaged)
	store := &adoptedSourceStore{
		app:       adoptedApplication(t, appID, "ops", saved),
		component: adoptedSecretComponent(t, keyring, appID, "database", source),
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		candidate := action.(k8stesting.CreateAction).GetObject().(*corev1.Secret)
		replacement := source.DeepCopy()
		replacement.UID = newUID
		replacement.ResourceVersion = "2"
		replacement.Annotations = map[string]string{
			config.AnnotationAdoptedRecreationToken: candidate.Annotations[config.AnnotationAdoptedRecreationToken],
			"external.example/revision":             "preserve",
		}
		replacement.Data["password"] = []byte("stale-password")
		replacement.Data["external-token"] = []byte("external-value")
		require.NoError(t, client.Tracker().Add(replacement))
		return true, nil, k8serrors.NewAlreadyExists(schema.GroupResource{Resource: "secrets"}, candidate.Name)
	})
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        source.Name,
			Namespace:   source.Namespace,
			Annotations: map[string]string{"eruun.example/current": "true"},
		},
	}
	controller := newTestAdoptedSecretController(
		&model.JobTask{Name: "database", AppID: appID, Namespace: "ops", JobInfo: desired},
		client,
		store,
		keyring,
	)

	require.NoError(t, controller.run(ctx))
	live, err := client.CoreV1().Secrets("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []byte("source-password"), live.Data["password"])
	require.Equal(t, []byte("external-value"), live.Data["external-token"])
	require.Equal(t, "preserve", live.Annotations["external.example/revision"])
	require.Equal(t, "true", live.Annotations["eruun.example/current"])
	require.Equal(t, 1, countClientActions(client, "update", "secrets"))
	require.Equal(t, 1, store.componentCASCount)
	require.Equal(t, string(newUID), decodeTestAdoptionSnapshot(t, store.app).Resources[0].Source.UID)
	envelopes, err := decodeAdoptedSecretEnvelopes(store.component.AdoptedSecretData)
	require.NoError(t, err)
	envelope := envelopes[source.Name]["password"]
	require.NotEmpty(t, envelope.Ciphertext)
	require.NotContains(t, envelope.Ciphertext, "source-password")
	plaintext, err := keyring.Decrypt(
		envelope,
		importsecret.ResourceAAD(appID, source.Namespace, source.APIVersion, source.Kind, source.Name, "password"),
	)
	require.NoError(t, err)
	require.Equal(t, []byte("source-password"), plaintext)
	require.Empty(t, desired.Data)
}

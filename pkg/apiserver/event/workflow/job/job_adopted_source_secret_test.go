package job

import (
	"bytes"
	"context"

	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/importsecret"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestDeploySecretJobCtlRunAdoptedRejectsPlaintextTaskWrite(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("secret-uid")
	live := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: uid},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("current")},
	}
	resourceSnapshot := adoptedSnapshotResource(
		t,
		live,
		"mysql",
		"secret",
		adoption.OwnershipExclusive,
		adoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", resourceSnapshot)}
	desired := live.DeepCopy()
	desired.Data["password"] = []byte("replacement")
	client := fake.NewSimpleClientset(live)
	ctl := NewDeploySecretJobCtl(
		&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
		nil,
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must not contain plaintext")
	require.Equal(t, 0, countClientActions(client, "update", "secrets"))
	require.Equal(t, 0, countClientActions(client, "create", "secrets"))
}

func TestAdoptedNonSecretDependenciesRecreateFromSnapshotAndRotateUID(t *testing.T) {
	t.Run("service", func(t *testing.T) {
		ctx := WithCleanupTracker(context.Background())
		oldUID := types.UID("service-old")
		newUID := types.UID("service-new")
		source := &corev1.Service{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
			ObjectMeta: metav1.ObjectMeta{Name: "backend-service", Namespace: "ops", UID: oldUID},
			Spec: corev1.ServiceSpec{
				Type:      corev1.ServiceTypeClusterIP,
				ClusterIP: "10.0.0.20",
				Selector:  map[string]string{"app": "backend"},
				Ports:     []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8080)}},
			},
		}
		saved := adoptedSnapshotResource(t, source, "backend", "service", adoption.OwnershipExclusive, adoption.DispositionManaged)
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
		client := fake.NewSimpleClientset()
		client.Fake.PrependReactor("create", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
			object := action.(k8stesting.CreateAction).GetObject().(*corev1.Service)
			object.UID = newUID
			object.ResourceVersion = "11"
			return false, nil, nil
		})
		desired := applyv1.Service(source.Name, source.Namespace).
			WithSpec(applyv1.ServiceSpec().
				WithType(corev1.ServiceTypeClusterIP).
				WithSelector(map[string]string{"app": "backend"}).
				WithPorts(applyv1.ServicePort().WithName("http").WithPort(80).WithTargetPort(intstr.FromInt32(8080))))
		ctl := NewDeployServiceJobCtl(
			&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployService), JobInfo: desired},
			client,
			store,
			func() {},
			locker.NewNoopLocker(shareLockerPrefix),
		)

		require.NoError(t, ctl.run(ctx))
		created, err := client.CoreV1().Services("ops").Get(ctx, source.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, newUID, created.UID)
		require.Equal(t, "10.0.0.20", created.Spec.ClusterIP)
		require.Equal(t, string(newUID), decodeTestAdoptionSnapshot(t, store.app).Resources[0].Source.UID)
	})

	t.Run("ingress", func(t *testing.T) {
		ctx := WithCleanupTracker(context.Background())
		oldUID := types.UID("ingress-old")
		newUID := types.UID("ingress-new")
		pathType := networkingv1.PathTypePrefix
		source := &networkingv1.Ingress{
			TypeMeta:   metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "Ingress"},
			ObjectMeta: metav1.ObjectMeta{Name: "backend-ingress", Namespace: "ops", UID: oldUID},
			Spec: networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{
				Host: "example.test",
				IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Path:     "/",
						PathType: &pathType,
						Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
							Name: "backend-service",
							Port: networkingv1.ServiceBackendPort{Number: 80},
						}},
					}},
				}},
			}}},
		}
		saved := adoptedSnapshotResource(t, source, "backend", "ingress", adoption.OwnershipExclusive, adoption.DispositionManaged)
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
		client := fake.NewSimpleClientset()
		client.Fake.PrependReactor("create", "ingresses", func(action k8stesting.Action) (bool, runtime.Object, error) {
			object := action.(k8stesting.CreateAction).GetObject().(*networkingv1.Ingress)
			object.UID = newUID
			object.ResourceVersion = "12"
			return false, nil, nil
		})
		ctl := NewDeployIngressJobCtl(
			&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployIngress), JobInfo: source.DeepCopy()},
			client,
			store,
			func() {},
			locker.NewNoopLocker(shareLockerPrefix),
		)

		require.NoError(t, ctl.run(ctx))
		created, err := client.NetworkingV1().Ingresses("ops").Get(ctx, source.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, newUID, created.UID)
		require.Equal(t, string(newUID), decodeTestAdoptionSnapshot(t, store.app).Resources[0].Source.UID)
	})

	t.Run("configmap", func(t *testing.T) {
		ctx := WithCleanupTracker(context.Background())
		oldUID := types.UID("configmap-old")
		newUID := types.UID("configmap-new")
		source := &corev1.ConfigMap{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{Name: "backend-config", Namespace: "ops", UID: oldUID},
			Data:       map[string]string{"application.yaml": "source", "external.conf": "preserved"},
		}
		saved := adoptedSnapshotResource(t, source, "backend", "configmap", adoption.OwnershipExclusive, adoption.DispositionManaged)
		store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
		client := fake.NewSimpleClientset()
		client.Fake.PrependReactor("create", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
			object := action.(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap)
			object.UID = newUID
			object.ResourceVersion = "13"
			return false, nil, nil
		})
		desired := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace},
			Data:       map[string]string{"application.yaml": "updated"},
		}
		ctl := NewDeployConfigMapJobCtl(
			&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployConfigMap), JobInfo: desired},
			client,
			store,
			func() {},
			locker.NewNoopLocker(shareLockerPrefix),
			nil,
		)

		require.NoError(t, ctl.run(ctx))
		created, err := client.CoreV1().ConfigMaps("ops").Get(ctx, source.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, newUID, created.UID)
		require.Equal(t, "updated", created.Data["application.yaml"])
		require.Equal(t, "preserved", created.Data["external.conf"])
		require.Equal(t, string(newUID), decodeTestAdoptionSnapshot(t, store.app).Resources[0].Source.UID)
	})
}

func TestDeploySecretJobCtlRunAdoptedPreviousKeyRotatesOnKubernetesNoop(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	appID := "app-1"
	oldKey := bytes.Repeat([]byte{1}, 32)
	activeKey := bytes.Repeat([]byte{2}, 32)
	oldKeyring := testImportSecretKeyring(t, "old", map[string][]byte{"old": oldKey})
	rotatedKeyring := testImportSecretKeyring(t, "active", map[string][]byte{"old": oldKey, "active": activeKey})
	source := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: types.UID("secret-uid")},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("source-password")},
	}
	saved := adoptedSnapshotResource(t, source, "mysql", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
	component := adoptedSecretComponent(t, oldKeyring, appID, "mysql", source)
	store := &adoptedSourceStore{
		app:       adoptedApplication(t, appID, "ops", saved),
		component: component,
	}
	live := source.DeepCopy()
	live.Annotations = map[string]string{"external.example/revision": "preserve"}
	live.Data["external-token"] = []byte("external-value")
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace},
		Type:       source.Type,
	}
	client := fake.NewSimpleClientset(live)
	ctl := newTestAdoptedSecretController(
		&model.JobTask{Name: "mysql", AppID: appID, Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
		client,
		store,
		rotatedKeyring,
	)

	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 0, countClientActions(client, "update", "secrets"))
	require.Equal(t, 1, store.componentCASCount)
	require.Equal(t, 0, store.applicationCASCount)
	envelopes, err := decodeAdoptedSecretEnvelopes(store.component.AdoptedSecretData)
	require.NoError(t, err)
	rotatedEnvelope := envelopes[source.Name]["password"]
	require.Equal(t, "active", rotatedEnvelope.KeyID)
	plaintext, err := rotatedKeyring.Decrypt(
		rotatedEnvelope,
		importsecret.ResourceAAD(appID, source.Namespace, source.APIVersion, source.Kind, source.Name, "password"),
	)
	require.NoError(t, err)
	require.Equal(t, []byte("source-password"), plaintext)
	preserved, err := client.CoreV1().Secrets("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []byte("external-value"), preserved.Data["external-token"])
	require.Equal(t, "preserve", preserved.Annotations["external.example/revision"])
	require.Empty(t, desired.Data)
	require.Empty(t, desired.StringData)
}

func TestPersistRotatedAdoptedSecretDataRebasesStaleSourcesAndRetriesCAS(t *testing.T) {
	const (
		appID         = "app-1"
		componentName = "backend"
		firstSource   = "database-secret"
		secondSource  = "registry-secret"
	)
	envelope := func(keyID, ciphertext string) importsecret.Envelope {
		return importsecret.Envelope{
			Version:    "v1",
			KeyID:      keyID,
			Nonce:      "nonce",
			Ciphertext: ciphertext,
		}
	}
	initial := map[string]map[string]importsecret.Envelope{
		firstSource:  {"password": envelope("old", "first-old")},
		secondSource: {"token": envelope("old", "second-old")},
	}
	initialJSON, err := model.NewJSONStructByStruct(initial)
	require.NoError(t, err)
	component := &model.ApplicationComponent{
		AppID:             appID,
		Name:              componentName,
		AdoptedSecretData: initialJSON,
	}
	store := &adoptedSourceStore{
		component:             component,
		componentCASConflicts: 1,
	}
	staleUpdate := func(sourceName, key, ciphertext string) adoptedSecretCiphertextUpdate {
		payload := map[string]map[string]importsecret.Envelope{
			firstSource:  {"password": envelope("old", "first-old")},
			secondSource: {"token": envelope("old", "second-old")},
		}
		payload[sourceName][key] = envelope("active", ciphertext)
		encoded, encodeErr := model.NewJSONStructByStruct(payload)
		require.NoError(t, encodeErr)
		copy := *component
		copy.AdoptedSecretData = encoded
		return adoptedSecretCiphertextUpdate{
			component:     &copy,
			encryptedData: encoded,
		}
	}
	runtime := newJobRuntime(nil, nil, nil, nil, nil, nil)
	defer runtime.close()

	require.NoError(t, persistRotatedAdoptedSecretData(
		context.Background(),
		store,
		appID,
		firstSource,
		[]adoptedSecretCiphertextUpdate{staleUpdate(firstSource, "password", "first-active")},
		runtime,
	))
	require.NoError(t, persistRotatedAdoptedSecretData(
		context.Background(),
		store,
		appID,
		secondSource,
		[]adoptedSecretCiphertextUpdate{staleUpdate(secondSource, "token", "second-active")},
		runtime,
	))

	persisted, err := decodeAdoptedSecretEnvelopes(store.component.AdoptedSecretData)
	require.NoError(t, err)
	require.Equal(t, "active", persisted[firstSource]["password"].KeyID)
	require.Equal(t, "first-active", persisted[firstSource]["password"].Ciphertext)
	require.Equal(t, "active", persisted[secondSource]["token"].KeyID)
	require.Equal(t, "second-active", persisted[secondSource]["token"].Ciphertext)
	require.Equal(t, 2, store.componentCASCount)
	require.Zero(t, store.componentCASConflicts)
}

func TestDeploySecretJobCtlRunAdoptedPreviousKeyRotationPersistenceFailureIsNotIgnored(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	appID := "app-1"
	oldKey := bytes.Repeat([]byte{13}, 32)
	activeKey := bytes.Repeat([]byte{14}, 32)
	oldKeyring := testImportSecretKeyring(t, "old", map[string][]byte{"old": oldKey})
	rotatedKeyring := testImportSecretKeyring(t, "active", map[string][]byte{"old": oldKey, "active": activeKey})
	source := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: types.UID("secret-uid")},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("source-password")},
	}
	saved := adoptedSnapshotResource(t, source, "mysql", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
	component := adoptedSecretComponent(t, oldKeyring, appID, "mysql", source)
	store := &adoptedSourceStore{
		app:       adoptedApplication(t, appID, "ops", saved),
		component: component,
		putErr:    errors.New("database unavailable"),
	}
	client := fake.NewSimpleClientset(source.DeepCopy())
	desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace}}
	ctl := newTestAdoptedSecretController(
		&model.JobTask{Name: "mysql", AppID: appID, Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
		client,
		store,
		rotatedKeyring,
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "compare and swap adopted secret ciphertext")
	require.Equal(t, 0, countClientActions(client, "update", "secrets"))
	envelopes, decodeErr := decodeAdoptedSecretEnvelopes(store.component.AdoptedSecretData)
	require.NoError(t, decodeErr)
	require.Equal(t, "old", envelopes[source.Name]["password"].KeyID)
}

func TestDeploySecretJobCtlRunAdoptedSharedByTargetComponentsRotatesEveryHolder(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	appID := "app-1"
	oldKey := bytes.Repeat([]byte{11}, 32)
	activeKey := bytes.Repeat([]byte{12}, 32)
	oldKeyring := testImportSecretKeyring(t, "old", map[string][]byte{"old": oldKey})
	rotatedKeyring := testImportSecretKeyring(t, "active", map[string][]byte{"old": oldKey, "active": activeKey})
	source := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "shared-database-secret", Namespace: "ops", UID: types.UID("secret-uid")},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("source-password")},
	}
	saved := adoptedSnapshotResource(t, source, "", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
	backend := adoptedSecretComponent(t, oldKeyring, appID, "backend", source)
	mysql := adoptedSecretComponent(t, oldKeyring, appID, "mysql", source)
	mysql.ID = backend.ID + 1
	store := &adoptedSourceStore{
		app:        adoptedApplication(t, appID, "ops", saved),
		components: []*model.ApplicationComponent{backend, mysql},
	}
	client := fake.NewSimpleClientset(source.DeepCopy())
	desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace}}
	ctl := newTestAdoptedSecretController(
		&model.JobTask{Name: "mysql", AppID: appID, Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
		client,
		store,
		rotatedKeyring,
	)

	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 0, countClientActions(client, "update", "secrets"))
	require.Equal(t, 2, store.componentCASCount)
	for _, component := range store.components {
		envelopes, err := decodeAdoptedSecretEnvelopes(component.AdoptedSecretData)
		require.NoError(t, err)
		require.Equal(t, "active", envelopes[source.Name]["password"].KeyID)
	}
}

func TestDeploySecretJobCtlRunAdoptedRejectsCiphertextFailureBeforeKubernetesAccess(t *testing.T) {
	testCases := []struct {
		name   string
		mutate func(*importsecret.Envelope)
	}{
		{
			name: "tampered ciphertext",
			mutate: func(envelope *importsecret.Envelope) {
				envelope.Ciphertext += "A"
			},
		},
		{
			name: "unknown key",
			mutate: func(envelope *importsecret.Envelope) {
				envelope.KeyID = "removed"
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := WithCleanupTracker(context.Background())
			appID := "app-1"
			keyring := testImportSecretKeyring(t, "active", map[string][]byte{"active": bytes.Repeat([]byte{3}, 32)})
			source := &corev1.Secret{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
				ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: types.UID("secret-uid")},
				Type:       corev1.SecretTypeOpaque,
				Data:       map[string][]byte{"password": []byte("source-password")},
			}
			saved := adoptedSnapshotResource(t, source, "mysql", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
			component := adoptedSecretComponent(t, keyring, appID, "mysql", source)
			payload, err := decodeAdoptedSecretEnvelopes(component.AdoptedSecretData)
			require.NoError(t, err)
			envelope := payload[source.Name]["password"]
			testCase.mutate(&envelope)
			payload[source.Name]["password"] = envelope
			component.AdoptedSecretData, err = model.NewJSONStructByStruct(payload)
			require.NoError(t, err)
			store := &adoptedSourceStore{
				app:       adoptedApplication(t, appID, "ops", saved),
				component: component,
			}
			client := fake.NewSimpleClientset(source.DeepCopy())
			desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace}}
			ctl := newTestAdoptedSecretController(
				&model.JobTask{Name: "mysql", AppID: appID, Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
				client,
				store,
				keyring,
			)

			err = ctl.run(ctx)
			require.Error(t, err)
			require.Empty(t, client.Actions())
			require.Equal(t, 0, store.componentCASCount)
			require.Equal(t, 0, store.applicationCASCount)
		})
	}
}

func TestDeploySecretJobCtlRunAdoptedPreservesUnknownFieldsAndRepairsManagedKeys(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	appID := "app-1"
	keyring := testImportSecretKeyring(t, "active", map[string][]byte{"active": bytes.Repeat([]byte{4}, 32)})
	source := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: types.UID("secret-uid")},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("source-password")},
	}
	saved := adoptedSnapshotResource(t, source, "mysql", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
	component := adoptedSecretComponent(t, keyring, appID, "mysql", source)
	store := &adoptedSourceStore{
		app:       adoptedApplication(t, appID, "ops", saved),
		component: component,
	}
	live := source.DeepCopy()
	live.Labels = map[string]string{"external.example/owner": "database-team"}
	live.Annotations = map[string]string{"external.example/revision": "preserve"}
	live.Data["password"] = []byte("drifted-password")
	live.Data["external-token"] = []byte("external-value")
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      source.Name,
			Namespace: source.Namespace,
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun},
		},
		Type: source.Type,
	}
	client := fake.NewSimpleClientset(live)
	ctl := newTestAdoptedSecretController(
		&model.JobTask{Name: "mysql", AppID: appID, Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
		client,
		store,
		keyring,
	)

	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 1, countClientActions(client, "update", "secrets"))
	require.Equal(t, 0, store.componentCASCount)
	updated, err := client.CoreV1().Secrets("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []byte("source-password"), updated.Data["password"])
	require.Equal(t, []byte("external-value"), updated.Data["external-token"])
	require.Equal(t, "database-team", updated.Labels["external.example/owner"])
	require.Equal(t, config.ManagedByEruun, updated.Labels[config.LabelManagedBy])
	require.Equal(t, "preserve", updated.Annotations["external.example/revision"])
}

func TestDeploySecretJobCtlRunAdoptedRejectsUIDReplacement(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	appID := "app-1"
	keyring := testImportSecretKeyring(t, "active", map[string][]byte{"active": bytes.Repeat([]byte{5}, 32)})
	source := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: types.UID("source-uid")},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("source-password")},
	}
	saved := adoptedSnapshotResource(t, source, "mysql", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
	store := &adoptedSourceStore{
		app:       adoptedApplication(t, appID, "ops", saved),
		component: adoptedSecretComponent(t, keyring, appID, "mysql", source),
	}
	replacement := source.DeepCopy()
	replacement.UID = types.UID("replacement-uid")
	client := fake.NewSimpleClientset(replacement)
	desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace}}
	ctl := newTestAdoptedSecretController(
		&model.JobTask{Name: "mysql", AppID: appID, Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
		client,
		store,
		keyring,
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ownership conflict")
	require.Equal(t, 0, countClientActions(client, "update", "secrets"))
	require.Equal(t, 0, countClientActions(client, "create", "secrets"))
	require.Equal(t, 0, countClientActions(client, "delete", "secrets"))
}

func TestDeploySecretJobCtlRunAdoptedRejectsImmutableDifferences(t *testing.T) {
	testCases := []struct {
		name         string
		mutateLive   func(*corev1.Secret)
		mutateIntent func(*corev1.Secret)
		wantError    string
	}{
		{
			name: "managed payload drift",
			mutateLive: func(secret *corev1.Secret) {
				secret.Data["password"] = []byte("drifted-password")
			},
			mutateIntent: func(*corev1.Secret) {},
			wantError:    "immutable; managed payload differs",
		},
		{
			name:       "immutable flag change",
			mutateLive: func(*corev1.Secret) {},
			mutateIntent: func(secret *corev1.Secret) {
				value := false
				secret.Immutable = &value
			},
			wantError: "immutable flag changes are forbidden",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := WithCleanupTracker(context.Background())
			appID := "app-1"
			keyring := testImportSecretKeyring(t, "active", map[string][]byte{"active": bytes.Repeat([]byte{6}, 32)})
			immutable := true
			source := &corev1.Secret{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
				ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: types.UID("secret-uid")},
				Type:       corev1.SecretTypeOpaque,
				Immutable:  &immutable,
				Data:       map[string][]byte{"password": []byte("source-password")},
			}
			saved := adoptedSnapshotResource(t, source, "mysql", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
			store := &adoptedSourceStore{
				app:       adoptedApplication(t, appID, "ops", saved),
				component: adoptedSecretComponent(t, keyring, appID, "mysql", source),
			}
			live := source.DeepCopy()
			testCase.mutateLive(live)
			desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace}}
			testCase.mutateIntent(desired)
			client := fake.NewSimpleClientset(live)
			ctl := newTestAdoptedSecretController(
				&model.JobTask{Name: "mysql", AppID: appID, Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
				client,
				store,
				keyring,
			)

			err := ctl.run(ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.wantError)
			require.Equal(t, 0, countClientActions(client, "update", "secrets"))
		})
	}
}

func TestDeploySecretJobCtlRunAdoptedMissingRecreatesAndAtomicallyRotatesBindings(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	appID := "app-1"
	keyring := testImportSecretKeyring(t, "active", map[string][]byte{"active": bytes.Repeat([]byte{7}, 32)})
	oldUID := types.UID("secret-old")
	newUID := types.UID("secret-new")
	source := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: oldUID, Labels: map[string]string{"source": "imported"}},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("source-password")},
	}
	saved := adoptedSnapshotResource(t, source, "mysql", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
	store := &adoptedSourceStore{
		app:       adoptedApplication(t, appID, "ops", saved),
		component: adoptedSecretComponent(t, keyring, appID, "mysql", source),
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*corev1.Secret)
		object.UID = newUID
		object.ResourceVersion = "31"
		return false, nil, nil
	})
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        source.Name,
			Namespace:   source.Namespace,
			Annotations: map[string]string{"eruun.example/adopted": "true"},
		},
	}
	ctl := newTestAdoptedSecretController(
		&model.JobTask{Name: "mysql", AppID: appID, Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
		client,
		store,
		keyring,
	)

	require.NoError(t, ctl.run(ctx))
	created, err := client.CoreV1().Secrets("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newUID, created.UID)
	require.Equal(t, []byte("source-password"), created.Data["password"])
	require.Equal(t, "imported", created.Labels["source"])
	require.Equal(t, "true", created.Annotations["eruun.example/adopted"])
	require.Equal(t, 1, store.componentCASCount)
	require.Equal(t, 2, store.applicationCASCount)
	require.Equal(t, string(newUID), decodeTestAdoptionSnapshot(t, store.app).Resources[0].Source.UID)
	require.Empty(t, desired.Data)
	require.Empty(t, desired.StringData)
}

func TestDeploySecretJobCtlRunAdoptedRecreationPersistenceFailureRetainsLiveObjectAndPendingClaim(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	appID := "app-1"
	keyring := testImportSecretKeyring(t, "active", map[string][]byte{"active": bytes.Repeat([]byte{8}, 32)})
	oldUID := types.UID("secret-old")
	newUID := types.UID("secret-new")
	source := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: oldUID},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("source-password")},
	}
	saved := adoptedSnapshotResource(t, source, "mysql", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
	store := &adoptedSourceStore{
		app:                   adoptedApplication(t, appID, "ops", saved),
		component:             adoptedSecretComponent(t, keyring, appID, "mysql", source),
		componentCASConflicts: 1,
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*corev1.Secret)
		object.UID = newUID
		return false, nil, nil
	})
	desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace}}
	ctl := newTestAdoptedSecretController(
		&model.JobTask{Name: "mysql", AppID: appID, Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
		client,
		store,
		keyring,
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "persist recreated adopted secret binding")
	require.Contains(t, err.Error(), "pending claim retained")
	require.Equal(t, 1, countClientActions(client, "create", "secrets"))
	require.Equal(t, 0, countClientActions(client, "delete", "secrets"))
	live, getErr := client.CoreV1().Secrets("ops").Get(ctx, source.Name, metav1.GetOptions{})
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

func TestDeploySecretJobCtlRunAdoptedDispositionGatePreventsKubernetesWrites(t *testing.T) {
	testCases := []struct {
		name        string
		ownership   string
		disposition string
		wantError   bool
	}{
		{
			name:        "shared",
			ownership:   adoption.OwnershipShared,
			disposition: adoption.DispositionSharedPreserved,
		},
		{
			name:        "external",
			ownership:   adoption.OwnershipExternal,
			disposition: adoption.DispositionExcluded,
		},
		{
			name:        "blocked",
			ownership:   adoption.OwnershipExclusive,
			disposition: adoption.DispositionBlocked,
			wantError:   true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := WithCleanupTracker(context.Background())
			source := &corev1.Secret{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
				ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: types.UID("secret-uid")},
				Type:       corev1.SecretTypeOpaque,
				Data:       map[string][]byte{"password": []byte("source-password")},
			}
			saved := adoptedSnapshotResource(t, source, "mysql", "secret", testCase.ownership, testCase.disposition)
			store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
			client := fake.NewSimpleClientset(source.DeepCopy())
			desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace}}
			ctl := newTestAdoptedSecretController(
				&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: desired},
				client,
				store,
				nil,
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

func TestDeploySecretJobCtlRunAdoptedRejectsPlaintextJobInfoBeforeNetworkOrKubernetes(t *testing.T) {
	source := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "database-secret", Namespace: "ops", UID: types.UID("secret-uid")},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("source-password")},
	}
	saved := adoptedSnapshotResource(t, source, "mysql", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged)
	testCases := []struct {
		name    string
		jobInfo interface{}
	}{
		{
			name: "secret data",
			jobInfo: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace},
				Data:       map[string][]byte{"password": []byte("forbidden")},
			},
		},
		{
			name: "secret stringData",
			jobInfo: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace},
				StringData: map[string]string{"password": "forbidden"},
			},
		},
		{
			name: "secret input data",
			jobInfo: &model.SecretInput{
				Name:      source.Name,
				Namespace: source.Namespace,
				Data:      map[string]string{"password": "forbidden"},
			},
		},
		{
			name: "secret input URL",
			jobInfo: &model.SecretInput{
				Name:      source.Name,
				Namespace: source.Namespace,
				URL:       "http://127.0.0.1:1/must-not-fetch",
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := WithCleanupTracker(context.Background())
			store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
			client := fake.NewSimpleClientset(source.DeepCopy())
			ctl := newTestAdoptedSecretController(
				&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeploySecret), JobInfo: testCase.jobInfo},
				client,
				store,
				nil,
			)

			err := ctl.run(ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "must not contain plaintext")
			require.Empty(t, client.Actions())
		})
	}
}

func TestInitJobCtlInjectsImportSecretKeyringWithoutMutatingJobInfo(t *testing.T) {
	keyring := testImportSecretKeyring(t, "active", map[string][]byte{"active": bytes.Repeat([]byte{10}, 32)})
	jobInfo := &model.SecretInput{Name: "native-secret", Namespace: "ops"}
	jobTask := &model.JobTask{
		Name:      "native-secret",
		AppID:     "app-1",
		Namespace: "ops",
		JobType:   string(config.JobDeploySecret),
		JobInfo:   jobInfo,
	}
	runtime := newJobRuntime(nil, nil, nil, nil, nil, keyring)
	defer runtime.close()

	controller := initJobCtl(
		jobTask,
		fake.NewSimpleClientset(),
		&noopStore{},
		func() {},
		runtime,
	)
	secretController, ok := controller.(*DeploySecretJobCtl)
	require.True(t, ok)
	require.Same(t, keyring, secretController.importSecretKeyring)
	require.Same(t, jobInfo, jobTask.JobInfo)
	require.Empty(t, jobInfo.Data)
	require.Empty(t, jobInfo.URL)
}

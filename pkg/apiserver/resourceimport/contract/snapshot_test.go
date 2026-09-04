package contract

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestResourceSnapshotStripsRuntimeAndSecretPayload(t *testing.T) {
	secret := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]interface{}{
			"name":            "mysql",
			"namespace":       "prod",
			"uid":             "uid-1",
			"resourceVersion": "17",
			"managedFields":   []interface{}{map[string]interface{}{"manager": "kubectl"}},
			"annotations": map[string]interface{}{
				"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"v1","kind":"Secret","data":{"password":"c2VjcmV0"}}`,
				"example.com/plaintext":                            "secret-password",
			},
		},
		"type": "Opaque",
		"data": map[string]interface{}{
			"password": "c2VjcmV0",
			"username": "cm9vdA==",
		},
	}}
	secret.SetUID(types.UID("uid-1"))
	secret.SetResourceVersion("17")

	snapshot, err := ResourceSnapshotFromObject(secret, "mysql", "secret", OwnershipExclusive, DispositionManaged)
	require.NoError(t, err)
	require.Equal(t, []string{"password", "username"}, snapshot.SecretKeys)
	require.NotContains(t, string(snapshot.Manifest), "c2VjcmV0")
	require.NotContains(t, string(snapshot.Manifest), "secret-password")
	require.NotContains(t, string(snapshot.Manifest), "last-applied-configuration")
	require.NotContains(t, string(snapshot.Manifest), "resourceVersion")
	require.NotContains(t, string(snapshot.Manifest), "managedFields")
	require.Equal(t, "uid-1", snapshot.Source.UID)
	require.Equal(t, "17", snapshot.Source.ResourceVersion)
	require.Len(t, snapshot.Source.SpecDigest, 64)
}

func TestDigestDoesNotDeriveFromSecretValues(t *testing.T) {
	first := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]interface{}{"name": "db", "namespace": "prod"},
		"data":       map[string]interface{}{"password": "first-value"},
	}}
	second := first.DeepCopy()
	require.NoError(t, unstructured.SetNestedField(second.Object, "second-value", "data", "password"))
	firstDigest, err := DigestObject(first)
	require.NoError(t, err)
	secondDigest, err := DigestObject(second)
	require.NoError(t, err)
	require.Equal(t, firstDigest, secondDigest)
	require.NotContains(t, firstDigest, "first-value")

	withAnotherKey := first.DeepCopy()
	require.NoError(t, unstructured.SetNestedField(
		withAnotherKey.Object,
		map[string]interface{}{"password": "first-value", "username": "root"},
		"data",
	))
	keyDigest, err := DigestObject(withAnotherKey)
	require.NoError(t, err)
	require.NotEqual(t, firstDigest, keyDigest)
}

func TestSecretResourceVersionCarriesValueDriftWithoutChangingDigest(t *testing.T) {
	first := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]interface{}{
			"name":            "db",
			"namespace":       "prod",
			"uid":             "secret-uid",
			"resourceVersion": "10",
		},
		"data": map[string]interface{}{"password": "first-value"},
	}}
	second := first.DeepCopy()
	second.SetResourceVersion("11")
	require.NoError(t, unstructured.SetNestedField(second.Object, "second-value", "data", "password"))

	firstSnapshot, err := ResourceSnapshotFromObject(first, "db", "secret", OwnershipExclusive, DispositionManaged)
	require.NoError(t, err)
	secondSnapshot, err := ResourceSnapshotFromObject(second, "db", "secret", OwnershipExclusive, DispositionManaged)
	require.NoError(t, err)

	require.Equal(t, firstSnapshot.Source.SpecDigest, secondSnapshot.Source.SpecDigest)
	require.Equal(t, "10", firstSnapshot.Source.ResourceVersion)
	require.Equal(t, "11", secondSnapshot.Source.ResourceVersion)
}

func TestSnapshotSortAndValidate(t *testing.T) {
	snapshot := NewSnapshot("prod", []ResourceSnapshot{
		{Source: ResourceIdentity{APIVersion: "v1", Kind: "Service", Name: "b", UID: "2", SpecDigest: "b"}},
		{Source: ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Name: "a", UID: "1", SpecDigest: "a"}},
	})
	require.NoError(t, snapshot.Validate())
	require.Equal(t, "Deployment", snapshot.Resources[0].Source.Kind)
	_, err := json.Marshal(snapshot)
	require.NoError(t, err)
}

func TestSnapshotValidateLegacyAndPendingRecreationVersions(t *testing.T) {
	resource := ResourceSnapshot{
		Source: ResourceIdentity{
			APIVersion: "v1",
			Kind:       "ConfigMap",
			Namespace:  "prod",
			Name:       "settings",
			UID:        "uid-1",
			SpecDigest: "digest",
		},
		DependencyRole: "config",
		Ownership:      OwnershipExclusive,
		Disposition:    DispositionManaged,
		Manifest:       json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap"}`),
	}

	legacy := Snapshot{Version: 1, Namespace: "prod", Resources: []ResourceSnapshot{resource}}
	require.NoError(t, legacy.Validate())

	legacy.Resources[0].PendingRecreation = &RecreationClaim{Token: "claim-1"}
	require.ErrorContains(t, legacy.Validate(), "cannot carry pending recreation")

	current := legacy
	current.Version = SnapshotVersion
	require.NoError(t, current.Validate())

	current.Resources[0].PendingRecreation.Token = ""
	require.ErrorContains(t, current.Validate(), "state is incomplete")
}

func TestRecreationTokenDoesNotAffectSnapshotManifestOrDigest(t *testing.T) {
	base := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      "settings",
			"namespace": "prod",
			"uid":       "uid-1",
		},
		"data": map[string]interface{}{"mode": "safe"},
	}}
	withToken := base.DeepCopy()
	withToken.SetAnnotations(map[string]string{
		config.AnnotationAdoptedRecreationToken: "claim-1",
	})

	baseSnapshot, err := ResourceSnapshotFromObject(base, "app", "config", OwnershipExclusive, DispositionManaged)
	require.NoError(t, err)
	tokenSnapshot, err := ResourceSnapshotFromObject(withToken, "app", "config", OwnershipExclusive, DispositionManaged)
	require.NoError(t, err)

	require.Equal(t, baseSnapshot.Source.SpecDigest, tokenSnapshot.Source.SpecDigest)
	require.JSONEq(t, string(baseSnapshot.Manifest), string(tokenSnapshot.Manifest))
	require.NotContains(t, string(tokenSnapshot.Manifest), config.AnnotationAdoptedRecreationToken)
}

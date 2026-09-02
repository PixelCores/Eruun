package traits

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestAggregateTraitResultsKeepsSameNameAcrossGroupKinds(t *testing.T) {
	objects := []client.Object{
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "tenant"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "tenant"}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "tenant"}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "tenant"}},
	}

	result, err := aggregateTraitResults([]*TraitResult{{AdditionalObjects: objects}})

	require.NoError(t, err)
	require.Equal(t, objects, result.AdditionalObjects)
}

func TestAggregateTraitResultsDeduplicatesIdenticalObject(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "tenant"}}

	result, err := aggregateTraitResults([]*TraitResult{
		{AdditionalObjects: []client.Object{pvc}},
		{AdditionalObjects: []client.Object{pvc.DeepCopy()}},
	})

	require.NoError(t, err)
	require.Len(t, result.AdditionalObjects, 1)
}

func TestAggregateTraitResultsRejectsConflictingObject(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "tenant"},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	conflict := pvc.DeepCopy()
	conflict.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")

	result, err := aggregateTraitResults([]*TraitResult{
		{AdditionalObjects: []client.Object{pvc}},
		{AdditionalObjects: []client.Object{conflict}},
	})

	require.Nil(t, result)
	require.ErrorContains(t, err, "conflicting additional object PersistentVolumeClaim/tenant/data")
}

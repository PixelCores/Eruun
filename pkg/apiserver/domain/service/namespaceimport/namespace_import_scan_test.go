package namespaceimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestScanNamespaceResources_FiltersAssociatedClusterRBAC(t *testing.T) {
	namespace := "target-ns"
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset([]runtime.Object{
			&rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "rb-use-cluster-role", Namespace: namespace},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cr-from-rb"},
				Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa"}},
			},
			&rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "crb-in-namespace"},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cr-from-crb"},
				Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa", Namespace: namespace}},
			},
			&rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "crb-outside-namespace"},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cr-outside"},
				Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa", Namespace: "other-ns"}},
			},
			&rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "crb-cross-namespace"},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cr-cross"},
				Subjects: []rbacv1.Subject{
					{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa", Namespace: namespace},
					{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa", Namespace: "other-ns"},
				},
			},
			&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "cr-from-rb"}},
			&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "cr-from-crb"}},
			&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "cr-outside"}},
			&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "cr-cross"}},
		}...),
	}

	includeKinds := map[string]struct{}{
		importKindClusterRoles:        {},
		importKindClusterRoleBindings: {},
	}
	resources, warnings, err := svc.scanNamespaceResources(context.Background(), namespace, includeKinds)
	require.NoError(t, err)

	kindToNames := make(map[string][]string)
	for _, res := range resources {
		kindToNames[res.kindKey] = append(kindToNames[res.kindKey], res.name)
	}

	assert.ElementsMatch(t, []string{"cr-from-crb", "cr-from-rb"}, kindToNames[importKindClusterRoles])
	assert.ElementsMatch(t, []string{"crb-in-namespace"}, kindToNames[importKindClusterRoleBindings])
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "crb-cross-namespace")
	assert.Contains(t, warnings[0], "references serviceaccounts across namespaces")
}

func TestScanNamespaceResources_SkipsCronJobOwnedJobs(t *testing.T) {
	namespace := "target-ns"
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(
			&batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nightly-report",
					Namespace: namespace,
				},
			},
			&batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nightly-report-123",
					Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{
						{Kind: "CronJob", Name: "nightly-report"},
					},
				},
			},
			&batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "manual-job",
					Namespace: namespace,
				},
			},
		),
	}

	includeKinds := map[string]struct{}{
		importKindJobs:     {},
		importKindCronJobs: {},
	}
	resources, warnings, err := svc.scanNamespaceResources(context.Background(), namespace, includeKinds)
	require.NoError(t, err)
	assert.Empty(t, warnings)

	kindToNames := make(map[string][]string)
	for _, res := range resources {
		kindToNames[res.kindKey] = append(kindToNames[res.kindKey], res.name)
	}

	assert.ElementsMatch(t, []string{"manual-job"}, kindToNames[importKindJobs])
	assert.ElementsMatch(t, []string{"nightly-report"}, kindToNames[importKindCronJobs])
}

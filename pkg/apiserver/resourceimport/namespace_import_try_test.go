package resourceimport

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestTryImportNamespaceResources_DetectsStaleConfigComponent(t *testing.T) {
	const (
		namespace      = "project-ns"
		appID          = "26022513312d88jw"
		existingAppID  = "existing-app-id"
		configMapName  = "mahjongways2-26022513312d88jw-config"
		staleComponent = "legacy-config"
	)
	appName := "mahjongways2-" + appID

	props, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)

	store := newInMemoryAppStore()
	store.apps[existingAppID] = &model.Applications{
		ID:        existingAppID,
		Name:      appName,
		Namespace: namespace,
	}
	store.components[configMapName] = &model.ApplicationComponent{
		ID:            101,
		AppID:         existingAppID,
		Name:          configMapName,
		Namespace:     namespace,
		ComponentType: config.ConfJob,
		Properties:    props,
		Traits:        traits,
	}
	store.components[staleComponent] = &model.ApplicationComponent{
		ID:            102,
		AppID:         existingAppID,
		Name:          staleComponent,
		Namespace:     namespace,
		ComponentType: config.ConfJob,
		Properties:    props,
		Traits:        traits,
	}

	svc := &serviceImpl{
		KubeClient: fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      configMapName,
				Namespace: namespace,
			},
			Data: map[string]string{"key": "value"},
		}),
		AppRepo:       &mockAppRepo{store: store},
		ComponentRepo: &mockComponentRepo{store: store},
	}

	resp, err := svc.TryImportNamespaceResources(context.Background(), apisv1.TryImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		IncludeKinds: []string{importKindConfigMaps},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.Summary.AppsMatched)
	assert.Equal(t, 1, resp.Summary.OrphansDetected)
	require.Len(t, resp.OrphanComponents, 1)
	assert.Equal(t, existingAppID, resp.OrphanComponents[0].AppID)
	assert.Equal(t, appName, resp.OrphanComponents[0].AppName)
	assert.Equal(t, staleComponent, resp.OrphanComponents[0].ComponentName)
	assert.Equal(t, config.ConfJob, resp.OrphanComponents[0].ComponentType)
	assert.Equal(t, tryImportOrphanReasonMissingInNamespaceScan, resp.OrphanComponents[0].Reason)
	assert.Equal(t, []string{importKindConfigMaps}, resp.OrphanComponents[0].MatchedIncludeKinds)
}

func TestTryImportNamespaceResources_OmittedKindsDoNotReportOrphans(t *testing.T) {
	const (
		namespace      = "project-ns"
		appID          = "26022513312d88jw"
		existingAppID  = "existing-app-id"
		deploymentName = "mahjongways2-26022513312d88jw-backend"
		staleConfig    = "legacy-config"
	)
	appName := "mahjongways2-" + appID

	props, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)

	store := newInMemoryAppStore()
	store.apps[existingAppID] = &model.Applications{
		ID:        existingAppID,
		Name:      appName,
		Namespace: namespace,
	}
	store.components[deploymentName] = &model.ApplicationComponent{
		ID:            201,
		AppID:         existingAppID,
		Name:          deploymentName,
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Properties:    props,
		Traits:        traits,
	}
	store.components[staleConfig] = &model.ApplicationComponent{
		ID:            202,
		AppID:         existingAppID,
		Name:          staleConfig,
		Namespace:     namespace,
		ComponentType: config.ConfJob,
		Properties:    props,
		Traits:        traits,
	}

	svc := &serviceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "backend"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "backend", Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		AppRepo:       &mockAppRepo{store: store},
		ComponentRepo: &mockComponentRepo{store: store},
	}

	resp, err := svc.TryImportNamespaceResources(context.Background(), apisv1.TryImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.Summary.AppsMatched)
	assert.Equal(t, 0, resp.Summary.OrphansDetected)
	assert.Empty(t, resp.OrphanComponents)
}

func TestTryImportNamespaceResources_MarksCrossNamespaceClusterRoleBindingAsUnmanaged(t *testing.T) {
	const namespace = "project-ns"

	store := newInMemoryAppStore()
	svc := &serviceImpl{
		KubeClient: fake.NewSimpleClientset(&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "cross-ns-crb"},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "shared-role"},
			Subjects: []rbacv1.Subject{
				{Kind: rbacv1.ServiceAccountKind, Name: "sa-a", Namespace: namespace},
				{Kind: rbacv1.ServiceAccountKind, Name: "sa-b", Namespace: "other-ns"},
			},
		}),
		AppRepo:       &mockAppRepo{store: store},
		ComponentRepo: &mockComponentRepo{store: store},
	}

	resp, err := svc.TryImportNamespaceResources(context.Background(), apisv1.TryImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		IncludeKinds: []string{importKindClusterRoleBindings},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 0, resp.Summary.ResourcesScanned)
	require.NotEmpty(t, resp.Warnings)
	allWarnings := strings.Join(resp.Warnings, " | ")
	assert.Contains(t, allWarnings, "cross-ns-crb")
	assert.Contains(t, allWarnings, "references serviceaccounts across namespaces")
}

func TestTryImportNamespaceResources_DeploymentsOnlySkipsAmbiguousServerJobOrphans(t *testing.T) {
	const (
		namespace       = "project-ns"
		appID           = "26022513312d88jw"
		existingAppID   = "existing-app-id"
		currentWorkload = "mahjongways2-26022513312d88jw-current"
		legacyWorkload  = "mahjongways2-26022513312d88jw-legacy"
	)
	appName := "mahjongways2-" + appID

	props, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)

	store := newInMemoryAppStore()
	store.apps[existingAppID] = &model.Applications{
		ID:        existingAppID,
		Name:      appName,
		Namespace: namespace,
	}
	store.components[currentWorkload] = &model.ApplicationComponent{
		ID:            601,
		AppID:         existingAppID,
		Name:          currentWorkload,
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Properties:    props,
		Traits:        traits,
	}
	store.components[legacyWorkload] = &model.ApplicationComponent{
		ID:            602,
		AppID:         existingAppID,
		Name:          legacyWorkload,
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Properties:    props,
		Traits:        traits,
	}

	svc := &serviceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      currentWorkload,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "current"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "current", Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		AppRepo:       &mockAppRepo{store: store},
		ComponentRepo: &mockComponentRepo{store: store},
	}

	resp, err := svc.TryImportNamespaceResources(context.Background(), apisv1.TryImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.Summary.AppsMatched)
	assert.Equal(t, 0, resp.Summary.OrphansDetected)
	assert.Empty(t, resp.OrphanComponents)
}

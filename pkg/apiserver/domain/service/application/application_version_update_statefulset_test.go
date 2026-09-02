package application

import (
	"context"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateVersionAllowsStandalonePVCReuseWithinApp(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "shop",
		Version:   "1.0.0",
		Namespace: config.DefaultNamespace,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Traits: mustJSONStruct(&apisv1.Traits{
			Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "shared-cache", false)},
		}),
	}

	svc := newMockServiceWithStore(store)
	replicas := int32(1)
	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Action:        "add",
			Name:          "worker",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      &replicas,
			Traits: &apisv1.Traits{
				Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("worker-cache", "shared-cache", false)},
			},
		}},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, store.components["worker"])
	require.Equal(t, "2.0.0", store.apps["app-1"].Version)
}

func TestUpdateVersionPreservesExistingStandalonePVCClaimName(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "shop",
		Version:   "1.0.0",
		Namespace: config.DefaultNamespace,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Traits: mustJSONStruct(&apisv1.Traits{
			Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "old-cache", false)},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Action: "update",
			Name:   "api",
			Traits: &apisv1.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:      "cache-v2",
					Type:      config.StorageTypePersistent,
					MountPath: "/data/cache",
					ClaimName: "new-cache",
				}},
			},
		}},
		AutoExec: boolPtr(false),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "2.0.0", store.apps["app-1"].Version)

	var traits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["api"].Traits, &traits))
	require.Len(t, traits.Storage, 1)
	require.Equal(t, "cache-v2", traits.Storage[0].Name)
	require.Equal(t, "old-cache", traits.Storage[0].ClaimName)
}

func TestScopedStorageMountKeyIncludesSubPathExpr(t *testing.T) {
	scope := namedContainerScope("component", "api", 0)

	fixedSubPath := scopedStorageMountKey(scope, "/app/log", "logs", "")
	exprSubPath := scopedStorageMountKey(scope, "/app/log", "", "$(POD_IP)/logs")
	emptySubPath := scopedStorageMountKey(scope, "/app/log", "", "")

	require.NotEmpty(t, fixedSubPath)
	require.NotEmpty(t, exprSubPath)
	require.NotEqual(t, fixedSubPath, exprSubPath)
	require.NotEqual(t, emptySubPath, exprSubPath)
}

func TestUpdateVersionRejectsStorageSubPathAndSubPathExpr(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "shop",
		Version:   "1.0.0",
		Namespace: config.DefaultNamespace,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Traits: mustJSONStruct(&apisv1.Traits{
			Storage: []spec.StorageTraitSpec{{
				Name:      "logs",
				Type:      config.StorageTypePersistent,
				MountPath: "/app/log",
			}},
		}),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
	}

	svc := newMockServiceWithStore(store)
	_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Action: "update",
			Name:   "api",
			Traits: &apisv1.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:        "logs",
					Type:        config.StorageTypePersistent,
					MountPath:   "/app/log",
					SubPath:     "fixed/logs",
					SubPathExpr: "$(POD_IP)/logs",
				}},
			},
		}},
		AutoExec: boolPtr(false),
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "storage subPath and subPathExpr cannot both be set")
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)

	var traits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["api"].Traits, &traits))
	require.Len(t, traits.Storage, 1)
	require.Empty(t, traits.Storage[0].SubPath)
	require.Empty(t, traits.Storage[0].SubPathExpr)
}

func TestUpdateVersionRejectsStatefulSetVolumeClaimTemplateRename(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "shop",
		Version:   "1.0.0",
		Namespace: config.DefaultNamespace,
	}
	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.StoreJob,
		Image:         "mysql:8",
		Traits: mustJSONStruct(&apisv1.Traits{
			Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("data", "", true)},
		}),
	}

	svc := newMockServiceWithStore(store)
	_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Action: "update",
			Name:   "mysql",
			Traits: &apisv1.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:      "data-v2",
					Type:      config.StorageTypePersistent,
					MountPath: "/data/data",
					TmpCreate: true,
				}},
			},
		}},
		AutoExec: boolPtr(false),
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "volumeClaimTemplate name")
	require.Contains(t, bcode.SafeClientMessage(err), "volumeClaimTemplates.name")
	require.Contains(t, bcode.SafeClientMessage(err), "migration or recreation")
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)

	var traits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["mysql"].Traits, &traits))
	require.Equal(t, "data", traits.Storage[0].Name)
}

func TestUpdateVersionStorageModeErrorUsesPVCLanguageForNonStoreComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID: "app-1", Name: "shop", Version: "1.0.0", Namespace: config.DefaultNamespace,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name: "api", AppID: "app-1", Namespace: config.DefaultNamespace,
		ComponentType: config.ServerJob, Image: "nginx:latest",
		Traits: mustJSONStruct(&apisv1.Traits{
			Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("data", "", false)},
		}),
	}

	svc := newMockServiceWithStore(store)
	_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Action: "update",
			Name:   "api",
			Traits: &apisv1.Traits{
				Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("data", "", true)},
			},
		}},
		AutoExec: boolPtr(false),
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "changes storage mode")
	clientMessage := bcode.SafeClientMessage(err)
	require.Contains(t, clientMessage, "changes persistent storage mode")
	require.Contains(t, clientMessage, "PVC data migration")
	require.NotContains(t, clientMessage, "StatefulSet")
	require.NotContains(t, clientMessage, "volumeClaimTemplates")
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
}

func TestUpdateVersionRejectsStatefulSetImmutableTraitChangesBeforeCommit(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*apisv1.Traits)
		errField string
	}{
		{
			name: "service name",
			mutate: func(traits *apisv1.Traits) {
				traits.Service[0].Name = "mysql-headless-v2"
			},
			errField: "serviceName",
		},
		{
			name: "volume claim template size",
			mutate: func(traits *apisv1.Traits) {
				traits.Storage[0].Size = "2Gi"
			},
			errField: "volumeClaimTemplates[0].size",
		},
		{
			name: "volume claim template storage class",
			mutate: func(traits *apisv1.Traits) {
				traits.Storage[0].StorageClass = "premium"
			},
			errField: "volumeClaimTemplates[0].storageClass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{
				ID: "app-1", Name: "shop", Version: "1.0.0", Namespace: config.DefaultNamespace,
			}
			currentTraits := apisv1.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", TmpCreate: true, Size: "1Gi",
				}},
				Service: []spec.ServiceTraitSpec{{
					Name: "mysql-headless", Type: string(spec.ServiceAccessInternal), Headless: true,
					Selector: map[string]string{config.LabelComponentName: "mysql"},
					Ports:    []spec.ServicePortTraitSpec{{Name: "mysql", Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
				}},
			}
			store.components["mysql"] = &model.ApplicationComponent{
				Name: "mysql", AppID: "app-1", Namespace: config.DefaultNamespace,
				ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1, Traits: mustJSONStruct(&currentTraits),
			}
			desiredTraits := currentTraits
			desiredTraits.Storage = append([]spec.StorageTraitSpec(nil), currentTraits.Storage...)
			desiredTraits.Service = append([]spec.ServiceTraitSpec(nil), currentTraits.Service...)
			tt.mutate(&desiredTraits)

			svc := newMockServiceWithStore(store)
			_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:    "2.0.0",
				Components: []apisv1.ComponentUpdateSpec{{Action: "update", Name: "mysql", Traits: &desiredTraits}},
				AutoExec:   boolPtr(false),
			})

			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Contains(t, err.Error(), tt.errField)
			require.Contains(t, err.Error(), "migration or recreation is required")
			require.Contains(t, bcode.SafeClientMessage(err), tt.errField)
			require.Contains(t, bcode.SafeClientMessage(err), "migration or recreation is required")
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
			var persistedTraits apisv1.Traits
			require.NoError(t, decodeJSONStruct(store.components["mysql"].Traits, &persistedTraits))
			require.Equal(t, "mysql-headless", persistedTraits.Service[0].Name)
			require.Equal(t, "1Gi", persistedTraits.Storage[0].Size)
			require.Empty(t, persistedTraits.Storage[0].StorageClass)
		})
	}
}

func TestValidateStatefulSetImmutableTransitionRejectsSelectorChange(t *testing.T) {
	current := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"component": "mysql"}},
	}}
	desired := current.DeepCopy()
	desired.Spec.Selector.MatchLabels["component"] = "mysql-v2"

	err := validateStatefulSetImmutableTransition("mysql", current, desired)

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "selector")
	require.Contains(t, err.Error(), "migration or recreation is required")
}

func TestValidateStatefulSetImmutableTransitionRejectsIdentityChange(t *testing.T) {
	current := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "mysql-shop", Namespace: config.DefaultNamespace}}
	desired := current.DeepCopy()
	desired.Name = "mysql-mysql"

	err := validateStatefulSetImmutableTransition("mysql", current, desired)

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "changes StatefulSet identity")
	require.Contains(t, bcode.SafeClientMessage(err), "migrate the StatefulSet/PVC separately")
}

func TestStatefulSetPVCTemplatesToDeleteTargetsAffectedTemplates(t *testing.T) {
	template := func(name, size string) corev1.PersistentVolumeClaim {
		return corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{config.LabelStorageRole: "template"}},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				}},
			},
		}
	}
	current := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{
		VolumeClaimTemplates: []corev1.PersistentVolumeClaim{template("data", "1Gi")},
	}}

	t.Run("metadata-only changes keep existing PVCs", func(t *testing.T) {
		desired := current.DeepCopy()
		desired.Spec.VolumeClaimTemplates[0].Annotations["description"] = "metadata-only"
		require.Empty(t, statefulSetPVCTemplatesToDelete(current, desired))
	})

	t.Run("added templates remove any historical desired-name PVCs", func(t *testing.T) {
		desired := current.DeepCopy()
		desired.Spec.VolumeClaimTemplates = append(desired.Spec.VolumeClaimTemplates, template("cache", "1Gi"))
		require.Equal(t, []string{"cache"}, statefulSetPVCTemplatesToDelete(current, desired))
	})

	t.Run("same-name spec change deletes old PVCs", func(t *testing.T) {
		desired := current.DeepCopy()
		desired.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")
		require.Equal(t, []string{"data"}, statefulSetPVCTemplatesToDelete(current, desired))
	})

	t.Run("rename deletes old and historical desired-name PVCs", func(t *testing.T) {
		desired := current.DeepCopy()
		desired.Spec.VolumeClaimTemplates[0] = template("data-v2", "1Gi")
		require.Equal(t, []string{"data", "data-v2"}, statefulSetPVCTemplatesToDelete(current, desired))
	})
}

package application

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestDiffUpdateVersionDryRunReportsUpdatedAddedAndExtra(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{
		ID:        "source-app",
		Name:      "source",
		Version:   "1.0.1",
		Namespace: "default",
	}
	store.apps["target-app"] = &model.Applications{
		ID:        "target-app",
		Name:      "target",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "source-app",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "backend:v1.0.1",
		Replicas:      3,
		Properties: mustJSONStruct(&apisv1.Properties{
			Env: map[string]string{"LOG_LEVEL": "debug"},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{
			Resources: &spec.ResourceTraitsSpec{CPU: "500m", Memory: "512Mi"},
		}),
	}
	store.components["Backend"] = &model.ApplicationComponent{
		Name:          "Backend",
		AppID:         "target-app",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "backend:v1.0.0",
		Replicas:      2,
		Properties: mustJSONStruct(&apisv1.Properties{
			Env: map[string]string{"LOG_LEVEL": "info"},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{}),
	}
	store.components["worker"] = &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "source-app",
		Namespace:     "default",
		ComponentType: config.InstantJob,
		Image:         "worker:v1.0.1",
		Replicas:      1,
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}
	store.components["legacy"] = &model.ApplicationComponent{
		Name:          "legacy",
		AppID:         "target-app",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "legacy:v1",
		Replicas:      1,
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		DryRun:      true,
	})

	require.NoError(t, err)
	require.True(t, resp.DryRun)
	require.Equal(t, apisv1.DiffUpdateTargetOnlyStrategyPreserve, resp.TargetOnlyStrategy)
	require.True(t, resp.VersionChanged)
	require.True(t, resp.HasChanges)
	require.True(t, resp.Executable)
	require.Equal(t, "1.0.0", resp.TargetPreviousVersion)
	require.Equal(t, "1.0.1", resp.TargetVersion)
	require.Len(t, resp.UpdatedComponents, 1)
	require.Equal(t, "backend", resp.UpdatedComponents[0].Name)
	require.ElementsMatch(t, []string{"image", "replicas", "properties", "traits"}, diffFieldNames(resp.UpdatedComponents[0].Fields))
	require.Len(t, resp.AddedComponents, 1)
	require.Equal(t, "worker", resp.AddedComponents[0].Name)
	require.Len(t, resp.ExtraComponents, 1)
	require.Equal(t, "legacy", resp.ExtraComponents[0].Name)
	require.Equal(t, apisv1.DiffUpdateComponentActionPreserve, resp.ExtraComponents[0].Action)
	require.Equal(t, "target-only component is preserved", resp.ExtraComponents[0].Reason)
	require.Empty(t, resp.BlockedComponents)
	require.Equal(t, "backend:v1.0.0", store.components["Backend"].Image)
}

func TestDiffUpdateVersionRejectsSecretPropertiesBeforeAdoptedDryRunResponse(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{
		ID:      "source-app",
		Name:    "source",
		Version: "1.0.1",
	}
	store.apps["target-app"] = &model.Applications{
		ID:             "target-app",
		Name:           "target",
		Version:        "1.0.0",
		ManagementMode: config.ManagementModeAdopted,
	}
	uid := "deployment-uid"
	store.components["source-backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "source-app",
		ComponentType: config.ServerJob,
		Properties: mustJSONStruct(&apisv1.Properties{
			Secret: map[string]string{"password": "must-not-appear-in-response"},
		}),
	}
	store.components["target-backend"] = &model.ApplicationComponent{
		Name:                     "backend",
		AppID:                    "target-app",
		ComponentType:            config.ServerJob,
		Properties:               mustJSONStruct(&apisv1.Properties{}),
		SourceWorkloadAPIVersion: "apps/v1",
		SourceWorkloadKind:       "Deployment",
		SourceWorkloadName:       "legacy-backend",
		SourceWorkloadUID:        &uid,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		DryRun:      true,
	})
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.NotContains(t, err.Error(), "must-not-appear-in-response")
}

func TestDiffUpdateVersionDryRunBlocksAdoptedTargetOnlyRemoval(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{
		ID:      "source-app",
		Name:    "source",
		Version: "1.0.1",
	}
	store.apps["target-app"] = &model.Applications{
		ID:             "target-app",
		Name:           "target",
		Version:        "1.0.0",
		ManagementMode: config.ManagementModeAdopted,
	}
	apiUID := "api-uid"
	legacyUID := "legacy-uid"
	store.components["source-api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "source-app",
		ComponentType: config.ServerJob,
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}
	store.components["target-api"] = &model.ApplicationComponent{
		Name:                     "api",
		AppID:                    "target-app",
		ComponentType:            config.ServerJob,
		Properties:               mustJSONStruct(&apisv1.Properties{}),
		Traits:                   mustJSONStruct(&apisv1.Traits{}),
		SourceWorkloadAPIVersion: "apps/v1",
		SourceWorkloadKind:       "Deployment",
		SourceWorkloadName:       "api",
		SourceWorkloadUID:        &apiUID,
	}
	store.components["target-legacy"] = &model.ApplicationComponent{
		Name:                     "legacy",
		AppID:                    "target-app",
		ComponentType:            config.ServerJob,
		Properties:               mustJSONStruct(&apisv1.Properties{}),
		Traits:                   mustJSONStruct(&apisv1.Traits{}),
		SourceWorkloadAPIVersion: "apps/v1",
		SourceWorkloadKind:       "Deployment",
		SourceWorkloadName:       "legacy",
		SourceWorkloadUID:        &legacyUID,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID:        "source-app",
		DryRun:             true,
		TargetOnlyStrategy: apisv1.DiffUpdateTargetOnlyStrategyRemove,
	})

	require.NoError(t, err)
	require.False(t, resp.Executable)
	require.Empty(t, resp.ExtraComponents)
	require.Len(t, resp.BlockedComponents, 1)
	require.Equal(t, "legacy", resp.BlockedComponents[0].Name)
	require.Equal(t, apisv1.DiffUpdateComponentActionBlock, resp.BlockedComponents[0].Action)
}

func TestDiffUpdateVersionExecutesGeneratedUpdate(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{
		ID:        "source-app",
		Name:      "source",
		Version:   "2.0.0",
		Namespace: "default",
	}
	store.apps["target-app"] = &model.Applications{
		ID:        "target-app",
		Name:      "target",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "source-app",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "api:v2",
		Replicas:      4,
		Properties: mustJSONStruct(&apisv1.Properties{
			Env: map[string]string{"FEATURE": "enabled"},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{
			Resources: &spec.ResourceTraitsSpec{CPU: "1", Memory: "1Gi"},
		}),
	}
	store.components["API"] = &model.ApplicationComponent{
		Name:          "API",
		AppID:         "target-app",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "api:v1",
		Replicas:      1,
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}
	store.components["job"] = &model.ApplicationComponent{
		Name:          "job",
		AppID:         "source-app",
		Namespace:     "default",
		ComponentType: config.InstantJob,
		Image:         "job:v2",
		Replicas:      1,
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID:    "source-app",
		ExecutionScope: string(config.VersionUpdateExecutionScopeChangedComponents),
		AutoExec:       boolPtr(false),
	})

	require.NoError(t, err)
	require.False(t, resp.DryRun)
	require.NotNil(t, resp.UpdateResult)
	require.Equal(t, "2.0.0", resp.UpdateResult.Version)
	require.Equal(t, string(config.VersionUpdateExecutionScopeChangedComponents), resp.UpdateResult.ExecutionScope)
	require.Equal(t, "2.0.0", store.apps["target-app"].Version)
	require.Equal(t, "api:v2", store.components["API"].Image)
	require.Equal(t, int32(4), store.components["API"].Replicas)
	require.Equal(t, "target-app", store.components["job"].AppID)

	var props apisv1.Properties
	require.NoError(t, decodeJSONStruct(store.components["API"].Properties, &props))
	require.Equal(t, map[string]string{"FEATURE": "enabled"}, props.Env)
	var traits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["API"].Traits, &traits))
	require.Equal(t, "1Gi", traits.Resources.Memory)
}

func TestDiffUpdateVersionRemovesTargetOnlyComponentsWithStrategy(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{ID: "source-app", Name: "source", Version: "1.0.1"}
	store.apps["target-app"] = &model.Applications{ID: "target-app", Name: "target", Version: "1.0.0"}
	store.components["legacy"] = &model.ApplicationComponent{
		Name:  "legacy",
		AppID: "target-app",
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID:        "source-app",
		TargetOnlyStrategy: apisv1.DiffUpdateTargetOnlyStrategyRemove,
		AutoExec:           boolPtr(false),
	})

	require.NoError(t, err)
	require.True(t, resp.Executable)
	require.Equal(t, apisv1.DiffUpdateTargetOnlyStrategyRemove, resp.TargetOnlyStrategy)
	require.Len(t, resp.ExtraComponents, 1)
	require.Equal(t, string(config.ComponentActionRemove), resp.ExtraComponents[0].Action)
	require.NotNil(t, resp.UpdateResult)
	require.Contains(t, resp.UpdateResult.RemovedComponents, "legacy")
	require.Equal(t, "1.0.1", store.apps["target-app"].Version)
	require.Nil(t, store.components["legacy"])
}

func TestDiffUpdateVersionBlocksTargetOnlyComponentsWithStrategy(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{ID: "source-app", Name: "source", Version: "1.0.1"}
	store.apps["target-app"] = &model.Applications{ID: "target-app", Name: "target", Version: "1.0.0"}
	store.components["legacy"] = &model.ApplicationComponent{
		Name:  "legacy",
		AppID: "target-app",
	}

	svc := newMockServiceWithStore(store)
	dryRunResp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID:        "source-app",
		TargetOnlyStrategy: apisv1.DiffUpdateTargetOnlyStrategyBlock,
		DryRun:             true,
	})
	require.NoError(t, err)
	require.False(t, dryRunResp.Executable)
	require.Equal(t, apisv1.DiffUpdateTargetOnlyStrategyBlock, dryRunResp.TargetOnlyStrategy)
	require.Len(t, dryRunResp.ExtraComponents, 1)
	require.Equal(t, apisv1.DiffUpdateComponentActionBlock, dryRunResp.ExtraComponents[0].Action)
	require.Equal(t, "target-only component blocks update", dryRunResp.ExtraComponents[0].Reason)

	_, err = svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID:        "source-app",
		TargetOnlyStrategy: apisv1.DiffUpdateTargetOnlyStrategyBlock,
		AutoExec:           boolPtr(false),
	})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Equal(t, "1.0.0", store.apps["target-app"].Version)
	require.NotNil(t, store.components["legacy"])
}

func TestDiffUpdateVersionRejectsInvalidTargetOnlyStrategy(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{ID: "source-app", Name: "source", Version: "1.0.1"}
	store.apps["target-app"] = &model.Applications{ID: "target-app", Name: "target", Version: "1.0.0"}

	svc := newMockServiceWithStore(store)
	_, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID:        "source-app",
		TargetOnlyStrategy: "archive",
	})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestDiffUpdateVersionBlocksTypeMismatch(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{ID: "source-app", Name: "source", Version: "1.0.1"}
	store.apps["target-app"] = &model.Applications{ID: "target-app", Name: "target", Version: "1.0.0"}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "source-app",
		ComponentType: config.StoreJob,
		Image:         "redis:7",
		Replicas:      1,
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}
	store.components["Backend"] = &model.ApplicationComponent{
		Name:          "Backend",
		AppID:         "target-app",
		ComponentType: config.ServerJob,
		Image:         "backend:v1",
		Replicas:      1,
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}

	svc := newMockServiceWithStore(store)
	dryRunResp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		DryRun:      true,
	})
	require.NoError(t, err)
	require.False(t, dryRunResp.Executable)
	require.Len(t, dryRunResp.BlockedComponents, 1)
	require.Equal(t, "component type mismatch", dryRunResp.BlockedComponents[0].Reason)

	_, err = svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		AutoExec:    boolPtr(false),
	})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Equal(t, "1.0.0", store.apps["target-app"].Version)
	require.Equal(t, "backend:v1", store.components["Backend"].Image)
}

func TestDiffUpdateVersionDryRunBlocksStatefulSetImmutableTraitChange(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{
		ID: "source-app", Name: "source", Version: "2.0.0", Namespace: config.DefaultNamespace,
	}
	store.apps["target-app"] = &model.Applications{
		ID: "target-app", Name: "target", Version: "1.0.0", Namespace: config.DefaultNamespace,
	}
	targetTraits := apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", TmpCreate: true, Size: "1Gi",
		}},
		Service: []spec.ServiceTraitSpec{{
			Name: "mysql-headless", Type: string(config.ServiceAccessInternal), Headless: true,
			Selector: map[string]string{config.LabelComponentName: "mysql"},
			Ports:    []spec.ServicePortTraitSpec{{Name: "mysql", Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
	}
	sourceTraits := targetTraits
	sourceTraits.Storage = append([]spec.StorageTraitSpec(nil), targetTraits.Storage...)
	sourceTraits.Service = append([]spec.ServiceTraitSpec(nil), targetTraits.Service...)
	sourceTraits.Service[0].Name = "mysql-headless-v2"
	store.components["source-mysql"] = &model.ApplicationComponent{
		Name: "mysql", AppID: "source-app", Namespace: config.DefaultNamespace,
		ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1, Traits: mustJSONStruct(&sourceTraits),
	}
	store.components["target-mysql"] = &model.ApplicationComponent{
		Name: "mysql", AppID: "target-app", Namespace: config.DefaultNamespace,
		ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1, Traits: mustJSONStruct(&targetTraits),
	}

	svc := newMockServiceWithStore(store)
	dryRunResp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		DryRun:      true,
	})

	require.NoError(t, err)
	require.False(t, dryRunResp.Executable)
	require.Empty(t, dryRunResp.UpdatedComponents)
	require.Len(t, dryRunResp.BlockedComponents, 1)
	require.Equal(t, "mysql", dryRunResp.BlockedComponents[0].Name)
	require.Equal(t, apisv1.DiffUpdateComponentActionBlock, dryRunResp.BlockedComponents[0].Action)
	require.Contains(t, dryRunResp.BlockedComponents[0].Reason, "serviceName")
	require.Contains(t, dryRunResp.BlockedComponents[0].Reason, "migration or recreation")

	_, err = svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		AutoExec:    boolPtr(false),
	})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, bcode.SafeClientMessage(err), "serviceName")
	require.Contains(t, bcode.SafeClientMessage(err), "migration or recreation")
	require.Equal(t, "1.0.0", store.apps["target-app"].Version)
	var persistedTraits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["target-mysql"].Traits, &persistedTraits))
	require.Equal(t, "mysql-headless", persistedTraits.Service[0].Name)
}

func TestDiffUpdateVersionDryRunPreservesStandalonePVCSnapshots(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{
		ID: "source-app", Name: "source", Version: "2.0.0", Namespace: config.DefaultNamespace,
	}
	store.apps["target-app"] = &model.Applications{
		ID: "target-app", Name: "target", Version: "1.0.0", Namespace: config.DefaultNamespace,
	}
	sourceTraits := apisv1.Traits{Storage: []spec.StorageTraitSpec{{
		Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", ClaimName: "source-data-pvc",
	}}}
	targetTraits := apisv1.Traits{Storage: []spec.StorageTraitSpec{{
		Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", ClaimName: "target-data-pvc",
	}}}
	store.components["source-mysql"] = &model.ApplicationComponent{
		Name: "mysql", AppID: "source-app", Namespace: config.DefaultNamespace,
		ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1, Traits: mustJSONStruct(&sourceTraits),
	}
	store.components["target-mysql"] = &model.ApplicationComponent{
		Name: "mysql", AppID: "target-app", Namespace: config.DefaultNamespace,
		ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1, Traits: mustJSONStruct(&targetTraits),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		DryRun:      true,
	})

	require.NoError(t, err)
	require.True(t, resp.Executable)
	require.Len(t, resp.UpdatedComponents, 1)
	componentDiff := resp.UpdatedComponents[0]
	require.Equal(t, "target-data-pvc", componentDiff.Before.Traits.Storage[0].ClaimName)
	require.Equal(t, "source-data-pvc", componentDiff.After.Traits.Storage[0].ClaimName)

	var traitsField apisv1.VersionComponentField
	for _, field := range componentDiff.Fields {
		if field.Field == versionDiffFieldTraits {
			traitsField = field
			break
		}
	}
	beforeTraits, ok := traitsField.Before.(apisv1.Traits)
	require.True(t, ok)
	afterTraits, ok := traitsField.After.(apisv1.Traits)
	require.True(t, ok)
	require.Equal(t, "target-data-pvc", beforeTraits.Storage[0].ClaimName)
	require.Equal(t, "source-data-pvc", afterTraits.Storage[0].ClaimName)

	var persistedSourceTraits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["source-mysql"].Traits, &persistedSourceTraits))
	require.Equal(t, "source-data-pvc", persistedSourceTraits.Storage[0].ClaimName)
	var persistedTargetTraits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["target-mysql"].Traits, &persistedTargetTraits))
	require.Equal(t, "target-data-pvc", persistedTargetTraits.Storage[0].ClaimName)
}

func TestDiffUpdateVersionDryRunBlocksPendingStatefulSetCleanup(t *testing.T) {
	tests := []struct {
		name           string
		cleanupVersion int
		status         config.Status
	}{
		{
			name:           "failed StatefulSet deletion v2",
			cleanupVersion: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
			status:         config.StatusFailed,
		},
		{
			name:           "cancelled StatefulSet PVC deletion v3",
			cleanupVersion: model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
			status:         config.StatusCancelled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, svc := newDiffUpdatePendingCleanupFixture(t, tt.cleanupVersion, tt.status)
			req := apisv1.DiffUpdateVersionRequest{
				SourceAppID: "source-app",
				AutoExec:    boolPtr(false),
			}

			dryRunReq := req
			dryRunReq.DryRun = true
			dryRunResp, err := svc.DiffUpdateVersion(context.Background(), "target-app", dryRunReq)

			require.NoError(t, err)
			require.False(t, dryRunResp.Executable)
			require.Len(t, dryRunResp.UpdatedComponents, 1)
			require.Contains(t, dryRunResp.UpdatedComponents[0].Reason, "unfinished StatefulSet migration")

			resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", req)

			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Nil(t, resp)
			require.Contains(t, bcode.SafeClientMessage(err), "unfinished StatefulSet migration")
			require.Equal(t, "api:v1", store.components["target-api"].Image)
		})
	}
}

func TestDiffUpdateVersionPendingStatefulSetCleanupAllowsVersionOnlyChange(t *testing.T) {
	store, svc := newDiffUpdatePendingCleanupFixture(
		t,
		model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
		config.StatusFailed,
	)
	store.components["source-api"].Image = store.components["target-api"].Image

	dryRunResp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		DryRun:      true,
	})

	require.NoError(t, err)
	require.True(t, dryRunResp.VersionChanged)
	require.Empty(t, dryRunResp.UpdatedComponents)
	require.True(t, dryRunResp.Executable)

	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		AutoExec:    boolPtr(false),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.UpdateResult)
	require.Equal(t, "2.0.0", store.apps["target-app"].Version)
}

func TestDiffUpdateVersionDryRunPropagatesPendingHistoryReadError(t *testing.T) {
	store, svc := newDiffUpdatePendingCleanupFixture(
		t,
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		config.StatusFailed,
	)
	wantErr := errors.New("list workflow history")
	svc.Store = &diffUpdateWorkflowTaskListErrorStore{
		inMemoryAppStore: store,
		err:              wantErr,
	}

	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		DryRun:      true,
	})

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, resp)
}

func TestDiffUpdateVersionExecutesVersionOnlyChange(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{ID: "source-app", Name: "source", Version: "1.0.1"}
	store.apps["target-app"] = &model.Applications{ID: "target-app", Name: "target", Version: "1.0.0"}

	svc := newMockServiceWithStore(store)
	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app",
		AutoExec:    boolPtr(false),
	})

	require.NoError(t, err)
	require.True(t, resp.VersionChanged)
	require.Empty(t, resp.UpdatedComponents)
	require.Empty(t, resp.AddedComponents)
	require.NotNil(t, resp.UpdateResult)
	require.Equal(t, "1.0.1", store.apps["target-app"].Version)
}

func newDiffUpdatePendingCleanupFixture(
	t *testing.T,
	cleanupVersion int,
	status config.Status,
) (*inMemoryAppStore, *applicationsServiceImpl) {
	t.Helper()
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{
		ID: "source-app", Name: "source", Version: "2.0.0", Namespace: config.DefaultNamespace,
	}
	store.apps["target-app"] = &model.Applications{
		ID: "target-app", Name: "target", Version: "1.0.0", Namespace: config.DefaultNamespace,
	}
	store.components["source-api"] = &model.ApplicationComponent{
		ID: 1, AppID: "source-app", Name: "api", Namespace: config.DefaultNamespace,
		ComponentType: config.ServerJob, Image: "api:v2", Replicas: 1,
		Properties: mustJSONStruct(&apisv1.Properties{}), Traits: mustJSONStruct(&apisv1.Traits{}),
	}
	store.components["target-api"] = &model.ApplicationComponent{
		ID: 2, AppID: "target-app", Name: "api", Namespace: config.DefaultNamespace,
		ComponentType: config.ServerJob, Image: "api:v1", Replicas: 1,
		Properties: mustJSONStruct(&apisv1.Properties{}), Traits: mustJSONStruct(&apisv1.Traits{}),
	}
	mysqlTraits := apisv1.Traits{Storage: []spec.StorageTraitSpec{{
		Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", TmpCreate: true, Size: "1Gi",
	}}}
	targetMySQL := &model.ApplicationComponent{
		ID: 3, AppID: "target-app", Name: "mysql", Namespace: config.DefaultNamespace, ResourceAppName: "target",
		ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1,
		Properties: mustJSONStruct(&apisv1.Properties{}), Traits: mustJSONStruct(&mysqlTraits),
	}
	store.components["target-mysql"] = targetMySQL
	sourceMySQL := *targetMySQL
	sourceMySQL.ID = 4
	sourceMySQL.AppID = "source-app"
	sourceMySQL.ResourceAppName = "source"
	store.components["source-mysql"] = &sourceMySQL

	templates := []string(nil)
	if cleanupVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
		templates = []string{"data"}
	}
	cleanupInfo := model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: cleanupVersion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component:                       targetMySQL,
			ResourceAppName:                 "target",
			RequireStatefulSetDeletion:      true,
			StatefulSetPVCTemplatesToDelete: templates,
		}},
	}
	cleanupPayload, err := json.Marshal(cleanupInfo)
	require.NoError(t, err)
	jobMarker, err := versionUpdateCleanupJobInfoMarker(true, templates)
	require.NoError(t, err)
	store.tasks["pending-cleanup"] = &model.WorkflowQueue{
		TaskID: "pending-cleanup", AppID: "target-app", Status: status, CleanupInfo: string(cleanupPayload),
	}
	store.jobs = append(store.jobs, &model.JobInfo{
		TaskID: "pending-cleanup", Type: string(config.JobCleanupResources), ServiceName: "mysql",
		Status: string(status), InternalInfo: jobMarker,
	})
	return store, newMockServiceWithStore(store)
}

type diffUpdateWorkflowTaskListErrorStore struct {
	*inMemoryAppStore
	err error
}

func (s *diffUpdateWorkflowTaskListErrorStore) List(
	ctx context.Context,
	query datastore.Entity,
	opts *datastore.ListOptions,
) ([]datastore.Entity, error) {
	if _, ok := query.(*model.WorkflowQueue); ok {
		return nil, s.err
	}
	return s.inMemoryAppStore.List(ctx, query, opts)
}

func TestDiffUpdateVersionUsesMaterializedTemplateComponents(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{
		ID:              "tmpl-1",
		Name:            "tmpl",
		Version:         "1.0.0",
		Namespace:       "default",
		TemplateEnabled: true,
	}
	store.apps[templateApp.ID] = templateApp
	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         templateApp.ID,
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Image:         "mysql:8",
		Replicas:      1,
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}
	store.apps["target-app"] = &model.Applications{
		ID:        "target-app",
		Name:      "target",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["DB"] = &model.ApplicationComponent{
		Name:          "DB",
		AppID:         "target-app",
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Image:         "mysql:5.7",
		Replicas:      1,
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}

	svc := newMockServiceWithStore(store)
	created, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "source",
		Namespace: "default",
		Version:   "1.0.1",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "db",
			ComponentType: config.StoreJob,
			Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
		}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)

	resp, err := svc.DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: created.ID,
		DryRun:      true,
	})

	require.NoError(t, err)
	require.Len(t, resp.UpdatedComponents, 1)
	require.Equal(t, "db", resp.UpdatedComponents[0].Name)
	require.Equal(t, "mysql:8", resp.UpdatedComponents[0].After.Image)
	require.Equal(t, "mysql:5.7", resp.UpdatedComponents[0].Before.Image)
}

func TestIncrementVersion(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"1.0.0", "1.0.1"},
		{"2.3.5", "2.3.6"},
		{"1.0.9", "1.0.10"},
		{"0.0.0", "0.0.1"},
		{"", "1.0.1"},
		{"1", "2"},
		{"10", "11"},
	}

	for _, tc := range tests {
		result := incrementVersion(tc.input)
		require.Equal(t, tc.expected, result, "incrementVersion(%q) should be %q", tc.input, tc.expected)
	}
}

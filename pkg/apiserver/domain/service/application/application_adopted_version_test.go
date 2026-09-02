package application

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateAdoptedVersionRequiresWorkflowIdleWithoutAutoExec(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:             "app-1",
		Name:           "legacy",
		Namespace:      config.DefaultNamespace,
		Version:        "1.0.0",
		ManagementMode: config.ManagementModeAdopted,
	}
	store.tasks["task-running"] = &model.WorkflowQueue{
		TaskID: "task-running",
		AppID:  "app-1",
		Status: config.StatusRunning,
	}

	svc := newMockServiceWithStore(store)
	response, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:  "1.1.0",
		AutoExec: boolPtr(false),
	})

	require.Nil(t, response)
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
}

func TestValidateAdoptedVersionUpdateActionsRequiresExistingSourceBindings(t *testing.T) {
	uid := "source-uid"
	components := map[string]*model.ApplicationComponent{
		"backend": {
			Name:                     "backend",
			ComponentType:            config.ServerJob,
			SourceWorkloadAPIVersion: "apps/v1",
			SourceWorkloadKind:       "Deployment",
			SourceWorkloadName:       "legacy-backend",
			SourceWorkloadUID:        &uid,
		},
		"unbound": {Name: "unbound", ComponentType: config.ServerJob},
	}

	require.NoError(t, validateAdoptedVersionUpdateActions([]apisv1.ComponentUpdateSpec{{
		Name:   "backend",
		Action: string(config.ComponentActionUpdate),
	}}, components))

	for _, spec := range []apisv1.ComponentUpdateSpec{
		{Name: "new", Action: string(config.ComponentActionAdd)},
		{Name: "unbound", Action: string(config.ComponentActionUpdate)},
		{Name: "backend", Action: string(config.ComponentActionRemove)},
		{
			Name:   "backend",
			Action: string(config.ComponentActionUpdate),
			Properties: &apisv1.Properties{
				Secret: map[string]string{"password": "must-not-enter-properties"},
			},
		},
	} {
		err := validateAdoptedVersionUpdateActions([]apisv1.ComponentUpdateSpec{spec}, components)
		require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	}
}

func TestValidateAdoptedVersionUpdateCompatibilityRejectsPropertiesChanges(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:          "backend",
		ComponentType: config.ServerJob,
		Properties: mustJSONStruct(&apisv1.Properties{
			Env: map[string]string{"EXISTING": "value"},
		}),
	}
	changed := apisv1.Properties{Env: map[string]string{"EXISTING": "changed"}}

	err := validateAdoptedVersionUpdateCompatibility(component, apisv1.ComponentUpdateSpec{
		Name:       component.Name,
		Properties: &changed,
	})

	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.ErrorContains(t, err, "properties changes require an explicit managed-field contract")
}

func TestValidateAdoptedVersionUpdateCompatibilityAllowsStandalonePVCGrowth(t *testing.T) {
	current := apisv1.Traits{Storage: []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      config.StorageTypePersistent,
		MountPath: "/var/lib/data",
		ClaimName: "legacy-data",
		Size:      "10Gi",
	}}}
	component := &model.ApplicationComponent{
		Name:          "backend",
		ComponentType: config.ServerJob,
		Traits:        mustJSONStruct(&current),
	}
	desired := current
	desired.Storage = append([]spec.StorageTraitSpec(nil), current.Storage...)
	desired.Storage[0].Size = "20Gi"

	require.NoError(t, validateAdoptedVersionUpdateCompatibility(component, apisv1.ComponentUpdateSpec{
		Name:   component.Name,
		Traits: &desired,
	}))
}

func TestValidateAdoptedVersionUpdateCompatibilityRejectsStandalonePVCShrink(t *testing.T) {
	current := apisv1.Traits{Storage: []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      config.StorageTypePersistent,
		MountPath: "/var/lib/data",
		ClaimName: "legacy-data",
		Size:      "10Gi",
	}}}
	component := &model.ApplicationComponent{
		Name:          "backend",
		ComponentType: config.ServerJob,
		Traits:        mustJSONStruct(&current),
	}
	desired := current
	desired.Storage = append([]spec.StorageTraitSpec(nil), current.Storage...)
	desired.Storage[0].Size = "5Gi"

	err := validateAdoptedVersionUpdateCompatibility(component, apisv1.ComponentUpdateSpec{
		Name:   component.Name,
		Traits: &desired,
	})

	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.ErrorContains(t, err, "must grow")
}

func TestValidateAdoptedVersionUpdateCompatibilityRejectsVCTResize(t *testing.T) {
	current := apisv1.Traits{Storage: []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      config.StorageTypePersistent,
		MountPath: "/var/lib/mysql",
		TmpCreate: true,
		Size:      "10Gi",
	}}}
	component := &model.ApplicationComponent{
		Name:          "mysql",
		ComponentType: config.StoreJob,
		Traits:        mustJSONStruct(&current),
	}
	desired := current
	desired.Storage = append([]spec.StorageTraitSpec(nil), current.Storage...)
	desired.Storage[0].Size = "20Gi"

	err := validateAdoptedVersionUpdateCompatibility(component, apisv1.ComponentUpdateSpec{
		Name:   component.Name,
		Traits: &desired,
	})

	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.ErrorContains(t, err, "data migration is required")
}

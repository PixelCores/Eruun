package workflow

import (
	"context"
	"encoding/json"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestVersionUpdateCleanupInfoAcceptsStatefulSetDeletionContract(t *testing.T) {
	payload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component:                  &model.ApplicationComponent{Name: "mysql", AppID: "app-1", ComponentType: config.StoreJob},
			RequireStatefulSetDeletion: true,
		}},
	})
	require.NoError(t, err)
	task := &model.WorkflowQueue{CleanupInfo: string(payload)}

	cleanupInfo, ok, err := versionUpdateCleanupInfoFromTask(task)

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, cleanupInfo.Version)
	require.Len(t, cleanupInfo.Components, 1)
	require.True(t, cleanupInfo.Components[0].RequireStatefulSetDeletion)
}

func TestVersionUpdateCleanupInfoAcceptsStatefulSetPVCDeletionContract(t *testing.T) {
	payload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component:                       &model.ApplicationComponent{Name: "mysql", AppID: "app-1", ComponentType: config.StoreJob},
			RequireStatefulSetDeletion:      true,
			StatefulSetPVCTemplatesToDelete: []string{"data"},
		}},
	})
	require.NoError(t, err)
	task := &model.WorkflowQueue{CleanupInfo: string(payload)}

	cleanupInfo, ok, err := versionUpdateCleanupInfoFromTask(task)

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, cleanupInfo.Version)
	require.Equal(t, []string{"data"}, cleanupInfo.Components[0].StatefulSetPVCTemplatesToDelete)
}

func TestVersionUpdateCleanupInfoRejectsPVCDeletionWithoutV3(t *testing.T) {
	payload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component:                       &model.ApplicationComponent{Name: "mysql", AppID: "app-1", ComponentType: config.StoreJob},
			RequireStatefulSetDeletion:      true,
			StatefulSetPVCTemplatesToDelete: []string{"data"},
		}},
	})
	require.NoError(t, err)

	_, ok, err := versionUpdateCleanupInfoFromTask(&model.WorkflowQueue{CleanupInfo: string(payload)})

	require.Error(t, err)
	require.True(t, ok)
	require.Contains(t, err.Error(), "maximum component contract version 3")
}

func TestVersionUpdateCleanupInfoRejectsContractVersionDowngrade(t *testing.T) {
	tests := []struct {
		name        string
		version     int
		cleanupOnly bool
		component   model.VersionUpdateCleanupComponent
		want        string
	}{
		{
			name:    "v1 cannot require StatefulSet deletion",
			version: model.VersionUpdateCleanupInfoVersionV1,
			component: model.VersionUpdateCleanupComponent{
				Component:                  &model.ApplicationComponent{Name: "mysql", AppID: "app-1", ComponentType: config.StoreJob},
				RequireStatefulSetDeletion: true,
			},
			want: "maximum component contract version 2",
		},
		{
			name:    "v2 requires a component deletion contract",
			version: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
			component: model.VersionUpdateCleanupComponent{
				Component: &model.ApplicationComponent{Name: "api", AppID: "app-1", ComponentType: config.ServerJob},
			},
			want: "maximum component contract version 1",
		},
		{
			name:    "StatefulSet deletion is store only",
			version: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
			component: model.VersionUpdateCleanupComponent{
				Component:                  &model.ApplicationComponent{Name: "api", AppID: "app-1", ComponentType: config.ServerJob},
				RequireStatefulSetDeletion: true,
			},
			want: "only valid for store components",
		},
		{
			name:        "cleanup-only cannot carry a destructive migration",
			version:     model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
			cleanupOnly: true,
			component: model.VersionUpdateCleanupComponent{
				Component:                  &model.ApplicationComponent{Name: "mysql", AppID: "app-1", ComponentType: config.StoreJob},
				RequireStatefulSetDeletion: true,
			},
			want: "cleanup-only task cannot carry",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(model.VersionUpdateCleanupInfo{
				Source:      config.JobInfoSourceVersionUpdateRemove,
				Version:     tt.version,
				CleanupOnly: tt.cleanupOnly,
				Components:  []model.VersionUpdateCleanupComponent{tt.component},
			})
			require.NoError(t, err)

			_, ok, err := versionUpdateCleanupInfoFromTask(&model.WorkflowQueue{CleanupInfo: string(payload)})

			require.True(t, ok)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestVersionUpdateCleanupInfoAcceptsMixedV3ComponentContracts(t *testing.T) {
	payload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
		Components: []model.VersionUpdateCleanupComponent{
			{Component: &model.ApplicationComponent{Name: "api", AppID: "app-1", ComponentType: config.ServerJob}},
			{
				Component:                  &model.ApplicationComponent{Name: "mysql", AppID: "app-1", ComponentType: config.StoreJob},
				RequireStatefulSetDeletion: true,
			},
			{
				Component:                  &model.ApplicationComponent{Name: "redis", AppID: "app-1", ComponentType: config.StoreJob},
				RequireStatefulSetDeletion: true, StatefulSetPVCTemplatesToDelete: []string{"data"},
			},
		},
	})
	require.NoError(t, err)

	cleanupInfo, ok, err := versionUpdateCleanupInfoFromTask(&model.WorkflowQueue{CleanupInfo: string(payload)})

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, cleanupInfo.Version)
}

func TestLoadPersistedCleanupJobInfosRejectsMismatchedStatefulSetPVCTemplates(t *testing.T) {
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	cleanupPayload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component:                       component,
			RequireStatefulSetDeletion:      true,
			StatefulSetPVCTemplatesToDelete: []string{"data"},
		}},
	})
	require.NoError(t, err)
	markerPayload, err := json.Marshal(map[string]interface{}{
		"source":                          config.JobInfoSourceVersionUpdateRemove,
		"version":                         model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
		"requireStatefulSetDeletion":      true,
		"statefulSetPVCTemplatesToDelete": []string{"cache"},
	})
	require.NoError(t, err)
	task := &model.WorkflowQueue{TaskID: "task-1", CleanupInfo: string(cleanupPayload)}
	store := &fakeDataStore{jobInfos: []*model.JobInfo{{
		Type: string(config.JobCleanupResources), TaskID: task.TaskID, ServiceName: component.Name,
		InternalInfo: string(markerPayload),
	}}}

	_, err = loadPersistedCleanupJobInfos(context.Background(), task, store)

	require.Error(t, err)
	require.Contains(t, err.Error(), "templates do not match")
}

func TestLoadPersistedCleanupJobInfosAcceptsLegacyStatefulSetDeletionMarker(t *testing.T) {
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	cleanupPayload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component:                  component,
			RequireStatefulSetDeletion: true,
		}},
	})
	require.NoError(t, err)
	task := &model.WorkflowQueue{TaskID: "task-1", CleanupInfo: string(cleanupPayload)}
	store := &fakeDataStore{jobInfos: []*model.JobInfo{{
		Type: string(config.JobCleanupResources), TaskID: task.TaskID, ServiceName: component.Name,
		InternalInfo: `{"source":"` + config.JobInfoSourceVersionUpdateRemove + `","requireStatefulSetDeletion":true}`,
	}}}

	records, err := loadPersistedCleanupJobInfos(context.Background(), task, store)

	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, config.StoreJob, records[0].Component.ComponentType)
}

func TestLoadPersistedCleanupJobInfosRejectsMissingStatefulSetDeletionMarker(t *testing.T) {
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	cleanupPayload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component: component, RequireStatefulSetDeletion: true,
		}},
	})
	require.NoError(t, err)
	task := &model.WorkflowQueue{TaskID: "task-1", CleanupInfo: string(cleanupPayload)}
	store := &fakeDataStore{jobInfos: []*model.JobInfo{{
		Type: string(config.JobCleanupResources), TaskID: task.TaskID, ServiceName: component.Name,
		InternalInfo: `{"source":"` + config.JobInfoSourceVersionUpdateRemove + `"}`,
	}}}

	_, err = loadPersistedCleanupJobInfos(context.Background(), task, store)

	require.Error(t, err)
	require.Contains(t, err.Error(), "StatefulSet deletion requirement does not match")
}

func TestLoadPersistedCleanupJobInfosRejectsVersionedNonPVCMarker(t *testing.T) {
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	cleanupPayload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component: component, RequireStatefulSetDeletion: true,
		}},
	})
	require.NoError(t, err)
	task := &model.WorkflowQueue{TaskID: "task-1", CleanupInfo: string(cleanupPayload)}
	store := &fakeDataStore{jobInfos: []*model.JobInfo{{
		Type: string(config.JobCleanupResources), TaskID: task.TaskID, ServiceName: component.Name,
		InternalInfo: `{"source":"` + config.JobInfoSourceVersionUpdateRemove + `","version":2,"requireStatefulSetDeletion":true}`,
	}}}

	_, err = loadPersistedCleanupJobInfos(context.Background(), task, store)

	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match non-PVC cleanup marker version 0")
}

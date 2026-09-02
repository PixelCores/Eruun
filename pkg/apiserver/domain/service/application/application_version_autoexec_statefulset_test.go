package application

import (
	"context"
	"encoding/json"

	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateVersionFullRebuildAllowsOnlyRecreatedStatefulSetImmutableChanges(t *testing.T) {
	tests := []struct {
		name          string
		shareStrategy string
		wantError     bool
	}{
		{name: "non-shared StatefulSet is recreated"},
		{name: "shared StatefulSet remains protected", shareStrategy: string(spec.ShareStrategyDefault), wantError: true},
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
			if tt.shareStrategy != "" {
				currentTraits.Share = &spec.ShareTraitSpec{Strategy: tt.shareStrategy}
			}
			store.components["mysql"] = &model.ApplicationComponent{
				Name: "mysql", AppID: "app-1", Namespace: config.DefaultNamespace,
				ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1,
				Status: string(config.ComponentStatusRunning), Traits: mustJSONStruct(&currentTraits),
			}
			store.workflows["wf-1"] = &model.Workflow{
				ID: "wf-1", AppID: "app-1", WorkflowType: config.WorkflowTaskTypeWorkflow,
				Steps: mustJSONStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{
					{Name: "mysql", WorkflowType: config.JobDeploy},
				}}),
			}
			desiredTraits := currentTraits
			desiredTraits.Storage = append([]spec.StorageTraitSpec(nil), currentTraits.Storage...)
			desiredTraits.Service = append([]spec.ServiceTraitSpec(nil), currentTraits.Service...)
			desiredTraits.Storage[0].Name = "data-v2"
			desiredTraits.Storage[0].Size = "2Gi"
			desiredTraits.Storage[0].StorageClass = "premium"
			desiredTraits.Service[0].Name = "mysql-headless-v2"

			svc := newMockServiceWithStore(store)
			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version: "2.0.0",
				Components: []apisv1.ComponentUpdateSpec{
					{Action: "remove", Name: "cleanup_all"},
					{Action: "add", Name: "all"},
					{Action: "update", Name: "mysql", Traits: &desiredTraits},
				},
			})

			if tt.wantError {
				require.ErrorIs(t, err, bcode.ErrApplicationConfig)
				require.Nil(t, resp)
				require.Equal(t, "1.0.0", store.apps["app-1"].Version)
				require.Empty(t, store.tasks)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotEmpty(t, resp.TaskID)
			require.Equal(t, []string{"mysql"}, resp.UpdatedComponents)
			require.Equal(t, "2.0.0", store.apps["app-1"].Version)
			var persistedTraits apisv1.Traits
			require.NoError(t, decodeJSONStruct(store.components["mysql"].Traits, &persistedTraits))
			require.Equal(t, "data-v2", persistedTraits.Storage[0].Name)
			require.Equal(t, "2Gi", persistedTraits.Storage[0].Size)
			require.Equal(t, "premium", persistedTraits.Storage[0].StorageClass)
			require.Equal(t, "mysql-headless-v2", persistedTraits.Service[0].Name)
			cleanupInfo := requireVersionUpdateCleanupInfoVersion(
				t,
				store.tasks[resp.TaskID],
				model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
			)
			cleanupComponent := requireVersionUpdateCleanupComponent(t, cleanupInfo, "mysql")
			require.True(t, cleanupComponent.RequireStatefulSetDeletion)
			require.Equal(t, []string{"data", "data-v2"}, cleanupComponent.StatefulSetPVCTemplatesToDelete)
			require.Len(t, store.jobs, 1)
			var marker struct {
				Version                         int      `json:"version"`
				RequireStatefulSetDeletion      bool     `json:"requireStatefulSetDeletion"`
				StatefulSetPVCTemplatesToDelete []string `json:"statefulSetPVCTemplatesToDelete"`
			}
			require.NoError(t, json.Unmarshal([]byte(store.jobs[0].InternalInfo), &marker))
			require.Equal(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, marker.Version)
			require.True(t, marker.RequireStatefulSetDeletion)
			require.Equal(t, []string{"data", "data-v2"}, marker.StatefulSetPVCTemplatesToDelete)
		})
	}
}

func TestUpdateVersionFullRebuildRejectsStatefulSetIdentityChange(t *testing.T) {
	tests := []struct {
		name             string
		currentShare     bool
		desiredShare     bool
		wantPersistShare bool
	}{
		{name: "non-shared to force shared", desiredShare: true},
		{name: "force shared to non-shared", currentShare: true, wantPersistShare: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)
			require.NotNil(t, req.Components[2].Traits)
			if tt.currentShare {
				var currentTraits apisv1.Traits
				require.NoError(t, decodeJSONStruct(store.components["mysql"].Traits, &currentTraits))
				currentTraits.Share = &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyForce)}
				store.components["mysql"].Traits = mustJSONStruct(&currentTraits)
			}
			if tt.desiredShare {
				req.Components[2].Traits.Share = &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyForce)}
			}

			resp, err := svc.UpdateVersion(context.Background(), "app-1", req)

			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Nil(t, resp)
			require.Contains(t, bcode.SafeClientMessage(err), "changes StatefulSet identity")
			require.Contains(t, bcode.SafeClientMessage(err), "migrate the StatefulSet/PVC separately")
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
			var persistedTraits apisv1.Traits
			require.NoError(t, decodeJSONStruct(store.components["mysql"].Traits, &persistedTraits))
			require.Equal(t, tt.wantPersistShare, persistedTraits.Share != nil)
			require.Equal(t, "data", persistedTraits.Storage[0].Name)
			require.Empty(t, store.tasks)
			require.Empty(t, store.jobs)
		})
	}
}

func TestUpdateVersionFullRebuildRestoresPendingStatefulSetPVCPlan(t *testing.T) {
	statuses := []config.Status{
		config.StatusCancelled,
		config.StatusFailed,
		config.StatusTimeout,
		config.StatusReject,
	}
	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)

			first, err := svc.UpdateVersion(context.Background(), "app-1", req)
			require.NoError(t, err)
			firstCleanup := requireVersionUpdateCleanupInfoVersion(
				t,
				store.tasks[first.TaskID],
				model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
			)
			firstComponent := requireVersionUpdateCleanupComponent(t, firstCleanup, "mysql")
			require.Equal(t, []string{"data", "data-v2"}, firstComponent.StatefulSetPVCTemplatesToDelete)
			store.tasks[first.TaskID].Status = status
			firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
			firstJob.Status = string(status)

			second, err := svc.UpdateVersion(context.Background(), "app-1", req)
			require.NoError(t, err)
			require.NotEqual(t, first.TaskID, second.TaskID)
			secondCleanup := requireVersionUpdateCleanupInfoVersion(
				t,
				store.tasks[second.TaskID],
				model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
			)
			secondComponent := requireVersionUpdateCleanupComponent(t, secondCleanup, "mysql")
			require.True(t, secondComponent.RequireStatefulSetDeletion)
			require.Equal(t, []string{"data", "data-v2"}, secondComponent.StatefulSetPVCTemplatesToDelete)
			var restoredTraits apisv1.Traits
			require.NoError(t, decodeJSONStruct(secondComponent.Component.Traits, &restoredTraits))
			require.Equal(t, "data", restoredTraits.Storage[0].Name)
			require.Equal(t, "1Gi", restoredTraits.Storage[0].Size)
			require.Equal(t, "mysql-headless", restoredTraits.Service[0].Name)
			secondJob := requireVersionUpdateCleanupJobForTask(t, store, second.TaskID, "mysql")
			require.Equal(t, string(config.StatusQueued), secondJob.Status)
			var marker versionUpdateCleanupJobMarker
			require.NoError(t, json.Unmarshal([]byte(secondJob.InternalInfo), &marker))
			require.Equal(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, marker.Version)
			require.Equal(t, []string{"data", "data-v2"}, marker.StatefulSetPVCTemplatesToDelete)
		})
	}
}

func TestUpdateVersionRejectsUpdatesWhileStatefulSetPVCMigrationIsPending(t *testing.T) {
	tests := []struct {
		name     string
		autoExec *bool
	}{
		{name: "auto exec"},
		{name: "manual update", autoExec: boolPtr(false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)
			first, err := svc.UpdateVersion(context.Background(), "app-1", req)
			require.NoError(t, err)
			store.tasks[first.TaskID].Status = config.StatusCancelled
			firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
			firstJob.Status = string(config.StatusCancelled)

			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:  "2.1.0",
				AutoExec: tt.autoExec,
				Components: []apisv1.ComponentUpdateSpec{{
					Action: "update", Name: "mysql", Image: "mysql:8.1",
				}},
			})

			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Nil(t, resp)
			require.Contains(t, bcode.SafeClientMessage(err), "unfinished StatefulSet migration")
			require.Equal(t, "2.0.0", store.apps["app-1"].Version)
			require.Equal(t, "mysql:8", store.components["mysql"].Image)
			require.Len(t, store.tasks, 1)
			require.Len(t, store.jobs, 1)
		})
	}
}

func TestUpdateVersionRejectsFullWorkflowForAnotherComponentWhileStatefulSetPVCMigrationIsPending(t *testing.T) {
	store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)
	store.components["api"] = &model.ApplicationComponent{
		Name: "api", AppID: "app-1", Namespace: config.DefaultNamespace,
		ComponentType: config.ServerJob, Image: "api:v1", Replicas: 1,
	}
	store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{
		{Name: "mysql", WorkflowType: config.JobDeploy},
		{Name: "api", WorkflowType: config.JobDeploy},
	}})
	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusCancelled
	for _, jobInfo := range store.jobs {
		if jobInfo != nil && jobInfo.TaskID == first.TaskID {
			jobInfo.Status = string(config.StatusCancelled)
		}
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "2.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Action: "update", Name: "api", Image: "api:v2",
		}},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Nil(t, resp)
	require.Contains(t, bcode.SafeClientMessage(err), "unfinished StatefulSet migration")
	require.Equal(t, "2.0.0", store.apps["app-1"].Version)
	require.Equal(t, "api:v1", store.components["api"].Image)
	require.Len(t, store.tasks, 1)
}

func TestUpdateVersionAllowsMetadataOnlyChangeWhileStatefulSetPVCMigrationIsPending(t *testing.T) {
	store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)
	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusCancelled
	firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
	firstJob.Status = string(config.StatusCancelled)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:     "2.1.0",
		Description: "metadata only while migration is pending",
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "2.1.0", store.apps["app-1"].Version)
	require.Equal(t, "metadata only while migration is pending", store.apps["app-1"].Description)
	require.NotEmpty(t, resp.TaskID)
	require.NotEqual(t, first.TaskID, resp.TaskID)
	require.Empty(t, resp.WorkflowID)
	require.Len(t, store.tasks, 1, "metadata-only update must not enqueue another workflow task")
}

func TestUpdateVersionPendingStatefulSetPVCMigrationRequiresExplicitVCTResume(t *testing.T) {
	store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)
	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusCancelled
	firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
	firstJob.Status = string(config.StatusCancelled)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "2.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "cleanup_all"},
			{Action: "add", Name: "all"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Nil(t, resp)
	require.Contains(t, bcode.SafeClientMessage(err), "immutable StatefulSet update")
	require.Equal(t, "2.0.0", store.apps["app-1"].Version)
	require.Len(t, store.tasks, 1)
	require.Len(t, store.jobs, 1)
}

func TestUpdateVersionFullRebuildRejectsMismatchedPendingStatefulSetPVCPlan(t *testing.T) {
	store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)

	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusCancelled
	firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
	firstJob.Status = string(config.StatusCancelled)
	firstJob.InternalInfo = `{"source":"version_update_remove","version":3,"requireStatefulSetDeletion":true,"statefulSetPVCTemplatesToDelete":["unexpected"]}`

	second, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Nil(t, second)
	require.Contains(t, err.Error(), "does not match")
	require.Len(t, store.tasks, 1)
}

func TestUpdateVersionFullRebuildRejectsIncompleteHistoricalStatefulSetPVCPlan(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*model.VersionUpdateCleanupInfo)
		want   string
	}{
		{
			name: "missing descriptor",
			mutate: func(cleanupInfo *model.VersionUpdateCleanupInfo) {
				cleanupInfo.Components[0].Component = nil
			},
			want: "missing its component descriptor",
		},
		{
			name: "missing template contract",
			mutate: func(cleanupInfo *model.VersionUpdateCleanupInfo) {
				cleanupInfo.Components[0].StatefulSetPVCTemplatesToDelete = nil
			},
			want: "has no StatefulSet PVC deletion contract",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)
			first, err := svc.UpdateVersion(context.Background(), "app-1", req)
			require.NoError(t, err)
			store.tasks[first.TaskID].Status = config.StatusCancelled
			firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
			firstJob.Status = string(config.StatusCancelled)

			var cleanupInfo model.VersionUpdateCleanupInfo
			require.NoError(t, json.Unmarshal([]byte(store.tasks[first.TaskID].CleanupInfo), &cleanupInfo))
			tt.mutate(&cleanupInfo)
			payload, err := json.Marshal(cleanupInfo)
			require.NoError(t, err)
			store.tasks[first.TaskID].CleanupInfo = string(payload)

			second, err := svc.UpdateVersion(context.Background(), "app-1", req)
			require.Error(t, err)
			require.Nil(t, second)
			require.Contains(t, err.Error(), tt.want)
			require.Len(t, store.tasks, 1)
		})
	}
}

func TestPendingStatefulSetPVCDeletionKeepsResourceIdentitiesSeparate(t *testing.T) {
	traits := apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", TmpCreate: true, Size: "1Gi",
		}},
		Service: []spec.ServiceTraitSpec{{
			Name: "mysql-headless", Type: string(spec.ServiceAccessInternal), Headless: true,
			Selector: map[string]string{config.LabelComponentName: "mysql"},
		}},
	}
	nonShared := &model.ApplicationComponent{
		ID: 101, AppID: "app-1", Name: "mysql", Namespace: config.DefaultNamespace,
		ResourceAppName: "shop", ComponentType: config.StoreJob, Traits: mustJSONStruct(&traits),
	}
	sharedTraits := traits
	sharedTraits.Share = &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyDefault)}
	shared := *nonShared
	shared.Traits = mustJSONStruct(&sharedTraits)
	nonSharedCleanup := model.VersionUpdateCleanupComponent{
		Component: nonShared, ResourceAppName: "shop", RequireStatefulSetDeletion: true,
		StatefulSetPVCTemplatesToDelete: []string{"data"},
	}
	sharedCleanup := model.VersionUpdateCleanupComponent{
		Component: &shared, ResourceAppName: "shop", RequireStatefulSetDeletion: true,
		StatefulSetPVCTemplatesToDelete: []string{"data"},
	}
	nonSharedIdentity, err := versionUpdateCleanupStatefulSetIdentity(nonSharedCleanup)
	require.NoError(t, err)
	sharedIdentity, err := versionUpdateCleanupStatefulSetIdentity(sharedCleanup)
	require.NoError(t, err)
	require.NotEqual(t, nonSharedIdentity, sharedIdentity)

	marker, err := versionUpdateCleanupJobInfoMarker(true, []string{"data"})
	require.NoError(t, err)
	pending := make(map[string]map[string]*pendingStatefulSetPVCDeletion)
	failedJob := func(taskID string) []*model.JobInfo {
		return []*model.JobInfo{{
			TaskID: taskID, Type: string(config.JobCleanupResources), ServiceName: "mysql",
			Status: string(config.StatusFailed), InternalInfo: marker,
		}}
	}
	valid, err := updatePendingStatefulSetPVCDeletion(
		pending,
		&model.WorkflowQueue{TaskID: "task-old", Status: config.StatusFailed},
		failedJob("task-old"),
		nonSharedCleanup,
	)
	require.NoError(t, err)
	require.True(t, valid)

	completedSharedJob := failedJob("task-shared-completed")
	completedSharedJob[0].Status = string(config.StatusCompleted)
	valid, err = updatePendingStatefulSetPVCDeletion(
		pending,
		&model.WorkflowQueue{TaskID: "task-shared-completed", Status: config.StatusCompleted},
		completedSharedJob,
		sharedCleanup,
	)
	require.NoError(t, err)
	require.True(t, valid)
	require.Contains(t, pending["mysql"], nonSharedIdentity)
	require.NotContains(t, pending["mysql"], sharedIdentity)

	valid, err = updatePendingStatefulSetPVCDeletion(
		pending,
		&model.WorkflowQueue{TaskID: "task-shared-failed", Status: config.StatusFailed},
		failedJob("task-shared-failed"),
		sharedCleanup,
	)
	require.NoError(t, err)
	require.True(t, valid)
	require.Len(t, pending["mysql"], 2)
	_, err = selectPendingStatefulSetPVCDeletion(pending["mysql"], nonShared)
	require.Error(t, err)
	require.Contains(t, err.Error(), "multiple resource identities")
}

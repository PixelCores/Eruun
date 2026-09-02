package application

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateVersionPendingStatefulSetPVCDeletionRejectsPartialTemplateReplay(t *testing.T) {
	store, svc, req := newStatefulSetPVCTwoTemplateRetryFixture(t)
	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusFailed
	requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql").Status = string(config.StatusFailed)

	retry := cloneStatefulSetPVCTwoTemplateRetryRequest(req)
	retry.Version = "2.1.0"
	desiredTraits := retry.Components[2].Traits
	desiredTraits.Storage[0] = spec.StorageTraitSpec{
		Name:      "data",
		Type:      config.StorageTypePersistent,
		MountPath: "/data",
		TmpCreate: true,
		Size:      "1Gi",
	}
	desiredTraits.Storage[1].Name = "logs-v2"
	taskCount := len(store.tasks)
	jobCount := len(store.jobs)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", retry)

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.ErrorContains(t, err, "does not reproduce the pending VCT deletion plan")
	require.Nil(t, resp)
	require.Len(t, store.tasks, taskCount)
	require.Len(t, store.jobs, jobCount)
	require.Equal(t, "2.0.0", store.apps["app-1"].Version)
	var persistedTraits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["mysql"].Traits, &persistedTraits))
	require.Equal(t, "data-v2", persistedTraits.Storage[0].Name)
	require.Equal(t, "logs", persistedTraits.Storage[1].Name)
}

func TestUpdateVersionPendingStatefulSetPVCDeletionAllowsCoveredReplayWithAdditionalTemplates(t *testing.T) {
	store, svc, req := newStatefulSetPVCTwoTemplateRetryFixture(t)
	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusFailed
	requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql").Status = string(config.StatusFailed)

	retry := cloneStatefulSetPVCTwoTemplateRetryRequest(req)
	retry.Version = "2.1.0"
	retry.Components[2].Traits.Storage[1].Name = "logs-v2"

	second, err := svc.UpdateVersion(context.Background(), "app-1", retry)

	require.NoError(t, err)
	require.NotNil(t, second)
	cleanupInfo := requireVersionUpdateCleanupInfoVersion(
		t,
		store.tasks[second.TaskID],
		model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
	)
	require.Equal(t, []string{first.TaskID}, cleanupInfo.ResolvesTaskIDs)
	cleanupComponent := requireVersionUpdateCleanupComponent(t, cleanupInfo, "mysql")
	require.Equal(t, []string{"data", "data-v2", "logs", "logs-v2"}, cleanupComponent.StatefulSetPVCTemplatesToDelete)
	var persistedTraits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["mysql"].Traits, &persistedTraits))
	require.Equal(t, "data-v2", persistedTraits.Storage[0].Name)
	require.Equal(t, "logs-v2", persistedTraits.Storage[1].Name)
}

func newStatefulSetPVCTwoTemplateRetryFixture(t *testing.T) (*inMemoryAppStore, *applicationsServiceImpl, apisv1.UpdateVersionRequest) {
	t.Helper()
	store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)
	logsStorage := spec.StorageTraitSpec{
		Name:      "logs",
		Type:      config.StorageTypePersistent,
		MountPath: "/logs",
		TmpCreate: true,
		Size:      "1Gi",
	}

	var currentTraits apisv1.Traits
	require.NoError(t, decodeJSONStruct(store.components["mysql"].Traits, &currentTraits))
	currentTraits.Storage = append(currentTraits.Storage, logsStorage)
	store.components["mysql"].Traits = mustJSONStruct(&currentTraits)

	desiredTraits := *req.Components[2].Traits
	desiredTraits.Storage = append([]spec.StorageTraitSpec(nil), desiredTraits.Storage...)
	desiredTraits.Storage = append(desiredTraits.Storage, logsStorage)
	req.Components[2].Traits = &desiredTraits
	return store, svc, req
}

func cloneStatefulSetPVCTwoTemplateRetryRequest(req apisv1.UpdateVersionRequest) apisv1.UpdateVersionRequest {
	cloned := req
	cloned.Components = append([]apisv1.ComponentUpdateSpec(nil), req.Components...)
	update := cloned.Components[2]
	desiredTraits := *update.Traits
	desiredTraits.Storage = append([]spec.StorageTraitSpec(nil), desiredTraits.Storage...)
	update.Traits = &desiredTraits
	cloned.Components[2] = update
	return cloned
}

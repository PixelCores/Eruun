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

func newGatewayRemovalFixture() *inMemoryAppStore {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "DemoApp", Version: "1.0.0", Namespace: "default"}
	store.components["web"] = &model.ApplicationComponent{
		Name:          "web",
		AppID:         "app-1",
		Image:         "web:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
		Traits:        mustJSONStruct(&spec.Traits{Gateway: &spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}}}),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{Name: "web", WorkflowType: config.JobDeploy}}}),
	}
	return store
}

func TestUpdateVersionRemovingGatewayDeletesRouteInSameTask(t *testing.T) {
	store := newGatewayRemovalFixture()
	resp, err := newMockServiceWithStore(store).UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:    "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{Name: "web", Traits: &apisv1.Traits{}}},
	})
	require.NoError(t, err)

	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)
	var info model.VersionUpdateResourceActionInfo
	require.NoError(t, json.Unmarshal([]byte(task.ResourceActionInfo), &info))
	require.Equal(t, []string{"web"}, info.RouteDeleteComponents)
}

func TestUpdateVersionRemovingGatewayRequiresAutoExec(t *testing.T) {
	store := newGatewayRemovalFixture()
	autoExec := false
	resp, err := newMockServiceWithStore(store).UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:    "1.1.0",
		AutoExec:   &autoExec,
		Components: []apisv1.ComponentUpdateSpec{{Name: "web", Traits: &apisv1.Traits{}}},
	})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "removing traits.gateway requires autoExec=true")
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)

	stored, err := store.components["web"].ComponentSpec()
	require.NoError(t, err)
	require.NotNil(t, stored.Traits.Gateway, "the rejected update must not drop the stored gateway")
}

// Updates that keep the gateway, or never had one, record no route deletion.
func TestUpdateVersionKeepingGatewayRecordsNoRouteDeletion(t *testing.T) {
	store := newGatewayRemovalFixture()
	resp, err := newMockServiceWithStore(store).UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:    "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{Name: "web", Image: "web:v2"}},
	})
	require.NoError(t, err)

	var info model.VersionUpdateResourceActionInfo
	require.NoError(t, json.Unmarshal([]byte(store.tasks[resp.TaskID].ResourceActionInfo), &info))
	require.Empty(t, info.RouteDeleteComponents)
}

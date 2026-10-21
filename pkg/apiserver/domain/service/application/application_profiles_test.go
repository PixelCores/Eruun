package application

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestApplicationWritesRejectMissingNodePool(t *testing.T) {
	for _, operation := range []string{"create", "update", "template"} {
		t.Run(operation, func(t *testing.T) {
			store := newInMemoryAppStore()
			app := &model.Applications{ID: "app", Name: "demo", Namespace: config.DefaultNamespace, Version: "1.0.0", TemplateEnabled: operation == "template"}
			store.apps[app.ID] = app
			store.components["api"] = &model.ApplicationComponent{
				Name: "api", AppID: app.ID, Namespace: app.Namespace,
				ComponentType: config.ServerJob, Image: "nginx:1.27", Replicas: 1,
				Properties: mustJSONStruct(apisv1.Properties{}), Traits: mustJSONStruct(apisv1.Traits{}),
			}
			service := newMockServiceWithStore(store)
			traits := apisv1.Traits{NodePool: "missing"}
			var err error
			switch operation {
			case "create":
				_, err = service.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
					Name: "other", Components: []apisv1.CreateComponentRequest{{Name: "worker", ComponentType: config.ServerJob, Image: "nginx:1.27", Traits: traits}},
				})
			case "update":
				_, err = service.UpdateVersion(context.Background(), app.ID, apisv1.UpdateVersionRequest{
					Version: "2.0.0", AutoExec: boolPtr(false), Components: []apisv1.ComponentUpdateSpec{{Name: "api", Traits: &traits}},
				})
			case "template":
				_, err = service.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
					Name: "clone", Components: []apisv1.CreateComponentRequest{{Name: "worker", Template: &apisv1.TemplateRef{ID: app.ID, Target: "api"}, Traits: traits}},
				})
			}
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.ErrorContains(t, err, "nodePool")
			require.ErrorContains(t, err, "missing")
			require.Len(t, store.apps, 1)
			require.Len(t, store.components, 1)
			require.Equal(t, "1.0.0", app.Version)
		})
	}
}

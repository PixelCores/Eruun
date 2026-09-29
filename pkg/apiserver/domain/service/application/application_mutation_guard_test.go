package application

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestApplicationMutationsPreserveIDAndReadOnlyGuards(t *testing.T) {
	operations := []struct {
		name string
		run  func(*applicationsServiceImpl, string) error
	}{
		{"stop", func(s *applicationsServiceImpl, id string) error {
			_, err := s.StopApplicationDeployments(context.Background(), id, apisv1.ApplicationLifecycleRequest{})
			return err
		}},
		{"start", func(s *applicationsServiceImpl, id string) error {
			_, err := s.StartApplicationDeployments(context.Background(), id, apisv1.ApplicationLifecycleRequest{})
			return err
		}},
		{"restart", func(s *applicationsServiceImpl, id string) error {
			_, err := s.RestartApplicationWorkloads(context.Background(), id, apisv1.ApplicationLifecycleRequest{})
			return err
		}},
		{"version", func(s *applicationsServiceImpl, id string) error {
			_, err := s.UpdateVersion(context.Background(), id, apisv1.UpdateVersionRequest{Version: "1.1.0", AutoExec: boolPtr(false)})
			return err
		}},
		{"workflow", func(s *applicationsServiceImpl, id string) error {
			_, err := s.UpdateApplicationWorkflow(context.Background(), id, apisv1.UpdateApplicationWorkflowRequest{
				Workflow: []apisv1.CreateWorkflowStepRequest{{Name: "deploy", WorkflowType: config.JobDeploy}},
			})
			return err
		}},
	}
	for _, operation := range operations {
		for _, tc := range []struct {
			name  string
			appID string
			mode  spec.ManagementMode
			want  error
		}{
			{name: "empty", want: bcode.ErrApplicationNotExist},
			{name: "whitespace", appID: " ", want: bcode.ErrApplicationNotExist},
			{name: "padded", appID: " app-1 ", want: bcode.ErrApplicationNotExist},
			{name: "missing", appID: "missing", want: bcode.ErrApplicationNotExist},
			{name: "observe", appID: "app-1", mode: spec.ManagementModeObserve, want: bcode.ErrApplicationManagementMode},
		} {
			t.Run(operation.name+"/"+tc.name, func(t *testing.T) {
				store := newInMemoryAppStore()
				store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "shop", Version: "1.0.0", ManagementMode: tc.mode}
				svc := newMockServiceWithStore(store)
				client := fake.NewSimpleClientset()
				svc.KubeClient = client

				require.ErrorIs(t, operation.run(svc, tc.appID), tc.want)
				require.Equal(t, "1.0.0", store.apps["app-1"].Version)
				require.Empty(t, store.tasks)
				require.Empty(t, store.workflows)
				require.Empty(t, client.Actions())
			})
		}
	}
}

func TestApplicationMutationValidationOrder(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "shop"}
	svc := newMockServiceWithStore(store)

	_, err := svc.StopApplicationDeployments(context.Background(), " app-1 ", apisv1.ApplicationLifecycleRequest{})
	require.EqualError(t, err, "kube client is nil")
	_, err = svc.StartApplicationDeployments(context.Background(), " app-1 ", apisv1.ApplicationLifecycleRequest{})
	require.EqualError(t, err, "kube client is nil")
	_, err = svc.RestartApplicationWorkloads(context.Background(), " app-1 ", apisv1.ApplicationLifecycleRequest{})
	require.EqualError(t, err, "kube client is nil")
	_, err = svc.UpdateApplicationWorkflow(context.Background(), " app-1 ", apisv1.UpdateApplicationWorkflowRequest{})
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
}

func TestUpdateVersionPreservesDatastoreIDResolution(t *testing.T) {
	for _, requestedID := range []string{"APP-1", " app-1 "} {
		t.Run(requestedID, func(t *testing.T) {
			store := newInMemoryAppStore()
			app := &model.Applications{ID: "app-1", Name: "shop", Version: "1.0.0"}
			store.apps[app.ID] = app
			// Model a datastore whose comparison resolves an equivalent ID.
			store.apps[requestedID] = app
			svc := newMockServiceWithStore(store)

			_, err := svc.UpdateVersion(context.Background(), requestedID, apisv1.UpdateVersionRequest{Version: "1.1.0", AutoExec: boolPtr(false)})

			require.NoError(t, err)
			require.Equal(t, "1.1.0", store.apps[app.ID].Version)
		})
	}
}

func TestUpdateVersionChecksManagementModeOfRawID(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "shop", Version: "1.0.0"}
	store.apps[" app-1 "] = &model.Applications{ID: " app-1 ", Name: "observed-shop", ManagementMode: spec.ManagementModeObserve}
	svc := newMockServiceWithStore(store)

	_, err := svc.UpdateVersion(context.Background(), " app-1 ", apisv1.UpdateVersionRequest{Version: "1.1.0", AutoExec: boolPtr(false)})

	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
}

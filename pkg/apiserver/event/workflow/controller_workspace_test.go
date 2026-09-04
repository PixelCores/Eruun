package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

// The persistence recovery tests use a synthetic application and workspace;
// their task/CAS behavior continues to come from the original fault-injecting store.
type workspaceControllerTestStore struct {
	datastore.DataStore
	appID string
}

func (s *workspaceControllerTestStore) Get(ctx context.Context, e datastore.Entity) error {
	switch v := e.(type) {
	case *model.Applications:
		if v.ID == s.appID {
			*v = model.Applications{ID: s.appID, WorkspaceID: "workspace", Namespace: config.DefaultNamespace}
			return nil
		}
	case *model.Workspace:
		if v.ID == "workspace" {
			*v = model.Workspace{ID: "workspace", Namespace: config.DefaultNamespace}
			return nil
		}
	}
	if err := s.DataStore.Get(ctx, e); err != nil {
		return err
	}
	if workflow, ok := e.(*model.Workflow); ok {
		workflow.AppID = s.appID
		workflow.Namespace = config.DefaultNamespace
	}
	return nil
}

func (s *workspaceControllerTestStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.DataStore.(datastore.Transactional).WithTransaction(ctx, func(tx datastore.DataStore) error {
		return fn(&workspaceControllerTestStore{DataStore: tx, appID: s.appID})
	})
}

func (s *workspaceControllerTestStore) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	return s.DataStore.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
}
func (s *workspaceControllerTestStore) CompareAndSwapWithConditions(ctx context.Context, e datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	return s.DataStore.(datastore.ConditionalCompareAndSwap).CompareAndSwapWithConditions(ctx, e, conditions, updates)
}

func runControllerRecoveryForTest(t *testing.T, w *Workflow, ctl *WorkflowCtl) error {
	t.Helper()
	store := &workspaceControllerTestStore{DataStore: w.Store, appID: ctl.snapshotTask().AppID}
	w.Store = store
	ctl.Store = store
	w.Cfg.Accounts = &spec.AccountConfig{}
	ctl.accountConfig = w.Cfg.Accounts
	w.KubeConfig = &rest.Config{Host: "https://kubernetes.example.invalid"}
	ctl.KubeConfig = w.KubeConfig
	return w.runWorkflowControllerWithPersistenceRecovery(context.Background(), ctl, 1)
}

func TestWorkflowRequiresPersistedWorkspaceBeforeExecution(t *testing.T) {
	store := &controllerTestStore{}
	task := &model.WorkflowQueue{TaskID: "task", AppID: "app", Status: config.StatusQueued}
	ctl := newTestWorkflowController(t, task, nil, store)
	require.ErrorContains(t, ctl.Run(context.Background(), 1), "workspace configuration is required")
	require.Equal(t, config.StatusFailed, ctl.snapshotTask().Status)
	ctl.accountConfig = &spec.AccountConfig{}
	ctl.KubeConfig = &rest.Config{Host: "https://kubernetes.example.invalid"}
	ctl.Store = &workspaceControllerTestStore{DataStore: store, appID: "app"}
	ctx, err := ctl.prepareWorkspace(context.Background())
	require.NoError(t, err)
	scope, ok := access.FromContext(ctx)
	require.True(t, ok)
	require.Equal(t, "workspace", scope.WorkspaceID)
	require.Empty(t, scope.UserID)
	require.Equal(t, "system:serviceaccount:default:eruun-runner", ctl.KubeConfig.Impersonate.UserName)
	// Resolving ownership does not create a namespace or require a live client.
	require.Nil(t, ctl.workspaceManager.Client)
}

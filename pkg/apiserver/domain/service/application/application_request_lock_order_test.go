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

// Request rules run before the application lock: while another operation
// holds it, an invalid request still fails with its own 400 error instead of
// ErrApplicationOperationLocked.
func TestRequestRulesRunBeforeApplicationLock(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default", Version: "1.0.0"}
	store.components["mysql"] = &model.ApplicationComponent{ID: 1, AppID: "app-1", Name: "mysql", Namespace: "default", ComponentType: config.StoreJob}
	svc := newMockServiceWithStore(store)
	releaseLock := holdApplicationTestAppScheduleLock(t, svc.ScheduleLocker, "app-1")
	defer releaseLock()
	ctx := context.Background()

	_, err := svc.UpdateVersion(ctx, "app-1", apisv1.UpdateVersionRequest{})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)

	for _, req := range []apisv1.DatabaseResetRequest{
		{},
		{Components: []string{" "}},
		{Components: []string{"mysql"}, InitSQLURL: "not-a-url"},
	} {
		_, err = svc.ResetApplicationDatabases(ctx, "app-1", req)
		require.ErrorIs(t, err, bcode.ErrApplicationConfig, "%+v", req)
	}

	for _, req := range []apisv1.UpdateApplicationWorkflowRequest{
		{},
		{WorkflowType: "bogus", Workflow: []apisv1.CreateWorkflowStepRequest{{Name: "deploy", Components: []string{"mysql"}}}},
	} {
		_, err = svc.UpdateApplicationWorkflow(ctx, "app-1", req)
		require.ErrorIs(t, err, bcode.ErrWorkflowConfig, "%+v", req)
	}

	// A valid request still waits for the lock.
	_, err = svc.ResetApplicationDatabases(ctx, "app-1", apisv1.DatabaseResetRequest{Components: []string{"mysql"}})
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
}

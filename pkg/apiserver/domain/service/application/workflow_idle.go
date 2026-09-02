package application

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/urlpolicy"
)

func EnsureAppWorkflowIdle(ctx context.Context, store datastore.DataStore, appID string) error {
	return workflowservice.EnsureAppWorkflowIdle(ctx, store, appID)
}

func EnsureNoPendingStatefulSetCleanup(ctx context.Context, store datastore.DataStore, appID string) error {
	return workflowservice.EnsureNoPendingStatefulSetCleanup(ctx, store, appID)
}

func ConvertComponent(req *apisv1.CreateComponentRequest, appID string) *model.ApplicationComponent {
	return workflowservice.ConvertComponent(req, appID)
}

func isWorkflowActiveStatus(status config.Status) bool {
	return workflowservice.IsWorkflowActiveStatus(status)
}

func taskHasActiveJobs(ctx context.Context, store datastore.DataStore, taskID string) (bool, error) {
	return workflowservice.TaskHasActiveJobs(ctx, store, taskID)
}

func normalizeExecuteAt(executeAt int64) (int64, error) {
	return workflowservice.NormalizeExecuteAt(executeAt)
}

func validateWorkflowTaskEnqueue(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, requireComponentInventory bool) error {
	return workflowservice.ValidateWorkflowTaskEnqueue(ctx, store, workflow, requireComponentInventory)
}

func createWorkflowQueueTaskWithCleanupInfo(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey, cleanupInfo string) (*model.WorkflowQueue, error) {
	return workflowservice.CreateWorkflowQueueTaskWithCleanupInfo(ctx, store, workflow, executeAt, idempotencyKey, cleanupInfo)
}

func createWorkflowQueueTaskWithCallback(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey, cleanupInfo string, callback *model.JSONStruct) (*model.WorkflowQueue, error) {
	return workflowservice.CreateWorkflowQueueTaskWithCallback(ctx, store, workflow, executeAt, idempotencyKey, cleanupInfo, callback)
}

func createWorkflowQueueTaskWithResourceActionInfoAndCallback(ctx context.Context, store datastore.DataStore, workflow *model.Workflow, executeAt int64, idempotencyKey, cleanupInfo, resourceActionInfo string, callback *model.JSONStruct) (*model.WorkflowQueue, error) {
	return workflowservice.CreateWorkflowQueueTaskWithResourceActionInfoAndCallback(ctx, store, workflow, executeAt, idempotencyKey, cleanupInfo, resourceActionInfo, callback)
}

func triggerWorkflowTerminalCallbackAsync(ctx context.Context, store datastore.DataStore, cfg *config.Config, provider *urlpolicy.Provider, task *model.WorkflowQueue, status config.Status, reason string) {
	workflowservice.TriggerWorkflowTerminalCallbackAsync(ctx, store, cfg, provider, task, status, reason)
}

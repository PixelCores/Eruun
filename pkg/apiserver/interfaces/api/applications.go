package api

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

type namespaceImportService interface {
	ImportNamespaceResources(context.Context, apisv1.ImportNamespaceApplicationsRequest) (*apisv1.ImportNamespaceApplicationsResponse, error)
	TryImportNamespaceResources(context.Context, apisv1.TryImportNamespaceApplicationsRequest) (*apisv1.TryImportNamespaceApplicationsResponse, error)
}

type applications struct {
	ApplicationService     service.ApplicationsService       `inject:""`
	RuntimeComponentReader applicationRuntimeComponentReader `inject:""`
	WorkflowService        service.WorkflowService           `inject:""`
	ValidationService      service.ValidationService         `inject:""`
	ConversionService      service.ConversionService         `inject:""`
	ImportService          namespaceImportService            `inject:""`
}

// NewApplications new applications manage
func NewApplications() Interface {
	return &applications{}
}

func (app *applications) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/applications", app.listApplications)
	group.GET("/applications/templates", app.listTemplateApplications)
	group.GET("/cronjobs", app.listCronJobs)
	group.GET("/scheduledjobs", app.listScheduledJobs)
	group.POST("/applications", app.createApplications)
	group.POST("/applications/create-and-exec", app.createAndExecApplications)
	group.POST("/applications/query", app.batchGetApplications)
	group.POST("/applications/convert", app.convertApplications)
	group.POST("/applications/import/namespace", app.importNamespaceApplications)
	group.POST("/applications/import/namespace/try", app.tryImportNamespaceApplications)
	group.GET("/applications/:appID/workflows", app.listApplicationWorkflows)
	group.GET("/applications/:appID/status", app.getApplicationStatus)
	group.GET("/applications/:appID/components", app.listApplicationComponents)
	group.GET("/applications/:appID/components/status", app.getApplicationComponentStatus)
	group.GET("/applications/:appID/components/:componentName/containers", app.listComponentContainers)
	group.POST("/applications/components/status", app.listBatchApplicationComponentStatus)
	group.GET("/applications/:appID/components/:componentName/logs", app.streamComponentLogs)
	group.POST("/applications/:appID/components/:componentName/files/export", app.exportComponentFilesZip)
	group.POST("/applications/:appID/components/:componentName/shell/exec", app.execComponentShellScript)
	group.POST("/applications/:appID/components/:componentName/shell/stream", app.streamComponentShellScript)
	group.DELETE("/applications/:appID", app.deleteApplication)
	group.PUT("/applications/:appID/workflow", app.updateApplicationWorkflow)
	group.GET("/applications/:appID/workflow/schedules", app.listWorkflowSchedules)
	group.POST("/applications/:appID/workflow/schedule", app.upsertWorkflowSchedule)
	group.DELETE("/applications/:appID/workflow/schedule/:workflowID", app.deleteWorkflowSchedule)
	group.POST("/applications/:appID/resources/cleanup-plan", app.planApplicationResourceCleanup)
	group.DELETE("/applications/:appID/resources", app.deleteApplicationResources)
	group.POST("/applications/:appID/database-reset", app.resetApplicationDatabases)
	group.POST("/applications/:appID/log-archives", app.downloadLogArchive)
	group.POST("/applications/:appID/restart", app.restartApplicationWorkloads)
	group.POST("/applications/:appID/stop", app.stopApplicationDeployments)
	group.POST("/applications/:appID/start", app.startApplicationDeployments)
	group.POST("/applications/:appID/workflow/exec", app.execApplicationWorkflow)
	group.POST("/applications/:appID/workflow/cancel", app.cancelApplicationWorkflow)
	group.POST("/applications/:appID/workflow/tasks/cancel-all", app.cancelAllApplicationWorkflows)
	group.GET("/applications/:appID/workflow/tasks", app.listApplicationTasks)
	group.POST("/workflow/tasks/:taskID/approval", app.approveWorkflowTask)
	group.GET("/workflow/tasks/:taskID/status", app.getWorkflowTaskStatus)
	group.GET("/workflow/tasks/:taskID/stages", app.getWorkflowTaskStages)
	group.POST("/applications/:appID/version", app.updateVersion)
	group.POST("/applications/:appID/version/diff-update", app.diffUpdateVersion)
	group.POST("/applications/:appID/version/cancel", app.cancelDelayedVersionUpdate)
	group.POST("/applications/try", app.tryApplication)
	group.POST("/applications/:appID/workflow/try", app.tryWorkflow)
}

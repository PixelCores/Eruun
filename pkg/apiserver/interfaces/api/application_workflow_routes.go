package api

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/gin-gonic/gin"
)

// applicationWorkflows handles workflow operations that do not need the
// application service.
type applicationWorkflows struct {
	WorkflowService service.WorkflowService `inject:""`
}

func (app *applicationWorkflows) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/applications/:appID/workflow/schedules", app.listWorkflowSchedules)
	group.POST("/applications/:appID/workflow/schedule", app.upsertWorkflowSchedule)
	group.DELETE("/applications/:appID/workflow/schedule/:workflowID", app.deleteWorkflowSchedule)
	group.POST("/applications/:appID/workflow/exec", app.execApplicationWorkflow)
	group.POST("/applications/:appID/workflow/cancel", app.cancelApplicationWorkflow)
	group.POST("/applications/:appID/workflow/tasks/cancel-all", app.cancelAllApplicationWorkflows)
	group.POST("/workflow/tasks/:taskID/approval", app.approveWorkflowTask)
	group.GET("/workflow/tasks/:taskID/status", app.getWorkflowTaskStatus)
	group.GET("/workflow/tasks/:taskID/stages", app.getWorkflowTaskStages)
	group.POST("/applications/:appID/version/cancel", app.cancelDelayedVersionUpdate)
}

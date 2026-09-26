package application

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

type operationJobRecord struct {
	name   string
	status config.Status
	info   string
	errMsg string
}

func (c *applicationsServiceImpl) attachOperationTaskWithCallback(ctx context.Context, app *model.Applications, taskType config.WorkflowTaskType, name string, startTime, endTime int64, jobs []operationJobRecord, failedResources []string, callback *model.JSONStruct) (string, *model.WorkflowQueue, error) {
	if app == nil {
		return "", nil, fmt.Errorf("app is nil")
	}
	status := operationTaskTerminalStatus(failedResources)
	task, err := c.recordAppOperationTask(ctx, app, taskType, name, "", status, startTime, endTime, jobs, callback)
	if err != nil {
		return "", nil, err
	}
	return task.TaskID, task, nil
}

func (c *applicationsServiceImpl) triggerOperationTaskCallback(ctx context.Context, task *model.WorkflowQueue, callback *model.JSONStruct, failedResources []string) {
	if callback == nil || task == nil {
		return
	}
	triggerWorkflowTerminalCallbackAsync(ctx, c.Store, c.Cfg, c.URLSecurityPolicyProvider, task, operationTaskTerminalStatus(failedResources), "")
}

func operationTaskTerminalStatus(failedResources []string) config.Status {
	if len(failedResources) > 0 {
		return config.StatusFailed
	}
	return config.StatusCompleted
}

func (c *applicationsServiceImpl) recordAppOperationTask(ctx context.Context, app *model.Applications, taskType config.WorkflowTaskType, name, workflowID string, status config.Status, startTime, endTime int64, jobs []operationJobRecord, callback *model.JSONStruct) (*model.WorkflowQueue, error) {
	txStore, ok := c.Store.(datastore.Transactional)
	if !ok {
		return nil, fmt.Errorf("record %s task requires transactional datastore", name)
	}
	var task *model.WorkflowQueue
	err := txStore.WithTransaction(ctx, func(tx datastore.DataStore) error {
		var err error
		task, err = recordAppOperationTaskInStore(ctx, tx, app, taskType, name, workflowID, status, startTime, endTime, jobs, callback)
		return err
	})
	if err != nil {
		return nil, err
	}
	return task, nil
}

func recordAppOperationTaskInStore(ctx context.Context, store datastore.DataStore, app *model.Applications, taskType config.WorkflowTaskType, name, workflowID string, status config.Status, startTime, endTime int64, jobs []operationJobRecord, callback *model.JSONStruct) (*model.WorkflowQueue, error) {
	if app == nil {
		return nil, fmt.Errorf("app is nil")
	}
	if store == nil {
		return nil, fmt.Errorf("datastore is nil")
	}
	task := &model.WorkflowQueue{
		TaskID:              utils.RandStringByNumLowercase(24),
		AppID:               app.ID,
		ProjectID:           app.Project,
		WorkflowName:        name,
		WorkflowDisplayName: name,
		WorkflowID:          strings.TrimSpace(workflowID),
		Type:                taskType,
		Status:              status,
		Callback:            callback,
		BaseModel: model.BaseModel{
			CreateTime: time.Unix(startTime, 0),
			UpdateTime: time.Unix(endTime, 0),
		},
	}
	if err := store.Add(ctx, task); err != nil {
		return nil, fmt.Errorf("record %s task: %w", name, err)
	}
	for _, job := range jobs {
		serviceName := strings.TrimSpace(job.name)
		if serviceName == "" {
			serviceName = name
		}
		if err := store.Add(ctx, &model.JobInfo{
			Type:        string(taskType),
			ProductID:   app.Project,
			AppID:       app.ID,
			TaskID:      task.TaskID,
			Status:      string(job.status),
			StartTime:   startTime,
			EndTime:     endTime,
			Info:        job.info,
			Error:       job.errMsg,
			ServiceName: serviceName,
		}); err != nil {
			return nil, fmt.Errorf("record %s job info for %s: %w", name, serviceName, err)
		}
	}
	return task, nil
}

func buildCleanupJobRecords(reporter *cleanupReporter) []operationJobRecord {
	if reporter == nil {
		return nil
	}
	records := buildResourceJobRecords(reporter.deletedResources, config.StatusCompleted, "deleted")
	records = append(records, buildFailedResourceJobRecords(reporter.failedResources)...)
	return records
}

func buildRestartJobRecords(reporter *restartReporter) []operationJobRecord {
	if reporter == nil {
		return nil
	}
	records := buildResourceJobRecords(reporter.restartedResources, config.StatusCompleted, "restarted")
	records = append(records, buildResourceJobRecords(reporter.skippedResources, config.StatusSkipped, "skipped")...)
	records = append(records, buildFailedResourceJobRecords(reporter.failedResources)...)
	return records
}

func buildStopJobRecords(reporter *deploymentScaleReporter) []operationJobRecord {
	if reporter == nil {
		return nil
	}
	records := buildResourceJobRecords(reporter.successfulResources, config.StatusCompleted, "stopped")
	records = append(records, buildResourceJobRecords(reporter.skippedResources, config.StatusSkipped, "skipped")...)
	records = append(records, buildFailedResourceJobRecords(reporter.failedResources)...)
	return records
}

func buildStartJobRecords(reporter *deploymentScaleReporter) []operationJobRecord {
	if reporter == nil {
		return nil
	}
	records := buildResourceJobRecords(reporter.successfulResources, config.StatusCompleted, "started")
	records = append(records, buildResourceJobRecords(reporter.skippedResources, config.StatusSkipped, "skipped")...)
	records = append(records, buildFailedResourceJobRecords(reporter.failedResources)...)
	return records
}

func buildUpdateJobRecords(app *model.Applications, req apisv1.UpdateVersionRequest, updated, added, removed []string) []operationJobRecord {
	records := make([]operationJobRecord, 0, len(updated)+len(added)+len(removed))
	for _, name := range updated {
		records = append(records, operationJobRecord{name: name, status: config.StatusCompleted, info: "updated"})
	}
	for _, name := range added {
		records = append(records, operationJobRecord{name: name, status: config.StatusCompleted, info: "added"})
	}
	for _, name := range removed {
		records = append(records, operationJobRecord{name: name, status: config.StatusCompleted, info: "removed"})
	}
	if len(records) == 0 {
		target := ""
		if app != nil {
			target = app.Name
		}
		info := fmt.Sprintf("version set to %s", strings.TrimSpace(req.Version))
		records = append(records, operationJobRecord{name: target, status: config.StatusCompleted, info: info})
	}
	return records
}

func buildResourceJobRecords(resources []string, status config.Status, info string) []operationJobRecord {
	records := make([]operationJobRecord, 0, len(resources))
	for _, resource := range resources {
		target, errMsg := parseResourceFailure(resource)
		if target == "" {
			continue
		}
		record := operationJobRecord{
			name:   target,
			status: status,
			info:   info,
			errMsg: errMsg,
		}
		records = append(records, record)
	}
	return records
}

func buildFailedResourceJobRecords(resources []string) []operationJobRecord {
	records := make([]operationJobRecord, 0, len(resources))
	for _, resource := range resources {
		target, errMsg := parseResourceFailure(resource)
		if target == "" {
			continue
		}
		if errMsg == "" {
			errMsg = "operation failed"
		}
		records = append(records, operationJobRecord{
			name:   target,
			status: config.StatusFailed,
			info:   "failed",
			errMsg: errMsg,
		})
	}
	return records
}

func parseResourceFailure(resource string) (string, string) {
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return "", ""
	}
	idx := strings.LastIndex(resource, " (")
	if idx == -1 || !strings.HasSuffix(resource, ")") {
		return resource, ""
	}
	target := strings.TrimSpace(resource[:idx])
	errMsg := strings.TrimSuffix(resource[idx+2:], ")")
	return target, strings.TrimSpace(errMsg)
}

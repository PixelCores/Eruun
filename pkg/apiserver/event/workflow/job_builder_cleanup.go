package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func buildPersistedCleanupExecutions(
	ctx context.Context,
	task *model.WorkflowQueue,
	ds datastore.DataStore,
	defaultJobTimeoutSeconds int64,
	stepCount int,
	logger klog.Logger,
) map[int]*StepExecution {
	cleanupExecutions := make(map[int]*StepExecution)
	addCleanupExecution := func(index int, cleanupExecution *StepExecution) {
		if cleanupExecution == nil || bucketsEmpty(cleanupExecution.Jobs) {
			return
		}
		if index < 0 {
			index = 0
		}
		if index > stepCount {
			index = stepCount
		}
		execution := cleanupExecutions[index]
		if execution == nil {
			execution = &StepExecution{
				Name:     versionUpdateCleanupStepName,
				Mode:     config.WorkflowModeStepByStep,
				StepType: config.WorkflowStepTypeComponent,
				Jobs:     newJobBuckets(),
			}
			cleanupExecutions[index] = execution
		}
		mergeJobBuckets(execution.Jobs, cleanupExecution.Jobs)
	}
	if cleanupRecords, err := loadPersistedCleanupJobInfos(ctx, task, ds); err != nil {
		taskID := ""
		if task != nil {
			taskID = task.TaskID
		}
		logger.Error(err, "Failed to load persisted cleanup jobs", "taskID", taskID)
		addCleanupExecution(0, failedPersistedCleanupExecution(task, defaultJobTimeoutSeconds, err))
	} else {
		for _, cleanupRecord := range cleanupRecords {
			jobTask := persistedCleanupJobTask(cleanupRecord, task, defaultJobTimeoutSeconds)
			if jobTask == nil {
				continue
			}
			addCleanupExecution(cleanupRecord.InsertBeforeStepIndex, &StepExecution{
				Name:     versionUpdateCleanupStepName,
				Mode:     config.WorkflowModeStepByStep,
				StepType: config.WorkflowStepTypeComponent,
				Jobs: map[int][]*model.JobTask{
					config.JobPriorityLow: {jobTask},
				},
			})
		}
	}
	return cleanupExecutions
}

type persistedCleanupJobInfo struct {
	Record                *model.JobInfo
	Component             *model.ApplicationComponent
	InsertBeforeStepIndex int
}

func loadPersistedCleanupJobInfos(ctx context.Context, task *model.WorkflowQueue, ds datastore.DataStore) ([]persistedCleanupJobInfo, error) {
	cleanupInfo, ok, err := versionUpdateCleanupInfoFromTask(task)
	if err != nil || !ok || len(cleanupInfo.Components) == 0 {
		return nil, err
	}
	cleanupComponents := cleanupInfo.Components
	if ds == nil {
		return nil, fmt.Errorf("store is nil")
	}
	if strings.TrimSpace(task.TaskID) == "" {
		return nil, fmt.Errorf("task id is empty")
	}

	opts := &datastore.ListOptions{}
	opts.FilterOptions.In = append(opts.FilterOptions.In, datastore.InQueryOption{
		Key:    "type",
		Values: []string{string(config.JobCleanupResources)},
	})
	entities, err := ds.List(ctx, &model.JobInfo{TaskID: strings.TrimSpace(task.TaskID)}, opts)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, nil
		}
		return nil, err
	}
	recordsByName := make(map[string]*model.JobInfo, len(entities))
	for _, entity := range entities {
		record, ok := entity.(*model.JobInfo)
		if !ok || record == nil {
			return nil, datastore.ErrEntityInvalid
		}
		if strings.TrimSpace(record.TaskID) != strings.TrimSpace(task.TaskID) {
			continue
		}
		if strings.TrimSpace(record.Type) != string(config.JobCleanupResources) {
			continue
		}
		marked, err := isVersionUpdateCleanupJobInfo(record)
		if err != nil {
			return nil, err
		}
		if !marked {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(record.ServiceName))
		if key == "" {
			return nil, fmt.Errorf("cleanup job info service name is empty")
		}
		if _, exists := recordsByName[key]; exists {
			return nil, fmt.Errorf("duplicate cleanup job info for component %s", record.ServiceName)
		}
		recordsByName[key] = record
	}

	records := make([]persistedCleanupJobInfo, 0, len(cleanupComponents))
	for _, cleanupComponent := range cleanupComponents {
		component := cleanupComponent.Component
		if _, err := validatePersistedCleanupComponent(component); err != nil {
			return nil, err
		}
		if cleanupComponent.InsertBeforeStepIndex < 0 {
			return nil, fmt.Errorf("cleanup insert step index is negative")
		}
		componentName := strings.TrimSpace(component.Name)
		record := recordsByName[strings.ToLower(componentName)]
		if record == nil {
			return nil, fmt.Errorf("precreated cleanup job info not found for component %s", componentName)
		}
		if err := validatePersistedCleanupJobContract(cleanupInfo.Version, cleanupComponent, record); err != nil {
			return nil, fmt.Errorf("validate cleanup component %s contract: %w", componentName, err)
		}
		componentCopy := *component
		componentCopy.ResourceAppName = strings.TrimSpace(cleanupComponent.ResourceAppName)
		records = append(records, persistedCleanupJobInfo{
			Record:                record,
			Component:             &componentCopy,
			InsertBeforeStepIndex: cleanupComponent.InsertBeforeStepIndex,
		})
	}
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].InsertBeforeStepIndex != records[j].InsertBeforeStepIndex {
			return records[i].InsertBeforeStepIndex < records[j].InsertBeforeStepIndex
		}
		if records[i].Record.ID != records[j].Record.ID {
			return records[i].Record.ID < records[j].Record.ID
		}
		return records[i].Record.ServiceName < records[j].Record.ServiceName
	})
	return records, nil
}

func persistedCleanupJobTask(cleanupRecord persistedCleanupJobInfo, task *model.WorkflowQueue, defaultJobTimeoutSeconds int64) *model.JobTask {
	record := cleanupRecord.Record
	component := cleanupRecord.Component
	if record == nil || component == nil || task == nil {
		return nil
	}
	name := strings.TrimSpace(record.ServiceName)
	if name == "" {
		name = strings.TrimSpace(record.Info)
	}
	if name == "" {
		name = "cleanup-resources"
	}

	namespace := config.DefaultNamespace
	resourceAppName := ""
	if strings.TrimSpace(component.Namespace) == "" {
		component.Namespace = namespace
	}
	namespace = component.Namespace
	resourceAppName = component.ResourceNameKey()
	if strings.TrimSpace(component.Name) != "" {
		name = component.Name
	}

	jobTask := NewJobTask(name, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, resourceAppName)
	jobTask.JobType = string(config.JobCleanupResources)
	jobTask.JobInfo = component
	jobTask.Info = strings.TrimSpace(record.Info)
	if jobTask.Info == "" {
		jobTask.Info = fmt.Sprintf("cleanup resources: %s/%s", namespace, name)
	}
	jobTask.InternalInfo = record.InternalInfo
	jobTask.Status = config.Status(strings.TrimSpace(record.Status))
	if jobTask.Status == "" {
		jobTask.Status = config.StatusQueued
	}
	setDeployTimeout(jobTask)
	return jobTask
}

func versionUpdateCleanupComponentsFromTask(task *model.WorkflowQueue) ([]model.VersionUpdateCleanupComponent, bool, error) {
	cleanupInfo, ok, err := versionUpdateCleanupInfoFromTask(task)
	if err != nil || !ok {
		return nil, ok, err
	}
	return cleanupInfo.Components, true, nil
}

func versionUpdateCleanupOnlyFromTask(task *model.WorkflowQueue) bool {
	cleanupInfo, ok, err := versionUpdateCleanupInfoFromTask(task)
	return err == nil && ok && cleanupInfo.CleanupOnly
}

func versionUpdateCleanupInfoFromTask(task *model.WorkflowQueue) (model.VersionUpdateCleanupInfo, bool, error) {
	if task == nil {
		return model.VersionUpdateCleanupInfo{}, false, nil
	}
	raw := strings.TrimSpace(task.CleanupInfo)
	if raw == "" {
		return model.VersionUpdateCleanupInfo{}, false, nil
	}
	var marker struct {
		Source  string `json:"source"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		return model.VersionUpdateCleanupInfo{}, true, err
	}
	if marker.Source != config.JobInfoSourceVersionUpdateRemove {
		return model.VersionUpdateCleanupInfo{}, true, fmt.Errorf("unsupported cleanup info source %q", marker.Source)
	}
	switch marker.Version {
	case model.VersionUpdateCleanupInfoVersionV1,
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion:
	default:
		return model.VersionUpdateCleanupInfo{}, true, fmt.Errorf("unsupported cleanup info version %d", marker.Version)
	}
	var cleanupInfo model.VersionUpdateCleanupInfo
	if err := json.Unmarshal([]byte(raw), &cleanupInfo); err != nil {
		return model.VersionUpdateCleanupInfo{}, true, err
	}
	if err := validateVersionUpdateCleanupInfo(cleanupInfo); err != nil {
		return model.VersionUpdateCleanupInfo{}, true, err
	}
	return cleanupInfo, true, nil
}

func validateVersionUpdateCleanupInfo(cleanupInfo model.VersionUpdateCleanupInfo) error {
	maxComponentVersion := model.VersionUpdateCleanupInfoVersionV1
	for _, component := range cleanupInfo.Components {
		templates := normalizedCleanupPVCTemplates(component.StatefulSetPVCTemplatesToDelete)
		componentVersion := model.VersionUpdateCleanupInfoVersionV1
		if component.RequireStatefulSetDeletion {
			if component.Component == nil || component.Component.ComponentType != config.StoreJob {
				return fmt.Errorf("cleanup StatefulSet deletion is only valid for store components")
			}
			componentVersion = model.VersionUpdateCleanupInfoVersionStatefulSetDeletion
		}
		if len(templates) > 0 {
			if !component.RequireStatefulSetDeletion {
				return fmt.Errorf("cleanup PVC deletion templates require StatefulSet deletion")
			}
			componentVersion = model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion
		}
		if componentVersion > maxComponentVersion {
			maxComponentVersion = componentVersion
		}
	}
	if cleanupInfo.Version != maxComponentVersion {
		return fmt.Errorf("cleanup info version %d does not match maximum component contract version %d", cleanupInfo.Version, maxComponentVersion)
	}
	if cleanupInfo.CleanupOnly && cleanupInfo.Version >= model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
		return fmt.Errorf("cleanup-only task cannot carry StatefulSet deletion contract version %d", cleanupInfo.Version)
	}
	return nil
}

func validatePersistedCleanupJobContract(cleanupInfoVersion int, component model.VersionUpdateCleanupComponent, record *model.JobInfo) error {
	if record == nil {
		return fmt.Errorf("cleanup job info is nil")
	}
	expectedTemplates := normalizedCleanupPVCTemplates(component.StatefulSetPVCTemplatesToDelete)
	var marker struct {
		Source                          string   `json:"source"`
		Version                         int      `json:"version"`
		RequireStatefulSetDeletion      bool     `json:"requireStatefulSetDeletion"`
		StatefulSetPVCTemplatesToDelete []string `json:"statefulSetPVCTemplatesToDelete"`
	}
	if err := json.Unmarshal([]byte(record.InternalInfo), &marker); err != nil {
		return fmt.Errorf("decode cleanup job info marker: %w", err)
	}
	actualTemplates := normalizedCleanupPVCTemplates(marker.StatefulSetPVCTemplatesToDelete)
	if !equalCleanupPVCTemplates(expectedTemplates, actualTemplates) {
		return fmt.Errorf("cleanup job info StatefulSet PVC deletion templates do not match task cleanup info")
	}
	if marker.RequireStatefulSetDeletion != component.RequireStatefulSetDeletion {
		return fmt.Errorf("cleanup job info StatefulSet deletion requirement does not match task cleanup info")
	}
	if len(expectedTemplates) == 0 {
		if marker.Version != 0 {
			return fmt.Errorf("cleanup job info version %d does not match non-PVC cleanup marker version 0", marker.Version)
		}
		return nil
	}
	if cleanupInfoVersion != model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion || marker.Version != cleanupInfoVersion {
		return fmt.Errorf("cleanup job info version %d does not match task cleanup info version %d", marker.Version, cleanupInfoVersion)
	}
	if !marker.RequireStatefulSetDeletion || !component.RequireStatefulSetDeletion {
		return fmt.Errorf("cleanup job info StatefulSet PVC deletion requires StatefulSet deletion")
	}
	return nil
}

func normalizedCleanupPVCTemplates(rawTemplates []string) []string {
	seen := make(map[string]struct{}, len(rawTemplates))
	templates := make([]string, 0, len(rawTemplates))
	for _, rawName := range rawTemplates {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		templates = append(templates, name)
	}
	sort.Strings(templates)
	return templates
}

func equalCleanupPVCTemplates(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func isVersionUpdateCleanupJobInfo(record *model.JobInfo) (bool, error) {
	if record == nil {
		return false, nil
	}
	raw := strings.TrimSpace(record.InternalInfo)
	if raw == "" {
		return false, nil
	}
	var marker struct {
		Source string `json:"source"`
	}
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		return false, nil
	}
	return marker.Source == config.JobInfoSourceVersionUpdateRemove, nil
}

func validatePersistedCleanupComponent(component *model.ApplicationComponent) (*model.ApplicationComponent, error) {
	if component == nil {
		return nil, fmt.Errorf("component is nil")
	}
	if strings.TrimSpace(component.Name) == "" {
		return nil, fmt.Errorf("component name is empty")
	}
	if strings.TrimSpace(component.AppID) == "" {
		return nil, fmt.Errorf("component appID is empty")
	}
	return component, nil
}

func failedPersistedCleanupExecution(task *model.WorkflowQueue, defaultJobTimeoutSeconds int64, err error) *StepExecution {
	workflowID, projectID, appID, taskID := "", "", "", ""
	if task != nil {
		workflowID = task.WorkflowID
		projectID = task.ProjectID
		appID = task.AppID
		taskID = task.TaskID
	}
	jobTask := NewJobTask("cleanup-resources", config.DefaultNamespace, workflowID, projectID, appID, taskID, defaultJobTimeoutSeconds)
	jobTask.JobType = string(config.JobCleanupResources)
	jobTask.JobInfo = fmt.Sprintf("load persisted cleanup jobs: %v", err)
	jobTask.Info = "cleanup resources: failed to load persisted cleanup jobs"
	setDeployTimeout(jobTask)
	return &StepExecution{
		Name:     versionUpdateCleanupStepName,
		Mode:     config.WorkflowModeStepByStep,
		StepType: config.WorkflowStepTypeComponent,
		Jobs: map[int][]*model.JobTask{
			config.JobPriorityLow: {jobTask},
		},
	}
}

func failedWorkflowGenerationExecution(task *model.WorkflowQueue, defaultJobTimeoutSeconds int64, err error) *StepExecution {
	workflowID, projectID, appID, taskID := "", "", "", ""
	if task != nil {
		workflowID = task.WorkflowID
		projectID = task.ProjectID
		appID = task.AppID
		taskID = task.TaskID
	}
	jobTask := NewJobTask("workflow-generation", config.DefaultNamespace, workflowID, projectID, appID, taskID, defaultJobTimeoutSeconds)
	jobTask.JobType = string(config.JobCleanupResources)
	jobTask.JobInfo = fmt.Sprintf("prepare workflow job tasks: %v", err)
	jobTask.Info = "workflow generation failed"
	setDeployTimeout(jobTask)
	executions := []StepExecution{{
		Name:     "workflow-generation",
		Mode:     config.WorkflowModeStepByStep,
		StepType: config.WorkflowStepTypeComponent,
		Jobs: map[int][]*model.JobTask{
			config.JobPriorityLow: {jobTask},
		},
	}}
	applyWorkflowExecutionIdentity(executions, task)
	return &executions[0]
}

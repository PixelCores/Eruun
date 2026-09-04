package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

const jobInfoSaveMaxAttempts = 8

func withJobInfoOwnership(
	ctx context.Context,
	store datastore.DataStore,
	job *model.JobTask,
	persist func(datastore.DataStore) error,
) error {
	if job == nil {
		return datastore.ErrNilEntity
	}
	ownerGeneration := job.OwnerRunGeneration
	if ownerGeneration == 0 {
		ownerGeneration = job.RunGeneration
	}
	ownerStatus := job.OwnerStatus
	if ownerStatus == "" {
		ownerStatus = config.StatusRunning
	}
	owner := &model.WorkflowQueue{
		Status:        ownerStatus,
		TaskID:        job.TaskID,
		RunGeneration: ownerGeneration,
		RunToken:      job.RunToken,
		WorkerID:      job.WorkerID,
	}
	return repository.WithWorkflowTaskOwnership(ctx, store, owner, persist)
}

func addJobInfoIfWorkflowOwned(ctx context.Context, store datastore.DataStore, job *model.JobTask, jobInfo *model.JobInfo) error {
	return withJobInfoOwnership(ctx, store, job, func(tx datastore.DataStore) error {
		return tx.Add(ctx, jobInfo)
	})
}

func putJobInfoIfWorkflowOwned(ctx context.Context, store datastore.DataStore, job *model.JobTask, jobInfo *model.JobInfo) error {
	return withJobInfoOwnership(ctx, store, job, func(tx datastore.DataStore) error {
		return tx.Put(ctx, jobInfo)
	})
}

func compareAndSwapJobInfoIfWorkflowOwned(
	ctx context.Context,
	store datastore.DataStore,
	job *model.JobTask,
	jobInfo *model.JobInfo,
	conditions map[string]interface{},
	updates map[string]interface{},
) (bool, error) {
	var updated bool
	err := withJobInfoOwnership(ctx, store, job, func(tx datastore.DataStore) error {
		conditional, ok := tx.(datastore.ConditionalCompareAndSwap)
		if !ok {
			return fmt.Errorf("job info fencing requires multi-condition compare-and-swap")
		}
		var err error
		updated, err = conditional.CompareAndSwapWithConditions(ctx, jobInfo, conditions, updates)
		return err
	})
	return updated, err
}

func compareAndSwapJobInfoFieldIfWorkflowOwned(
	ctx context.Context,
	store datastore.DataStore,
	job *model.JobTask,
	jobInfo *model.JobInfo,
	field string,
	value interface{},
	updates map[string]interface{},
) (bool, error) {
	var updated bool
	err := withJobInfoOwnership(ctx, store, job, func(tx datastore.DataStore) error {
		var err error
		updated, err = tx.CompareAndSwap(ctx, jobInfo, field, value, updates)
		return err
	})
	return updated, err
}

// saveJobInfo persists the normalized job snapshot used by workflow queries.
func saveJobInfo(ctx context.Context, store datastore.DataStore, job *model.JobTask) error {
	if strings.TrimSpace(job.ExecutionKey) == "" {
		jobInfo := buildJobInfoRecord(job)
		return addJobInfoIfWorkflowOwned(ctx, store, job, &jobInfo)
	}
	return saveExecutionJobInfo(ctx, store, job)
}

func saveExecutionJobInfo(ctx context.Context, store datastore.DataStore, job *model.JobTask) error {
	return withJobInfoOwnership(ctx, store, job, func(tx datastore.DataStore) error {
		desired := buildJobInfoRecord(job)
		conditionalStore, ok := tx.(datastore.ConditionalCompareAndSwap)
		if !ok {
			return fmt.Errorf("save job info: datastore does not support conditional compare-and-swap")
		}
		for attempt := 1; attempt <= jobInfoSaveMaxAttempts; attempt++ {
			existing, err := findExistingJobInfo(ctx, tx, job)
			if err != nil {
				return err
			}
			if existing == nil {
				if err := tx.Add(ctx, &desired); err != nil {
					if errors.Is(err, datastore.ErrRecordExist) {
						continue
					}
					return err
				}
				return nil
			}
			if existing.Attempt > desired.Attempt {
				return nil
			}
			if shouldKeepExistingJobInfoStatus(existing.Status, config.Status(desired.Status)) {
				return nil
			}
			updated, err := conditionalStore.CompareAndSwapWithConditions(
				ctx,
				existing,
				map[string]interface{}{
					"status":         existing.Status,
					"execution_key":  jobInfoExecutionKey(*existing),
					"run_generation": existing.RunGeneration,
					"attempt":        existing.Attempt,
				},
				versionUpdateCleanupJobInfoUpdates(desired, true),
			)
			if err != nil {
				return fmt.Errorf("save job info: %w", err)
			}
			if updated {
				return nil
			}
		}
		return fmt.Errorf("save job info: concurrent execution state changes did not converge after %d attempts", jobInfoSaveMaxAttempts)
	})
}

func saveOrUpdateJobInfo(ctx context.Context, store datastore.DataStore, job *model.JobTask) error {
	jobInfo := buildJobInfoRecord(job)
	existing, err := findExistingJobInfo(ctx, store, job)
	if err != nil {
		return err
	}
	if existing == nil {
		return addJobInfoIfWorkflowOwned(ctx, store, job, &jobInfo)
	}
	if job.JobType == string(config.JobDatabaseReset) {
		if err := preservePreparedDatabaseResetCheckpointForSave(job, existing, &jobInfo); err != nil {
			return err
		}
	}
	updated := *existing
	copyJobInfoRecord(&updated, jobInfo)
	return putJobInfoIfWorkflowOwned(ctx, store, job, &updated)
}

func saveOrUpdateVersionUpdateCleanupJobInfo(ctx context.Context, store datastore.DataStore, job *model.JobTask) error {
	desired := buildJobInfoRecord(job)
	for attempt := 1; attempt <= jobInfoSaveMaxAttempts; attempt++ {
		existing, err := findExistingVersionUpdateCleanupJobInfo(ctx, store, job)
		if err != nil {
			return err
		}
		if existing == nil {
			if err := addJobInfoIfWorkflowOwned(ctx, store, job, &desired); err != nil {
				if errors.Is(err, datastore.ErrRecordExist) {
					continue
				}
				return err
			}
			return nil
		}
		if versionUpdateCleanupJobInfoWriteIsStale(*existing, desired) {
			klog.V(2).InfoS("ignore stale version update cleanup job info write",
				"taskID", desired.TaskID,
				"serviceName", desired.ServiceName,
				"storedRunGeneration", existing.RunGeneration,
				"writeRunGeneration", desired.RunGeneration,
				"storedAttempt", existing.Attempt,
				"writeAttempt", desired.Attempt)
			return nil
		}
		if !versionUpdateCleanupJobInfoNeedsFencing(*existing, desired) {
			updated := *existing
			copyJobInfoRecord(&updated, desired)
			return putJobInfoIfWorkflowOwned(ctx, store, job, &updated)
		}
		updated, err := compareAndSwapJobInfoIfWorkflowOwned(
			ctx,
			store,
			job,
			existing,
			versionUpdateCleanupJobInfoConditions(existing),
			versionUpdateCleanupJobInfoUpdates(desired, true),
		)
		if err != nil {
			return fmt.Errorf("save version update cleanup job info: %w", err)
		}
		if updated {
			return nil
		}
	}
	return fmt.Errorf("save version update cleanup job info: concurrent identity or state changes did not converge after %d attempts", jobInfoSaveMaxAttempts)
}

func versionUpdateCleanupJobInfoNeedsFencing(existing, desired model.JobInfo) bool {
	return existing.RunGeneration > 0 || desired.RunGeneration > 0 ||
		jobInfoExecutionKey(existing) != "" || jobInfoExecutionKey(desired) != ""
}

func versionUpdateCleanupJobInfoWriteIsStale(existing, desired model.JobInfo) bool {
	if !versionUpdateCleanupJobInfoNeedsFencing(existing, desired) {
		return false
	}
	if desired.RunGeneration != existing.RunGeneration {
		return desired.RunGeneration < existing.RunGeneration
	}
	if storedKey := jobInfoExecutionKey(existing); storedKey != "" && storedKey != jobInfoExecutionKey(desired) {
		return true
	}
	return desired.Attempt < existing.Attempt
}

func versionUpdateCleanupJobInfoSameExecution(existing, desired model.JobInfo) bool {
	if !versionUpdateCleanupJobInfoNeedsFencing(existing, desired) {
		return true
	}
	if existing.RunGeneration != desired.RunGeneration || existing.Attempt != desired.Attempt {
		return false
	}
	storedKey := jobInfoExecutionKey(existing)
	return storedKey == "" || storedKey == jobInfoExecutionKey(desired)
}

func versionUpdateCleanupJobInfoOwnershipMatches(existing, desired model.JobInfo) bool {
	if !versionUpdateCleanupJobInfoNeedsFencing(existing, desired) {
		return true
	}
	storedKey := jobInfoExecutionKey(existing)
	desiredKey := jobInfoExecutionKey(desired)
	return existing.RunGeneration == desired.RunGeneration &&
		existing.Attempt == desired.Attempt &&
		storedKey != "" &&
		storedKey == desiredKey
}

func jobInfoExecutionKey(jobInfo model.JobInfo) string {
	if jobInfo.ExecutionKey == nil {
		return ""
	}
	return strings.TrimSpace(*jobInfo.ExecutionKey)
}

func versionUpdateCleanupJobInfoConditions(existing *model.JobInfo) map[string]interface{} {
	conditions := map[string]interface{}{
		"status":         existing.Status,
		"internal_info":  existing.InternalInfo,
		"execution_key":  nil,
		"run_generation": existing.RunGeneration,
		"attempt":        existing.Attempt,
	}
	if existing.ExecutionKey != nil {
		conditions["execution_key"] = *existing.ExecutionKey
	}
	return conditions
}

func versionUpdateCleanupJobInfoUpdates(jobInfo model.JobInfo, includeInternalInfo bool) map[string]interface{} {
	updates := map[string]interface{}{
		"type":             jobInfo.Type,
		"workflow_id":      jobInfo.WorkflowID,
		"product_id":       jobInfo.ProductID,
		"workspace_id":     jobInfo.WorkspaceID,
		"app_id":           jobInfo.AppID,
		"task_id":          jobInfo.TaskID,
		"status":           jobInfo.Status,
		"start_time":       jobInfo.StartTime,
		"end_time":         jobInfo.EndTime,
		"info":             jobInfo.Info,
		"service_name":     jobInfo.ServiceName,
		"error":            jobInfo.Error,
		"production":       jobInfo.Production,
		"target_env":       jobInfo.TargetEnv,
		"execution_key":    nil,
		"run_generation":   jobInfo.RunGeneration,
		"attempt":          jobInfo.Attempt,
		"delay_state":      jobInfo.DelayState,
		"delay_execute_at": jobInfo.DelayExecuteAt,
		"delay_payload":    jobInfo.DelayPayload,
	}
	if includeInternalInfo {
		updates["internal_info"] = jobInfo.InternalInfo
	}
	if executionKey := jobInfoExecutionKey(jobInfo); executionKey != "" {
		updates["execution_key"] = executionKey
	}
	return updates
}

func copyJobInfoRecord(existing *model.JobInfo, jobInfo model.JobInfo) {
	if existing == nil {
		return
	}
	existing.Type = jobInfo.Type
	existing.WorkflowID = jobInfo.WorkflowID
	existing.ProductID = jobInfo.ProductID
	existing.WorkspaceID = jobInfo.WorkspaceID
	existing.AppID = jobInfo.AppID
	existing.TaskID = jobInfo.TaskID
	existing.Status = jobInfo.Status
	existing.StartTime = jobInfo.StartTime
	existing.EndTime = jobInfo.EndTime
	existing.Info = jobInfo.Info
	existing.InternalInfo = jobInfo.InternalInfo
	existing.ServiceName = jobInfo.ServiceName
	existing.Error = jobInfo.Error
	existing.Production = jobInfo.Production
	existing.TargetEnv = jobInfo.TargetEnv
	existing.ExecutionKey = jobInfo.ExecutionKey
	existing.RunGeneration = jobInfo.RunGeneration
	existing.Attempt = jobInfo.Attempt
	existing.DelayState = jobInfo.DelayState
	existing.DelayExecuteAt = jobInfo.DelayExecuteAt
	existing.DelayPayload = jobInfo.DelayPayload
}

func findExistingJobInfo(ctx context.Context, store datastore.DataStore, job *model.JobTask) (*model.JobInfo, error) {
	if job == nil {
		return nil, nil
	}
	candidates, err := loadJobInfos(ctx, store, job.TaskID, job.JobType, resolveJobServiceName(job))
	if err != nil {
		return nil, err
	}
	if job.RunGeneration > 0 {
		generationCandidates := make([]*model.JobInfo, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate != nil && candidate.RunGeneration == job.RunGeneration {
				generationCandidates = append(generationCandidates, candidate)
			}
		}
		candidates = generationCandidates
	}
	if job.JobType != string(config.JobDatabaseReset) && job.JobType != string(config.JobDeployCloud) {
		executionKey := strings.TrimSpace(job.ExecutionKey)
		if executionKey != "" {
			for _, candidate := range candidates {
				if candidate != nil && jobInfoExecutionKey(*candidate) == executionKey {
					return candidate, nil
				}
			}
			return nil, nil
		}
	}
	if job.JobType == string(config.JobDatabaseReset) {
		return selectDatabaseResetCheckpointJobInfo(job, candidates)
	}
	if job.JobType != string(config.JobDeployCloud) {
		if job.ExecutionKey != "" {
			for _, candidate := range candidates {
				if candidate != nil && candidate.ExecutionKey != nil && *candidate.ExecutionKey == job.ExecutionKey {
					return candidate, nil
				}
			}
			return nil, nil
		}
		if len(candidates) == 0 {
			return nil, nil
		}
		return candidates[0], nil
	}
	return selectCloudCheckpointJobInfo(job, candidates), nil
}

func findExistingVersionUpdateCleanupJobInfo(ctx context.Context, store datastore.DataStore, job *model.JobTask) (*model.JobInfo, error) {
	if job == nil {
		return nil, nil
	}
	candidates, err := loadJobInfos(ctx, store, job.TaskID, job.JobType, resolveJobServiceName(job))
	if err != nil {
		return nil, err
	}
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if isVersionUpdateRemoveCleanupInternalInfo(candidate.InternalInfo) {
			return candidate, nil
		}
	}
	return nil, nil
}

func isVersionUpdateRemoveCleanupInternalInfo(raw string) bool {
	_, marked := parseVersionUpdateCleanupInternalInfo(raw)
	return marked
}

func versionUpdateCleanupRequiresStatefulSetDeletion(raw string) bool {
	marker, marked := parseVersionUpdateCleanupInternalInfo(raw)
	return marked && marker.RequireStatefulSetDeletion
}

func versionUpdateCleanupStatefulSetPVCTemplatesToDelete(raw string) []string {
	marker, marked := parseVersionUpdateCleanupInternalInfo(raw)
	if !marked || marker.Version != model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion || !marker.RequireStatefulSetDeletion || len(marker.StatefulSetPVCTemplatesToDelete) == 0 {
		return nil
	}
	return normalizedVersionUpdateCleanupPVCTemplates(marker.StatefulSetPVCTemplatesToDelete)
}

func normalizedVersionUpdateCleanupPVCTemplates(rawTemplates []string) []string {
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

type versionUpdateCleanupInternalInfo struct {
	Source                          string   `json:"source"`
	Version                         int      `json:"version"`
	RequireStatefulSetDeletion      bool     `json:"requireStatefulSetDeletion"`
	StatefulSetPVCTemplatesToDelete []string `json:"statefulSetPVCTemplatesToDelete"`
}

func parseVersionUpdateCleanupInternalInfo(raw string) (versionUpdateCleanupInternalInfo, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return versionUpdateCleanupInternalInfo{}, false
	}
	var marker versionUpdateCleanupInternalInfo
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		return versionUpdateCleanupInternalInfo{}, false
	}
	return marker, marker.Source == config.JobInfoSourceVersionUpdateRemove
}

func validateVersionUpdateCleanupInternalInfo(raw string, component *model.ApplicationComponent) error {
	marker, marked := parseVersionUpdateCleanupInternalInfo(raw)
	if !marked {
		return nil
	}
	templates := normalizedVersionUpdateCleanupPVCTemplates(marker.StatefulSetPVCTemplatesToDelete)
	switch marker.Version {
	case 0:
		if len(templates) > 0 {
			return fmt.Errorf("version update cleanup PVC deletion marker is missing version")
		}
		return nil
	case model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion:
		if !marker.RequireStatefulSetDeletion {
			return fmt.Errorf("version update cleanup PVC deletion requires StatefulSet deletion")
		}
		if component == nil || component.ComponentType != config.StoreJob {
			return fmt.Errorf("version update cleanup PVC deletion is only valid for store components")
		}
		if len(templates) == 0 {
			return fmt.Errorf("version update cleanup PVC deletion templates are empty")
		}
		return nil
	default:
		return fmt.Errorf("unsupported version update cleanup job info version %d", marker.Version)
	}
}

func loadLatestJobInfo(ctx context.Context, store datastore.DataStore, taskID, jobType, serviceName string) (*model.JobInfo, error) {
	jobInfos, err := loadJobInfos(ctx, store, taskID, jobType, serviceName)
	if err != nil {
		return nil, err
	}
	if len(jobInfos) == 0 {
		return nil, nil
	}
	return jobInfos[0], nil
}

func loadJobInfos(ctx context.Context, store datastore.DataStore, taskID, jobType, serviceName string) ([]*model.JobInfo, error) {
	if store == nil || strings.TrimSpace(taskID) == "" {
		return nil, nil
	}
	query := &model.JobInfo{TaskID: strings.TrimSpace(taskID)}
	if isResourceImportJobType(config.JobType(jobType)) {
		if scope, ok := access.FromContext(ctx); ok {
			query.WorkspaceID = scope.WorkspaceID
		}
	}
	opts := datastore.ListOptions{
		SortBy: []datastore.SortOption{
			{Key: "create_time", Order: datastore.SortOrderDescending},
		},
	}
	if jobType != "" {
		opts.FilterOptions.In = append(opts.FilterOptions.In, datastore.InQueryOption{Key: "type", Values: []string{jobType}})
	}
	if serviceName != "" {
		opts.FilterOptions.In = append(opts.FilterOptions.In, datastore.InQueryOption{Key: "service_name", Values: []string{serviceName}})
	}
	entities, err := store.List(ctx, query, &opts)
	if err != nil {
		return nil, err
	}
	if len(entities) == 0 {
		return nil, nil
	}
	jobInfos := make([]*model.JobInfo, 0, len(entities))
	for _, entity := range entities {
		jobInfo, ok := entity.(*model.JobInfo)
		if !ok || jobInfo == nil {
			return nil, datastore.ErrEntityInvalid
		}
		jobInfos = append(jobInfos, jobInfo)
	}
	return jobInfos, nil
}

func buildJobInfoRecord(job *model.JobTask) model.JobInfo {
	record := model.JobInfo{
		Type:           job.JobType,
		WorkflowID:     job.WorkflowID,
		ProductID:      job.ProjectID,
		WorkspaceID:    job.WorkspaceID,
		AppID:          job.AppID,
		TaskID:         job.TaskID,
		Status:         string(job.Status),
		StartTime:      job.StartTime,
		EndTime:        job.EndTime,
		Info:           job.Info,
		InternalInfo:   job.InternalInfo,
		Error:          job.Error,
		ServiceName:    resolveJobServiceName(job),
		RunGeneration:  job.RunGeneration,
		Attempt:        job.Attempt,
		DelayState:     job.DelayState,
		DelayExecuteAt: job.DelayExecuteAt,
		DelayPayload:   job.DelayPayload,
	}
	currentAttempt := uint(job.RetryCount + 1)
	if currentAttempt > record.Attempt {
		record.Attempt = currentAttempt
	}
	if job.ExecutionKey != "" {
		executionKey := job.ExecutionKey
		record.ExecutionKey = &executionKey
	}
	return record
}

func resolveJobServiceName(job *model.JobTask) string {
	if job == nil {
		return ""
	}
	if name := rawComponentNameFromJobInfo(job.JobInfo); name != "" {
		return name
	}
	if name := strings.TrimSpace(job.Name); name != "" {
		return name
	}
	return componentNameFromJobInfo(job.JobInfo)
}

func componentNameFromJobInfo(info interface{}) string {
	if name := rawComponentNameFromJobInfo(info); name != "" {
		return name
	}
	return strings.TrimSpace(jobInfoLabels(info)[config.LabelComponentName])
}

func rawComponentNameFromJobInfo(info interface{}) string {
	return strings.TrimSpace(jobInfoAnnotations(info)[config.AnnotationComponentName])
}

func shareStrategyFromJobInfo(info interface{}) (domainspec.ShareStrategy, bool) {
	labels := jobInfoLabels(info)
	if len(labels) == 0 {
		return "", false
	}
	shareName := strings.TrimSpace(labels[config.LabelShareName])
	if shareName == "" {
		return "", false
	}
	raw := strings.TrimSpace(labels[config.LabelShareStrategy])
	strategy, ok := domainspec.NormalizeShareStrategy(raw)
	if !ok && raw != "" {
		return "", false
	}
	return strategy, true
}

func jobInfoLabels(info interface{}) map[string]string {
	switch v := info.(type) {
	case metav1.Object:
		if v == nil {
			return nil
		}
		return v.GetLabels()
	case *applyv1.ServiceApplyConfiguration:
		if v == nil {
			return nil
		}
		return v.Labels
	case *model.ConfigMapInput:
		if v == nil {
			return nil
		}
		return v.Labels
	case *model.SecretInput:
		if v == nil {
			return nil
		}
		return v.Labels
	default:
		return nil
	}
}

func jobInfoAnnotations(info interface{}) map[string]string {
	switch v := info.(type) {
	case metav1.Object:
		if v == nil {
			return nil
		}
		return v.GetAnnotations()
	case *applyv1.ServiceApplyConfiguration:
		if v == nil {
			return nil
		}
		return v.Annotations
	case *model.ConfigMapInput:
		if v == nil {
			return nil
		}
		return v.Annotations
	case *model.SecretInput:
		if v == nil {
			return nil
		}
		return v.Annotations
	default:
		return nil
	}
}

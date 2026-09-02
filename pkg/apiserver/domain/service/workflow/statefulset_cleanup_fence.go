package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type statefulSetCleanupJobMarker struct {
	Source                          string   `json:"source"`
	Version                         int      `json:"version,omitempty"`
	RequireStatefulSetDeletion      bool     `json:"requireStatefulSetDeletion,omitempty"`
	StatefulSetPVCTemplatesToDelete []string `json:"statefulSetPVCTemplatesToDelete,omitempty"`
}

type pendingStatefulSetCleanup struct {
	name      string
	version   int
	templates map[string]struct{}
}

type statefulSetCleanupFenceAttempt struct {
	task            *model.WorkflowQueue
	cleanupInfo     model.VersionUpdateCleanupInfo
	jobs            []*model.JobInfo
	resolvesTaskIDs []string
	succeeded       bool
}

// EnsureNoPendingStatefulSetCleanup prevents ordinary workflow execution from
// deploying desired state while an earlier destructive StatefulSet migration
// still needs an explicit UpdateVersion resume.
func EnsureNoPendingStatefulSetCleanup(ctx context.Context, store datastore.DataStore, appID string) error {
	appID = strings.TrimSpace(appID)
	if store == nil || appID == "" {
		return nil
	}
	tasks, err := repository.FindWorkflowTasksByAppID(ctx, store, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil
		}
		return fmt.Errorf("list workflow tasks for StatefulSet cleanup fence: %w", err)
	}
	// Repository ordering is not causal: create_time comes from the API replica's
	// wall clock. Persisted task references are the only resolution relationship.
	attempts := make([]*statefulSetCleanupFenceAttempt, 0, len(tasks))
	attemptsByTaskID := make(map[string]*statefulSetCleanupFenceAttempt, len(tasks))
	for _, task := range tasks {
		cleanupInfo, marked, err := statefulSetCleanupInfoForFence(task)
		if err != nil {
			return err
		}
		if !marked {
			continue
		}
		taskID := strings.TrimSpace(task.TaskID)
		if taskID == "" {
			return fmt.Errorf("StatefulSet cleanup task has an empty task ID")
		}
		if taskID != task.TaskID {
			return fmt.Errorf("StatefulSet cleanup task ID %q is not normalized", task.TaskID)
		}
		if _, exists := attemptsByTaskID[taskID]; exists {
			return fmt.Errorf("duplicate StatefulSet cleanup task ID %s", taskID)
		}
		jobs, err := statefulSetCleanupJobsForFence(ctx, store, task.TaskID)
		if err != nil {
			return err
		}
		resolvesTaskIDs, err := normalizeStatefulSetCleanupResolutionTaskIDs(taskID, cleanupInfo.ResolvesTaskIDs)
		if err != nil {
			return err
		}
		attempt := &statefulSetCleanupFenceAttempt{
			task:            task,
			cleanupInfo:     cleanupInfo,
			jobs:            jobs,
			resolvesTaskIDs: resolvesTaskIDs,
		}
		validationPending := make(map[string]*pendingStatefulSetCleanup)
		if err := applyStatefulSetCleanupFenceAttempt(validationPending, attempt); err != nil {
			return err
		}
		attempt.succeeded = len(validationPending) == 0
		attempts = append(attempts, attempt)
		attemptsByTaskID[taskID] = attempt
	}

	resolvedTaskIDs, err := resolvedStatefulSetCleanupFenceTaskIDs(attempts, attemptsByTaskID)
	if err != nil {
		return err
	}
	pending := make(map[string]*pendingStatefulSetCleanup)
	for _, attempt := range attempts {
		if attempt.succeeded {
			continue
		}
		if _, resolved := resolvedTaskIDs[attempt.task.TaskID]; resolved {
			continue
		}
		if err := applyStatefulSetCleanupFenceAttempt(pending, attempt); err != nil {
			return err
		}
	}
	if len(pending) == 0 {
		return nil
	}
	names := make([]string, 0, len(pending))
	seen := make(map[string]struct{}, len(pending))
	for _, cleanup := range pending {
		name := cleanup.name
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	internalErr := fmt.Errorf("%w: unfinished StatefulSet cleanup for components %s", bcode.ErrApplicationConfig, strings.Join(names, ","))
	return bcode.WithSafeClientMessage(internalErr, fmt.Sprintf(
		"components %s have an unfinished StatefulSet migration; resume it with the version update API before executing another workflow",
		strings.Join(names, ","),
	))
}

func applyStatefulSetCleanupFenceAttempt(
	pending map[string]*pendingStatefulSetCleanup,
	attempt *statefulSetCleanupFenceAttempt,
) error {
	if attempt == nil || attempt.task == nil {
		return fmt.Errorf("StatefulSet cleanup attempt is nil")
	}
	task := attempt.task
	cleanupInfo := attempt.cleanupInfo
	hasRequiredContract := false
	hasPVCDeletionContract := false
	for _, component := range cleanupInfo.Components {
		if !component.RequireStatefulSetDeletion {
			continue
		}
		hasRequiredContract = true
		componentVersion, err := statefulSetCleanupFenceComponentVersion(task, cleanupInfo.Version, component)
		if err != nil {
			return err
		}
		if componentVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
			hasPVCDeletionContract = true
		}
	}
	if !hasRequiredContract {
		return fmt.Errorf("task %s cleanup info version %d has no required StatefulSet deletion contract", task.TaskID, cleanupInfo.Version)
	}
	if cleanupInfo.Version == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion && !hasPVCDeletionContract {
		return fmt.Errorf("task %s cleanup info version %d has no StatefulSet PVC deletion contract", task.TaskID, cleanupInfo.Version)
	}
	taskStatus := config.Status(strings.TrimSpace(string(task.Status)))
	if taskStatus == "" || isWorkflowActiveStatus(taskStatus) {
		return bcode.ErrWorkflowTaskRunning
	}
	taskCompleted := taskStatus == config.StatusCompleted
	taskRequiresRetry := statefulSetCleanupFenceTaskRequiresRetry(taskStatus)
	if !taskCompleted && !taskRequiresRetry {
		return fmt.Errorf("task %s has unsupported terminal status %q for StatefulSet cleanup fence", task.TaskID, task.Status)
	}
	for _, component := range cleanupInfo.Components {
		if !component.RequireStatefulSetDeletion {
			continue
		}
		componentVersion, err := statefulSetCleanupFenceComponentVersion(task, cleanupInfo.Version, component)
		if err != nil {
			return err
		}
		key, name, err := statefulSetCleanupFenceComponentKey(task, component)
		if err != nil {
			return err
		}
		job, err := matchingStatefulSetCleanupFenceJob(task, componentVersion, component, attempt.jobs)
		if err != nil {
			return err
		}
		templates := normalizedCleanupPVCTemplateNames(component.StatefulSetPVCTemplatesToDelete)
		jobStatus := config.Status("")
		jobMissing := job == nil
		if job != nil {
			jobStatus = config.Status(strings.TrimSpace(job.Status))
		}
		switch {
		case jobStatus == config.StatusCompleted && taskCompleted:
			resolved := pending[key]
			if resolved == nil {
				continue
			}
			if componentVersion == model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
				if resolved.version == model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
					delete(pending, key)
				}
				continue
			}
			if resolved.version == model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
				delete(pending, key)
				continue
			}
			for _, template := range templates {
				delete(resolved.templates, template)
			}
			if len(resolved.templates) == 0 {
				delete(pending, key)
			}
		case jobMissing,
			taskRequiresRetry && shouldTerminalizePrecreatedCleanupStatus(jobStatus),
			jobStatus == config.StatusCompleted && taskRequiresRetry,
			jobStatus == config.StatusPassed,
			jobStatus == config.StatusSkipped,
			jobStatus == config.StatusFailed,
			jobStatus == config.StatusTimeout,
			jobStatus == config.StatusCancelled,
			jobStatus == config.StatusReject:
			unresolved := pending[key]
			if unresolved == nil {
				unresolved = &pendingStatefulSetCleanup{name: name, version: componentVersion}
				pending[key] = unresolved
			}
			if componentVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
				unresolved.version = componentVersion
				if unresolved.templates == nil {
					unresolved.templates = make(map[string]struct{}, len(templates))
				}
				for _, template := range templates {
					unresolved.templates[template] = struct{}{}
				}
			}
		default:
			if jobStatus == "" || isWorkflowActiveStatus(jobStatus) {
				if taskStatus == config.StatusCancelled {
					return bcode.ErrWorkflowTaskCancelling
				}
				return bcode.ErrWorkflowTaskRunning
			}
			return fmt.Errorf(
				"task %s component %s cleanup job has unsupported status %q",
				task.TaskID,
				name,
				jobStatus,
			)
		}
	}
	return nil
}

func resolvedStatefulSetCleanupFenceTaskIDs(
	attempts []*statefulSetCleanupFenceAttempt,
	attemptsByTaskID map[string]*statefulSetCleanupFenceAttempt,
) (map[string]struct{}, error) {
	for _, attempt := range attempts {
		for _, resolvedTaskID := range attempt.resolvesTaskIDs {
			target := attemptsByTaskID[resolvedTaskID]
			if target == nil {
				return nil, fmt.Errorf("task %s resolves unknown StatefulSet cleanup task %s", attempt.task.TaskID, resolvedTaskID)
			}
			if target.succeeded {
				return nil, fmt.Errorf("task %s resolves StatefulSet cleanup task %s that has no pending cleanup", attempt.task.TaskID, resolvedTaskID)
			}
		}
	}
	if err := validateStatefulSetCleanupResolutionGraph(attemptsByTaskID); err != nil {
		return nil, err
	}
	resolved := make(map[string]struct{})
	resolverByTaskID := make(map[string]string)
	for _, attempt := range attempts {
		if !attempt.succeeded || len(attempt.resolvesTaskIDs) == 0 {
			continue
		}
		covered := make(map[string]*pendingStatefulSetCleanup)
		for _, resolvedTaskID := range attempt.resolvesTaskIDs {
			if previousResolver := resolverByTaskID[resolvedTaskID]; previousResolver != "" {
				return nil, fmt.Errorf("StatefulSet cleanup task %s is resolved by both tasks %s and %s", resolvedTaskID, previousResolver, attempt.task.TaskID)
			}
			if err := applyStatefulSetCleanupFenceAttempt(covered, attemptsByTaskID[resolvedTaskID]); err != nil {
				return nil, err
			}
		}
		if err := applyStatefulSetCleanupFenceAttempt(covered, attempt); err != nil {
			return nil, err
		}
		if len(covered) != 0 {
			return nil, fmt.Errorf("task %s does not cover every referenced StatefulSet cleanup task", attempt.task.TaskID)
		}
		for _, resolvedTaskID := range attempt.resolvesTaskIDs {
			resolved[resolvedTaskID] = struct{}{}
			resolverByTaskID[resolvedTaskID] = attempt.task.TaskID
		}
	}
	return resolved, nil
}

func validateStatefulSetCleanupResolutionGraph(attemptsByTaskID map[string]*statefulSetCleanupFenceAttempt) error {
	const (
		resolutionVisiting = iota + 1
		resolutionVisited
	)
	states := make(map[string]int, len(attemptsByTaskID))
	var visit func(string) error
	visit = func(taskID string) error {
		switch states[taskID] {
		case resolutionVisiting:
			return fmt.Errorf("StatefulSet cleanup resolution graph contains a cycle at task %s", taskID)
		case resolutionVisited:
			return nil
		}
		attempt := attemptsByTaskID[taskID]
		if attempt == nil {
			return fmt.Errorf("StatefulSet cleanup resolution references unknown task %s", taskID)
		}
		states[taskID] = resolutionVisiting
		for _, resolvedTaskID := range attempt.resolvesTaskIDs {
			if err := visit(resolvedTaskID); err != nil {
				return err
			}
		}
		states[taskID] = resolutionVisited
		return nil
	}
	for taskID := range attemptsByTaskID {
		if err := visit(taskID); err != nil {
			return err
		}
	}
	return nil
}

func normalizeStatefulSetCleanupResolutionTaskIDs(taskID string, values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	taskID = strings.TrimSpace(taskID)
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, rawValue := range values {
		value := strings.TrimSpace(rawValue)
		if value == "" {
			return nil, fmt.Errorf("task %s has an empty resolvesTaskIDs entry", taskID)
		}
		if value == taskID {
			return nil, fmt.Errorf("task %s cannot resolve itself", taskID)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("task %s resolves StatefulSet cleanup task %s more than once", taskID, value)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func statefulSetCleanupFenceTaskRequiresRetry(status config.Status) bool {
	switch status {
	case config.StatusFailed, config.StatusTimeout, config.StatusCancelled, config.StatusReject, config.StatusPassed, config.StatusSkipped:
		return true
	default:
		return false
	}
}

func statefulSetCleanupFenceComponentVersion(
	task *model.WorkflowQueue,
	cleanupInfoVersion int,
	component model.VersionUpdateCleanupComponent,
) (int, error) {
	templates := normalizedCleanupPVCTemplateNames(component.StatefulSetPVCTemplatesToDelete)
	switch cleanupInfoVersion {
	case model.VersionUpdateCleanupInfoVersionStatefulSetDeletion:
		if len(templates) > 0 {
			return 0, fmt.Errorf("task %s cleanup info v2 cannot contain a StatefulSet PVC deletion plan", task.TaskID)
		}
		return model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, nil
	case model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion:
		if len(templates) > 0 {
			return model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, nil
		}
		return model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, nil
	default:
		return 0, fmt.Errorf("task %s has unsupported cleanup info version %d", task.TaskID, cleanupInfoVersion)
	}
}

func statefulSetCleanupInfoForFence(task *model.WorkflowQueue) (model.VersionUpdateCleanupInfo, bool, error) {
	if task == nil || strings.TrimSpace(task.CleanupInfo) == "" {
		return model.VersionUpdateCleanupInfo{}, false, nil
	}
	var cleanupInfo model.VersionUpdateCleanupInfo
	if err := json.Unmarshal([]byte(task.CleanupInfo), &cleanupInfo); err != nil {
		return model.VersionUpdateCleanupInfo{}, false, fmt.Errorf("decode cleanup info for task %s: %w", task.TaskID, err)
	}
	if cleanupInfo.Source != config.JobInfoSourceVersionUpdateRemove || cleanupInfo.Version < model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
		return model.VersionUpdateCleanupInfo{}, false, nil
	}
	if cleanupInfo.Version > model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
		return model.VersionUpdateCleanupInfo{}, false, fmt.Errorf("task %s has unsupported cleanup info version %d", task.TaskID, cleanupInfo.Version)
	}
	return cleanupInfo, true, nil
}

func statefulSetCleanupJobsForFence(ctx context.Context, store datastore.DataStore, taskID string) ([]*model.JobInfo, error) {
	entities, err := store.List(ctx, &model.JobInfo{TaskID: taskID}, &datastore.ListOptions{})
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("list cleanup jobs for task %s: %w", taskID, err)
	}
	jobs := make([]*model.JobInfo, 0, len(entities))
	for _, entity := range entities {
		job, ok := entity.(*model.JobInfo)
		if !ok || job == nil {
			return nil, datastore.ErrEntityInvalid
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func statefulSetCleanupFenceComponentKey(task *model.WorkflowQueue, cleanupComponent model.VersionUpdateCleanupComponent) (string, string, error) {
	component := cleanupComponent.Component
	if component == nil {
		return "", "", fmt.Errorf("task %s StatefulSet cleanup contract is missing its component descriptor", task.TaskID)
	}
	name := strings.ToLower(strings.TrimSpace(component.Name))
	if name == "" {
		return "", "", fmt.Errorf("task %s StatefulSet cleanup contract has an empty component name", task.TaskID)
	}
	resourceAppName := strings.ToLower(strings.TrimSpace(component.ResourceAppName))
	if resourceAppName == "" {
		resourceAppName = strings.ToLower(strings.TrimSpace(cleanupComponent.ResourceAppName))
	}
	if resourceAppName == "" {
		resourceAppName = strings.ToLower(strings.TrimSpace(component.AppID))
	}
	key := fmt.Sprintf("%d:%s:%s:%s:%s", component.ID, strings.ToLower(strings.TrimSpace(component.AppID)), strings.ToLower(strings.TrimSpace(component.Namespace)), resourceAppName, name)
	return key, name, nil
}

func matchingStatefulSetCleanupFenceJob(
	task *model.WorkflowQueue,
	cleanupVersion int,
	cleanupComponent model.VersionUpdateCleanupComponent,
	jobs []*model.JobInfo,
) (*model.JobInfo, error) {
	if cleanupComponent.Component == nil {
		return nil, fmt.Errorf("task %s StatefulSet cleanup contract is missing its component descriptor", task.TaskID)
	}
	componentName := strings.ToLower(strings.TrimSpace(cleanupComponent.Component.Name))
	expectedTemplates := normalizedCleanupPVCTemplateNames(cleanupComponent.StatefulSetPVCTemplatesToDelete)
	if cleanupVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion && len(expectedTemplates) == 0 {
		return nil, fmt.Errorf("task %s component %s StatefulSet PVC cleanup contract has no templates", task.TaskID, componentName)
	}
	var matched *model.JobInfo
	for _, job := range jobs {
		if job == nil || strings.TrimSpace(job.Type) != string(config.JobCleanupResources) ||
			strings.ToLower(strings.TrimSpace(job.ServiceName)) != componentName {
			continue
		}
		raw := strings.TrimSpace(job.InternalInfo)
		if raw == "" {
			continue
		}
		marked, err := isVersionUpdateRemoveCleanupJobInfo(job)
		if err != nil {
			return nil, err
		}
		if !marked {
			continue
		}
		var marker statefulSetCleanupJobMarker
		if err := json.Unmarshal([]byte(raw), &marker); err != nil {
			return nil, fmt.Errorf("decode cleanup job marker for task %s component %s: %w", task.TaskID, componentName, err)
		}
		if !marker.RequireStatefulSetDeletion {
			return nil, fmt.Errorf("task %s cleanup job marker does not require StatefulSet deletion for component %s", task.TaskID, componentName)
		}
		if cleanupVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
			if marker.Version != cleanupVersion || !equalCleanupPVCTemplateNames(marker.StatefulSetPVCTemplatesToDelete, expectedTemplates) {
				return nil, fmt.Errorf("task %s cleanup job marker does not match component %s PVC cleanup contract", task.TaskID, componentName)
			}
		} else if marker.Version != 0 || len(normalizedCleanupPVCTemplateNames(marker.StatefulSetPVCTemplatesToDelete)) != 0 {
			return nil, fmt.Errorf("task %s cleanup job marker does not match component %s StatefulSet deletion contract", task.TaskID, componentName)
		}
		if matched != nil {
			return nil, fmt.Errorf("task %s has duplicate cleanup jobs for component %s", task.TaskID, componentName)
		}
		matched = job
	}
	if matched == nil {
		return nil, nil
	}
	return matched, nil
}

func equalCleanupPVCTemplateNames(left, right []string) bool {
	left = normalizedCleanupPVCTemplateNames(left)
	right = normalizedCleanupPVCTemplateNames(right)
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func normalizedCleanupPVCTemplateNames(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			seen[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

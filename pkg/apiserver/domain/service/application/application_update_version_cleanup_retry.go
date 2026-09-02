package application

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
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type pendingStatefulSetPVCDeletion struct {
	component       *model.ApplicationComponent
	resourceAppName string
	templates       map[string]struct{}
	taskIDs         map[string]struct{}
	cleanupVersion  int
}

type versionUpdateStatefulSetCleanupAttempt struct {
	task            *model.WorkflowQueue
	cleanupInfo     model.VersionUpdateCleanupInfo
	jobs            []*model.JobInfo
	resolvesTaskIDs []string
	succeeded       bool
}

type versionUpdateCleanupJobMarker struct {
	Source                          string   `json:"source"`
	Version                         int      `json:"version,omitempty"`
	RequireStatefulSetDeletion      bool     `json:"requireStatefulSetDeletion,omitempty"`
	StatefulSetPVCTemplatesToDelete []string `json:"statefulSetPVCTemplatesToDelete,omitempty"`
}

func mergePendingVersionUpdateStatefulSetPVCDeletions(
	specs []apisv1.ComponentUpdateSpec,
	cleanupInfo *model.VersionUpdateCleanupInfo,
	pending map[string]map[string]*pendingStatefulSetPVCDeletion,
) error {
	if len(pending) == 0 {
		return nil
	}
	if cleanupInfo == nil || len(cleanupInfo.Components) == 0 {
		return unfinishedStatefulSetPVCDeletionError(pending)
	}
	if err := validatePendingVersionUpdateStatefulSetPVCResume(specs, pending); err != nil {
		return err
	}
	updateSpecs, err := versionUpdateComponentUpdateSpecsByName(specs)
	if err != nil {
		return err
	}
	merged := make(map[string]struct{}, len(pending))
	for index := range cleanupInfo.Components {
		cleanupComponent := &cleanupInfo.Components[index]
		if cleanupComponent.Component == nil {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(cleanupComponent.Component.Name))
		update, exists := updateSpecs[key]
		if !exists {
			continue
		}
		plan, err := selectPendingStatefulSetPVCDeletion(pending[key], cleanupComponent.Component)
		if err != nil {
			return err
		}
		if plan == nil {
			continue
		}
		descriptor, err := versionUpdateCleanupComponentDescriptor(plan.component)
		if err != nil {
			return fmt.Errorf("restore component %s pending cleanup descriptor: %w", cleanupComponent.Component.Name, err)
		}
		cleanupComponent.Component = descriptor
		cleanupComponent.ResourceAppName = plan.resourceAppName
		cleanupComponent.RequireStatefulSetDeletion = true
		if plan.cleanupVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
			transitionTemplates, err := versionUpdateStatefulSetPVCTemplatesToDelete(plan.component, update)
			if err != nil {
				return fmt.Errorf("restore component %s pending StatefulSet PVC plan: %w", cleanupComponent.Component.Name, err)
			}
			cleanupComponent.StatefulSetPVCTemplatesToDelete = mergeVersionUpdatePVCTemplateNames(
				cleanupComponent.StatefulSetPVCTemplatesToDelete,
				pendingStatefulSetPVCTemplateNames(plan),
				transitionTemplates,
			)
			cleanupInfo.Version = model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion
		} else if cleanupInfo.Version < model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
			cleanupInfo.Version = model.VersionUpdateCleanupInfoVersionStatefulSetDeletion
		}
		merged[key] = struct{}{}
	}
	for key := range pending {
		if _, exists := merged[key]; !exists {
			return pendingStatefulSetPVCResumeError(key, "cleanup plan does not contain the pending component")
		}
	}
	cleanupInfo.ResolvesTaskIDs = pendingStatefulSetCleanupTaskIDs(pending)
	if len(cleanupInfo.ResolvesTaskIDs) == 0 {
		return fmt.Errorf("pending StatefulSet cleanup has no causal task IDs")
	}
	return nil
}

func pendingVersionUpdateStatefulSetPVCDeletionsForRequest(
	ctx context.Context,
	store datastore.DataStore,
	appID string,
	specs []apisv1.ComponentUpdateSpec,
	allowResume bool,
) (map[string]map[string]*pendingStatefulSetPVCDeletion, error) {
	pending, err := loadPendingVersionUpdateStatefulSetPVCDeletions(ctx, store, appID)
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		return pending, nil
	}
	if !allowResume {
		return nil, unfinishedStatefulSetPVCDeletionError(pending)
	}
	if err := validatePendingVersionUpdateStatefulSetPVCResume(specs, pending); err != nil {
		return nil, err
	}
	return pending, nil
}

func validatePendingVersionUpdateStatefulSetPVCResume(
	specs []apisv1.ComponentUpdateSpec,
	pending map[string]map[string]*pendingStatefulSetPVCDeletion,
) error {
	if len(pending) == 0 {
		return nil
	}
	updates, err := versionUpdateComponentUpdateSpecsByName(specs)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		plans := pending[key]
		if len(plans) != 1 {
			return pendingStatefulSetPVCResumeError(key, fmt.Sprintf("found %d pending resource identities", len(plans)))
		}
		var plan *pendingStatefulSetPVCDeletion
		for _, candidate := range plans {
			plan = candidate
		}
		if plan == nil || plan.component == nil {
			return pendingStatefulSetPVCResumeError(key, "pending cleanup descriptor is incomplete")
		}
		update, exists := updates[key]
		if !exists {
			return pendingStatefulSetPVCResumeError(key, "request does not repeat the immutable StatefulSet update")
		}
		switch plan.cleanupVersion {
		case model.VersionUpdateCleanupInfoVersionStatefulSetDeletion:
			changed, err := versionUpdateStatefulSetRequiresDeletion(plan.component, update)
			if err != nil {
				return fmt.Errorf("validate component %s pending StatefulSet recreation resume: %w", key, err)
			}
			if !changed {
				return pendingStatefulSetPVCResumeError(key, "request does not reproduce a StatefulSet immutable field change")
			}
		case model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion:
			if len(plan.templates) == 0 {
				return pendingStatefulSetPVCResumeError(key, "pending cleanup descriptor is missing its VCT deletion plan")
			}
			transitionTemplates, err := versionUpdateStatefulSetPVCTemplatesToDelete(plan.component, update)
			if err != nil {
				return fmt.Errorf("validate component %s pending StatefulSet PVC resume: %w", key, err)
			}
			if len(transitionTemplates) == 0 {
				return pendingStatefulSetPVCResumeError(key, "request does not reproduce a VCT identity or spec change")
			}
			transitionTemplateSet := make(map[string]struct{}, len(transitionTemplates))
			for _, template := range transitionTemplates {
				transitionTemplateSet[template] = struct{}{}
			}
			missingTemplates := make([]string, 0)
			for _, template := range pendingStatefulSetPVCTemplateNames(plan) {
				if _, covered := transitionTemplateSet[template]; !covered {
					missingTemplates = append(missingTemplates, template)
				}
			}
			if len(missingTemplates) > 0 {
				return pendingStatefulSetPVCResumeError(key, fmt.Sprintf(
					"request does not reproduce the pending VCT deletion plan for templates %s",
					strings.Join(missingTemplates, ","),
				))
			}
		default:
			return pendingStatefulSetPVCResumeError(key, fmt.Sprintf("unsupported pending cleanup version %d", plan.cleanupVersion))
		}
	}
	return nil
}

func unfinishedStatefulSetPVCDeletionError(pending map[string]map[string]*pendingStatefulSetPVCDeletion) error {
	names := make([]string, 0, len(pending))
	for name := range pending {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	joined := strings.Join(names, ",")
	internalErr := fmt.Errorf("%w: unfinished StatefulSet migration for components %s", bcode.ErrApplicationConfig, joined)
	return bcode.WithSafeClientMessage(internalErr, fmt.Sprintf(
		"components %s have an unfinished StatefulSet migration; retry with remove cleanup_all, add all, and repeat the immutable StatefulSet update",
		joined,
	))
}

func pendingStatefulSetPVCResumeError(componentName, reason string) error {
	internalErr := fmt.Errorf(
		"%w: component %s unfinished StatefulSet migration cannot resume: %s",
		bcode.ErrApplicationConfig,
		componentName,
		reason,
	)
	return bcode.WithSafeClientMessage(internalErr, fmt.Sprintf(
		"component %s has an unfinished StatefulSet migration; retry with remove cleanup_all, add all, and repeat the immutable StatefulSet update",
		componentName,
	))
}

func loadPendingVersionUpdateStatefulSetPVCDeletions(
	ctx context.Context,
	store datastore.DataStore,
	appID string,
) (map[string]map[string]*pendingStatefulSetPVCDeletion, error) {
	pending := make(map[string]map[string]*pendingStatefulSetPVCDeletion)
	if store == nil || strings.TrimSpace(appID) == "" {
		return pending, nil
	}
	tasks, err := repository.FindWorkflowTasksByAppID(ctx, store, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return pending, nil
		}
		return nil, fmt.Errorf("list previous workflow tasks: %w", err)
	}
	// Repository ordering is not causal: create_time comes from the API replica's
	// wall clock. Persisted task references are the only resolution relationship.
	attempts := make([]*versionUpdateStatefulSetCleanupAttempt, 0, len(tasks))
	attemptsByTaskID := make(map[string]*versionUpdateStatefulSetCleanupAttempt, len(tasks))
	for _, task := range tasks {
		cleanupInfo, marked, err := versionUpdateCleanupInfoForPVCRetry(task)
		if err != nil {
			return nil, err
		}
		if !marked {
			continue
		}
		taskID := strings.TrimSpace(task.TaskID)
		if taskID == "" {
			return nil, fmt.Errorf("StatefulSet cleanup task has an empty task ID")
		}
		if taskID != task.TaskID {
			return nil, fmt.Errorf("StatefulSet cleanup task ID %q is not normalized", task.TaskID)
		}
		if _, exists := attemptsByTaskID[taskID]; exists {
			return nil, fmt.Errorf("duplicate StatefulSet cleanup task ID %s", taskID)
		}
		jobs, err := versionUpdateCleanupJobsForTask(ctx, store, task.TaskID)
		if err != nil {
			return nil, err
		}
		resolvesTaskIDs, err := normalizeVersionUpdateCleanupResolutionTaskIDs(taskID, cleanupInfo.ResolvesTaskIDs)
		if err != nil {
			return nil, err
		}
		attempt := &versionUpdateStatefulSetCleanupAttempt{
			task:            task,
			cleanupInfo:     cleanupInfo,
			jobs:            jobs,
			resolvesTaskIDs: resolvesTaskIDs,
		}
		validationPending := make(map[string]map[string]*pendingStatefulSetPVCDeletion)
		if err := applyVersionUpdateStatefulSetCleanupAttempt(validationPending, attempt); err != nil {
			return nil, err
		}
		attempt.succeeded = len(validationPending) == 0
		attempts = append(attempts, attempt)
		attemptsByTaskID[taskID] = attempt
	}

	resolvedTaskIDs, err := resolvedVersionUpdateStatefulSetCleanupTaskIDs(attempts, attemptsByTaskID)
	if err != nil {
		return nil, err
	}
	for _, attempt := range attempts {
		if attempt.succeeded {
			continue
		}
		if _, resolved := resolvedTaskIDs[attempt.task.TaskID]; resolved {
			continue
		}
		if err := applyVersionUpdateStatefulSetCleanupAttempt(pending, attempt); err != nil {
			return nil, err
		}
	}
	return pending, nil
}

func applyVersionUpdateStatefulSetCleanupAttempt(
	pending map[string]map[string]*pendingStatefulSetPVCDeletion,
	attempt *versionUpdateStatefulSetCleanupAttempt,
) error {
	if attempt == nil || attempt.task == nil {
		return fmt.Errorf("StatefulSet cleanup attempt is nil")
	}
	hasPVCDeletionContract := false
	for _, cleanupComponent := range attempt.cleanupInfo.Components {
		if cleanupComponent.RequireStatefulSetDeletion &&
			len(normalizeVersionUpdatePVCTemplateNames(cleanupComponent.StatefulSetPVCTemplatesToDelete)) > 0 {
			hasPVCDeletionContract = true
			break
		}
	}
	if attempt.cleanupInfo.Version == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion && !hasPVCDeletionContract {
		return fmt.Errorf("task %s cleanup info version %d has no StatefulSet PVC deletion contract", attempt.task.TaskID, attempt.cleanupInfo.Version)
	}
	validContracts := 0
	for _, cleanupComponent := range attempt.cleanupInfo.Components {
		valid, err := updatePendingStatefulSetDeletion(
			pending,
			attempt.task,
			attempt.jobs,
			attempt.cleanupInfo.Version,
			cleanupComponent,
		)
		if err != nil {
			return err
		}
		if valid {
			validContracts++
		}
	}
	if validContracts == 0 {
		contractName := "StatefulSet deletion"
		if attempt.cleanupInfo.Version == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
			contractName = "StatefulSet PVC deletion"
		}
		return fmt.Errorf("task %s cleanup info version %d has no %s contract", attempt.task.TaskID, attempt.cleanupInfo.Version, contractName)
	}
	return nil
}

func resolvedVersionUpdateStatefulSetCleanupTaskIDs(
	attempts []*versionUpdateStatefulSetCleanupAttempt,
	attemptsByTaskID map[string]*versionUpdateStatefulSetCleanupAttempt,
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
	if err := validateVersionUpdateCleanupResolutionGraph(attemptsByTaskID); err != nil {
		return nil, err
	}
	resolved := make(map[string]struct{})
	resolverByTaskID := make(map[string]string)
	for _, attempt := range attempts {
		if !attempt.succeeded || len(attempt.resolvesTaskIDs) == 0 {
			continue
		}
		covered := make(map[string]map[string]*pendingStatefulSetPVCDeletion)
		for _, resolvedTaskID := range attempt.resolvesTaskIDs {
			if previousResolver := resolverByTaskID[resolvedTaskID]; previousResolver != "" {
				return nil, fmt.Errorf("StatefulSet cleanup task %s is resolved by both tasks %s and %s", resolvedTaskID, previousResolver, attempt.task.TaskID)
			}
			if err := applyVersionUpdateStatefulSetCleanupAttempt(covered, attemptsByTaskID[resolvedTaskID]); err != nil {
				return nil, err
			}
		}
		if err := applyVersionUpdateStatefulSetCleanupAttempt(covered, attempt); err != nil {
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

func validateVersionUpdateCleanupResolutionGraph(attemptsByTaskID map[string]*versionUpdateStatefulSetCleanupAttempt) error {
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

func normalizeVersionUpdateCleanupResolutionTaskIDs(taskID string, values []string) ([]string, error) {
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

func versionUpdateCleanupInfoForPVCRetry(task *model.WorkflowQueue) (model.VersionUpdateCleanupInfo, bool, error) {
	if task == nil || strings.TrimSpace(task.CleanupInfo) == "" {
		return model.VersionUpdateCleanupInfo{}, false, nil
	}
	var cleanupInfo model.VersionUpdateCleanupInfo
	if err := json.Unmarshal([]byte(task.CleanupInfo), &cleanupInfo); err != nil {
		return model.VersionUpdateCleanupInfo{}, false, fmt.Errorf("decode cleanup info for task %s: %w", task.TaskID, err)
	}
	if cleanupInfo.Source != config.JobInfoSourceVersionUpdateRemove {
		return model.VersionUpdateCleanupInfo{}, false, nil
	}
	if cleanupInfo.Version < model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
		return model.VersionUpdateCleanupInfo{}, false, nil
	}
	if cleanupInfo.Version != model.VersionUpdateCleanupInfoVersionStatefulSetDeletion &&
		cleanupInfo.Version != model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
		return model.VersionUpdateCleanupInfo{}, false, fmt.Errorf("task %s has unsupported cleanup info version %d", task.TaskID, cleanupInfo.Version)
	}
	return cleanupInfo, true, nil
}

func versionUpdateCleanupJobsForTask(ctx context.Context, store datastore.DataStore, taskID string) ([]*model.JobInfo, error) {
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

func updatePendingStatefulSetPVCDeletion(
	pending map[string]map[string]*pendingStatefulSetPVCDeletion,
	task *model.WorkflowQueue,
	jobs []*model.JobInfo,
	cleanupComponent model.VersionUpdateCleanupComponent,
) (bool, error) {
	return updatePendingStatefulSetDeletion(
		pending,
		task,
		jobs,
		model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
		cleanupComponent,
	)
}

func updatePendingStatefulSetDeletion(
	pending map[string]map[string]*pendingStatefulSetPVCDeletion,
	task *model.WorkflowQueue,
	jobs []*model.JobInfo,
	cleanupInfoVersion int,
	cleanupComponent model.VersionUpdateCleanupComponent,
) (bool, error) {
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return false, fmt.Errorf("StatefulSet cleanup task ID is empty")
	}
	component := cleanupComponent.Component
	templates := normalizeVersionUpdatePVCTemplateNames(cleanupComponent.StatefulSetPVCTemplatesToDelete)
	if !cleanupComponent.RequireStatefulSetDeletion {
		return false, nil
	}
	switch cleanupInfoVersion {
	case model.VersionUpdateCleanupInfoVersionStatefulSetDeletion:
		if len(templates) > 0 {
			return false, fmt.Errorf("task %s cleanup info v2 cannot contain a StatefulSet PVC deletion plan", task.TaskID)
		}
	case model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion:
		// v3 is the task-wide maximum contract version. Components without a
		// PVC plan still carry their v2 marker in a mixed v2/v3 task.
	default:
		return false, fmt.Errorf("task %s has unsupported cleanup info version %d", task.TaskID, cleanupInfoVersion)
	}
	planVersion := model.VersionUpdateCleanupInfoVersionStatefulSetDeletion
	if len(templates) > 0 {
		planVersion = model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion
	}
	if component == nil {
		return false, fmt.Errorf("task %s StatefulSet cleanup contract is missing its component descriptor", task.TaskID)
	}
	if component.ComponentType != config.StoreJob {
		return false, fmt.Errorf("task %s has invalid StatefulSet cleanup contract for component %s", task.TaskID, component.Name)
	}
	key := strings.ToLower(strings.TrimSpace(component.Name))
	if key == "" {
		return false, fmt.Errorf("task %s has StatefulSet PVC cleanup with empty component name", task.TaskID)
	}
	if strings.TrimSpace(component.AppID) == "" {
		return false, fmt.Errorf("task %s component %s StatefulSet PVC cleanup has empty appID", task.TaskID, component.Name)
	}
	identity, err := versionUpdateCleanupStatefulSetIdentity(cleanupComponent)
	if err != nil {
		return false, fmt.Errorf("task %s component %s StatefulSet identity: %w", task.TaskID, component.Name, err)
	}
	taskStatus := config.Status(strings.TrimSpace(string(task.Status)))
	if taskStatus == "" || isWorkflowActiveStatus(taskStatus) {
		return false, bcode.ErrWorkflowTaskRunning
	}
	taskCompleted := taskStatus == config.StatusCompleted
	taskRequiresRetry := isRetryableTerminalVersionUpdateTaskStatus(taskStatus)
	if !taskCompleted && !taskRequiresRetry {
		return false, fmt.Errorf("task %s has unsupported terminal status %q for StatefulSet cleanup recovery", task.TaskID, task.Status)
	}
	job, err := matchingVersionUpdateCleanupJob(task, jobs, key, templates)
	if err != nil {
		return false, err
	}
	status := config.Status("")
	jobMissing := job == nil
	if job != nil {
		status = config.Status(strings.TrimSpace(job.Status))
	}
	plans := pending[key]
	plan := plans[identity]
	if plan != nil {
		if err := validatePendingStatefulSetPVCDeletionComponent(plan.component, component); err != nil {
			return false, fmt.Errorf("task %s component %s pending StatefulSet identity %s: %w", task.TaskID, component.Name, identity, err)
		}
	}
	switch {
	case status == config.StatusCompleted && taskCompleted:
		if plan == nil {
			return true, nil
		}
		if len(templates) == 0 {
			if plan.cleanupVersion == model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
				delete(plans, identity)
				if len(plans) == 0 {
					delete(pending, key)
				}
			}
			return true, nil
		}
		for _, template := range templates {
			delete(plan.templates, template)
		}
		if len(plan.templates) == 0 {
			delete(plans, identity)
			if len(plans) == 0 {
				delete(pending, key)
			}
		}
		return true, nil
	case jobMissing,
		taskRequiresRetry && isPreStartVersionUpdateCleanupStatus(status),
		status == config.StatusCompleted && taskRequiresRetry,
		status == config.StatusPassed,
		status == config.StatusSkipped,
		status == config.StatusFailed,
		status == config.StatusTimeout,
		status == config.StatusCancelled,
		status == config.StatusReject:
		if plan == nil {
			if plans == nil {
				plans = make(map[string]*pendingStatefulSetPVCDeletion)
				pending[key] = plans
			}
			plan = &pendingStatefulSetPVCDeletion{
				component:       component,
				resourceAppName: strings.TrimSpace(cleanupComponent.ResourceAppName),
				templates:       make(map[string]struct{}, len(templates)),
				taskIDs:         make(map[string]struct{}),
				cleanupVersion:  planVersion,
			}
			plans[identity] = plan
		}
		if plan.taskIDs == nil {
			plan.taskIDs = make(map[string]struct{})
		}
		plan.taskIDs[strings.TrimSpace(task.TaskID)] = struct{}{}
		if planVersion > plan.cleanupVersion {
			plan.cleanupVersion = planVersion
		}
		for _, template := range templates {
			plan.templates[template] = struct{}{}
		}
		return true, nil
	default:
		if isActiveVersionUpdateCleanupStatus(status) {
			if task.Status == config.StatusCancelled {
				return false, bcode.ErrWorkflowTaskCancelling
			}
			return false, bcode.ErrWorkflowTaskRunning
		}
		return false, fmt.Errorf("task %s component %s cleanup job is %q; wait for it to finish before retrying StatefulSet PVC migration", task.TaskID, component.Name, job.Status)
	}
}

func isRetryableTerminalVersionUpdateTaskStatus(status config.Status) bool {
	switch status {
	case config.StatusFailed, config.StatusTimeout, config.StatusCancelled, config.StatusReject, config.StatusPassed, config.StatusSkipped:
		return true
	default:
		return false
	}
}

func isActiveVersionUpdateCleanupStatus(status config.Status) bool {
	return status == "" || isWorkflowActiveStatus(status)
}

func isPreStartVersionUpdateCleanupStatus(status config.Status) bool {
	switch status {
	case "", config.StatusCreated, config.StatusQueued, config.StatusWaiting, config.QueueItemPending:
		return true
	default:
		return false
	}
}

func versionUpdateCleanupStatefulSetIdentity(cleanupComponent model.VersionUpdateCleanupComponent) (string, error) {
	if cleanupComponent.Component == nil {
		return "", fmt.Errorf("component descriptor is nil")
	}
	component := *cleanupComponent.Component
	resourceAppName := strings.TrimSpace(cleanupComponent.ResourceAppName)
	component.ResourceAppName = strings.TrimSpace(component.ResourceAppName)
	if component.ResourceAppName == "" {
		component.ResourceAppName = resourceAppName
	} else if resourceAppName != "" && component.ResourceAppName != resourceAppName {
		return "", fmt.Errorf("resourceAppName mismatch between component and cleanup contract")
	}
	statefulSet, err := renderVersionUpdateStatefulSet(&component)
	if err != nil {
		return "", err
	}
	namespace := strings.TrimSpace(statefulSet.Namespace)
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	name := strings.TrimSpace(statefulSet.Name)
	if name == "" {
		return "", fmt.Errorf("rendered StatefulSet name is empty")
	}
	return strings.ToLower(namespace) + "/" + strings.ToLower(name), nil
}

func selectPendingStatefulSetPVCDeletion(
	plans map[string]*pendingStatefulSetPVCDeletion,
	current *model.ApplicationComponent,
) (*pendingStatefulSetPVCDeletion, error) {
	if len(plans) == 0 {
		return nil, nil
	}
	if len(plans) != 1 {
		return nil, fmt.Errorf("component %s has pending StatefulSet PVC cleanup for multiple resource identities", current.Name)
	}
	for _, plan := range plans {
		if plan == nil || plan.component == nil {
			return nil, fmt.Errorf("component %s pending StatefulSet PVC cleanup descriptor is missing", current.Name)
		}
		if err := validatePendingStatefulSetPVCDeletionComponent(plan.component, current); err != nil {
			return nil, fmt.Errorf("component %s pending StatefulSet PVC cleanup: %w", current.Name, err)
		}
		return plan, nil
	}
	return nil, nil
}

func validatePendingStatefulSetPVCDeletionComponent(previous, current *model.ApplicationComponent) error {
	if previous == nil || current == nil {
		return fmt.Errorf("component descriptor is nil")
	}
	if previous.ID != current.ID {
		return fmt.Errorf("component ID changed from %d to %d", previous.ID, current.ID)
	}
	if strings.TrimSpace(previous.AppID) != strings.TrimSpace(current.AppID) {
		return fmt.Errorf("component appID changed from %q to %q", previous.AppID, current.AppID)
	}
	if !strings.EqualFold(strings.TrimSpace(previous.Name), strings.TrimSpace(current.Name)) {
		return fmt.Errorf("component name changed from %q to %q", previous.Name, current.Name)
	}
	return nil
}

func matchingVersionUpdateCleanupJob(
	task *model.WorkflowQueue,
	jobs []*model.JobInfo,
	componentKey string,
	expectedTemplates []string,
) (*model.JobInfo, error) {
	var matched *model.JobInfo
	for _, job := range jobs {
		if job == nil || strings.TrimSpace(job.Type) != string(config.JobCleanupResources) ||
			strings.ToLower(strings.TrimSpace(job.ServiceName)) != componentKey {
			continue
		}
		var marker versionUpdateCleanupJobMarker
		if err := json.Unmarshal([]byte(strings.TrimSpace(job.InternalInfo)), &marker); err != nil {
			continue
		}
		if marker.Source != config.JobInfoSourceVersionUpdateRemove {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("task %s has duplicate cleanup jobs for component %s", task.TaskID, componentKey)
		}
		expectedMarkerVersion := 0
		if len(expectedTemplates) > 0 {
			expectedMarkerVersion = model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion
		}
		if marker.Version != expectedMarkerVersion || !marker.RequireStatefulSetDeletion ||
			!equalVersionUpdatePVCTemplateNames(marker.StatefulSetPVCTemplatesToDelete, expectedTemplates) {
			return nil, fmt.Errorf("task %s cleanup job marker does not match component %s PVC cleanup contract", task.TaskID, componentKey)
		}
		matched = job
	}
	if matched == nil {
		return nil, nil
	}
	return matched, nil
}

func versionUpdateComponentUpdateSpecsByName(specs []apisv1.ComponentUpdateSpec) (map[string]apisv1.ComponentUpdateSpec, error) {
	updates := make(map[string]apisv1.ComponentUpdateSpec, len(specs))
	for _, update := range specs {
		action, err := parseVersionUpdateComponentAction(update)
		if err != nil {
			return nil, err
		}
		if action != config.ComponentActionUpdate {
			continue
		}
		if key := strings.ToLower(strings.TrimSpace(update.Name)); key != "" {
			if _, exists := updates[key]; exists {
				return nil, fmt.Errorf("%w: component %s cannot be updated more than once in one version update request", bcode.ErrDuplicateComponentName, strings.TrimSpace(update.Name))
			}
			updates[key] = update
		}
	}
	return updates, nil
}

func pendingStatefulSetPVCTemplateNames(plan *pendingStatefulSetPVCDeletion) []string {
	if plan == nil || len(plan.templates) == 0 {
		return nil
	}
	names := make([]string, 0, len(plan.templates))
	for name := range plan.templates {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func pendingStatefulSetCleanupTaskIDs(pending map[string]map[string]*pendingStatefulSetPVCDeletion) []string {
	seen := make(map[string]struct{})
	for _, plans := range pending {
		for _, plan := range plans {
			if plan == nil {
				continue
			}
			for rawTaskID := range plan.taskIDs {
				taskID := strings.TrimSpace(rawTaskID)
				if taskID != "" {
					seen[taskID] = struct{}{}
				}
			}
		}
	}
	taskIDs := make([]string, 0, len(seen))
	for taskID := range seen {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)
	return taskIDs
}

func mergeVersionUpdatePVCTemplateNames(groups ...[]string) []string {
	seen := make(map[string]struct{})
	for _, group := range groups {
		for _, rawName := range group {
			name := strings.TrimSpace(rawName)
			if name != "" {
				seen[name] = struct{}{}
			}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func normalizeVersionUpdatePVCTemplateNames(names []string) []string {
	return mergeVersionUpdatePVCTemplateNames(names)
}

func equalVersionUpdatePVCTemplateNames(left, right []string) bool {
	left = normalizeVersionUpdatePVCTemplateNames(left)
	right = normalizeVersionUpdatePVCTemplateNames(right)
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

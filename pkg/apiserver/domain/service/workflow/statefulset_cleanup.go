package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobdeploy"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowtraits "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

// errPendingStatefulSetCleanup marks the expected fence outcome; other fence
// errors report corrupt contracts or storage failures.
var errPendingStatefulSetCleanup = errors.New("unfinished StatefulSet cleanup")

// PendingStatefulSetCleanup is one StatefulSet whose destructive migration has
// not completed and must be resumed through UpdateVersion.
type PendingStatefulSetCleanup struct {
	Component       *model.ApplicationComponent
	ResourceAppName string
	CleanupVersion  int
	Templates       map[string]struct{}
	TaskIDs         map[string]struct{}
}

// PendingStatefulSetCleanups is keyed by lower-cased component name, then by
// the rendered StatefulSet identity "namespace/name".
type PendingStatefulSetCleanups map[string]map[string]*PendingStatefulSetCleanup

type statefulSetCleanupAttempt struct {
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
	pending, err := LoadPendingStatefulSetCleanups(ctx, store, appID)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	names := make([]string, 0, len(pending))
	for name := range pending {
		names = append(names, name)
	}
	sort.Strings(names)
	internalErr := fmt.Errorf("%w: %w for components %s", bcode.ErrApplicationConfig, errPendingStatefulSetCleanup, strings.Join(names, ","))
	return bcode.WithSafeClientMessage(internalErr, fmt.Sprintf(
		"components %s have an unfinished StatefulSet migration; resume it with the version update API before executing another workflow",
		strings.Join(names, ","),
	))
}

// LoadPendingStatefulSetCleanups replays the application's StatefulSet
// migration tasks and returns the cleanups no successful task has resolved.
func LoadPendingStatefulSetCleanups(ctx context.Context, store datastore.DataStore, appID string) (PendingStatefulSetCleanups, error) {
	if store == nil {
		return nil, fmt.Errorf("datastore is required")
	}
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, fmt.Errorf("application id is required")
	}
	pending := make(PendingStatefulSetCleanups)
	tasks, err := repository.FindWorkflowTasksByAppID(ctx, store, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return pending, nil
		}
		return nil, fmt.Errorf("list workflow tasks for StatefulSet cleanup: %w", err)
	}
	// Repository ordering is not causal: create_time comes from the API replica's
	// wall clock. Persisted task references are the only resolution relationship.
	attempts := make([]*statefulSetCleanupAttempt, 0, len(tasks))
	attemptsByTaskID := make(map[string]*statefulSetCleanupAttempt, len(tasks))
	for _, task := range tasks {
		cleanupInfo, marked, err := statefulSetCleanupInfo(task)
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
		jobs, err := statefulSetCleanupJobs(ctx, store, taskID)
		if err != nil {
			return nil, err
		}
		resolvesTaskIDs, err := model.NormalizeVersionUpdateCleanupResolutionTaskIDs(taskID, cleanupInfo.ResolvesTaskIDs)
		if err != nil {
			return nil, err
		}
		attempt := &statefulSetCleanupAttempt{
			task:            task,
			cleanupInfo:     cleanupInfo,
			jobs:            jobs,
			resolvesTaskIDs: resolvesTaskIDs,
		}
		ownPending := make(PendingStatefulSetCleanups)
		if err := ownPending.apply(attempt); err != nil {
			return nil, err
		}
		attempt.succeeded = len(ownPending) == 0
		attempts = append(attempts, attempt)
		attemptsByTaskID[taskID] = attempt
	}

	resolvedTaskIDs, err := resolvedStatefulSetCleanupTaskIDs(attempts, attemptsByTaskID)
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
		if err := pending.apply(attempt); err != nil {
			return nil, err
		}
	}
	return pending, nil
}

func (p PendingStatefulSetCleanups) apply(attempt *statefulSetCleanupAttempt) error {
	if attempt == nil || attempt.task == nil {
		return fmt.Errorf("StatefulSet cleanup attempt is nil")
	}
	task := attempt.task
	if err := validateStatefulSetCleanupContract(task, attempt.cleanupInfo); err != nil {
		return err
	}
	taskStatus := config.Status(strings.TrimSpace(string(task.Status)))
	if taskStatus == "" || model.IsWorkflowActiveStatus(taskStatus) {
		return bcode.ErrWorkflowTaskRunning
	}
	taskCompleted := taskStatus == config.StatusCompleted
	taskRequiresRetry := model.IsRetryableVersionUpdateCleanupTaskStatus(taskStatus)
	if !taskCompleted && !taskRequiresRetry {
		return fmt.Errorf("task %s has unsupported terminal status %q for StatefulSet cleanup", task.TaskID, task.Status)
	}
	for _, cleanupComponent := range attempt.cleanupInfo.Components {
		if !cleanupComponent.RequireStatefulSetDeletion {
			continue
		}
		if err := p.applyComponent(attempt, taskCompleted, taskRequiresRetry, cleanupComponent); err != nil {
			return err
		}
	}
	return nil
}

func (p PendingStatefulSetCleanups) applyComponent(
	attempt *statefulSetCleanupAttempt,
	taskCompleted bool,
	taskRequiresRetry bool,
	cleanupComponent model.VersionUpdateCleanupComponent,
) error {
	task := attempt.task
	componentVersion, err := statefulSetCleanupComponentVersion(task, attempt.cleanupInfo.Version, cleanupComponent)
	if err != nil {
		return err
	}
	component := cleanupComponent.Component
	if component == nil {
		return fmt.Errorf("task %s StatefulSet cleanup contract is missing its component descriptor", task.TaskID)
	}
	if component.ComponentType != config.StoreJob {
		return fmt.Errorf("task %s has invalid StatefulSet cleanup contract for component %s", task.TaskID, component.Name)
	}
	key := strings.ToLower(strings.TrimSpace(component.Name))
	if key == "" {
		return fmt.Errorf("task %s StatefulSet cleanup contract has an empty component name", task.TaskID)
	}
	if strings.TrimSpace(component.AppID) == "" {
		return fmt.Errorf("task %s component %s StatefulSet cleanup contract has an empty appID", task.TaskID, component.Name)
	}
	identity, err := statefulSetCleanupIdentity(cleanupComponent)
	if err != nil {
		return fmt.Errorf("task %s component %s StatefulSet identity: %w", task.TaskID, component.Name, err)
	}
	templates := model.NormalizeVersionUpdateCleanupPVCTemplateNames(cleanupComponent.StatefulSetPVCTemplatesToDelete)
	job, err := matchingStatefulSetCleanupJob(task, componentVersion, key, templates, attempt.jobs)
	if err != nil {
		return err
	}
	plan := p[key][identity]
	if plan != nil {
		if err := ValidateStatefulSetCleanupComponent(plan.Component, component); err != nil {
			return fmt.Errorf("task %s component %s pending StatefulSet identity %s: %w", task.TaskID, component.Name, identity, err)
		}
	}
	jobStatus := config.Status("")
	if job != nil {
		jobStatus = config.Status(strings.TrimSpace(job.Status))
	}
	switch {
	case jobStatus == config.StatusCompleted && taskCompleted:
		p.resolve(key, identity, componentVersion, templates)
	case job == nil,
		taskRequiresRetry && shouldTerminalizePrecreatedCleanupStatus(jobStatus),
		jobStatus == config.StatusCompleted && taskRequiresRetry,
		jobStatus == config.StatusPassed,
		jobStatus == config.StatusSkipped,
		jobStatus == config.StatusFailed,
		jobStatus == config.StatusTimeout,
		jobStatus == config.StatusCancelled,
		jobStatus == config.StatusReject:
		if plan == nil {
			if p[key] == nil {
				p[key] = make(map[string]*PendingStatefulSetCleanup)
			}
			plan = &PendingStatefulSetCleanup{
				Component:       component,
				ResourceAppName: strings.TrimSpace(cleanupComponent.ResourceAppName),
				CleanupVersion:  componentVersion,
				Templates:       make(map[string]struct{}, len(templates)),
				TaskIDs:         make(map[string]struct{}),
			}
			p[key][identity] = plan
		}
		plan.TaskIDs[task.TaskID] = struct{}{}
		if componentVersion > plan.CleanupVersion {
			plan.CleanupVersion = componentVersion
		}
		for _, template := range templates {
			plan.Templates[template] = struct{}{}
		}
	default:
		if jobStatus == "" || model.IsWorkflowActiveStatus(jobStatus) {
			if task.Status == config.StatusCancelled {
				return bcode.ErrWorkflowTaskCancelling
			}
			return bcode.ErrWorkflowTaskRunning
		}
		return fmt.Errorf("task %s component %s cleanup job has unsupported status %q", task.TaskID, component.Name, jobStatus)
	}
	return nil
}

func (p PendingStatefulSetCleanups) resolve(key, identity string, componentVersion int, templates []string) {
	plan := p[key][identity]
	if plan == nil {
		return
	}
	switch {
	case componentVersion == model.VersionUpdateCleanupInfoVersionStatefulSetDeletion:
		if plan.CleanupVersion != model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
			return
		}
	case plan.CleanupVersion != model.VersionUpdateCleanupInfoVersionStatefulSetDeletion:
		for _, template := range templates {
			delete(plan.Templates, template)
		}
		if len(plan.Templates) > 0 {
			return
		}
	}
	delete(p[key], identity)
	if len(p[key]) == 0 {
		delete(p, key)
	}
}

func validateStatefulSetCleanupContract(task *model.WorkflowQueue, cleanupInfo model.VersionUpdateCleanupInfo) error {
	hasRequiredContract := false
	hasPVCDeletionContract := false
	for _, component := range cleanupInfo.Components {
		if !component.RequireStatefulSetDeletion {
			continue
		}
		hasRequiredContract = true
		componentVersion, err := statefulSetCleanupComponentVersion(task, cleanupInfo.Version, component)
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
	return nil
}

func resolvedStatefulSetCleanupTaskIDs(
	attempts []*statefulSetCleanupAttempt,
	attemptsByTaskID map[string]*statefulSetCleanupAttempt,
) (map[string]struct{}, error) {
	references := make(map[string][]string, len(attempts))
	for _, attempt := range attempts {
		references[attempt.task.TaskID] = attempt.resolvesTaskIDs
	}
	if err := model.ValidateVersionUpdateCleanupResolutionGraph(references); err != nil {
		return nil, err
	}
	for _, attempt := range attempts {
		for _, resolvedTaskID := range attempt.resolvesTaskIDs {
			if attemptsByTaskID[resolvedTaskID].succeeded {
				return nil, fmt.Errorf("task %s resolves StatefulSet cleanup task %s that has no pending cleanup", attempt.task.TaskID, resolvedTaskID)
			}
		}
	}
	resolved := make(map[string]struct{})
	resolverByTaskID := make(map[string]string)
	for _, attempt := range attempts {
		if !attempt.succeeded || len(attempt.resolvesTaskIDs) == 0 {
			continue
		}
		covered := make(PendingStatefulSetCleanups)
		for _, resolvedTaskID := range attempt.resolvesTaskIDs {
			if previousResolver := resolverByTaskID[resolvedTaskID]; previousResolver != "" {
				return nil, fmt.Errorf("StatefulSet cleanup task %s is resolved by both tasks %s and %s", resolvedTaskID, previousResolver, attempt.task.TaskID)
			}
			if err := covered.apply(attemptsByTaskID[resolvedTaskID]); err != nil {
				return nil, err
			}
		}
		if err := covered.apply(attempt); err != nil {
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

// statefulSetCleanupComponentVersion derives a component's contract version:
// a v3 task may still carry v2 components that delete no PVC templates.
func statefulSetCleanupComponentVersion(
	task *model.WorkflowQueue,
	cleanupInfoVersion int,
	component model.VersionUpdateCleanupComponent,
) (int, error) {
	templates := model.NormalizeVersionUpdateCleanupPVCTemplateNames(component.StatefulSetPVCTemplatesToDelete)
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

func statefulSetCleanupInfo(task *model.WorkflowQueue) (model.VersionUpdateCleanupInfo, bool, error) {
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

func statefulSetCleanupJobs(ctx context.Context, store datastore.DataStore, taskID string) ([]*model.JobInfo, error) {
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

// statefulSetCleanupIdentity names the Kubernetes StatefulSet the cleanup
// contract protects, so every caller agrees on what is pending.
func statefulSetCleanupIdentity(cleanupComponent model.VersionUpdateCleanupComponent) (string, error) {
	component := *cleanupComponent.Component
	resourceAppName := strings.TrimSpace(cleanupComponent.ResourceAppName)
	component.ResourceAppName = strings.TrimSpace(component.ResourceAppName)
	if component.ResourceAppName == "" {
		component.ResourceAppName = resourceAppName
	} else if resourceAppName != "" && component.ResourceAppName != resourceAppName {
		return "", fmt.Errorf("resourceAppName mismatch between component and cleanup contract")
	}
	statefulSet, err := RenderComponentStatefulSet(&component)
	if err != nil {
		return "", err
	}
	namespace := strings.TrimSpace(statefulSet.Namespace)
	name := strings.TrimSpace(statefulSet.Name)
	if name == "" {
		return "", fmt.Errorf("rendered StatefulSet name is empty")
	}
	return strings.ToLower(namespace) + "/" + strings.ToLower(name), nil
}

// RenderComponentStatefulSet renders the StatefulSet a store component deploys.
func RenderComponentStatefulSet(component *model.ApplicationComponent) (*appsv1.StatefulSet, error) {
	snapshot := *component
	result, err := jobdeploy.GenerateStoreService(&snapshot, workflowtraits.IdentityProfiles(&snapshot))
	if err != nil {
		return nil, fmt.Errorf("%w: render StatefulSet for component %s: %w", bcode.ErrApplicationConfig, component.Name, err)
	}
	if result == nil {
		return nil, fmt.Errorf("%w: component %s cannot render desired StatefulSet", bcode.ErrApplicationConfig, component.Name)
	}
	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	if !ok || statefulSet == nil {
		return nil, fmt.Errorf("%w: component %s rendered unexpected StatefulSet type %T", bcode.ErrApplicationConfig, component.Name, result.Service)
	}
	return statefulSet, nil
}

// ValidateStatefulSetCleanupComponent rejects a pending cleanup whose
// component identity changed between attempts.
func ValidateStatefulSetCleanupComponent(previous, current *model.ApplicationComponent) error {
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

// matchingStatefulSetCleanupJob ignores jobs that are not version-update
// removal cleanups and rejects a removal marker that breaks the contract.
func matchingStatefulSetCleanupJob(
	task *model.WorkflowQueue,
	componentVersion int,
	componentName string,
	expectedTemplates []string,
	jobs []*model.JobInfo,
) (*model.JobInfo, error) {
	if componentVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion && len(expectedTemplates) == 0 {
		return nil, fmt.Errorf("task %s component %s StatefulSet PVC cleanup contract has no templates", task.TaskID, componentName)
	}
	var matched *model.JobInfo
	for _, job := range jobs {
		if job == nil || strings.ToLower(strings.TrimSpace(job.ServiceName)) != componentName {
			continue
		}
		marked, err := isVersionUpdateRemoveCleanupJobInfo(job)
		if err != nil {
			return nil, err
		}
		if !marked {
			continue
		}
		var marker model.VersionUpdateCleanupJobMarker
		if err := json.Unmarshal([]byte(strings.TrimSpace(job.InternalInfo)), &marker); err != nil {
			return nil, fmt.Errorf("decode cleanup job marker for task %s component %s: %w", task.TaskID, componentName, err)
		}
		if !marker.RequireStatefulSetDeletion {
			return nil, fmt.Errorf("task %s cleanup job marker does not require StatefulSet deletion for component %s", task.TaskID, componentName)
		}
		if componentVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
			if marker.Version != componentVersion || !model.EqualVersionUpdateCleanupPVCTemplateNames(marker.StatefulSetPVCTemplatesToDelete, expectedTemplates) {
				return nil, fmt.Errorf("task %s cleanup job marker does not match component %s PVC cleanup contract", task.TaskID, componentName)
			}
		} else if marker.Version != 0 || len(model.NormalizeVersionUpdateCleanupPVCTemplateNames(marker.StatefulSetPVCTemplatesToDelete)) != 0 {
			return nil, fmt.Errorf("task %s cleanup job marker does not match component %s StatefulSet deletion contract", task.TaskID, componentName)
		}
		if matched != nil {
			return nil, fmt.Errorf("task %s has duplicate cleanup jobs for component %s", task.TaskID, componentName)
		}
		matched = job
	}
	return matched, nil
}

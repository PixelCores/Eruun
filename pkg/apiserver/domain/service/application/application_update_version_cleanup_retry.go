package application

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/workflow"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func mergePendingVersionUpdateStatefulSetPVCDeletions(
	specs []apisv1.ComponentUpdateSpec,
	cleanupInfo *model.VersionUpdateCleanupInfo,
	pending workflowservice.PendingStatefulSetCleanups,
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
		update, exists := updateSpecs[cleanupComponent.Component.Name]
		if !exists {
			continue
		}
		plan, err := selectPendingStatefulSetPVCDeletion(pending[cleanupComponent.Component.Name], cleanupComponent.Component)
		if err != nil {
			return err
		}
		if plan == nil {
			continue
		}
		descriptor, err := versionUpdateCleanupComponentDescriptor(plan.Component)
		if err != nil {
			return fmt.Errorf("restore component %s pending cleanup descriptor: %w", cleanupComponent.Component.Name, err)
		}
		cleanupComponent.Component = descriptor
		cleanupComponent.ResourceAppName = plan.ResourceAppName
		cleanupComponent.RequireStatefulSetDeletion = true
		if len(plan.Templates) > 0 {
			transitionTemplates, err := versionUpdateStatefulSetPVCTemplatesToDelete(plan.Component, update)
			if err != nil {
				return fmt.Errorf("restore component %s pending StatefulSet PVC plan: %w", cleanupComponent.Component.Name, err)
			}
			cleanupComponent.StatefulSetPVCTemplatesToDelete = model.NormalizeVersionUpdateCleanupPVCTemplateNames(
				cleanupComponent.StatefulSetPVCTemplatesToDelete,
				pendingStatefulSetPVCTemplateNames(plan),
				transitionTemplates,
			)
		}
		merged[cleanupComponent.Component.Name] = struct{}{}
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
) (workflowservice.PendingStatefulSetCleanups, error) {
	pending, err := workflowservice.LoadPendingStatefulSetCleanups(ctx, store, appID)
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
	pending workflowservice.PendingStatefulSetCleanups,
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
		var plan *workflowservice.PendingStatefulSetCleanup
		for _, candidate := range plans {
			plan = candidate
		}
		if plan == nil || plan.Component == nil {
			return pendingStatefulSetPVCResumeError(key, "pending cleanup descriptor is incomplete")
		}
		update, exists := updates[key]
		if !exists {
			return pendingStatefulSetPVCResumeError(key, "request does not repeat the immutable StatefulSet update")
		}
		if len(plan.Templates) == 0 {
			changed, err := versionUpdateStatefulSetRequiresDeletion(plan.Component, update)
			if err != nil {
				return fmt.Errorf("validate component %s pending StatefulSet recreation resume: %w", key, err)
			}
			if !changed {
				return pendingStatefulSetPVCResumeError(key, "request does not reproduce a StatefulSet immutable field change")
			}
			continue
		}
		transitionTemplates, err := versionUpdateStatefulSetPVCTemplatesToDelete(plan.Component, update)
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
	}
	return nil
}

func unfinishedStatefulSetPVCDeletionError(pending workflowservice.PendingStatefulSetCleanups) error {
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

func selectPendingStatefulSetPVCDeletion(
	plans map[string]*workflowservice.PendingStatefulSetCleanup,
	current *model.ApplicationComponent,
) (*workflowservice.PendingStatefulSetCleanup, error) {
	if len(plans) == 0 {
		return nil, nil
	}
	if len(plans) != 1 {
		return nil, fmt.Errorf("component %s has pending StatefulSet PVC cleanup for multiple resource identities", current.Name)
	}
	for _, plan := range plans {
		if plan == nil || plan.Component == nil {
			return nil, fmt.Errorf("component %s pending StatefulSet PVC cleanup descriptor is missing", current.Name)
		}
		if err := workflowservice.ValidateStatefulSetCleanupComponent(plan.Component, current); err != nil {
			return nil, fmt.Errorf("component %s pending StatefulSet PVC cleanup: %w", current.Name, err)
		}
		return plan, nil
	}
	return nil, nil
}

func versionUpdateComponentUpdateSpecsByName(specs []apisv1.ComponentUpdateSpec) (map[string]apisv1.ComponentUpdateSpec, error) {
	updates := make(map[string]apisv1.ComponentUpdateSpec, len(specs))
	for _, update := range specs {
		action, err := parseVersionUpdateComponentAction(update)
		if err != nil {
			return nil, err
		}
		if action != domainspec.ComponentActionUpdate {
			continue
		}
		if key := update.Name; key != "" {
			if _, exists := updates[key]; exists {
				return nil, fmt.Errorf("%w: component %s cannot be updated more than once in one version update request", bcode.ErrDuplicateComponentName, update.Name)
			}
			updates[key] = update
		}
	}
	return updates, nil
}

func pendingStatefulSetPVCTemplateNames(plan *workflowservice.PendingStatefulSetCleanup) []string {
	if plan == nil || len(plan.Templates) == 0 {
		return nil
	}
	names := make([]string, 0, len(plan.Templates))
	for name := range plan.Templates {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func pendingStatefulSetCleanupTaskIDs(pending workflowservice.PendingStatefulSetCleanups) []string {
	seen := make(map[string]struct{})
	for _, plans := range pending {
		for _, plan := range plans {
			if plan == nil {
				continue
			}
			for rawTaskID := range plan.TaskIDs {
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

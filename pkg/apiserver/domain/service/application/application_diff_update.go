package application

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const (
	versionDiffFieldImage      = "image"
	versionDiffFieldReplicas   = "replicas"
	versionDiffFieldProperties = "properties"
	versionDiffFieldTraits     = "traits"
)

// DiffUpdateVersion compares the materialized source app with the target app and,
// unless dryRun is set, applies the executable differences through UpdateVersion.
func (c *applicationsServiceImpl) DiffUpdateVersion(ctx context.Context, targetAppID string, req apisv1.DiffUpdateVersionRequest) (*apisv1.DiffUpdateVersionResponse, error) {
	targetAppID = strings.TrimSpace(targetAppID)
	sourceAppID := strings.TrimSpace(req.SourceAppID)
	if targetAppID == "" || sourceAppID == "" {
		return nil, bcode.ErrApplicationConfig
	}
	targetOnlyStrategy, err := normalizeDiffUpdateTargetOnlyStrategy(req.TargetOnlyStrategy)
	if err != nil {
		return nil, err
	}

	targetApp, err := c.AppRepo.FindByID(ctx, targetAppID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	sourceApp, err := c.AppRepo.FindByID(ctx, sourceAppID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	if !req.DryRun && targetApp.EffectiveManagementMode() == config.ManagementModeObserve {
		return nil, fmt.Errorf("%w: observe applications are read-only", bcode.ErrApplicationManagementMode)
	}
	sourceVersion := strings.TrimSpace(sourceApp.Version)
	if sourceVersion == "" {
		return nil, bcode.ErrApplicationConfig
	}

	sourceComponents, err := c.ComponentRepo.FindByAppID(ctx, sourceApp.ID)
	if err != nil {
		return nil, err
	}
	targetComponents, err := c.ComponentRepo.FindByAppID(ctx, targetApp.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(targetComponents, applicationResourceNameKey(targetApp))
	targetComponentMap := make(map[string]*model.ApplicationComponent, len(targetComponents))
	for _, component := range targetComponents {
		if component == nil {
			continue
		}
		targetComponentMap[strings.ToLower(strings.TrimSpace(component.Name))] = component
	}
	if targetApp.EffectiveManagementMode() == config.ManagementModeAdopted {
		if err := validateAdoptedDiffUpdateInputs(sourceComponents, targetComponents); err != nil {
			return nil, err
		}
	}

	diff, err := buildVersionDiff(sourceComponents, targetComponents)
	if err != nil {
		return nil, err
	}
	if targetApp.EffectiveManagementMode() == config.ManagementModeAdopted {
		blockUnsupportedAdoptedVersionDiffs(diff, targetComponents)
	}
	applyDiffUpdateTargetOnlyStrategy(diff, targetOnlyStrategy)
	if targetApp.EffectiveManagementMode() == config.ManagementModeAdopted {
		blockUnsupportedAdoptedTargetOnlyRemovals(diff)
	}
	if err := blockNonExecutableStatefulSetVersionDiffs(targetComponentMap, diff); err != nil {
		return nil, err
	}
	updateReq := buildDiffUpdateVersionRequest(sourceVersion, req, diff, targetOnlyStrategy)
	executable := len(diff.blocked) == 0 &&
		(targetOnlyStrategy != apisv1.DiffUpdateTargetOnlyStrategyBlock || len(diff.extra) == 0)
	if req.DryRun && executable && len(updateReq.Components) > 0 {
		blocked, err := blockDiffUpdateForPendingStatefulSetCleanup(ctx, c.Store, targetApp.ID, updateReq.Components, diff)
		if err != nil {
			return nil, err
		}
		executable = !blocked
	}

	resp := &apisv1.DiffUpdateVersionResponse{
		TargetAppID:           targetApp.ID,
		SourceAppID:           sourceApp.ID,
		TargetPreviousVersion: targetApp.Version,
		TargetVersion:         sourceVersion,
		SourceVersion:         sourceVersion,
		DryRun:                req.DryRun,
		TargetOnlyStrategy:    targetOnlyStrategy,
		VersionChanged:        targetApp.Version != sourceVersion,
		Executable:            executable,
		UpdatedComponents:     diff.updated,
		AddedComponents:       diff.added,
		ExtraComponents:       diff.extra,
		BlockedComponents:     diff.blocked,
	}
	resp.HasChanges = resp.VersionChanged ||
		len(resp.UpdatedComponents) > 0 ||
		len(resp.AddedComponents) > 0 ||
		len(resp.ExtraComponents) > 0 ||
		len(resp.BlockedComponents) > 0

	if req.DryRun {
		return resp, nil
	}
	if !resp.Executable {
		if diff.blockedErr != nil {
			return nil, diff.blockedErr
		}
		return nil, bcode.ErrApplicationConfig
	}
	shouldRemoveTargetOnly := targetOnlyStrategy == apisv1.DiffUpdateTargetOnlyStrategyRemove && len(resp.ExtraComponents) > 0
	if !resp.VersionChanged && len(resp.UpdatedComponents) == 0 && len(resp.AddedComponents) == 0 && !shouldRemoveTargetOnly {
		return resp, nil
	}

	updateResp, err := c.UpdateVersion(ctx, targetApp.ID, updateReq)
	if err != nil {
		return nil, err
	}
	resp.UpdateResult = updateResp
	if updateResp != nil {
		resp.TargetVersion = updateResp.Version
	}
	return resp, nil
}

func blockDiffUpdateForPendingStatefulSetCleanup(
	ctx context.Context,
	store datastore.DataStore,
	appID string,
	components []apisv1.ComponentUpdateSpec,
	diff *versionDiffResult,
) (bool, error) {
	if len(components) == 0 {
		return false, nil
	}
	if _, err := pendingVersionUpdateStatefulSetPVCDeletionsForRequest(ctx, store, appID, components, false); err != nil {
		var bc *bcode.Bcode
		if !errors.As(err, &bc) {
			return false, err
		}
		reason := bcode.SafeClientMessage(err)
		if reason == "" {
			reason = bc.Message
		}
		annotateVersionDiffComponentActions(diff, reason)
		return true, nil
	}
	return false, nil
}

func annotateVersionDiffComponentActions(diff *versionDiffResult, reason string) {
	if diff == nil || strings.TrimSpace(reason) == "" {
		return
	}
	annotate := func(items []apisv1.VersionComponentDiff) {
		for index := range items {
			if strings.TrimSpace(items[index].Reason) == "" {
				items[index].Reason = reason
			} else {
				items[index].Reason += "; " + reason
			}
		}
	}
	annotate(diff.updated)
	annotate(diff.added)
	for index := range diff.extra {
		if diff.extra[index].Action == string(config.ComponentActionRemove) {
			annotate(diff.extra[index : index+1])
		}
	}
}

func blockUnsupportedAdoptedVersionDiffs(
	diff *versionDiffResult,
	targetComponents []*model.ApplicationComponent,
) {
	if diff == nil || len(diff.updated) == 0 {
		return
	}
	targets := make(map[string]*model.ApplicationComponent, len(targetComponents))
	for _, component := range targetComponents {
		if component != nil {
			targets[strings.ToLower(strings.TrimSpace(component.Name))] = component
		}
	}
	executable := make([]apisv1.VersionComponentDiff, 0, len(diff.updated))
	for _, item := range diff.updated {
		target := targets[strings.ToLower(strings.TrimSpace(item.Name))]
		if target == nil || item.After == nil {
			item.Action = apisv1.DiffUpdateComponentActionBlock
			item.Reason = "adopted source binding is missing"
			diff.blocked = append(diff.blocked, item)
			continue
		}
		update := apisv1.ComponentUpdateSpec{
			Action: string(config.ComponentActionUpdate),
			Name:   item.Name,
		}
		for _, field := range item.Fields {
			switch field.Field {
			case versionDiffFieldImage:
				update.Image = item.After.Image
			case versionDiffFieldReplicas:
				replicas := item.After.Replicas
				update.Replicas = &replicas
			case versionDiffFieldProperties:
				properties := item.After.Properties
				update.Properties = &properties
			case versionDiffFieldTraits:
				traits := item.After.Traits
				update.Traits = &traits
			}
		}
		if err := validateAdoptedVersionUpdateCompatibility(target, update); err != nil {
			item.Action = apisv1.DiffUpdateComponentActionBlock
			item.Reason = err.Error()
			diff.blocked = append(diff.blocked, item)
			continue
		}
		executable = append(executable, item)
	}
	diff.updated = executable
}

func blockUnsupportedAdoptedTargetOnlyRemovals(diff *versionDiffResult) {
	if diff == nil || len(diff.extra) == 0 {
		return
	}
	executable := make([]apisv1.VersionComponentDiff, 0, len(diff.extra))
	for _, item := range diff.extra {
		if item.Action != string(config.ComponentActionRemove) {
			executable = append(executable, item)
			continue
		}
		item.Action = apisv1.DiffUpdateComponentActionBlock
		item.Reason = "adopted components must be detached through an adoption-aware operation"
		diff.blocked = append(diff.blocked, item)
	}
	diff.extra = executable
}

func validateAdoptedDiffUpdateInputs(
	sourceComponents []*model.ApplicationComponent,
	targetComponents []*model.ApplicationComponent,
) error {
	targets := make(map[string]*model.ApplicationComponent, len(targetComponents))
	for _, component := range targetComponents {
		if component == nil {
			continue
		}
		if err := validateAdoptedComponentSourceBinding(component); err != nil {
			return err
		}
		if hasSecret, err := componentHasOrdinarySecretProperties(component); err != nil {
			return err
		} else if hasSecret {
			return fmt.Errorf(
				"%w: adopted component %q contains ordinary Secret properties",
				bcode.ErrApplicationManagementMode,
				component.Name,
			)
		}
		targets[strings.ToLower(strings.TrimSpace(component.Name))] = component
	}
	for _, source := range sourceComponents {
		if source == nil {
			continue
		}
		target := targets[strings.ToLower(strings.TrimSpace(source.Name))]
		if target == nil {
			return fmt.Errorf(
				"%w: adopted applications cannot add component %q from a diff source",
				bcode.ErrApplicationManagementMode,
				source.Name,
			)
		}
		if source.ComponentType != target.ComponentType {
			return fmt.Errorf(
				"%w: adopted component %q source type %q does not match target type %q",
				bcode.ErrApplicationManagementMode,
				source.Name,
				source.ComponentType,
				target.ComponentType,
			)
		}
		if hasSecret, err := componentHasOrdinarySecretProperties(source); err != nil {
			return err
		} else if hasSecret {
			return fmt.Errorf(
				"%w: adopted Secret values require the encrypted keyring-aware update path",
				bcode.ErrApplicationManagementMode,
			)
		}
	}
	return nil
}

func componentHasOrdinarySecretProperties(component *model.ApplicationComponent) (bool, error) {
	if component == nil || component.Properties == nil {
		return false, nil
	}
	var properties apisv1.Properties
	if err := decodeJSONStruct(component.Properties, &properties); err != nil {
		return false, fmt.Errorf("decode component %q properties: %w", component.Name, err)
	}
	return properties.Secret != nil, nil
}

type versionDiffResult struct {
	updated    []apisv1.VersionComponentDiff
	added      []apisv1.VersionComponentDiff
	extra      []apisv1.VersionComponentDiff
	blocked    []apisv1.VersionComponentDiff
	blockedErr error
}

func buildVersionDiff(sourceComponents, targetComponents []*model.ApplicationComponent) (*versionDiffResult, error) {
	sourceSnapshots, err := componentSnapshotsByName(sourceComponents)
	if err != nil {
		return nil, err
	}
	targetSnapshots, err := componentSnapshotsByName(targetComponents)
	if err != nil {
		return nil, err
	}

	result := &versionDiffResult{}
	for _, name := range sortedSnapshotKeys(sourceSnapshots) {
		source := sourceSnapshots[name]
		target, exists := targetSnapshots[name]
		if !exists {
			result.added = append(result.added, apisv1.VersionComponentDiff{
				Action: string(config.ComponentActionAdd),
				Name:   source.Name,
				Type:   source.Type,
				After:  cloneVersionComponentState(source),
			})
			continue
		}
		if source.Type != target.Type {
			result.blocked = append(result.blocked, apisv1.VersionComponentDiff{
				Action: string(config.ComponentActionUpdate),
				Name:   source.Name,
				Type:   source.Type,
				Reason: "component type mismatch",
				Before: cloneVersionComponentState(target),
				After:  cloneVersionComponentState(source),
			})
			continue
		}

		fields := diffComponentFields(target, source)
		if len(fields) == 0 {
			continue
		}
		result.updated = append(result.updated, apisv1.VersionComponentDiff{
			Action: string(config.ComponentActionUpdate),
			Name:   source.Name,
			Type:   source.Type,
			Fields: fields,
			Before: cloneVersionComponentState(target),
			After:  cloneVersionComponentState(source),
		})
	}

	for _, name := range sortedSnapshotKeys(targetSnapshots) {
		if _, exists := sourceSnapshots[name]; exists {
			continue
		}
		target := targetSnapshots[name]
		result.extra = append(result.extra, apisv1.VersionComponentDiff{
			Name:   target.Name,
			Type:   target.Type,
			Before: cloneVersionComponentState(target),
		})
	}
	return result, nil
}

func normalizeDiffUpdateTargetOnlyStrategy(raw string) (string, error) {
	strategy := strings.ToLower(strings.TrimSpace(raw))
	if strategy == "" {
		return apisv1.DiffUpdateTargetOnlyStrategyPreserve, nil
	}
	switch strategy {
	case apisv1.DiffUpdateTargetOnlyStrategyPreserve,
		apisv1.DiffUpdateTargetOnlyStrategyRemove,
		apisv1.DiffUpdateTargetOnlyStrategyBlock:
		return strategy, nil
	default:
		return "", bcode.ErrApplicationConfig
	}
}

func applyDiffUpdateTargetOnlyStrategy(diff *versionDiffResult, strategy string) {
	if diff == nil {
		return
	}
	for i := range diff.extra {
		switch strategy {
		case apisv1.DiffUpdateTargetOnlyStrategyRemove:
			diff.extra[i].Action = string(config.ComponentActionRemove)
			diff.extra[i].Reason = "target-only component will be removed"
		case apisv1.DiffUpdateTargetOnlyStrategyBlock:
			diff.extra[i].Action = apisv1.DiffUpdateComponentActionBlock
			diff.extra[i].Reason = "target-only component blocks update"
		default:
			diff.extra[i].Action = apisv1.DiffUpdateComponentActionPreserve
			diff.extra[i].Reason = "target-only component is preserved"
		}
	}
}

func blockNonExecutableStatefulSetVersionDiffs(componentMap map[string]*model.ApplicationComponent, diff *versionDiffResult) error {
	if diff == nil || len(diff.updated) == 0 {
		return nil
	}
	executable := make([]apisv1.VersionComponentDiff, 0, len(diff.updated))
	for _, item := range diff.updated {
		component := componentMap[strings.ToLower(strings.TrimSpace(item.Name))]
		if component == nil || component.ComponentType != config.StoreJob {
			executable = append(executable, item)
			continue
		}
		spec, ok := versionComponentDiffUpdateSpec(item)
		if !ok {
			executable = append(executable, item)
			continue
		}
		if _, err := preflightVersionUpdateStatefulSets(componentMap, []apisv1.ComponentUpdateSpec{spec}, false); err != nil {
			var bc *bcode.Bcode
			if !errors.As(err, &bc) {
				return err
			}
			if diff.blockedErr == nil {
				diff.blockedErr = err
			}
			item.Action = apisv1.DiffUpdateComponentActionBlock
			item.Reason = bcode.SafeClientMessage(err)
			if item.Reason == "" {
				item.Reason = bc.Message
			}
			diff.blocked = append(diff.blocked, item)
			continue
		}
		executable = append(executable, item)
	}
	diff.updated = executable
	return nil
}

func componentSnapshotsByName(components []*model.ApplicationComponent) (map[string]*apisv1.VersionComponentState, error) {
	snapshots := make(map[string]*apisv1.VersionComponentState, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(component.Name))
		if key == "" {
			continue
		}
		snapshot, err := componentVersionState(component)
		if err != nil {
			return nil, err
		}
		snapshots[key] = snapshot
	}
	return snapshots, nil
}

func componentVersionState(component *model.ApplicationComponent) (*apisv1.VersionComponentState, error) {
	var properties apisv1.Properties
	if err := decodeJSONStruct(component.Properties, &properties); err != nil {
		return nil, err
	}
	var traits apisv1.Traits
	if err := decodeJSONStruct(component.Traits, &traits); err != nil {
		return nil, err
	}
	return &apisv1.VersionComponentState{
		Name:       component.Name,
		Type:       component.ComponentType,
		Image:      component.Image,
		Replicas:   component.Replicas,
		Properties: properties,
		Traits:     traits,
	}, nil
}

func sortedSnapshotKeys(snapshots map[string]*apisv1.VersionComponentState) []string {
	keys := make([]string, 0, len(snapshots))
	for key := range snapshots {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func diffComponentFields(before, after *apisv1.VersionComponentState) []apisv1.VersionComponentField {
	fields := make([]apisv1.VersionComponentField, 0, 4)
	if before.Image != after.Image {
		fields = append(fields, apisv1.VersionComponentField{Field: versionDiffFieldImage, Before: before.Image, After: after.Image})
	}
	if before.Replicas != after.Replicas {
		fields = append(fields, apisv1.VersionComponentField{Field: versionDiffFieldReplicas, Before: before.Replicas, After: after.Replicas})
	}
	if !reflect.DeepEqual(before.Properties, after.Properties) {
		fields = append(fields, apisv1.VersionComponentField{Field: versionDiffFieldProperties, Before: before.Properties, After: after.Properties})
	}
	if !reflect.DeepEqual(before.Traits, after.Traits) {
		fields = append(fields, apisv1.VersionComponentField{Field: versionDiffFieldTraits, Before: before.Traits, After: after.Traits})
	}
	return fields
}

func buildDiffUpdateVersionRequest(version string, req apisv1.DiffUpdateVersionRequest, diff *versionDiffResult, targetOnlyStrategy string) apisv1.UpdateVersionRequest {
	updateReq := apisv1.UpdateVersionRequest{
		Version:        version,
		Strategy:       req.Strategy,
		ExecutionScope: req.ExecutionScope,
		WorkflowID:     req.WorkflowID,
		ExecuteAt:      req.ExecuteAt,
		AutoExec:       req.AutoExec,
		Description:    req.Description,
		Components:     make([]apisv1.ComponentUpdateSpec, 0, len(diff.updated)+len(diff.added)+len(diff.extra)),
	}
	for _, item := range diff.updated {
		spec, ok := versionComponentDiffUpdateSpec(item)
		if !ok {
			continue
		}
		updateReq.Components = append(updateReq.Components, spec)
	}
	for _, item := range diff.added {
		if item.After == nil {
			continue
		}
		replicas := item.After.Replicas
		properties := item.After.Properties
		traits := item.After.Traits
		updateReq.Components = append(updateReq.Components, apisv1.ComponentUpdateSpec{
			Action:        string(config.ComponentActionAdd),
			Name:          item.After.Name,
			Image:         item.After.Image,
			Replicas:      &replicas,
			ComponentType: item.After.Type,
			Properties:    &properties,
			Traits:        &traits,
		})
	}
	if targetOnlyStrategy == apisv1.DiffUpdateTargetOnlyStrategyRemove {
		for _, item := range diff.extra {
			if item.Before == nil {
				continue
			}
			updateReq.Components = append(updateReq.Components, apisv1.ComponentUpdateSpec{
				Action: string(config.ComponentActionRemove),
				Name:   item.Before.Name,
			})
		}
	}
	return updateReq
}

func versionComponentDiffUpdateSpec(item apisv1.VersionComponentDiff) (apisv1.ComponentUpdateSpec, bool) {
	if item.After == nil {
		return apisv1.ComponentUpdateSpec{}, false
	}
	spec := apisv1.ComponentUpdateSpec{
		Action: string(config.ComponentActionUpdate),
		Name:   item.After.Name,
	}
	for _, field := range item.Fields {
		switch field.Field {
		case versionDiffFieldImage:
			spec.Image = item.After.Image
		case versionDiffFieldReplicas:
			replicas := item.After.Replicas
			spec.Replicas = &replicas
		case versionDiffFieldProperties:
			properties := item.After.Properties
			spec.Properties = &properties
		case versionDiffFieldTraits:
			traits := cloneVersionComponentTraitsForUpdate(item.After.Traits)
			spec.Traits = &traits
		}
	}
	return spec, true
}

func cloneVersionComponentTraitsForUpdate(source apisv1.Traits) apisv1.Traits {
	copied := source
	copied.Storage = append(source.Storage[:0:0], source.Storage...)
	copied.Init = append(source.Init[:0:0], source.Init...)
	for index := range copied.Init {
		copied.Init[index].Traits = cloneVersionComponentTraitsForUpdate(source.Init[index].Traits)
	}
	copied.Sidecar = append(source.Sidecar[:0:0], source.Sidecar...)
	for index := range copied.Sidecar {
		copied.Sidecar[index].Traits = cloneVersionComponentTraitsForUpdate(source.Sidecar[index].Traits)
	}
	return copied
}

func cloneVersionComponentState(source *apisv1.VersionComponentState) *apisv1.VersionComponentState {
	if source == nil {
		return nil
	}
	copied := *source
	return &copied
}

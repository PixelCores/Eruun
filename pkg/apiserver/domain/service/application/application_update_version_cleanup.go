package application

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func buildVersionUpdateCleanupInfo(
	specs []apisv1.ComponentUpdateSpec,
	componentMap map[string]*model.ApplicationComponent,
	cleanupStepIndexes map[string]int,
	cleanupAppendStepIndex int,
) (*model.VersionUpdateCleanupInfo, error) {
	components := make([]model.VersionUpdateCleanupComponent, 0)
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return nil, err
		}
		if action != config.ComponentActionRemove {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(spec.Name))
		if key == "" {
			continue
		}
		comp, exists := componentMap[key]
		if !exists || comp == nil {
			continue
		}
		insertBeforeStepIndex := cleanupAppendStepIndex
		if idx, ok := cleanupStepIndexes[strings.ToLower(strings.TrimSpace(comp.Name))]; ok {
			insertBeforeStepIndex = idx
		}
		cleanupComponent, err := versionUpdateCleanupComponentDescriptor(comp)
		if err != nil {
			return nil, fmt.Errorf("build cleanup component %s descriptor: %w", comp.Name, err)
		}
		if strings.TrimSpace(cleanupComponent.Name) == "" {
			return nil, fmt.Errorf("cleanup component name is empty")
		}
		if strings.TrimSpace(cleanupComponent.AppID) == "" {
			return nil, fmt.Errorf("cleanup component %s appID is empty", cleanupComponent.Name)
		}
		if insertBeforeStepIndex < 0 {
			return nil, fmt.Errorf("cleanup component %s insert step index is negative", cleanupComponent.Name)
		}
		components = append(components, model.VersionUpdateCleanupComponent{
			Component:             cleanupComponent,
			ResourceAppName:       strings.TrimSpace(comp.ResourceAppName),
			InsertBeforeStepIndex: insertBeforeStepIndex,
		})
	}
	if len(components) == 0 {
		return nil, nil
	}
	return &model.VersionUpdateCleanupInfo{
		Source:     config.JobInfoSourceVersionUpdateRemove,
		Version:    model.VersionUpdateCleanupInfoVersionV1,
		Components: components,
	}, nil
}

func buildVersionUpdateFullCleanupInfo(
	componentMap map[string]*model.ApplicationComponent,
	specs []apisv1.ComponentUpdateSpec,
	insertBeforeStepIndex int,
	cleanupOnly bool,
	requireStatefulSetRecreation bool,
) (*model.VersionUpdateCleanupInfo, error) {
	if insertBeforeStepIndex < 0 {
		return nil, fmt.Errorf("cleanup insert step index is negative")
	}
	components := make([]model.VersionUpdateCleanupComponent, 0, len(componentMap))
	cleanupInfoVersion := model.VersionUpdateCleanupInfoVersionV1
	updateSpecs, err := versionUpdateComponentUpdateSpecsByName(specs)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(componentMap))
	for key, comp := range componentMap {
		if comp == nil {
			continue
		}
		if strings.TrimSpace(key) == "" {
			key = strings.ToLower(strings.TrimSpace(comp.Name))
		}
		if key == "" {
			continue
		}
		names = append(names, key)
	}
	sort.Strings(names)
	for _, key := range names {
		comp := componentMap[key]
		if comp == nil {
			continue
		}
		cleanupComponent, err := versionUpdateCleanupComponentDescriptor(comp)
		if err != nil {
			return nil, fmt.Errorf("build cleanup component %s descriptor: %w", comp.Name, err)
		}
		if strings.TrimSpace(cleanupComponent.Name) == "" {
			return nil, fmt.Errorf("cleanup component name is empty")
		}
		if strings.TrimSpace(cleanupComponent.AppID) == "" {
			return nil, fmt.Errorf("cleanup component %s appID is empty", cleanupComponent.Name)
		}
		requireStatefulSetDeletion := false
		var statefulSetPVCTemplatesToDelete []string
		if requireStatefulSetRecreation && versionUpdateRecreatesStatefulSet(comp, true) {
			if update, exists := updateSpecs[strings.ToLower(strings.TrimSpace(comp.Name))]; exists {
				requireStatefulSetDeletion, err = versionUpdateStatefulSetRequiresDeletion(comp, update)
				if err != nil {
					return nil, fmt.Errorf("build cleanup component %s StatefulSet deletion plan: %w", comp.Name, err)
				}
			}
		}
		if requireStatefulSetDeletion {
			if cleanupInfoVersion < model.VersionUpdateCleanupInfoVersionStatefulSetDeletion {
				cleanupInfoVersion = model.VersionUpdateCleanupInfoVersionStatefulSetDeletion
			}
			if update, exists := updateSpecs[strings.ToLower(strings.TrimSpace(comp.Name))]; exists {
				statefulSetPVCTemplatesToDelete, err = versionUpdateStatefulSetPVCTemplatesToDelete(comp, update)
				if err != nil {
					return nil, fmt.Errorf("build cleanup component %s StatefulSet PVC plan: %w", comp.Name, err)
				}
				if len(statefulSetPVCTemplatesToDelete) > 0 {
					cleanupInfoVersion = model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion
				}
			}
		}
		components = append(components, model.VersionUpdateCleanupComponent{
			Component:                       cleanupComponent,
			ResourceAppName:                 strings.TrimSpace(comp.ResourceAppName),
			InsertBeforeStepIndex:           insertBeforeStepIndex,
			RequireStatefulSetDeletion:      requireStatefulSetDeletion,
			StatefulSetPVCTemplatesToDelete: statefulSetPVCTemplatesToDelete,
		})
	}
	if len(components) == 0 {
		return nil, bcode.ErrApplicationNoComponents
	}
	return &model.VersionUpdateCleanupInfo{
		Source:      config.JobInfoSourceVersionUpdateRemove,
		Version:     cleanupInfoVersion,
		CleanupOnly: cleanupOnly,
		Components:  components,
	}, nil
}

func marshalVersionUpdateCleanupInfo(cleanupInfo *model.VersionUpdateCleanupInfo) (string, error) {
	if cleanupInfo == nil || len(cleanupInfo.Components) == 0 {
		return "", nil
	}
	payload, err := json.Marshal(cleanupInfo)
	if err != nil {
		return "", fmt.Errorf("marshal cleanup info: %w", err)
	}
	return string(payload), nil
}

func versionUpdateCleanupComponentDescriptor(component *model.ApplicationComponent) (*model.ApplicationComponent, error) {
	if component == nil {
		return nil, nil
	}
	descriptor := &model.ApplicationComponent{
		ID:              component.ID,
		AppID:           strings.TrimSpace(component.AppID),
		Name:            strings.TrimSpace(component.Name),
		Namespace:       strings.TrimSpace(component.Namespace),
		ComponentType:   component.ComponentType,
		ResourceAppName: strings.TrimSpace(component.ResourceAppName),
	}
	properties, err := versionUpdateCleanupPropertiesDescriptor(component.Properties)
	if err != nil {
		return nil, err
	}
	descriptor.Properties = properties
	traits, err := versionUpdateCleanupTraitsDescriptor(component.Traits)
	if err != nil {
		return nil, err
	}
	descriptor.Traits = traits
	return descriptor, nil
}

func versionUpdateCleanupPropertiesDescriptor(raw *model.JSONStruct) (*model.JSONStruct, error) {
	if raw == nil {
		return nil, nil
	}
	var properties model.Properties
	if err := decodeJSONStruct(raw, &properties); err != nil {
		return nil, fmt.Errorf("decode cleanup component properties: %w", err)
	}
	descriptor := model.Properties{
		Ports:    append([]model.Ports(nil), properties.Ports...),
		Schedule: strings.TrimSpace(properties.Schedule),
	}
	if len(descriptor.Ports) == 0 && descriptor.Schedule == "" {
		return nil, nil
	}
	return model.NewJSONStructByStruct(descriptor)
}

func versionUpdateCleanupTraitsDescriptor(raw *model.JSONStruct) (*model.JSONStruct, error) {
	if raw == nil {
		return nil, nil
	}
	var traits spec.Traits
	if err := decodeJSONStruct(raw, &traits); err != nil {
		return nil, fmt.Errorf("decode cleanup component traits: %w", err)
	}
	descriptor := spec.Traits{
		Storage: versionUpdateCleanupStorageTraits(traits.Storage),
		Init:    versionUpdateCleanupInitTraits(traits.Init),
		Sidecar: versionUpdateCleanupSidecarTraits(traits.Sidecar),
		Service: versionUpdateCleanupServiceTraits(traits.Service),
		Ingress: versionUpdateCleanupIngressTraits(traits.Ingress),
		RBAC:    versionUpdateCleanupRBACTraits(traits.RBAC),
	}
	if traits.Share != nil {
		descriptor.Share = &spec.ShareTraitSpec{Strategy: strings.TrimSpace(traits.Share.Strategy)}
	}
	if descriptor.Share == nil &&
		len(descriptor.Storage) == 0 &&
		len(descriptor.Init) == 0 &&
		len(descriptor.Sidecar) == 0 &&
		len(descriptor.Service) == 0 &&
		len(descriptor.Ingress) == 0 &&
		len(descriptor.RBAC) == 0 {
		return nil, nil
	}
	return model.NewJSONStructByStruct(descriptor)
}

func versionUpdateCleanupStorageTraits(storage []spec.StorageTraitSpec) []spec.StorageTraitSpec {
	if len(storage) == 0 {
		return nil
	}
	out := make([]spec.StorageTraitSpec, 0, len(storage))
	for _, item := range storage {
		out = append(out, spec.StorageTraitSpec{
			Name:         strings.TrimSpace(item.Name),
			Type:         strings.TrimSpace(item.Type),
			MountPath:    strings.TrimSpace(item.MountPath),
			TmpCreate:    item.TmpCreate,
			Size:         strings.TrimSpace(item.Size),
			ClaimName:    strings.TrimSpace(item.ClaimName),
			StorageClass: strings.TrimSpace(item.StorageClass),
		})
	}
	return out
}

func versionUpdateCleanupInitTraits(initTraits []spec.InitTraitSpec) []spec.InitTraitSpec {
	if len(initTraits) == 0 {
		return nil
	}
	out := make([]spec.InitTraitSpec, 0, len(initTraits))
	for _, item := range initTraits {
		storage := versionUpdateCleanupStorageTraits(item.Traits.Storage)
		if len(storage) == 0 {
			continue
		}
		out = append(out, spec.InitTraitSpec{
			Name:  strings.TrimSpace(item.Name),
			Image: strings.TrimSpace(item.Image),
			Traits: spec.Traits{
				Storage: storage,
			},
		})
	}
	return out
}

func versionUpdateCleanupSidecarTraits(sidecars []spec.SidecarTraitsSpec) []spec.SidecarTraitsSpec {
	if len(sidecars) == 0 {
		return nil
	}
	out := make([]spec.SidecarTraitsSpec, 0, len(sidecars))
	for _, item := range sidecars {
		storage := versionUpdateCleanupStorageTraits(item.Traits.Storage)
		if len(storage) == 0 {
			continue
		}
		out = append(out, spec.SidecarTraitsSpec{
			Name:  strings.TrimSpace(item.Name),
			Image: strings.TrimSpace(item.Image),
			Traits: spec.Traits{
				Storage: storage,
			},
		})
	}
	return out
}

func versionUpdateCleanupServiceTraits(services []spec.ServiceTraitSpec) []spec.ServiceTraitSpec {
	if len(services) == 0 {
		return nil
	}
	out := make([]spec.ServiceTraitSpec, 0, len(services))
	for _, item := range services {
		out = append(out, spec.ServiceTraitSpec{
			Name:         strings.TrimSpace(item.Name),
			Type:         strings.TrimSpace(item.Type),
			ExternalName: strings.TrimSpace(item.ExternalName),
			Headless:     item.Headless,
			Ports:        versionUpdateCleanupServicePorts(item.Ports),
		})
	}
	return out
}

func versionUpdateCleanupServicePorts(ports []spec.ServicePortTraitSpec) []spec.ServicePortTraitSpec {
	if len(ports) == 0 {
		return nil
	}
	out := make([]spec.ServicePortTraitSpec, 0, len(ports))
	for _, port := range ports {
		out = append(out, spec.ServicePortTraitSpec{
			Name:       strings.TrimSpace(port.Name),
			Port:       port.Port,
			TargetPort: port.TargetPort,
			Protocol:   strings.TrimSpace(port.Protocol),
		})
	}
	return out
}

func versionUpdateCleanupIngressTraits(ingresses []spec.IngressTraitsSpec) []spec.IngressTraitsSpec {
	if len(ingresses) == 0 {
		return nil
	}
	out := make([]spec.IngressTraitsSpec, 0, len(ingresses))
	for _, item := range ingresses {
		out = append(out, spec.IngressTraitsSpec{
			Name:             strings.TrimSpace(item.Name),
			Namespace:        strings.TrimSpace(item.Namespace),
			Hosts:            append([]string(nil), item.Hosts...),
			IngressClassName: strings.TrimSpace(item.IngressClassName),
			DefaultPathType:  strings.TrimSpace(item.DefaultPathType),
			Routes:           versionUpdateCleanupIngressRoutes(item.Routes),
		})
	}
	return out
}

func versionUpdateCleanupIngressRoutes(routes []spec.IngressRoutes) []spec.IngressRoutes {
	if len(routes) == 0 {
		return nil
	}
	out := make([]spec.IngressRoutes, 0, len(routes))
	for _, route := range routes {
		out = append(out, spec.IngressRoutes{
			Path:     strings.TrimSpace(route.Path),
			PathType: strings.TrimSpace(route.PathType),
			Host:     strings.TrimSpace(route.Host),
			Backend: spec.IngressRoute{
				ServiceName: strings.TrimSpace(route.Backend.ServiceName),
				ServicePort: route.Backend.ServicePort,
				Weight:      route.Backend.Weight,
			},
		})
	}
	return out
}

func versionUpdateCleanupRBACTraits(policies []spec.RBACPolicySpec) []spec.RBACPolicySpec {
	if len(policies) == 0 {
		return nil
	}
	out := make([]spec.RBACPolicySpec, 0, len(policies))
	for _, policy := range policies {
		out = append(out, spec.RBACPolicySpec{
			ServiceAccount: strings.TrimSpace(policy.ServiceAccount),
			Namespace:      strings.TrimSpace(policy.Namespace),
			ClusterScope:   policy.ClusterScope,
			RoleName:       strings.TrimSpace(policy.RoleName),
			BindingName:    strings.TrimSpace(policy.BindingName),
			Rules:          versionUpdateCleanupRBACRules(policy.Rules),
		})
	}
	return out
}

func versionUpdateCleanupRBACRules(rules []spec.RBACRuleSpec) []spec.RBACRuleSpec {
	if len(rules) == 0 {
		return nil
	}
	out := make([]spec.RBACRuleSpec, 0, len(rules))
	for _, rule := range rules {
		out = append(out, spec.RBACRuleSpec{
			APIGroups:       append([]string(nil), rule.APIGroups...),
			Resources:       append([]string(nil), rule.Resources...),
			ResourceNames:   append([]string(nil), rule.ResourceNames...),
			NonResourceURLs: append([]string(nil), rule.NonResourceURLs...),
			Verbs:           append([]string(nil), rule.Verbs...),
		})
	}
	return out
}

func recordVersionUpdateCleanupJobs(
	ctx context.Context,
	store datastore.DataStore,
	task *model.WorkflowQueue,
	cleanupInfo *model.VersionUpdateCleanupInfo,
) error {
	if cleanupInfo == nil || len(cleanupInfo.Components) == 0 {
		return nil
	}
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return fmt.Errorf("workflow task is nil")
	}
	for _, cleanupComponent := range cleanupInfo.Components {
		comp := cleanupComponent.Component
		if comp == nil {
			continue
		}
		if strings.TrimSpace(comp.Name) == "" {
			return fmt.Errorf("cleanup component name is empty")
		}
		internalInfo, err := versionUpdateCleanupJobInfoMarker(
			cleanupComponent.RequireStatefulSetDeletion,
			cleanupComponent.StatefulSetPVCTemplatesToDelete,
		)
		if err != nil {
			return err
		}
		namespace := strings.TrimSpace(comp.Namespace)
		if namespace == "" {
			namespace = config.DefaultNamespace
		}
		jobInfo := &model.JobInfo{
			Type:         string(config.JobCleanupResources),
			WorkflowID:   task.WorkflowID,
			ProductID:    task.ProjectID,
			AppID:        task.AppID,
			TaskID:       task.TaskID,
			Status:       string(config.StatusQueued),
			Info:         fmt.Sprintf("cleanup resources: %s/%s", namespace, comp.Name),
			InternalInfo: internalInfo,
			ServiceName:  strings.TrimSpace(comp.Name),
		}
		if err := store.Add(ctx, jobInfo); err != nil {
			return fmt.Errorf("persist cleanup component %s: %w", comp.Name, err)
		}
	}
	return nil
}

func versionUpdateCleanupJobInfoMarker(requireStatefulSetDeletion bool, statefulSetPVCTemplatesToDelete []string) (string, error) {
	markerVersion := 0
	if len(statefulSetPVCTemplatesToDelete) > 0 {
		markerVersion = model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion
	}
	payload, err := json.Marshal(versionUpdateCleanupJobMarker{
		Source:                          config.JobInfoSourceVersionUpdateRemove,
		Version:                         markerVersion,
		RequireStatefulSetDeletion:      requireStatefulSetDeletion,
		StatefulSetPVCTemplatesToDelete: normalizeVersionUpdatePVCTemplateNames(statefulSetPVCTemplatesToDelete),
	})
	if err != nil {
		return "", fmt.Errorf("marshal cleanup job info marker: %w", err)
	}
	return string(payload), nil
}

func versionUpdateCleanupStepIndexes(
	workflow *model.Workflow,
	specs []apisv1.ComponentUpdateSpec,
	componentMap map[string]*model.ApplicationComponent,
) (map[string]int, int, error) {
	var steps model.WorkflowSteps
	if workflow != nil && workflow.Steps != nil {
		if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
			return nil, 0, err
		}
	}
	removedSet, err := versionUpdateRemovedComponentSet(specs, componentMap)
	if err != nil {
		return nil, 0, err
	}
	appendIndex := versionUpdatePostRemovalAppendIndex(&steps, removedSet)
	indexes := make(map[string]int)
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return nil, 0, err
		}
		if action != config.ComponentActionRemove {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(spec.Name))
		if key == "" {
			continue
		}
		comp, exists := componentMap[key]
		if !exists || comp == nil {
			continue
		}
		originalIndex := findWorkflowStepIndexForComponent(&steps, comp.Name, len(steps.Steps))
		indexes[strings.ToLower(strings.TrimSpace(comp.Name))] = versionUpdatePostRemovalStepIndex(&steps, removedSet, originalIndex)
	}
	return indexes, appendIndex, nil
}

func versionUpdateFullCleanupInsertStepIndex(workflow *model.Workflow) (int, error) {
	var steps model.WorkflowSteps
	if workflow != nil && workflow.Steps != nil {
		if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
			return 0, err
		}
	}
	index := 0
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if config.ParseWorkflowStepType(string(step.StepType)) != config.WorkflowStepTypeApproval {
			break
		}
		index++
	}
	return index, nil
}

func versionUpdateRemovedComponentSet(specs []apisv1.ComponentUpdateSpec, componentMap map[string]*model.ApplicationComponent) (map[string]struct{}, error) {
	removedSet := make(map[string]struct{})
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return nil, err
		}
		if action != config.ComponentActionRemove {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(spec.Name))
		if key == "" {
			continue
		}
		if comp, exists := componentMap[key]; !exists || comp == nil {
			continue
		}
		removedSet[key] = struct{}{}
	}
	return removedSet, nil
}

func versionUpdatePostRemovalAppendIndex(steps *model.WorkflowSteps, removedSet map[string]struct{}) int {
	if steps == nil {
		return 0
	}
	index := 0
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if versionUpdateWorkflowStepSurvivesRemoval(step, removedSet) {
			index++
		}
	}
	return index
}

func versionUpdatePostRemovalStepIndex(steps *model.WorkflowSteps, removedSet map[string]struct{}, originalIndex int) int {
	if steps == nil || originalIndex <= 0 {
		return 0
	}
	if originalIndex > len(steps.Steps) {
		originalIndex = len(steps.Steps)
	}
	index := 0
	for i := 0; i < originalIndex; i++ {
		step := steps.Steps[i]
		if step == nil {
			continue
		}
		if versionUpdateWorkflowStepSurvivesRemoval(step, removedSet) {
			index++
		}
	}
	return index
}

func versionUpdateWorkflowStepSurvivesRemoval(step *model.WorkflowStep, removedSet map[string]struct{}) bool {
	if step == nil {
		return false
	}
	if config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeApproval {
		return true
	}
	if len(step.SubSteps) > 0 {
		hasComponentNames := false
		for _, sub := range step.SubSteps {
			hasNames, survives := versionUpdateComponentNamesSurviveRemoval(sub.ComponentNames(), removedSet)
			if hasNames {
				hasComponentNames = true
			}
			if survives {
				return true
			}
		}
		return !hasComponentNames
	}
	hasNames, survives := versionUpdateComponentNamesSurviveRemoval(step.ComponentNames(), removedSet)
	if !hasNames {
		return true
	}
	return survives
}

func versionUpdateComponentNamesSurviveRemoval(names []string, removedSet map[string]struct{}) (bool, bool) {
	hasNames := false
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		hasNames = true
		if _, removed := removedSet[key]; !removed {
			return true, true
		}
	}
	return hasNames, false
}

func findWorkflowStepIndexForComponent(steps *model.WorkflowSteps, componentName string, fallbackIndex int) int {
	if steps == nil {
		return fallbackIndex
	}
	target := strings.ToLower(strings.TrimSpace(componentName))
	if target == "" {
		return fallbackIndex
	}
	for index, step := range steps.Steps {
		if step == nil {
			continue
		}
		if workflowComponentNamesContain(step.ComponentNames(), target) {
			return index
		}
		for _, sub := range step.SubSteps {
			if workflowComponentNamesContain(sub.ComponentNames(), target) {
				return index
			}
		}
	}
	return fallbackIndex
}

func workflowComponentNamesContain(names []string, target string) bool {
	for _, name := range names {
		if strings.ToLower(strings.TrimSpace(name)) == target {
			return true
		}
	}
	return false
}

func (c *applicationsServiceImpl) cleanupVersionUpdateRemovedComponent(ctx context.Context, comp *model.ApplicationComponent) error {
	if comp == nil {
		return nil
	}
	if skip, reason := c.shouldSkipSharedComponentCleanup(ctx, comp); skip {
		klog.Infof("version update remove: skip cleanup for shared component %s/%s (%s)", comp.Namespace, comp.Name, reason)
		return nil
	} else if reason != "" {
		klog.Infof("version update remove: continue shared component cleanup %s/%s (%s)", comp.Namespace, comp.Name, reason)
	}
	reporter := newCleanupReporter()
	if err := c.deleteComponentResources(ctx, comp, reporter); err != nil {
		return err
	}
	return reporter.err()
}

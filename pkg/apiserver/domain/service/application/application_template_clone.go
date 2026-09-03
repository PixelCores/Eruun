package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/access"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type componentOverride struct {
	name                string
	compType            config.JobType
	properties          apisv1.Properties
	traits              apisv1.Traits
	target              string
	defaultStorageClass string
	sourceIndex         int
}

type templateRequest struct {
	baseName  string
	overrides []componentOverride
}

type templateComponentPlan struct {
	templateComp        *model.ApplicationComponent
	targetName          string
	override            apisv1.Properties
	overrideTraits      apisv1.Traits
	defaultStorageClass string
	rewriteMap          *templateRewriteMap
	sourceIndex         int
}

const unmappedComponentSourceIndex = -1

func lastSegment(name string) string {
	parts := strings.Split(name, "-")
	return parts[len(parts)-1]
}

func allocateTemplateTargetName(base string, used map[string]int) string {
	base = strings.TrimSpace(base)
	if base == "" {
		base = "component"
	}
	if used == nil {
		return base
	}
	if _, ok := used[base]; !ok {
		used[base] = 0
		return base
	}
	next := used[base] + 1
	for {
		candidate := fmt.Sprintf("%s-%d", base, next)
		if _, ok := used[candidate]; !ok {
			used[base] = next
			used[candidate] = 0
			return candidate
		}
		next++
	}
}

func templateOverrideTargetsComponent(target, templateName string) bool {
	target = strings.TrimSpace(target)
	templateName = strings.TrimSpace(templateName)
	return target == templateName || target == fmt.Sprintf("tem-%s", templateName)
}

func (c *applicationsServiceImpl) resolveComponents(ctx context.Context, namespace, appName string, reqComponents []apisv1.CreateComponentRequest) ([]apisv1.CreateComponentRequest, error) {
	components, _, err := c.resolveComponentsWithSourceIndexes(ctx, namespace, appName, reqComponents)
	return components, err
}

func (c *applicationsServiceImpl) resolveComponentsWithSourceIndexes(ctx context.Context, namespace, appName string, reqComponents []apisv1.CreateComponentRequest) ([]apisv1.CreateComponentRequest, []int, error) {
	components := make([]apisv1.CreateComponentRequest, 0, len(reqComponents))
	sourceIndexes := make([]int, 0, len(reqComponents))
	templateOrder := make([]string, 0)
	templateMap := make(map[string]*templateRequest)

	for sourceIndex, comp := range reqComponents {
		if scope, ok := access.FromContext(ctx); ok && comp.Namespace != "" && comp.Namespace != scope.Namespace {
			return nil, nil, bcode.ErrForbidden
		}
		// 如果该组件没有使用模版，就直接组装这个模版
		if comp.Template == nil || strings.TrimSpace(comp.Template.ID) == "" {
			components = append(components, comp)
			sourceIndexes = append(sourceIndexes, sourceIndex)
			continue
		}
		// 以模板 ID 作为键，从 templateMap 中取出该模板的聚合请求，没有则新建 templateRequest 并记录到 templateOrder，用于后续按出现顺序处理。
		templateID := strings.TrimSpace(comp.Template.ID)
		tr, ok := templateMap[templateID]
		if !ok {
			tr = &templateRequest{baseName: strings.TrimSpace(appName)}
			templateMap[templateID] = tr
			templateOrder = append(templateOrder, templateID)
		}
		if tr.baseName == "" {
			tr.baseName = strings.TrimSpace(appName)
		}
		// 记录需要覆盖的组件名、类型和目标模板组件
		tr.overrides = append(tr.overrides, componentOverride{
			name:                strings.TrimSpace(comp.Name),
			compType:            comp.ComponentType,
			properties:          comp.Properties,
			traits:              comp.Traits,
			target:              strings.TrimSpace(comp.Template.Target),
			defaultStorageClass: strings.TrimSpace(comp.Template.DefaultStorageClass),
			sourceIndex:         sourceIndex,
		})
	}

	for _, templateID := range templateOrder {
		tr := templateMap[templateID]
		// 克隆
		clones, cloneSourceIndexes, err := c.cloneComponentsFromTemplate(ctx, namespace, templateID, tr)
		if err != nil {
			return nil, nil, err
		}
		components = append(components, clones...)
		sourceIndexes = append(sourceIndexes, cloneSourceIndexes...)
	}
	if scope, ok := access.FromContext(ctx); ok {
		if namespace != scope.Namespace || c.Cfg == nil || c.Cfg.Accounts == nil {
			return nil, nil, bcode.ErrForbidden
		}
		for i := range components {
			comp := &components[i]
			if comp.Namespace != "" && comp.Namespace != scope.Namespace {
				return nil, nil, bcode.ErrForbidden
			}
			comp.Namespace = scope.Namespace
			if comp.ComponentType == config.CloudJob {
				return nil, nil, bcode.ErrForbidden
			}
			if err := workspace.ValidateTraits(namespace, comp.Name, &comp.Traits, &comp.Properties, c.Cfg.Accounts.Workspace); err != nil {
				return nil, nil, err
			}
		}
	}
	return components, sourceIndexes, nil
}

func (c *applicationsServiceImpl) cloneComponentsFromTemplate(ctx context.Context, namespace, templateID string, tr *templateRequest) ([]apisv1.CreateComponentRequest, []int, error) {
	if tr == nil {
		return nil, nil, nil
	}
	if templateID == "" {
		return nil, nil, bcode.ErrTemplateIDMissing
	}

	templateApp, err := c.AppRepo.FindByID(ctx, templateID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, nil, bcode.ErrApplicationNotExist
		}
		return nil, nil, err
	}
	if !templateApp.TemplateEnabled {
		return nil, nil, bcode.ErrTemplateNotEnabled
	}

	templateComponents, err := c.ComponentRepo.FindByAppID(ctx, templateApp.ID)
	if err != nil {
		return nil, nil, err
	}
	if len(templateComponents) == 0 {
		return nil, nil, bcode.ErrTemplateComponentMissing
	}

	baseName := strings.TrimSpace(tr.baseName)
	if baseName == "" {
		baseName = templateApp.Name
	}

	type ovState struct {
		componentOverride
		used bool
	}
	overrides := make([]ovState, len(tr.overrides))
	for i, o := range tr.overrides {
		overrides[i] = ovState{componentOverride: o}
	}

	pickTargetOverrides := func(templateName string, jobType config.JobType) ([]*componentOverride, error) {
		var selected []*componentOverride
		for i := range overrides {
			if overrides[i].used {
				continue
			}
			if overrides[i].target != "" && templateOverrideTargetsComponent(overrides[i].target, templateName) {
				if overrides[i].compType != "" && overrides[i].compType != jobType {
					return nil, bcode.ErrTemplateTargetNotFound
				}
				overrides[i].used = true
				selected = append(selected, &overrides[i].componentOverride)
			}
		}
		return selected, nil
	}

	pickFallbackOverride := func(jobType config.JobType) (*componentOverride, error) {
		for i := range overrides {
			if overrides[i].used {
				continue
			}
			if overrides[i].target != "" {
				continue
			}
			if overrides[i].compType != "" && overrides[i].compType != jobType {
				continue
			}
			overrides[i].used = true
			return &overrides[i].componentOverride, nil
		}
		return nil, nil
	}

	plans := make([]templateComponentPlan, 0, len(templateComponents))
	usedTargetNames := make(map[string]int)
	for _, templateComp := range templateComponents {
		if templateComp == nil {
			continue
		}
		selectedOverrides, err := pickTargetOverrides(templateComp.Name, templateComp.ComponentType)
		if err != nil {
			return nil, nil, err
		}
		if len(selectedOverrides) == 0 {
			override, err := pickFallbackOverride(templateComp.ComponentType)
			if err != nil {
				return nil, nil, err
			}
			selectedOverrides = append(selectedOverrides, override)
		}

		for _, override := range selectedOverrides {
			targetName := templateComp.Name
			if override != nil && override.name != "" {
				targetName = override.name
			} else if baseName != "" {
				targetName = fmt.Sprintf("%s-%s", baseName, lastSegment(templateComp.Name))
			}
			targetName = allocateTemplateTargetName(targetName, usedTargetNames)

			var overrideProps apisv1.Properties
			var overrideTraits apisv1.Traits
			var defaultStorageClass string
			sourceIndex := unmappedComponentSourceIndex
			if override != nil {
				overrideProps = override.properties
				overrideTraits = override.traits
				defaultStorageClass = override.defaultStorageClass
				sourceIndex = override.sourceIndex
			}
			plans = append(plans, templateComponentPlan{
				templateComp:        templateComp,
				targetName:          targetName,
				override:            overrideProps,
				overrideTraits:      overrideTraits,
				defaultStorageClass: defaultStorageClass,
				sourceIndex:         sourceIndex,
			})
		}
	}

	if err := buildTemplateRewriteMaps(plans, namespace, baseName); err != nil {
		return nil, nil, err
	}

	clones := make([]apisv1.CreateComponentRequest, 0, len(plans))
	cloneSourceIndexes := make([]int, 0, len(plans))
	for _, plan := range plans {
		clone, err := convertComponentFromTemplate(plan.templateComp, plan.targetName, baseName, namespace, plan.override, plan.overrideTraits, plan.defaultStorageClass, plan.rewriteMap)
		if err != nil {
			return nil, nil, err
		}
		clones = append(clones, *clone)
		cloneSourceIndexes = append(cloneSourceIndexes, plan.sourceIndex)
	}
	for _, ov := range overrides {
		if ov.used {
			continue
		}
		if ov.target != "" {
			return nil, nil, bcode.ErrTemplateTargetNotFound
		}
	}
	return clones, cloneSourceIndexes, nil
}

func convertComponentFromTemplate(templateComp *model.ApplicationComponent, newName, baseName, namespace string, overrideProps apisv1.Properties, overrideTraits apisv1.Traits, defaultStorageClass string, rewriteMap *templateRewriteMap) (*apisv1.CreateComponentRequest, error) {
	var properties apisv1.Properties
	if err := decodeJSONStruct(templateComp.Properties, &properties); err != nil {
		return nil, fmt.Errorf("convert template component %s properties: %w", templateComp.Name, err)
	}
	var traits apisv1.Traits
	if err := decodeJSONStruct(templateComp.Traits, &traits); err != nil {
		return nil, fmt.Errorf("convert template component %s traits: %w", templateComp.Name, err)
	}

	originalPodLabels := copyStringMap(properties.Labels)
	rewritePropertiesForTemplate(&properties, rewriteMap)
	applyPropertyOverrides(&properties, overrideProps, templateComp.ComponentType)
	applyTraitOverrides(&traits, overrideTraits)
	applyDefaultStorageClass(&traits, defaultStorageClass)
	initEnvOverrideKeys := applyInitEnvOverrides(&traits, overrideTraits)
	if err := rewriteTraitsForTemplate(&traits, templateComp.Name, newName, baseName, namespace, rewriteMap, originalPodLabels, properties.Labels, initEnvOverrideKeys); err != nil {
		return nil, err
	}

	return &apisv1.CreateComponentRequest{
		Name:          newName,
		ComponentType: templateComp.ComponentType,
		Image:         templateComp.Image,
		Namespace:     namespace,
		Replicas:      templateComp.Replicas,
		Properties:    properties,
		Traits:        traits,
	}, nil
}

func decodeJSONStruct(raw *model.JSONStruct, target interface{}) error {
	if raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	if string(data) == "null" {
		return nil
	}
	return json.Unmarshal(data, target)
}

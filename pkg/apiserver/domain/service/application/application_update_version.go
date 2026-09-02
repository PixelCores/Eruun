package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// UpdateVersion 更新应用版本，支持组件的更新、新增、删除操作
func (c *applicationsServiceImpl) UpdateVersion(ctx context.Context, appID string, req apisv1.UpdateVersionRequest) (*apisv1.UpdateVersionResponse, error) {
	var response *apisv1.UpdateVersionResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "update-application-version", func(lockCtx context.Context, _ *model.Applications) error {
		var updateErr error
		response, updateErr = c.updateVersionLocked(lockCtx, appID, req)
		return updateErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) updateVersionLocked(ctx context.Context, appID string, req apisv1.UpdateVersionRequest) (*apisv1.UpdateVersionResponse, error) {
	return c.updateVersionUnlocked(ctx, appID, req)
}

func (c *applicationsServiceImpl) updateVersionUnlocked(ctx context.Context, appID string, req apisv1.UpdateVersionRequest) (*apisv1.UpdateVersionResponse, error) {
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	app, err := c.AppRepo.FindByID(ctx, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	if app.EffectiveManagementMode() == config.ManagementModeObserve {
		return nil, fmt.Errorf("%w: observe applications are read-only", bcode.ErrApplicationManagementMode)
	}
	defer func() {
		c.invalidateApplicationListCaches()
		c.invalidateApplicationComponentsCache(app.ID)
	}()

	run, err := c.prepareVersionUpdateRun(ctx, app, req)
	if err != nil {
		return nil, err
	}
	if err := c.selectVersionUpdateWorkflow(ctx, run); err != nil {
		return nil, err
	}
	if err := c.commitVersionUpdateRun(ctx, run); err != nil {
		return nil, err
	}
	return c.finalizeVersionUpdateRun(ctx, run), nil
}

func validateAdoptedVersionUpdateActions(
	specs []apisv1.ComponentUpdateSpec,
	components map[string]*model.ApplicationComponent,
) error {
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return err
		}
		name := strings.TrimSpace(spec.Name)
		if action == config.ComponentActionAdd {
			return fmt.Errorf(
				"%w: adopted applications cannot add component %q without an explicit source binding",
				bcode.ErrApplicationManagementMode,
				name,
			)
		}
		if action == config.ComponentActionRemove {
			return fmt.Errorf(
				"%w: adopted component %q must be detached through an adoption-aware operation",
				bcode.ErrApplicationManagementMode,
				name,
			)
		}
		if spec.Properties != nil && spec.Properties.Secret != nil {
			return fmt.Errorf(
				"%w: adopted Secret values require the encrypted keyring-aware update path",
				bcode.ErrApplicationManagementMode,
			)
		}
		component := components[strings.ToLower(name)]
		if component == nil {
			return fmt.Errorf(
				"%w: adopted component %q has no source workload binding",
				bcode.ErrApplicationManagementMode,
				name,
			)
		}
		if err := validateAdoptedComponentSourceBinding(component); err != nil {
			return err
		}
		if err := validateAdoptedVersionUpdateCompatibility(component, spec); err != nil {
			return err
		}
	}
	return nil
}

func validateAdoptedComponentSourceBinding(component *model.ApplicationComponent) error {
	if component == nil || !component.HasSourceWorkload() {
		return fmt.Errorf("%w: adopted component has no source workload binding", bcode.ErrApplicationManagementMode)
	}
	if strings.TrimSpace(component.SourceWorkloadAPIVersion) != "apps/v1" {
		return fmt.Errorf(
			"%w: adopted component %q has unsupported source apiVersion %q",
			bcode.ErrApplicationManagementMode,
			component.Name,
			component.SourceWorkloadAPIVersion,
		)
	}
	expectedKind := ""
	switch component.ComponentType {
	case config.ServerJob:
		expectedKind = "Deployment"
	case config.StoreJob:
		expectedKind = "StatefulSet"
	default:
		return fmt.Errorf(
			"%w: adopted component %q has unsupported type %q",
			bcode.ErrApplicationManagementMode,
			component.Name,
			component.ComponentType,
		)
	}
	if strings.TrimSpace(component.SourceWorkloadKind) != expectedKind {
		return fmt.Errorf(
			"%w: adopted component %q source kind %q does not match type %q",
			bcode.ErrApplicationManagementMode,
			component.Name,
			component.SourceWorkloadKind,
			component.ComponentType,
		)
	}
	return nil
}

// updateComponent 更新单个组件的配置
func (c *applicationsServiceImpl) updateComponent(ctx context.Context, comp *model.ApplicationComponent, spec apisv1.ComponentUpdateSpec) (bool, error) {
	changed, err := c.applyComponentUpdate(comp, spec)
	if err != nil {
		return false, err
	}
	if changed {
		if err := c.ComponentRepo.Update(ctx, comp); err != nil {
			klog.Errorf("update component %s failed: %v", comp.Name, err)
			return false, err
		}
	}
	return changed, nil
}

func (c *applicationsServiceImpl) updateComponentInStore(ctx context.Context, store datastore.DataStore, comp *model.ApplicationComponent, spec apisv1.ComponentUpdateSpec) (bool, error) {
	changed, err := c.applyComponentUpdate(comp, spec)
	if err != nil {
		return false, err
	}
	if changed {
		if err := store.Put(ctx, comp); err != nil {
			klog.Errorf("update component %s failed: %v", comp.Name, err)
			return false, err
		}
	}
	return changed, nil
}

func (c *applicationsServiceImpl) applyComponentUpdate(comp *model.ApplicationComponent, spec apisv1.ComponentUpdateSpec) (bool, error) {
	changed := false

	// 更新镜像
	if spec.Image != "" && spec.Image != comp.Image {
		comp.Image = spec.Image
		changed = true
	}

	// 更新副本数
	if spec.Replicas != nil && *spec.Replicas != comp.Replicas {
		comp.Replicas = *spec.Replicas
		changed = true
	}

	// 覆盖完整 Properties（用于差异更新生成的组件补丁）
	if spec.Properties != nil {
		if reserved := reservedComponentLabelsIn(spec.Properties.Labels); len(reserved) > 0 {
			return false, fmt.Errorf("%w: component %s properties.labels contains reserved keys: %s", bcode.ErrInvalidProperties, spec.Name, strings.Join(reserved, ","))
		}
		equal, err := componentPropertiesEqual(comp.Properties, *spec.Properties)
		if err != nil {
			return false, err
		}
		if !equal {
			props, err := model.NewJSONStructByStruct(spec.Properties)
			if err != nil {
				return false, fmt.Errorf("marshal properties: %w", err)
			}
			comp.Properties = props
			changed = true
		}
	}

	// 更新环境变量
	if len(spec.Env) > 0 {
		updated, err := c.mergeComponentEnv(comp, spec.Env)
		if err != nil {
			return false, err
		}
		if updated {
			changed = true
		}
	}

	// 覆盖完整 Traits（用于差异更新生成的组件补丁）
	if spec.Traits != nil {
		if err := validateComponentTraitsForWrite(comp.ComponentType, *spec.Traits, fmt.Sprintf("component[%s].traits", strings.TrimSpace(spec.Name))); err != nil {
			return false, err
		}
		equal, err := componentTraitsEqual(comp.Traits, *spec.Traits)
		if err != nil {
			return false, err
		}
		if !equal {
			traits, err := model.NewJSONStructByStruct(spec.Traits)
			if err != nil {
				return false, fmt.Errorf("marshal traits: %w", err)
			}
			comp.Traits = traits
			changed = true
		}
	}

	return changed, nil
}

// addComponent 新增组件
func (c *applicationsServiceImpl) addComponent(ctx context.Context, app *model.Applications, spec apisv1.ComponentUpdateSpec) error {
	component, err := newVersionUpdateComponent(app, spec)
	if err != nil {
		return err
	}
	return c.ComponentRepo.Create(ctx, component)
}

func (c *applicationsServiceImpl) addComponentInStore(ctx context.Context, store datastore.DataStore, app *model.Applications, spec apisv1.ComponentUpdateSpec) error {
	component, err := newVersionUpdateComponent(app, spec)
	if err != nil {
		return err
	}
	return store.Add(ctx, component)
}

func newVersionUpdateComponent(app *model.Applications, spec apisv1.ComponentUpdateSpec) (*model.ApplicationComponent, error) {
	replicas := int32(1)
	if spec.Replicas != nil {
		replicas = *spec.Replicas
	}

	component := &model.ApplicationComponent{
		Name:          spec.Name,
		AppID:         app.ID,
		Namespace:     app.Namespace,
		Image:         spec.Image,
		Replicas:      replicas,
		ComponentType: spec.ComponentType,
	}

	// 设置 Properties
	if spec.Properties != nil {
		if reserved := reservedComponentLabelsIn(spec.Properties.Labels); len(reserved) > 0 {
			return nil, fmt.Errorf("%w: component %s properties.labels contains reserved keys: %s", bcode.ErrInvalidProperties, spec.Name, strings.Join(reserved, ","))
		}
		props, err := model.NewJSONStructByStruct(spec.Properties)
		if err != nil {
			return nil, fmt.Errorf("marshal properties: %w", err)
		}
		component.Properties = props
	}

	// 设置 Traits
	if spec.Traits != nil {
		if err := validateComponentTraitsForWrite(spec.ComponentType, *spec.Traits, fmt.Sprintf("component[%s].traits", strings.TrimSpace(spec.Name))); err != nil {
			return nil, err
		}
		traits, err := model.NewJSONStructByStruct(spec.Traits)
		if err != nil {
			return nil, fmt.Errorf("marshal traits: %w", err)
		}
		component.Traits = traits
	}

	return component, nil
}

// mergeComponentEnv 合并组件环境变量
func (c *applicationsServiceImpl) mergeComponentEnv(comp *model.ApplicationComponent, envUpdates map[string]string) (bool, error) {
	var props apisv1.Properties
	if comp.Properties != nil {
		if err := decodeJSONStruct(comp.Properties, &props); err != nil {
			return false, err
		}
	}

	if props.Env == nil {
		props.Env = make(map[string]string)
	}
	changed := false
	for k, v := range envUpdates {
		if props.Env[k] == v {
			continue
		}
		props.Env[k] = v
		changed = true
	}
	if !changed {
		return false, nil
	}

	newProps, err := model.NewJSONStructByStruct(props)
	if err != nil {
		return false, err
	}
	comp.Properties = newProps
	return true, nil
}

func newWorkflowQueueTask(workflow *model.Workflow, executeAt int64) *model.WorkflowQueue {
	return &model.WorkflowQueue{
		TaskID:              utils.RandStringByNumLowercase(24),
		AppID:               workflow.AppID,
		WorkflowID:          workflow.ID,
		ProjectID:           workflow.ProjectID,
		WorkflowName:        workflow.Name,
		WorkflowDisplayName: workflow.Alias,
		Type:                workflow.WorkflowType,
		Status:              config.StatusWaiting,
		ExecuteAt:           executeAt,
	}
}

// incrementVersion 递增版本号 (如 1.0.0 -> 1.0.1)
func incrementVersion(version string) string {
	if version == "" {
		return "1.0.1"
	}
	parts := strings.Split(version, ".")
	if len(parts) == 0 {
		return version + ".1"
	}

	// 递增最后一位
	lastIdx := len(parts) - 1
	var lastNum int
	fmt.Sscanf(parts[lastIdx], "%d", &lastNum)
	parts[lastIdx] = fmt.Sprintf("%d", lastNum+1)

	return strings.Join(parts, ".")
}

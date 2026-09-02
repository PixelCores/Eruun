package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type versionUpdateRun struct {
	app             *model.Applications
	req             apisv1.UpdateVersionRequest
	normalReq       apisv1.UpdateVersionRequest
	componentMap    map[string]*model.ApplicationComponent
	resourceActions versionUpdateResourceActions

	previousVersion string
	newVersion      string
	startTime       int64
	autoExec        bool
	executeAt       int64
	executionScope  config.VersionUpdateExecutionScope
	strategy        config.UpdateStrategy

	selectedWorkflow         *model.Workflow
	autoExecWorkflow         *model.Workflow
	responseWorkflowID       string
	taskCallback             *model.JSONStruct
	readyComponents          []string
	requiresAutoExecWorkflow bool
	requiresNoopTaskCallback bool
	hasPlannedChanges        bool

	updatedComponents   []string
	addedComponents     []string
	removedComponents   []string
	restartedComponents []string
	autoExecTaskID      string
	noopCallbackTask    *model.WorkflowQueue
}

func (c *applicationsServiceImpl) prepareVersionUpdateRun(
	ctx context.Context,
	app *model.Applications,
	req apisv1.UpdateVersionRequest,
) (*versionUpdateRun, error) {
	run := &versionUpdateRun{
		app:             app,
		req:             req,
		previousVersion: app.Version,
		newVersion:      req.Version,
		startTime:       time.Now().Unix(),
		autoExec:        true,
		strategy:        config.ParseUpdateStrategy(req.Strategy),
	}
	if req.AutoExec != nil {
		run.autoExec = *req.AutoExec
	}

	var err error
	run.executeAt, err = normalizeExecuteAt(req.ExecuteAt)
	if err != nil {
		return nil, err
	}
	imageReadyTimeoutSeconds, err := normalizeVersionUpdateImageReadyTimeoutSeconds(req.ImageReadyTimeoutSeconds)
	if err != nil {
		return nil, err
	}
	run.executionScope, err = normalizeVersionUpdateExecutionScope(req.ExecutionScope)
	if err != nil {
		return nil, err
	}
	if err := c.selectRequestedVersionUpdateWorkflow(ctx, run); err != nil {
		return nil, err
	}

	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(components, applicationResourceNameKey(app))
	run.componentMap = make(map[string]*model.ApplicationComponent, len(components))
	for _, component := range components {
		if component != nil {
			run.componentMap[strings.ToLower(component.Name)] = component
		}
	}

	if err := validateVersionUpdateComponentActionConflicts(req.Components); err != nil {
		return nil, err
	}
	run.resourceActions, err = parseVersionUpdateResourceActions(req.Components)
	if err != nil {
		return nil, err
	}
	if app.EffectiveManagementMode() == config.ManagementModeAdopted && run.resourceActions.fullCleanup {
		return nil, fmt.Errorf("%w: adopted full cleanup requires an explicit cleanup plan fingerprint", bcode.ErrApplicationManagementMode)
	}
	if err := validateVersionUpdateExecutionScopeActions(run.executionScope, run.resourceActions); err != nil {
		return nil, err
	}
	if run.resourceActions.deployAll && !run.autoExec {
		return nil, fmt.Errorf("%w: add all requires autoExec=true", bcode.ErrApplicationConfig)
	}
	if run.resourceActions.fullCleanup && !run.autoExec {
		return nil, fmt.Errorf("%w: remove cleanup_all requires autoExec=true", bcode.ErrApplicationConfig)
	}
	if len(run.resourceActions.restartComponents) > 0 && !run.autoExec {
		return nil, fmt.Errorf("%w: restart requires autoExec=true", bcode.ErrApplicationConfig)
	}
	if err := validateVersionUpdateRestartActions(run.resourceActions.restartComponents, run.componentMap); err != nil {
		return nil, err
	}

	run.normalReq = req
	run.normalReq.Components = run.resourceActions.components
	run.normalReq.ImageReadyTimeoutSeconds = imageReadyTimeoutSeconds
	run.normalReq.ExecutionScope = string(run.executionScope)
	run.normalReq.Components, err = normalizeVersionUpdateJobFailurePolicies(run.normalReq.Components, run.componentMap)
	if err != nil {
		return nil, err
	}
	if err := c.preflightVersionUpdateRun(ctx, run, components); err != nil {
		return nil, err
	}
	return run, nil
}

func (c *applicationsServiceImpl) selectRequestedVersionUpdateWorkflow(ctx context.Context, run *versionUpdateRun) error {
	workflowID := strings.TrimSpace(run.req.WorkflowID)
	if !run.autoExec || workflowID == "" {
		return nil
	}
	workflow, err := c.WorkflowRepo.FindByID(ctx, workflowID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return bcode.ErrWorkflowNotExist
		}
		return err
	}
	if workflow.AppID != run.app.ID {
		return bcode.ErrWorkflowConfig
	}
	run.selectedWorkflow = workflow
	run.responseWorkflowID = workflow.ID
	return nil
}

func (c *applicationsServiceImpl) preflightVersionUpdateRun(
	ctx context.Context,
	run *versionUpdateRun,
	components []*model.ApplicationComponent,
) error {
	if err := validateVersionUpdateComponentActionConflicts(run.normalReq.Components); err != nil {
		return err
	}
	if err := validateVersionUpdateActionContract(run.normalReq.Components, run.componentMap); err != nil {
		return err
	}
	if run.app.EffectiveManagementMode() == config.ManagementModeAdopted {
		if err := validateAdoptedVersionUpdateActions(run.normalReq.Components, run.componentMap); err != nil {
			return err
		}
	}
	if err := validateVersionUpdateSharedRemovals(run.normalReq.Components, run.componentMap); err != nil {
		return err
	}
	var err error
	run.normalReq.Components, err = preflightVersionUpdateStatefulSets(
		run.componentMap,
		run.normalReq.Components,
		run.resourceActions.fullCleanup && run.resourceActions.deployAll,
	)
	if err != nil {
		return err
	}
	hasComponentOrResourceAction := len(run.normalReq.Components) > 0 ||
		run.resourceActions.fullCleanup ||
		run.resourceActions.deployAll ||
		len(run.resourceActions.restartComponents) > 0
	if hasComponentOrResourceAction {
		if _, err := pendingVersionUpdateStatefulSetPVCDeletionsForRequest(
			ctx,
			c.Store,
			run.app.ID,
			run.normalReq.Components,
			run.resourceActions.fullCleanup && run.resourceActions.deployAll,
		); err != nil {
			return fmt.Errorf("pending StatefulSet PVC migration: %w", err)
		}
	}
	requiresWorkflowStepSync, err := hasWorkflowStructureChanges(run.normalReq.Components, run.componentMap)
	if err != nil {
		return err
	}
	if requiresWorkflowStepSync {
		if err := EnsureAppWorkflowIdle(ctx, c.Store, run.app.ID); err != nil {
			return err
		}
	}

	validationApp := *run.app
	validationApp.Version = run.newVersion
	if validationApp.TemplateEnabled {
		if err := c.ensureTemplateApplicationKeyAvailable(ctx, validationApp.Namespace, validationApp.Name, validationApp.Version, validationApp.ID); err != nil {
			return err
		}
	}
	resolvedComponents, err := buildVersionUpdateResolvedComponents(components, run.normalReq.Components)
	if err != nil {
		return err
	}
	if err := c.validateApplicationResourceNames(ctx, &validationApp, resolvedComponents); err != nil {
		return err
	}
	run.hasPlannedChanges, err = hasVersionUpdateComponentChanges(run.componentMap, run.normalReq.Components)
	if err != nil {
		return err
	}
	if run.app.EffectiveManagementMode() == config.ManagementModeAdopted {
		if err := EnsureAppWorkflowIdle(ctx, c.Store, run.app.ID); err != nil {
			return fmt.Errorf("update adopted application version: %w", err)
		}
	}
	return nil
}

func (c *applicationsServiceImpl) selectVersionUpdateWorkflow(ctx context.Context, run *versionUpdateRun) error {
	run.autoExecWorkflow = run.selectedWorkflow
	run.requiresAutoExecWorkflow = run.autoExec && (run.hasPlannedChanges ||
		run.resourceActions.fullCleanup ||
		run.resourceActions.deployAll ||
		len(run.resourceActions.restartComponents) > 0)
	run.requiresNoopTaskCallback = run.autoExec && !run.requiresAutoExecWorkflow && !callbackIsEmpty(run.req.Callback)
	if run.requiresAutoExecWorkflow {
		readyTargets, err := versionUpdateImageReadyComponents(run.componentMap, run.normalReq.Components)
		if err != nil {
			return err
		}
		readyUpdateTargets, err := versionUpdateReadyUpdateComponents(run.componentMap, run.normalReq.Components)
		if err != nil {
			return err
		}
		if run.autoExecWorkflow == nil {
			workflows, err := c.WorkflowRepo.FindByAppID(ctx, run.app.ID)
			if err != nil {
				return fmt.Errorf("find workflow for auto exec: %w", err)
			}
			run.autoExecWorkflow = pickExecutableDefaultWorkflow(workflows, "", "")
		}
		if run.autoExecWorkflow == nil {
			return bcode.ErrWorkflowNotExist
		}
		run.responseWorkflowID = run.autoExecWorkflow.ID
		if err := validateVersionUpdateReadyWorkflowCoverage(run.autoExecWorkflow, readyUpdateTargets); err != nil {
			return err
		}
		run.readyComponents = readyTargets
		if err := validateWorkflowTaskEnqueue(ctx, c.Store, run.autoExecWorkflow, false); err != nil {
			return autoExecWorkflowValidationError(err)
		}
		if err := EnsureAppWorkflowIdle(ctx, c.Store, run.autoExecWorkflow.AppID); err != nil {
			return fmt.Errorf("auto exec workflow: %w", err)
		}
	}
	if run.requiresAutoExecWorkflow || run.requiresNoopTaskCallback {
		var err error
		run.taskCallback, err = c.resolveVersionUpdateTaskCallback(ctx, run.req.Callback)
		if err != nil {
			return err
		}
		if run.taskCallback == nil {
			run.requiresNoopTaskCallback = false
		}
	}
	return nil
}

func (c *applicationsServiceImpl) commitVersionUpdateRun(ctx context.Context, run *versionUpdateRun) error {
	run.updatedComponents = make([]string, 0)
	run.addedComponents = make([]string, 0)
	run.removedComponents = make([]string, 0)
	run.restartedComponents = make([]string, 0)
	var err error
	if run.requiresAutoExecWorkflow {
		run.updatedComponents, run.addedComponents, run.removedComponents, run.restartedComponents, run.autoExecTaskID, err = c.commitAutoExecVersionUpdate(
			ctx, run.app, run.componentMap, run.normalReq, run.newVersion, run.autoExecWorkflow, run.executeAt, run.taskCallback, run.resourceActions, run.readyComponents, run.executionScope,
		)
		return err
	}
	if run.requiresNoopTaskCallback {
		run.noopCallbackTask, err = c.commitNoopVersionUpdateWithCallbackTask(
			ctx, run.app, run.newVersion, run.req.Description, run.responseWorkflowID, run.startTime, time.Now().Unix(),
			buildUpdateJobRecords(run.app, run.normalReq, nil, nil, nil), run.taskCallback,
		)
		if err != nil {
			return fmt.Errorf("record update-version callback task: %w", err)
		}
		return nil
	}
	return c.commitDirectVersionUpdate(ctx, run)
}

func (c *applicationsServiceImpl) commitDirectVersionUpdate(ctx context.Context, run *versionUpdateRun) error {
	var err error
	run.updatedComponents, run.addedComponents, run.removedComponents, err = applyVersionUpdateComponentChanges(
		ctx,
		run.componentMap,
		run.normalReq.Components,
		versionUpdateComponentChangeHandlers{
			update: c.updateComponent,
			add: func(ctx context.Context, spec apisv1.ComponentUpdateSpec) error {
				return c.addComponent(ctx, run.app, spec)
			},
			remove: func(ctx context.Context, component *model.ApplicationComponent, spec apisv1.ComponentUpdateSpec) error {
				shouldCleanup := run.app.EffectiveManagementMode() == config.ManagementModeNative || !component.HasSourceWorkload()
				if shouldCleanup {
					if err := c.cleanupVersionUpdateRemovedComponent(ctx, component); err != nil {
						klog.Errorf("cleanup component resources %s failed: %v", spec.Name, err)
						return err
					}
				}
				if err := c.ComponentRepo.Delete(ctx, component); err != nil {
					klog.Errorf("delete component %s failed: %v", spec.Name, err)
					return err
				}
				return nil
			},
		},
	)
	if err != nil {
		return err
	}
	run.app.Version = run.newVersion
	if run.req.Description != "" {
		run.app.Description = run.req.Description
	}
	if err := c.AppRepo.Update(ctx, run.app); err != nil {
		return bcode.ErrVersionUpdateFailed
	}
	if len(run.addedComponents) > 0 || len(run.removedComponents) > 0 {
		if err := c.syncWorkflowSteps(ctx, run.app.ID, run.addedComponents, run.removedComponents); err != nil {
			klog.Warningf("sync workflow steps failed after version update committed appID=%s version=%s err=%v", run.app.ID, run.newVersion, err)
		}
	}
	return nil
}

func (c *applicationsServiceImpl) finalizeVersionUpdateRun(ctx context.Context, run *versionUpdateRun) *apisv1.UpdateVersionResponse {
	response := &apisv1.UpdateVersionResponse{
		AppID:               run.app.ID,
		Version:             run.newVersion,
		PreviousVersion:     run.previousVersion,
		Strategy:            string(run.strategy),
		ExecutionScope:      string(run.executionScope),
		WorkflowID:          run.responseWorkflowID,
		UpdatedComponents:   run.updatedComponents,
		AddedComponents:     run.addedComponents,
		RemovedComponents:   run.removedComponents,
		RestartedComponents: run.restartedComponents,
	}
	if run.noopCallbackTask != nil {
		response.TaskID = run.noopCallbackTask.TaskID
		triggerWorkflowTerminalCallbackAsync(ctx, c.Store, c.Cfg, c.URLSecurityPolicyProvider, run.noopCallbackTask, config.StatusCompleted, "")
	}
	if run.autoExecTaskID != "" {
		response.TaskID = run.autoExecTaskID
		if run.executeAt == 0 {
			componentsToMark := append([]string{}, run.updatedComponents...)
			componentsToMark = append(componentsToMark, run.addedComponents...)
			if err := c.markComponentsUpdating(ctx, run.app.ID, componentsToMark); err != nil {
				klog.ErrorS(err, "mark components updating failed after auto exec workflow queued", "appID", run.app.ID, "taskID", response.TaskID)
			}
		}
	}
	if response.TaskID == "" {
		endTime := time.Now().Unix()
		response.TaskID, _, _ = c.attachOperationTaskWithWorkflowIDAndCallback(
			ctx,
			run.app,
			config.WorkflowTaskTypeUpdate,
			operationTaskNameUpdateVersion,
			run.responseWorkflowID,
			run.startTime,
			endTime,
			buildUpdateJobRecords(run.app, run.normalReq, run.updatedComponents, run.addedComponents, run.removedComponents),
			nil,
			nil,
		)
	}
	klog.Infof(
		"AUDIT: update version appID=%s from=%s to=%s strategy=%s executeAt=%d updated=%v added=%v removed=%v taskID=%s",
		run.app.ID,
		run.previousVersion,
		run.newVersion,
		run.strategy,
		run.executeAt,
		run.updatedComponents,
		run.addedComponents,
		run.removedComponents,
		response.TaskID,
	)
	return response
}

package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (c *applicationsServiceImpl) commitAutoExecVersionUpdate(
	ctx context.Context,
	app *model.Applications,
	componentMap map[string]*model.ApplicationComponent,
	req apisv1.UpdateVersionRequest,
	newVersion string,
	workflow *model.Workflow,
	executeAt int64,
	taskCallback *model.JSONStruct,
	resourceActions versionUpdateResourceActions,
	readyComponents []string,
	executionScope config.VersionUpdateExecutionScope,
) ([]string, []string, []string, []string, string, error) {
	if workflow == nil {
		return nil, nil, nil, nil, "", bcode.ErrWorkflowNotExist
	}
	txStore, ok := c.Store.(datastore.Transactional)
	if !ok {
		return nil, nil, nil, nil, "", fmt.Errorf("%w: auto exec version update requires transactional datastore", bcode.ErrVersionUpdateFailed)
	}

	var (
		updatedComponents   []string
		addedComponents     []string
		removedComponents   []string
		restartedComponents []string
		cleanupInfo         *model.VersionUpdateCleanupInfo
		resourceActionInfo  *model.VersionUpdateResourceActionInfo
		taskID              string
	)
	commit := func(lockCtx context.Context) error {
		return txStore.WithTransaction(lockCtx, func(tx datastore.DataStore) error {
			if err := EnsureAppWorkflowIdle(lockCtx, tx, workflow.AppID); err != nil {
				return fmt.Errorf("auto exec workflow: %w", err)
			}
			if err := validateWorkflowTaskEnqueue(lockCtx, tx, workflow, false); err != nil {
				return autoExecWorkflowValidationError(err)
			}
			pendingStatefulSetPVCDeletions, err := pendingVersionUpdateStatefulSetPVCDeletionsForRequest(
				lockCtx,
				tx,
				app.ID,
				req.Components,
				resourceActions.fullCleanup && resourceActions.deployAll,
			)
			if err != nil {
				return fmt.Errorf("auto exec pending StatefulSet PVC migration: %w", err)
			}

			if resourceActions.fullCleanup {
				insertBeforeStepIndex, err := versionUpdateFullCleanupInsertStepIndex(workflow)
				if err != nil {
					return fmt.Errorf("auto exec cleanup placement: %w", err)
				}
				cleanupOnly := !resourceActions.deployAll && len(req.Components) == 0
				cleanupInfo, err = buildVersionUpdateFullCleanupInfo(
					componentMap,
					req.Components,
					insertBeforeStepIndex,
					cleanupOnly,
					resourceActions.deployAll,
				)
				if err != nil {
					return fmt.Errorf("auto exec cleanup state: %w", err)
				}
				if resourceActions.deployAll {
					if err := mergePendingVersionUpdateStatefulSetPVCDeletions(req.Components, cleanupInfo, pendingStatefulSetPVCDeletions); err != nil {
						return fmt.Errorf("auto exec cleanup retry state: %w", err)
					}
				}
			} else {
				cleanupStepIndexes, cleanupAppendStepIndex, err := versionUpdateCleanupStepIndexes(workflow, req.Components, componentMap)
				if err != nil {
					return fmt.Errorf("auto exec cleanup placement: %w", err)
				}
				cleanupInfo, err = buildVersionUpdateCleanupInfo(req.Components, componentMap, cleanupStepIndexes, cleanupAppendStepIndex)
				if err != nil {
					return fmt.Errorf("auto exec cleanup state: %w", err)
				}
			}
			cleanupInfoJSON, err := marshalVersionUpdateCleanupInfo(cleanupInfo)
			if err != nil {
				return fmt.Errorf("auto exec cleanup state: %w", err)
			}
			restartedComponents = append([]string{}, resourceActions.restartComponents...)
			if len(restartedComponents) > 0 || len(readyComponents) > 0 {
				resourceActionInfo = &model.VersionUpdateResourceActionInfo{
					RestartOnly:       !resourceActions.fullCleanup && !resourceActions.deployAll && len(req.Components) == 0,
					RestartComponents: restartedComponents,
				}
				resourceActionInfo.ImageReadyTimeoutSeconds = req.ImageReadyTimeoutSeconds
				if resourceActionInfo.ImageReadyTimeoutSeconds <= 0 {
					resourceActionInfo.ImageReadyTimeoutSeconds = int64(config.DefaultVersionUpdateImageReadyTimeout)
				}
				if len(readyComponents) > 0 {
					resourceActionInfo.ImageReadyComponents = append([]string{}, readyComponents...)
				}
			}

			updatedComponents, addedComponents, removedComponents, err = c.applyVersionUpdateChangesInStore(lockCtx, tx, app, componentMap, req, newVersion, workflow.ID)
			if err != nil {
				return err
			}
			hasChanges := len(updatedComponents) > 0 || len(addedComponents) > 0 || len(removedComponents) > 0 || len(restartedComponents) > 0 || resourceActions.deployAll || cleanupInfo != nil
			if !hasChanges {
				return nil
			}

			workflowForTask, err := repository.WorkflowByID(lockCtx, tx, workflow.ID)
			if err != nil {
				return fmt.Errorf("reload workflow for auto exec: %w", err)
			}
			if err := validateVersionUpdateWorkflowJobTypes(workflowForTask); err != nil {
				return err
			}
			if resourceActions.deployAll {
				currentComponents, err := repository.FindComponentsByAppID(lockCtx, tx, app.ID)
				if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
					return fmt.Errorf("list components for deploy all: %w", err)
				}
				if err := validateVersionUpdateDeployAllWorkflow(workflowForTask, currentComponents); err != nil {
					return err
				}
				if err := validateWorkflowTaskEnqueue(lockCtx, tx, workflowForTask, false); err != nil {
					return autoExecWorkflowValidationError(err)
				}
			} else if cleanupInfo == nil {
				if err := validateWorkflowTaskEnqueue(lockCtx, tx, workflowForTask, false); err != nil {
					return autoExecWorkflowValidationError(err)
				}
			}
			if executionScope == config.VersionUpdateExecutionScopeChangedComponents {
				executionComponents := versionUpdateExecutionComponents(updatedComponents, addedComponents)
				if err := validateVersionUpdateExecutionScopeWorkflowCoverage(workflowForTask, executionComponents); err != nil {
					return err
				}
				if resourceActionInfo == nil {
					resourceActionInfo = &model.VersionUpdateResourceActionInfo{}
				}
				resourceActionInfo.ExecutionScope = executionScope
				resourceActionInfo.ExecutionComponents = executionComponents
			}
			if resourceActionInfo == nil {
				resourceActionInfo = &model.VersionUpdateResourceActionInfo{}
			}
			resourceActionInfoJSON, err := marshalVersionUpdateResourceActionInfo(resourceActionInfo)
			if err != nil {
				return fmt.Errorf("auto exec resource action state: %w", err)
			}

			task, err := createWorkflowQueueTaskWithResourceActionInfoAndCallback(lockCtx, tx, workflowForTask, executeAt, "", cleanupInfoJSON, resourceActionInfoJSON, taskCallback)
			if err != nil {
				return fmt.Errorf("auto exec workflow: %w", err)
			}
			if err := recordVersionUpdateCleanupJobs(lockCtx, tx, task, cleanupInfo); err != nil {
				return fmt.Errorf("auto exec cleanup state: %w", err)
			}
			taskID = task.TaskID
			return nil
		})
	}
	var err error
	if applicationMutationLockHeld(ctx, app.ID) {
		err = commit(ctx)
	} else {
		lockProvider, lockErr := c.appScheduleLocker()
		if lockErr != nil {
			return nil, nil, nil, nil, "", lockErr
		}
		err = schedulelock.WithAppScheduleLock(ctx, lockProvider, app.ID, "auto-exec-version-update", true, func(lockCtx context.Context) error {
			lockCtx = context.WithValue(lockCtx, applicationMutationLockContextKey{}, app.ID)
			return commit(lockCtx)
		})
	}
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	return updatedComponents, addedComponents, removedComponents, restartedComponents, taskID, nil
}

func autoExecWorkflowValidationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, bcode.ErrExecWorkflow) {
		return fmt.Errorf("auto exec workflow: invalid workflow: %w", err)
	}
	return fmt.Errorf("auto exec workflow: %w", err)
}

func (c *applicationsServiceImpl) applyVersionUpdateChangesInStore(
	ctx context.Context,
	store datastore.DataStore,
	app *model.Applications,
	componentMap map[string]*model.ApplicationComponent,
	req apisv1.UpdateVersionRequest,
	newVersion string,
	syncWorkflowID string,
) ([]string, []string, []string, error) {
	updatedComponents, addedComponents, removedComponents, err := applyVersionUpdateComponentChanges(ctx, componentMap, req.Components, versionUpdateComponentChangeHandlers{
		update: func(ctx context.Context, comp *model.ApplicationComponent, spec apisv1.ComponentUpdateSpec) (bool, error) {
			return c.updateComponentInStore(ctx, store, comp, spec)
		},
		add: func(ctx context.Context, spec apisv1.ComponentUpdateSpec) error {
			return c.addComponentInStore(ctx, store, app, spec)
		},
		remove: func(ctx context.Context, comp *model.ApplicationComponent, spec apisv1.ComponentUpdateSpec) error {
			if err := store.Delete(ctx, comp); err != nil {
				klog.Errorf("delete component %s failed: %v", spec.Name, err)
				return err
			}
			return nil
		},
	})
	if err != nil {
		return nil, nil, nil, err
	}

	app.Version = newVersion
	if req.Description != "" {
		app.Description = req.Description
	}
	if err := store.Put(ctx, app); err != nil {
		return nil, nil, nil, bcode.ErrVersionUpdateFailed
	}

	if len(addedComponents) > 0 || len(removedComponents) > 0 {
		if err := syncWorkflowStepsInStore(ctx, store, app.ID, syncWorkflowID, addedComponents, removedComponents); err != nil {
			return nil, nil, nil, fmt.Errorf("sync workflow steps: %w", err)
		}
	}

	return updatedComponents, addedComponents, removedComponents, nil
}

type versionUpdateComponentChangeHandlers struct {
	update func(context.Context, *model.ApplicationComponent, apisv1.ComponentUpdateSpec) (bool, error)
	add    func(context.Context, apisv1.ComponentUpdateSpec) error
	remove func(context.Context, *model.ApplicationComponent, apisv1.ComponentUpdateSpec) error
}

func applyVersionUpdateComponentChanges(
	ctx context.Context,
	componentMap map[string]*model.ApplicationComponent,
	specs []apisv1.ComponentUpdateSpec,
	handlers versionUpdateComponentChangeHandlers,
) ([]string, []string, []string, error) {
	updatedComponents := make([]string, 0)
	addedComponents := make([]string, 0)
	removedComponents := make([]string, 0)

	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return nil, nil, nil, err
		}
		compName := strings.ToLower(strings.TrimSpace(spec.Name))

		switch action {
		case config.ComponentActionUpdate:
			comp, exists := componentMap[compName]
			if !exists {
				return nil, nil, nil, fmt.Errorf("%w: component %s not found for update", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
			updated, err := handlers.update(ctx, comp, spec)
			if err != nil {
				return nil, nil, nil, versionUpdateAddOrUpdateError("update", spec.Name, err)
			}
			if updated {
				updatedComponents = append(updatedComponents, spec.Name)
			}

		case config.ComponentActionAdd:
			if _, exists := componentMap[compName]; exists {
				return nil, nil, nil, fmt.Errorf("%w: component %s already exists for add", bcode.ErrComponentAlreadyExists, strings.TrimSpace(spec.Name))
			}
			if err := handlers.add(ctx, spec); err != nil {
				return nil, nil, nil, versionUpdateAddOrUpdateError("add", spec.Name, err)
			}
			addedComponents = append(addedComponents, spec.Name)

		case config.ComponentActionRemove:
			comp, exists := componentMap[compName]
			if !exists {
				return nil, nil, nil, fmt.Errorf("%w: component %s not found for remove", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
			if err := handlers.remove(ctx, comp, spec); err != nil {
				return nil, nil, nil, bcode.ErrVersionUpdateFailed
			}
			removedComponents = append(removedComponents, spec.Name)
			delete(componentMap, compName)
		}
	}

	return updatedComponents, addedComponents, removedComponents, nil
}

func versionUpdateAddOrUpdateError(action, componentName string, err error) error {
	klog.Errorf("%s component %s failed: %v", action, componentName, err)
	var businessErr *bcode.Bcode
	if errors.As(err, &businessErr) {
		return err
	}
	return bcode.ErrVersionUpdateFailed
}

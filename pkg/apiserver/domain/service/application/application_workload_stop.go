package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func (c *applicationsServiceImpl) StopApplicationDeployments(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.StopApplicationDeploymentsResponse, error) {
	var response *apisv1.StopApplicationDeploymentsResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "stop-application-deployments", func(lockCtx context.Context, _ *model.Applications) error {
		var stopErr error
		response, stopErr = c.stopApplicationDeploymentsLocked(lockCtx, appID, req)
		return stopErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) stopApplicationDeploymentsLocked(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.StopApplicationDeploymentsResponse, error) {
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	if c.KubeClient == nil {
		return nil, fmt.Errorf("kube client is nil")
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
	taskCallback, err := c.resolveOperationTaskCallback(ctx, req.Callback)
	if err != nil {
		return nil, err
	}
	defer c.invalidateApplicationComponentsCache(app.ID)
	startTime := time.Now().Unix()
	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(components, applicationResourceNameKey(app))

	reporter := newDeploymentScaleReporter()
	adopted := isAdoptedApplication(app)
	var task *model.WorkflowQueue
	var taskID string
	recordTask := func(taskCtx context.Context, taskApp *model.Applications) error {
		var taskErr error
		taskID, task, taskErr = c.attachOperationTaskWithCallback(
			taskCtx,
			taskApp,
			config.WorkflowTaskTypeStop,
			operationTaskNameStop,
			startTime,
			time.Now().Unix(),
			buildStopJobRecords(reporter),
			reporter.failedResources,
			taskCallback,
		)
		return taskErr
	}
	if adopted {
		lockedApp, err := c.withAdoptedLifecycleLock(
			ctx,
			app.ID,
			adoptedLifecycleStop,
			func(
				lockCtx context.Context,
				lockedApp *model.Applications,
				lockedComponents []*model.ApplicationComponent,
			) error {
				targets, skippedTargets, err := c.preflightAdoptedLifecycle(
					lockCtx,
					lockedComponents,
					adoptedLifecycleStop,
				)
				if err != nil {
					return err
				}
				for _, target := range skippedTargets {
					reporter.record(target.kind, target.namespace, target.name, true, nil)
				}
				if err := c.persistAdoptedResumeReplicas(lockCtx, targets); err != nil {
					return err
				}
				for _, target := range targets {
					if err := c.updateAdoptedLifecycleTarget(lockCtx, target, adoptedLifecycleStop, ""); err != nil {
						reporter.record(target.kind, target.namespace, target.name, false, err)
						continue
					}
					if err := c.markComponentStopped(lockCtx, target.component); err != nil {
						reporter.record(target.kind, target.namespace, target.name, false, err)
						continue
					}
					reporter.record(target.kind, target.namespace, target.name, false, nil)
				}
				if err := recordTask(lockCtx, lockedApp); err != nil {
					return fmt.Errorf("record stop task: %w", err)
				}
				return nil
			},
		)
		if err != nil {
			return nil, err
		}
		app = lockedApp
	} else {
		patch, err := buildDeploymentReplicasPatch(0)
		if err != nil {
			return nil, err
		}
		for _, component := range components {
			if component == nil || component.ComponentType != config.ServerJob {
				continue
			}
			deployNS, deployName := resolveDeploymentTarget(component)
			if strategy, shared := SharedLifecycleStrategyForComponent(component); shared {
				klog.Infof("stop: skip shared component %s/%s (strategy=%s)", component.Namespace, component.Name, strategy)
				reporter.record("Deployment", deployNS, deployName, true, nil)
				continue
			}
			skipped, err := c.scaleDeployment(ctx, deployNS, deployName, patch)
			if err != nil || skipped {
				reporter.record("Deployment", deployNS, deployName, skipped, err)
				continue
			}
			if err := c.markComponentStopped(ctx, component); err != nil {
				reporter.record("Deployment", deployNS, deployName, false, err)
				continue
			}
			reporter.record("Deployment", deployNS, deployName, false, nil)
		}
	}

	if !adopted {
		if taskErr := recordTask(ctx, app); taskErr != nil && taskCallback != nil {
			return nil, fmt.Errorf("record stop callback task: %w", taskErr)
		}
	}
	stoppedAt := time.Now().UTC().Format(time.RFC3339)
	resp := &apisv1.StopApplicationDeploymentsResponse{
		AppID:            app.ID,
		StoppedAt:        stoppedAt,
		StoppedResources: reporter.successfulResources,
		SkippedResources: reporter.skippedResources,
		TaskID:           taskID,
	}
	c.triggerOperationTaskCallback(ctx, task, taskCallback, reporter.failedResources)
	if len(reporter.failedResources) > 0 {
		resp.FailedResources = reporter.failedResources
		return resp, reporter.err()
	}
	return resp, nil
}

func (c *applicationsServiceImpl) StartApplicationDeployments(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.StartApplicationDeploymentsResponse, error) {
	var response *apisv1.StartApplicationDeploymentsResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "start-application-deployments", func(lockCtx context.Context, _ *model.Applications) error {
		var startErr error
		response, startErr = c.startApplicationDeploymentsLocked(lockCtx, appID, req)
		return startErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) startApplicationDeploymentsLocked(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.StartApplicationDeploymentsResponse, error) {
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	if c.KubeClient == nil {
		return nil, fmt.Errorf("kube client is nil")
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
	taskCallback, err := c.resolveOperationTaskCallback(ctx, req.Callback)
	if err != nil {
		return nil, err
	}
	defer c.invalidateApplicationComponentsCache(app.ID)
	startTime := time.Now().Unix()
	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(components, applicationResourceNameKey(app))

	reporter := newDeploymentScaleReporter()
	adopted := isAdoptedApplication(app)
	var task *model.WorkflowQueue
	var taskID string
	recordTask := func(taskCtx context.Context, taskApp *model.Applications) error {
		var taskErr error
		taskID, task, taskErr = c.attachOperationTaskWithCallback(
			taskCtx,
			taskApp,
			config.WorkflowTaskTypeStart,
			operationTaskNameStart,
			startTime,
			time.Now().Unix(),
			buildStartJobRecords(reporter),
			reporter.failedResources,
			taskCallback,
		)
		return taskErr
	}
	if adopted {
		lockedApp, err := c.withAdoptedLifecycleLock(
			ctx,
			app.ID,
			adoptedLifecycleStart,
			func(
				lockCtx context.Context,
				lockedApp *model.Applications,
				lockedComponents []*model.ApplicationComponent,
			) error {
				targets, skippedTargets, err := c.preflightAdoptedLifecycle(
					lockCtx,
					lockedComponents,
					adoptedLifecycleStart,
				)
				if err != nil {
					return err
				}
				for _, target := range skippedTargets {
					reporter.record(target.kind, target.namespace, target.name, true, nil)
				}
				for _, target := range targets {
					if err := c.updateAdoptedLifecycleTarget(lockCtx, target, adoptedLifecycleStart, ""); err != nil {
						reporter.record(target.kind, target.namespace, target.name, false, err)
						continue
					}
					if err := c.markComponentStarted(lockCtx, target.component); err != nil {
						reporter.record(target.kind, target.namespace, target.name, false, err)
						continue
					}
					reporter.record(target.kind, target.namespace, target.name, false, nil)
				}
				if err := recordTask(lockCtx, lockedApp); err != nil {
					return fmt.Errorf("record start task: %w", err)
				}
				return nil
			},
		)
		if err != nil {
			return nil, err
		}
		app = lockedApp
	} else {
		for _, component := range components {
			if component == nil || component.ComponentType != config.ServerJob {
				continue
			}
			deployNS, deployName := resolveDeploymentTarget(component)
			if strategy, shared := SharedLifecycleStrategyForComponent(component); shared {
				klog.Infof("start: skip shared component %s/%s (strategy=%s)", component.Namespace, component.Name, strategy)
				reporter.record("Deployment", deployNS, deployName, true, nil)
				continue
			}
			if component.Status != string(config.ComponentStatusStopped) {
				klog.Infof("start: skip non-stopped component %s/%s (status=%s)", component.Namespace, component.Name, component.Status)
				reporter.record("Deployment", deployNS, deployName, true, nil)
				continue
			}
			desiredReplicas := component.Replicas
			if desiredReplicas <= 0 {
				reporter.record("Deployment", deployNS, deployName, false, fmt.Errorf("stored replicas must be greater than 0"))
				continue
			}
			patch, err := buildDeploymentReplicasPatch(desiredReplicas)
			if err != nil {
				reporter.record("Deployment", deployNS, deployName, false, err)
				continue
			}
			skipped, err := c.scaleDeployment(ctx, deployNS, deployName, patch)
			if err != nil || skipped {
				reporter.record("Deployment", deployNS, deployName, skipped, err)
				continue
			}
			if err := c.markComponentStarted(ctx, component); err != nil {
				reporter.record("Deployment", deployNS, deployName, false, err)
				continue
			}
			reporter.record("Deployment", deployNS, deployName, false, nil)
		}
	}

	if !adopted {
		if taskErr := recordTask(ctx, app); taskErr != nil && taskCallback != nil {
			return nil, fmt.Errorf("record start callback task: %w", taskErr)
		}
	}
	startedAt := time.Now().UTC().Format(time.RFC3339)
	resp := &apisv1.StartApplicationDeploymentsResponse{
		AppID:            app.ID,
		StartedAt:        startedAt,
		StartedResources: reporter.successfulResources,
		SkippedResources: reporter.skippedResources,
		TaskID:           taskID,
	}
	c.triggerOperationTaskCallback(ctx, task, taskCallback, reporter.failedResources)
	if len(reporter.failedResources) > 0 {
		resp.FailedResources = reporter.failedResources
		return resp, reporter.err()
	}
	return resp, nil
}

func buildDeploymentReplicasPatch(replicas int32) ([]byte, error) {
	patch := map[string]interface{}{
		"spec": map[string]int32{
			"replicas": replicas,
		},
	}
	return json.Marshal(patch)
}

func resolveDeploymentTarget(component *model.ApplicationComponent) (string, string) {
	if component == nil {
		return "", ""
	}
	props := job.ParseProperties(component.Properties)
	componentCopy := *component
	if componentCopy.Namespace == "" {
		componentCopy.Namespace = config.DefaultNamespace
	}
	deployNS := componentCopy.Namespace
	deployName := naming.WebServiceName(component.Name, component.ResourceNameKey())
	if result := job.GenerateWebService(&componentCopy, &props); result != nil {
		if deploy, ok := result.Service.(*appsv1.Deployment); ok && deploy != nil {
			if deploy.Namespace != "" {
				deployNS = deploy.Namespace
			}
			if deploy.Name != "" {
				deployName = deploy.Name
			}
		}
	}
	return deployNS, deployName
}

func (c *applicationsServiceImpl) scaleDeployment(ctx context.Context, namespace, name string, patch []byte) (bool, error) {
	if name == "" {
		return true, nil
	}
	ns := pickNamespace(namespace, config.DefaultNamespace)
	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationWorkloadScaleTimeout)
	defer cancel()
	_, err := c.KubeClient.AppsV1().Deployments(ns).Patch(opCtx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (c *applicationsServiceImpl) markComponentStopped(ctx context.Context, component *model.ApplicationComponent) error {
	if component == nil {
		return nil
	}
	status := config.ComponentStatusStopped
	readyReplicas := int32(0)
	lastAbnormal := ""
	component.ReadyReplicas = readyReplicas
	component.Status = string(status)
	component.LastAbnormal = lastAbnormal
	return repository.UpdateComponentRuntimeFields(ctx, c.Store, component, map[string]interface{}{
		"status":         string(status),
		"ready_replicas": readyReplicas,
		"last_abnormal":  lastAbnormal,
	})
}

func (c *applicationsServiceImpl) markComponentStarted(ctx context.Context, component *model.ApplicationComponent) error {
	if component == nil {
		return nil
	}
	status := config.ComponentStatusStarting
	readyReplicas := int32(0)
	lastAbnormal := ""
	component.ReadyReplicas = readyReplicas
	component.Status = string(status)
	component.LastAbnormal = lastAbnormal
	return repository.UpdateComponentRuntimeFields(ctx, c.Store, component, map[string]interface{}{
		"status":         string(status),
		"ready_replicas": readyReplicas,
		"last_abnormal":  lastAbnormal,
	})
}

type deploymentScaleReporter struct {
	successfulResources []string
	skippedResources    []string
	failedResources     []string
	errs                []error
}

func newDeploymentScaleReporter() *deploymentScaleReporter {
	return &deploymentScaleReporter{
		successfulResources: []string{},
		skippedResources:    []string{},
		failedResources:     []string{},
	}
}

func (r *deploymentScaleReporter) record(kind, namespace, name string, skipped bool, err error) {
	if name == "" {
		return
	}
	target := formatResource(kind, namespace, name)
	if err != nil {
		r.failedResources = append(r.failedResources, fmt.Sprintf("%s (%v)", target, err))
		r.errs = append(r.errs, err)
		return
	}
	if skipped {
		r.skippedResources = append(r.skippedResources, target)
		return
	}
	r.successfulResources = append(r.successfulResources, target)
}

func (r *deploymentScaleReporter) err() error {
	if len(r.errs) == 0 {
		return nil
	}
	if len(r.errs) == 1 {
		return r.errs[0]
	}
	return fmt.Errorf("%d deployment scale operations failed; first error: %w", len(r.errs), r.errs[0])
}

package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func (c *applicationsServiceImpl) StopApplicationDeployments(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.StopApplicationDeploymentsResponse, error) {
	var response *apisv1.StopApplicationDeploymentsResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "stop-application-deployments", func(lockCtx context.Context, app *model.Applications) error {
		if c.KubeClient == nil {
			return fmt.Errorf("kube client is nil")
		}
		app, err := c.applicationForRequestedID(lockCtx, appID, app)
		if err != nil {
			return err
		}
		var stopErr error
		response, stopErr = c.stopApplicationDeploymentsLocked(lockCtx, app, req)
		return stopErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) stopApplicationDeploymentsLocked(ctx context.Context, app *model.Applications, req apisv1.ApplicationLifecycleRequest) (*apisv1.StopApplicationDeploymentsResponse, error) {
	taskCallback, err := c.resolveOperationTaskCallback(ctx, req.Callback)
	if err != nil {
		return nil, err
	}
	defer c.invalidateApplicationComponentsCache(ctx, app.ID)
	startTime := time.Now().Unix()
	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(components, applicationResourceNameKey(app))

	reporter := newOperationReporter("deployment scale")
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
			reporter.jobRecords("stopped"),
			reporter.failed,
			taskCallback,
		)
		return taskErr
	}
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

	if taskErr := recordTask(ctx, app); taskErr != nil {
		return nil, fmt.Errorf("Kubernetes stop may already have taken effect, but operation record commit could not be confirmed; inspect operation records and Kubernetes resources before retrying: %w", taskErr)
	}
	stoppedAt := time.Now().UTC().Format(time.RFC3339)
	resp := &apisv1.StopApplicationDeploymentsResponse{
		AppID:            app.ID,
		StoppedAt:        stoppedAt,
		StoppedResources: reporter.done,
		SkippedResources: reporter.skipped,
		TaskID:           taskID,
	}
	c.triggerOperationTaskCallback(ctx, task, taskCallback, reporter.failed)
	if len(reporter.failed) > 0 {
		resp.FailedResources = formatFailedResources(reporter.failed)
		return resp, reporter.err()
	}
	return resp, nil
}

func (c *applicationsServiceImpl) StartApplicationDeployments(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.StartApplicationDeploymentsResponse, error) {
	var response *apisv1.StartApplicationDeploymentsResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "start-application-deployments", func(lockCtx context.Context, app *model.Applications) error {
		if c.KubeClient == nil {
			return fmt.Errorf("kube client is nil")
		}
		app, err := c.applicationForRequestedID(lockCtx, appID, app)
		if err != nil {
			return err
		}
		var startErr error
		response, startErr = c.startApplicationDeploymentsLocked(lockCtx, app, req)
		return startErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) startApplicationDeploymentsLocked(ctx context.Context, app *model.Applications, req apisv1.ApplicationLifecycleRequest) (*apisv1.StartApplicationDeploymentsResponse, error) {
	taskCallback, err := c.resolveOperationTaskCallback(ctx, req.Callback)
	if err != nil {
		return nil, err
	}
	defer c.invalidateApplicationComponentsCache(ctx, app.ID)
	startTime := time.Now().Unix()
	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(components, applicationResourceNameKey(app))

	reporter := newOperationReporter("deployment scale")
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
			reporter.jobRecords("started"),
			reporter.failed,
			taskCallback,
		)
		return taskErr
	}
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

	if taskErr := recordTask(ctx, app); taskErr != nil {
		return nil, fmt.Errorf("Kubernetes start may already have taken effect, but operation record commit could not be confirmed; inspect operation records and Kubernetes resources before retrying: %w", taskErr)
	}
	startedAt := time.Now().UTC().Format(time.RFC3339)
	resp := &apisv1.StartApplicationDeploymentsResponse{
		AppID:            app.ID,
		StartedAt:        startedAt,
		StartedResources: reporter.done,
		SkippedResources: reporter.skipped,
		TaskID:           taskID,
	}
	c.triggerOperationTaskCallback(ctx, task, taskCallback, reporter.failed)
	if len(reporter.failed) > 0 {
		resp.FailedResources = formatFailedResources(reporter.failed)
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
	return pickNamespace(component.Namespace, config.DefaultNamespace), naming.WebServiceName(component.Name, component.ResourceNameKey())
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

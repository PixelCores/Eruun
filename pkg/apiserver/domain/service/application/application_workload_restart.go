package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func (c *applicationsServiceImpl) RestartApplicationWorkloads(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.RestartApplicationWorkloadsResponse, error) {
	var response *apisv1.RestartApplicationWorkloadsResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "restart-application-workloads", func(lockCtx context.Context, app *model.Applications) error {
		if c.KubeClient == nil {
			return fmt.Errorf("kube client is nil")
		}
		app, err := c.applicationForRequestedID(lockCtx, appID, app)
		if err != nil {
			return err
		}
		var restartErr error
		response, restartErr = c.restartApplicationWorkloadsLocked(lockCtx, app, req)
		return restartErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) restartApplicationWorkloadsLocked(ctx context.Context, app *model.Applications, req apisv1.ApplicationLifecycleRequest) (*apisv1.RestartApplicationWorkloadsResponse, error) {
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

	restartedAt := formatWorkloadRestartAt(time.Now())
	reporter := newOperationReporter("restart")
	restartedComponents := make([]string, 0, len(components))
	var markErr error
	var task *model.WorkflowQueue
	var taskID string
	recordTask := func(taskCtx context.Context, taskApp *model.Applications) error {
		var taskErr error
		taskID, task, taskErr = c.attachOperationTaskWithCallback(
			taskCtx,
			taskApp,
			config.WorkflowTaskTypeRestart,
			operationTaskNameRestart,
			startTime,
			time.Now().Unix(),
			reporter.jobRecords("restarted"),
			reporter.failed,
			taskCallback,
		)
		return taskErr
	}
	patch, err := buildRestartPatch(restartedAt)
	if err != nil {
		return nil, err
	}
	restartedComponents = c.restartNativeApplicationComponents(ctx, components, patch, reporter)

	if len(restartedComponents) > 0 {
		if err := c.markComponentsStatus(ctx, app.ID, restartedComponents, config.ComponentStatusRestarting); err != nil {
			markErr = fmt.Errorf("mark components restarting: %w", err)
			statusTarget := formatResource("ComponentStatus", app.Namespace, app.ID)
			reporter.failed = append(reporter.failed, operationJobRecord{name: statusTarget, errMsg: fmt.Sprint(markErr)})
		}
	}
	if taskErr := recordTask(ctx, app); taskErr != nil {
		return nil, fmt.Errorf("Kubernetes restart may already have taken effect, but operation record commit could not be confirmed; inspect operation records and Kubernetes resources before retrying: %w", taskErr)
	}

	resp := &apisv1.RestartApplicationWorkloadsResponse{
		AppID:              app.ID,
		RestartedAt:        restartedAt,
		RestartedResources: reporter.done,
		SkippedResources:   reporter.skipped,
		TaskID:             taskID,
	}
	c.triggerOperationTaskCallback(ctx, task, taskCallback, reporter.failed)
	if markErr != nil {
		klog.ErrorS(markErr, "mark components restarting failed after workloads restarted", "appID", app.ID, "taskID", resp.TaskID)
	}
	if len(reporter.failed) > 0 {
		resp.FailedResources = formatFailedResources(reporter.failed)
		if markErr != nil {
			return resp, errors.Join(reporter.err(), markErr)
		}
		return resp, reporter.err()
	}
	if markErr != nil {
		return resp, markErr
	}
	return resp, nil
}

func (c *applicationsServiceImpl) restartNativeApplicationComponents(ctx context.Context, components []*model.ApplicationComponent, patch []byte, reporter *operationReporter) []string {
	restartedComponents := make([]string, 0, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		if component.Status == string(config.ComponentStatusStopped) {
			recordRestartSkippedResource(component, reporter)
			klog.Infof("restart: skip stopped component %s/%s", component.Namespace, component.Name)
			continue
		}
		if strategy, shared := SharedLifecycleStrategyForComponent(component); shared {
			recordRestartSkippedResource(component, reporter)
			klog.Infof("restart: skip shared component %s/%s (strategy=%s)", component.Namespace, component.Name, strategy)
			continue
		}
		switch component.ComponentType {
		case config.ServerJob:
			componentCopy := *component
			deployNS, deployName := resolveDeploymentTarget(&componentCopy)
			skipped, err := c.restartDeployment(ctx, deployNS, deployName, patch)
			reporter.record("Deployment", deployNS, deployName, skipped, err)
			if err == nil && !skipped {
				restartedComponents = append(restartedComponents, component.Name)
			}
		case config.StoreJob:
			componentCopy := *component
			statefulNS := componentCopy.Namespace
			statefulName := naming.StoreServerName(component.Name, component.ResourceNameKey())
			skipped, err := c.restartStatefulSet(ctx, statefulNS, statefulName, patch)
			reporter.record("StatefulSet", statefulNS, statefulName, skipped, err)
			if err == nil && !skipped {
				restartedComponents = append(restartedComponents, component.Name)
			}
		}
	}
	return restartedComponents
}

func formatWorkloadRestartAt(now time.Time) string {
	return now.UTC().Format(time.RFC3339Nano)
}

func recordRestartSkippedResource(component *model.ApplicationComponent, reporter *operationReporter) {
	if component == nil || reporter == nil {
		return
	}
	if component.ComponentType != config.ServerJob && component.ComponentType != config.StoreJob {
		return
	}
	componentCopy := *component
	switch component.ComponentType {
	case config.ServerJob:
		deployNS, deployName := resolveDeploymentTarget(&componentCopy)
		reporter.record("Deployment", deployNS, deployName, true, nil)
	case config.StoreJob:
		statefulNS := componentCopy.Namespace
		statefulName := naming.StoreServerName(component.Name, component.ResourceNameKey())
		reporter.record("StatefulSet", statefulNS, statefulName, true, nil)
	}
}

func buildRestartPatch(restartedAt string) ([]byte, error) {
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]string{
						config.AnnotationWorkloadRestartAt: restartedAt,
					},
				},
			},
		},
	}
	return json.Marshal(patch)
}

func (c *applicationsServiceImpl) restartDeployment(ctx context.Context, namespace, name string, patch []byte) (bool, error) {
	if name == "" {
		return true, nil
	}
	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationWorkloadRestartTimeout)
	defer cancel()
	_, err := c.KubeClient.AppsV1().Deployments(namespace).Patch(opCtx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (c *applicationsServiceImpl) restartStatefulSet(ctx context.Context, namespace, name string, patch []byte) (bool, error) {
	if name == "" {
		return true, nil
	}
	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationWorkloadRestartTimeout)
	defer cancel()
	_, err := c.KubeClient.AppsV1().StatefulSets(namespace).Patch(opCtx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

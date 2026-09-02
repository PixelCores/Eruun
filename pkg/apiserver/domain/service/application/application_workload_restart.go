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
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func (c *applicationsServiceImpl) RestartApplicationWorkloads(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.RestartApplicationWorkloadsResponse, error) {
	var response *apisv1.RestartApplicationWorkloadsResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "restart-application-workloads", func(lockCtx context.Context, _ *model.Applications) error {
		var restartErr error
		response, restartErr = c.restartApplicationWorkloadsLocked(lockCtx, appID, req)
		return restartErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) restartApplicationWorkloadsLocked(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.RestartApplicationWorkloadsResponse, error) {
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

	restartedAt := formatWorkloadRestartAt(time.Now())
	reporter := newRestartReporter()
	restartedComponents := make([]string, 0, len(components))
	adopted := isAdoptedApplication(app)
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
			buildRestartJobRecords(reporter),
			reporter.failedResources,
			taskCallback,
		)
		return taskErr
	}
	if adopted {
		lockedApp, err := c.withAdoptedLifecycleLock(
			ctx,
			app.ID,
			adoptedLifecycleRestart,
			func(
				lockCtx context.Context,
				lockedApp *model.Applications,
				lockedComponents []*model.ApplicationComponent,
			) error {
				targets, skippedTargets, err := c.preflightAdoptedLifecycle(
					lockCtx,
					lockedComponents,
					adoptedLifecycleRestart,
				)
				if err != nil {
					return err
				}
				for _, target := range skippedTargets {
					reporter.record(target.kind, target.namespace, target.name, true, nil)
				}
				for _, target := range targets {
					err := c.updateAdoptedLifecycleTarget(lockCtx, target, adoptedLifecycleRestart, restartedAt)
					reporter.record(target.kind, target.namespace, target.name, false, err)
					if err == nil {
						restartedComponents = append(restartedComponents, target.component.Name)
					}
				}
				if len(restartedComponents) > 0 {
					if err := c.markComponentsRestarting(lockCtx, lockedApp.ID, restartedComponents); err != nil {
						markErr = fmt.Errorf("mark components restarting: %w", err)
						statusTarget := formatResource(
							"ComponentStatus",
							pickNamespace(lockedApp.Namespace, config.DefaultNamespace),
							lockedApp.ID,
						)
						reporter.failedResources = append(
							reporter.failedResources,
							fmt.Sprintf("%s (%v)", statusTarget, markErr),
						)
					}
				}
				if err := recordTask(lockCtx, lockedApp); err != nil {
					return fmt.Errorf("record restart task: %w", err)
				}
				return nil
			},
		)
		if err != nil {
			return nil, err
		}
		app = lockedApp
	} else {
		patch, err := buildRestartPatch(restartedAt)
		if err != nil {
			return nil, err
		}
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
				if componentCopy.Namespace == "" {
					componentCopy.Namespace = config.DefaultNamespace
				}
				deployNS, deployName := resolveDeploymentTarget(&componentCopy)
				skipped, err := c.restartDeployment(ctx, deployNS, deployName, patch)
				reporter.record("Deployment", deployNS, deployName, skipped, err)
				if err == nil && !skipped {
					restartedComponents = append(restartedComponents, component.Name)
				}
			case config.StoreJob:
				componentCopy := *component
				if componentCopy.Namespace == "" {
					componentCopy.Namespace = config.DefaultNamespace
				}
				statefulNS := componentCopy.Namespace
				statefulName := naming.StoreServerName(component.Name, component.ResourceNameKey())
				if result := job.GenerateStoreService(&componentCopy); result != nil {
					if sts, ok := result.Service.(*appsv1.StatefulSet); ok && sts != nil {
						if sts.Namespace != "" {
							statefulNS = sts.Namespace
						}
						if sts.Name != "" {
							statefulName = sts.Name
						}
					}
				}
				skipped, err := c.restartStatefulSet(ctx, statefulNS, statefulName, patch)
				reporter.record("StatefulSet", statefulNS, statefulName, skipped, err)
				if err == nil && !skipped {
					restartedComponents = append(restartedComponents, component.Name)
				}
			}
		}
	}

	if !adopted && len(restartedComponents) > 0 {
		if err := c.markComponentsRestarting(ctx, app.ID, restartedComponents); err != nil {
			markErr = fmt.Errorf("mark components restarting: %w", err)
			statusTarget := formatResource("ComponentStatus", pickNamespace(app.Namespace, config.DefaultNamespace), app.ID)
			reporter.failedResources = append(reporter.failedResources, fmt.Sprintf("%s (%v)", statusTarget, markErr))
		}
	}
	if !adopted {
		if taskErr := recordTask(ctx, app); taskErr != nil && taskCallback != nil {
			return nil, fmt.Errorf("record restart callback task: %w", taskErr)
		}
	}

	resp := &apisv1.RestartApplicationWorkloadsResponse{
		AppID:              app.ID,
		RestartedAt:        restartedAt,
		RestartedResources: reporter.restartedResources,
		SkippedResources:   reporter.skippedResources,
		TaskID:             taskID,
	}
	c.triggerOperationTaskCallback(ctx, task, taskCallback, reporter.failedResources)
	if markErr != nil {
		klog.ErrorS(markErr, "mark components restarting failed after workloads restarted", "appID", app.ID, "taskID", resp.TaskID)
	}
	if len(reporter.failedResources) > 0 {
		resp.FailedResources = reporter.failedResources
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

func formatWorkloadRestartAt(now time.Time) string {
	return now.UTC().Format(time.RFC3339Nano)
}

func recordRestartSkippedResource(component *model.ApplicationComponent, reporter *restartReporter) {
	if component == nil || reporter == nil {
		return
	}
	if component.ComponentType != config.ServerJob && component.ComponentType != config.StoreJob {
		return
	}
	componentCopy := *component
	if componentCopy.Namespace == "" {
		componentCopy.Namespace = config.DefaultNamespace
	}
	switch component.ComponentType {
	case config.ServerJob:
		deployNS, deployName := resolveDeploymentTarget(&componentCopy)
		reporter.record("Deployment", deployNS, deployName, true, nil)
	case config.StoreJob:
		statefulNS := componentCopy.Namespace
		statefulName := naming.StoreServerName(component.Name, component.ResourceNameKey())
		if result := job.GenerateStoreService(&componentCopy); result != nil {
			if sts, ok := result.Service.(*appsv1.StatefulSet); ok && sts != nil {
				if sts.Namespace != "" {
					statefulNS = sts.Namespace
				}
				if sts.Name != "" {
					statefulName = sts.Name
				}
			}
		}
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
	ns := pickNamespace(namespace, config.DefaultNamespace)
	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationWorkloadRestartTimeout)
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

func (c *applicationsServiceImpl) restartStatefulSet(ctx context.Context, namespace, name string, patch []byte) (bool, error) {
	if name == "" {
		return true, nil
	}
	ns := pickNamespace(namespace, config.DefaultNamespace)
	opCtx, cancel := context.WithTimeout(ctx, config.DefaultApplicationWorkloadRestartTimeout)
	defer cancel()
	_, err := c.KubeClient.AppsV1().StatefulSets(ns).Patch(opCtx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

type restartReporter struct {
	restartedResources []string
	skippedResources   []string
	failedResources    []string
	errs               []error
}

func newRestartReporter() *restartReporter {
	return &restartReporter{
		restartedResources: []string{},
		skippedResources:   []string{},
		failedResources:    []string{},
	}
}

func (r *restartReporter) record(kind, namespace, name string, skipped bool, err error) {
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
	r.restartedResources = append(r.restartedResources, target)
}

func (r *restartReporter) err() error {
	if len(r.errs) == 0 {
		return nil
	}
	if len(r.errs) == 1 {
		return r.errs[0]
	}
	return fmt.Errorf("%d restart operations failed; first error: %w", len(r.errs), r.errs[0])
}

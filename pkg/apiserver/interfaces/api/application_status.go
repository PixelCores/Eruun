package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

type applicationRuntimeComponentReader interface {
	ListApplicationRuntimeComponents(context.Context, string) ([]*model.ApplicationComponent, error)
}

func (app *applications) listApplicationComponents(c *gin.Context) {
	handlePathResult(
		c,
		appIDPathParam,
		func(ctx context.Context, appID string) (apis.ListApplicationComponentsResponse, error) {
			components, err := app.ApplicationService.ListApplicationComponents(ctx, appID)
			if err != nil {
				return apis.ListApplicationComponentsResponse{}, err
			}
			resp, err := assembler.ConvertComponentModelsToDTO(components)
			if err != nil {
				klog.ErrorS(err, "convert components dto failed", "appID", appID)
				return apis.ListApplicationComponentsResponse{}, err
			}
			return apis.ListApplicationComponentsResponse{Components: resp}, nil
		},
	)
}

func (app *applications) listBatchApplicationComponentStatus(c *gin.Context) {
	req, ok := bindRequest[apis.BatchApplicationComponentStatusRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	if len(req.AppIDs) == 0 {
		bcode.ReturnError(c, bcode.ErrApplicationConfig)
		return
	}

	ctx := c.Request.Context()
	resp := apis.BatchApplicationComponentStatusResponse{
		Results: make([]apis.BatchApplicationComponentStatusResult, 0, len(req.AppIDs)),
	}
	for _, appID := range req.AppIDs {
		result := apis.BatchApplicationComponentStatusResult{
			AppID: strings.TrimSpace(appID),
		}
		if result.AppID == "" {
			result.Error = "appId is required"
			resp.Results = append(resp.Results, result)
			continue
		}

		components, err := app.RuntimeComponentReader.ListApplicationRuntimeComponents(ctx, result.AppID)
		if err != nil {
			if errors.Is(err, bcode.ErrApplicationNotExist) {
				result.Error = bcode.ErrApplicationNotExist.Message
				resp.Results = append(resp.Results, result)
				continue
			}
			result.Error = batchLookupErrorMessage(err)
			resp.Results = append(resp.Results, result)
			continue
		}
		status, err := app.applicationAggregateStatus(ctx, result.AppID, components)
		if err != nil {
			result.Error = batchLookupErrorMessage(err)
			resp.Results = append(resp.Results, result)
			continue
		}
		result.Status = status
		resp.Results = append(resp.Results, result)
	}
	bcode.ReturnSuccess(c, resp)
}

func (app *applications) getApplicationStatus(c *gin.Context) {
	handlePathResult(c, appIDPathParam, app.applicationStatus)
}

func (app *applications) applicationStatus(ctx context.Context, appID string) (apis.ApplicationStatusResponse, error) {
	components, err := app.RuntimeComponentReader.ListApplicationRuntimeComponents(ctx, appID)
	if err != nil {
		return apis.ApplicationStatusResponse{}, err
	}
	status, err := app.applicationAggregateStatus(ctx, appID, components)
	if err != nil {
		return apis.ApplicationStatusResponse{}, err
	}
	return apis.ApplicationStatusResponse{
		AppID:  appID,
		Status: status,
	}, nil
}

func (app *applications) getApplicationComponentStatus(c *gin.Context) {
	handlePathResult(c, appIDPathParam, app.applicationComponentStatus)
}

func (app *applications) applicationComponentStatus(ctx context.Context, appID string) (apis.ApplicationComponentStatusResponse, error) {
	components, err := app.RuntimeComponentReader.ListApplicationRuntimeComponents(ctx, appID)
	if err != nil {
		return apis.ApplicationComponentStatusResponse{}, err
	}
	resp := apis.ApplicationComponentStatusResponse{
		AppID:      appID,
		Components: make([]apis.ApplicationComponentStatus, 0, len(components)),
	}
	for _, comp := range components {
		if comp == nil {
			continue
		}
		status := strings.TrimSpace(comp.Status)
		if status == "" {
			status = string(config.ComponentStatusNotDeploy)
		}
		resp.Components = append(resp.Components, apis.ApplicationComponentStatus{
			Name:          comp.Name,
			Namespace:     comp.Namespace,
			Type:          comp.ComponentType,
			Status:        status,
			Replicas:      comp.Replicas,
			ReadyReplicas: comp.ReadyReplicas,
			LastAbnormal:  comp.LastAbnormal,
		})
	}
	if scope, ok := access.FromContext(ctx); ok && scope.Role == "viewer" {
		for i := range resp.Components {
			resp.Components[i].LastAbnormal = ""
		}
	}
	return resp, nil
}

func normalizeComponentStatusValue(status string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case strings.ToLower(string(config.ComponentStatusRunning)):
		return string(config.ComponentStatusRunning), true
	case strings.ToLower(string(config.ComponentStatusPending)):
		return string(config.ComponentStatusPending), true
	case strings.ToLower(string(config.ComponentStatusFailed)):
		return string(config.ComponentStatusFailed), true
	case strings.ToLower(string(config.ComponentStatusUnknown)):
		return string(config.ComponentStatusUnknown), true
	case strings.ToLower(string(config.ComponentStatusNotDeploy)), "not_deploy", "notdeploy":
		return string(config.ComponentStatusNotDeploy), true
	case strings.ToLower(string(config.ComponentStatusCleaning)):
		return string(config.ComponentStatusCleaning), true
	case strings.ToLower(string(config.ComponentStatusDeploying)):
		return string(config.ComponentStatusDeploying), true
	case strings.ToLower(string(config.ComponentStatusUpdating)):
		return string(config.ComponentStatusUpdating), true
	case strings.ToLower(string(config.ComponentStatusRestarting)):
		return string(config.ComponentStatusRestarting), true
	case strings.ToLower(string(config.ComponentStatusStarting)):
		return string(config.ComponentStatusStarting), true
	case strings.ToLower(string(config.ComponentStatusStopped)):
		return string(config.ComponentStatusStopped), true
	default:
		return "", false
	}
}

func normalizeComponentRuntimeStatus(status string) string {
	value := strings.TrimSpace(status)
	if value == "" {
		return string(config.ComponentStatusNotDeploy)
	}
	normalized, ok := normalizeComponentStatusValue(value)
	if !ok {
		return string(config.ComponentStatusUnknown)
	}
	return normalized
}

func aggregateApplicationStatus(components []*model.ApplicationComponent) string {
	return aggregateApplicationStatusWithReferenceTime(components, time.Now())
}

func (app *applications) applicationAggregateStatus(ctx context.Context, appID string, components []*model.ApplicationComponent) (string, error) {
	status := aggregateApplicationStatus(components)
	if !canApplicationWorkflowTaskOverrideApplicationStatus(status) {
		return status, nil
	}
	hasTask, err := app.ApplicationService.HasImmediateActiveVersionUpdateTask(ctx, appID, time.Now().Unix())
	if err != nil {
		return "", err
	}
	if hasTask {
		return "updating", nil
	}
	return status, nil
}

func aggregateApplicationStatusWithReferenceTime(components []*model.ApplicationComponent, now time.Time) string {
	var all, managedServing, sharedServing applicationStatusFlags

	for _, comp := range components {
		if comp == nil {
			continue
		}
		status := effectiveApplicationComponentStatus(comp, now)
		all.add(status)
		if comp.ComponentType == config.ServerJob {
			if componentUsesManagedAvailability(comp) {
				managedServing.add(status)
			} else {
				sharedServing.add(status)
			}
		}
	}

	if all.counted == 0 {
		return "not_deploy"
	}
	if all.hasFailed {
		return "failed"
	}
	if all.hasDeploying {
		return "deploying"
	}
	if all.hasUpdating {
		return "updating"
	}
	if all.hasRestarting {
		return "restarting"
	}
	if all.hasStarting {
		return "starting"
	}
	if all.hasCleaning {
		return "cleaning"
	}

	availability := all
	if managedServing.counted > 0 {
		availability = managedServing
	} else if sharedServing.counted > 0 {
		availability = sharedServing
	}
	if availability.hasPending {
		return "pending"
	}
	if availability.hasRunning {
		return "running"
	}
	if availability.hasStopped {
		return "stopped"
	}
	if availability.hasNotDeploy {
		return "not_deploy"
	}
	if availability.hasUnknown {
		return "unknown"
	}
	return "unknown"
}

func componentUsesManagedAvailability(component *model.ApplicationComponent) bool {
	_, shared := applicationservice.SharedLifecycleStrategyForComponent(component)
	return !shared
}

func canApplicationWorkflowTaskOverrideApplicationStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running", "pending", "stopped", "not_deploy", "unknown":
		return true
	default:
		return false
	}
}

func effectiveApplicationComponentStatus(component *model.ApplicationComponent, now time.Time) string {
	status := normalizeComponentRuntimeStatus(component.Status)
	if status != string(config.ComponentStatusFailed) {
		return status
	}
	if !isTransientPodBackedFailedComponent(component, now) {
		return status
	}
	if component.Replicas > 0 && component.ReadyReplicas >= component.Replicas {
		return string(config.ComponentStatusRunning)
	}
	return string(config.ComponentStatusPending)
}

func isTransientPodBackedFailedComponent(component *model.ApplicationComponent, now time.Time) bool {
	if component == nil {
		return false
	}
	if !config.ComponentTypeUsesPods(component.ComponentType) {
		return false
	}
	if component.UpdateTime.IsZero() {
		return false
	}
	age := now.Sub(component.UpdateTime)
	return age >= 0 && age <= config.DefaultApplicationStatusTransientFailedWindow
}

type applicationStatusFlags struct {
	hasRunning    bool
	hasPending    bool
	hasFailed     bool
	hasUnknown    bool
	hasNotDeploy  bool
	hasCleaning   bool
	hasDeploying  bool
	hasUpdating   bool
	hasRestarting bool
	hasStarting   bool
	hasStopped    bool
	counted       int
}

func (f *applicationStatusFlags) add(status string) {
	f.counted++
	switch status {
	case string(config.ComponentStatusRunning):
		f.hasRunning = true
	case string(config.ComponentStatusPending):
		f.hasPending = true
	case string(config.ComponentStatusFailed):
		f.hasFailed = true
	case string(config.ComponentStatusUnknown):
		f.hasUnknown = true
	case string(config.ComponentStatusNotDeploy):
		f.hasNotDeploy = true
	case string(config.ComponentStatusCleaning):
		f.hasCleaning = true
	case string(config.ComponentStatusDeploying):
		f.hasDeploying = true
	case string(config.ComponentStatusUpdating):
		f.hasUpdating = true
	case string(config.ComponentStatusRestarting):
		f.hasRestarting = true
	case string(config.ComponentStatusStarting):
		f.hasStarting = true
	case string(config.ComponentStatusStopped):
		f.hasStopped = true
	}
}

func batchLookupErrorMessage(err error) string {
	var bc *bcode.Bcode
	if errors.As(err, &bc) && bc != nil {
		return bc.Message
	}
	return err.Error()
}

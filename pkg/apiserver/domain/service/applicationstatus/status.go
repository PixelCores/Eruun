// Package applicationstatus owns the runtime-status presentation rules shared
// by the HTTP and gRPC adapters.
package applicationstatus

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type RuntimeReader interface {
	ListApplicationRuntimeComponents(context.Context, string) ([]*model.ApplicationComponent, error)
}

type Service struct {
	Applications service.ApplicationsService
	Runtime      RuntimeReader
}

func (s Service) Aggregate(ctx context.Context, appID string, components []*model.ApplicationComponent) (string, error) {
	status := AggregateWithReferenceTime(components, time.Now())
	if !workflowMayOverride(status) {
		return status, nil
	}
	hasTask, err := s.Applications.HasImmediateActiveVersionUpdateTask(ctx, appID, time.Now().Unix())
	if err != nil {
		return "", err
	}
	if hasTask {
		return "updating", nil
	}
	return status, nil
}

func (s Service) GetStatus(ctx context.Context, appID string) (apis.ApplicationStatusResponse, error) {
	components, err := s.Runtime.ListApplicationRuntimeComponents(ctx, appID)
	if err != nil {
		return apis.ApplicationStatusResponse{}, err
	}
	status, err := s.Aggregate(ctx, appID, components)
	if err != nil {
		return apis.ApplicationStatusResponse{}, err
	}
	return apis.ApplicationStatusResponse{AppID: appID, Status: status}, nil
}

func (s Service) GetComponentStatus(ctx context.Context, appID string) (apis.ApplicationComponentStatusResponse, error) {
	components, err := s.Runtime.ListApplicationRuntimeComponents(ctx, appID)
	if err != nil {
		return apis.ApplicationComponentStatusResponse{}, err
	}
	resp := apis.ApplicationComponentStatusResponse{AppID: appID, Components: make([]apis.ApplicationComponentStatus, 0, len(components))}
	for _, comp := range components {
		if comp == nil {
			continue
		}
		status := strings.TrimSpace(comp.Status)
		if status == "" {
			status = string(config.ComponentStatusNotDeploy)
		}
		item := apis.ApplicationComponentStatus{
			Name: comp.Name, Namespace: comp.Namespace, Type: comp.ComponentType, Status: status,
			Replicas: comp.Replicas, ReadyReplicas: comp.ReadyReplicas, LastAbnormal: comp.LastAbnormal,
		}
		if scope, ok := access.FromContext(ctx); ok && scope.Role == "viewer" {
			item.LastAbnormal = ""
		}
		resp.Components = append(resp.Components, item)
	}
	return resp, nil
}

func (s Service) Batch(ctx context.Context, appIDs []string, errorMessage func(error) string) apis.BatchApplicationComponentStatusResponse {
	resp := apis.BatchApplicationComponentStatusResponse{Results: make([]apis.BatchApplicationComponentStatusResult, 0, len(appIDs))}
	for _, id := range appIDs {
		result := apis.BatchApplicationComponentStatusResult{AppID: strings.TrimSpace(id)}
		if result.AppID == "" {
			result.Error = "appId is required"
			resp.Results = append(resp.Results, result)
			continue
		}
		components, err := s.Runtime.ListApplicationRuntimeComponents(ctx, result.AppID)
		if err != nil {
			if errors.Is(err, bcode.ErrApplicationNotExist) {
				result.Error = bcode.ErrApplicationNotExist.Message
			} else {
				result.Error = errorMessage(err)
			}
			resp.Results = append(resp.Results, result)
			continue
		}
		result.Status, err = s.Aggregate(ctx, result.AppID, components)
		if err != nil {
			result.Error = errorMessage(err)
		}
		resp.Results = append(resp.Results, result)
	}
	return resp
}

func NormalizeComponentRuntimeStatus(status string) string {
	value := strings.TrimSpace(status)
	if value == "" {
		return string(config.ComponentStatusNotDeploy)
	}
	switch strings.ToLower(value) {
	case strings.ToLower(string(config.ComponentStatusRunning)):
		return string(config.ComponentStatusRunning)
	case strings.ToLower(string(config.ComponentStatusPending)):
		return string(config.ComponentStatusPending)
	case strings.ToLower(string(config.ComponentStatusFailed)):
		return string(config.ComponentStatusFailed)
	case strings.ToLower(string(config.ComponentStatusUnknown)):
		return string(config.ComponentStatusUnknown)
	case strings.ToLower(string(config.ComponentStatusNotDeploy)), "not_deploy", "notdeploy":
		return string(config.ComponentStatusNotDeploy)
	case strings.ToLower(string(config.ComponentStatusCleaning)):
		return string(config.ComponentStatusCleaning)
	case strings.ToLower(string(config.ComponentStatusDeploying)):
		return string(config.ComponentStatusDeploying)
	case strings.ToLower(string(config.ComponentStatusUpdating)):
		return string(config.ComponentStatusUpdating)
	case strings.ToLower(string(config.ComponentStatusRestarting)):
		return string(config.ComponentStatusRestarting)
	case strings.ToLower(string(config.ComponentStatusStarting)):
		return string(config.ComponentStatusStarting)
	case strings.ToLower(string(config.ComponentStatusStopped)):
		return string(config.ComponentStatusStopped)
	default:
		return string(config.ComponentStatusUnknown)
	}
}

func AggregateWithReferenceTime(components []*model.ApplicationComponent, now time.Time) string {
	var all, managedServing, sharedServing flags
	for _, comp := range components {
		if comp == nil {
			continue
		}
		status := effectiveStatus(comp, now)
		all.add(status)
		if comp.ComponentType == config.ServerJob {
			if _, shared := applicationservice.SharedLifecycleStrategyForComponent(comp); shared {
				sharedServing.add(status)
			} else {
				managedServing.add(status)
			}
		}
	}
	if all.counted == 0 {
		return "not_deploy"
	}
	for _, candidate := range []struct {
		flag  bool
		value string
	}{
		{all.failed, "failed"}, {all.deploying, "deploying"}, {all.updating, "updating"},
		{all.restarting, "restarting"}, {all.starting, "starting"}, {all.cleaning, "cleaning"},
	} {
		if candidate.flag {
			return candidate.value
		}
	}
	availability := all
	if managedServing.counted > 0 {
		availability = managedServing
	} else if sharedServing.counted > 0 {
		availability = sharedServing
	}
	for _, candidate := range []struct {
		flag  bool
		value string
	}{
		{availability.pending, "pending"}, {availability.running, "running"},
		{availability.stopped, "stopped"}, {availability.notDeploy, "not_deploy"},
		{availability.unknown, "unknown"},
	} {
		if candidate.flag {
			return candidate.value
		}
	}
	return "unknown"
}

func workflowMayOverride(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running", "pending", "stopped", "not_deploy", "unknown":
		return true
	default:
		return false
	}
}

func effectiveStatus(component *model.ApplicationComponent, now time.Time) string {
	status := NormalizeComponentRuntimeStatus(component.Status)
	if status != string(config.ComponentStatusFailed) || !transientPodFailure(component, now) {
		return status
	}
	if component.Replicas > 0 && component.ReadyReplicas >= component.Replicas {
		return string(config.ComponentStatusRunning)
	}
	return string(config.ComponentStatusPending)
}

func transientPodFailure(component *model.ApplicationComponent, now time.Time) bool {
	if component == nil || !config.ComponentTypeUsesPods(component.ComponentType) || component.UpdateTime.IsZero() {
		return false
	}
	age := now.Sub(component.UpdateTime)
	return age >= 0 && age <= config.DefaultApplicationStatusTransientFailedWindow
}

type flags struct {
	running, pending, failed, unknown, notDeploy, cleaning, deploying, updating, restarting, starting, stopped bool
	counted                                                                                                    int
}

func (f *flags) add(status string) {
	f.counted++
	switch status {
	case string(config.ComponentStatusRunning):
		f.running = true
	case string(config.ComponentStatusPending):
		f.pending = true
	case string(config.ComponentStatusFailed):
		f.failed = true
	case string(config.ComponentStatusUnknown):
		f.unknown = true
	case string(config.ComponentStatusNotDeploy):
		f.notDeploy = true
	case string(config.ComponentStatusCleaning):
		f.cleaning = true
	case string(config.ComponentStatusDeploying):
		f.deploying = true
	case string(config.ComponentStatusUpdating):
		f.updating = true
	case string(config.ComponentStatusRestarting):
		f.restarting = true
	case string(config.ComponentStatusStarting):
		f.starting = true
	case string(config.ComponentStatusStopped):
		f.stopped = true
	}
}

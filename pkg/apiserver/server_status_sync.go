package apiserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

func (s *restServer) findComponentForStatusSync(ctx context.Context, appID string, componentID int) (*model.ApplicationComponent, error) {
	query := &model.ApplicationComponent{
		AppID: appID,
	}
	entities, err := s.dataStore.List(ctx, query, &datastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, entity := range entities {
		component, ok := entity.(*model.ApplicationComponent)
		if !ok {
			return nil, fmt.Errorf("unexpected component entity type %T", entity)
		}
		if component != nil && component.ID == componentID {
			return component, nil
		}
	}
	return nil, nil
}

func (s *restServer) syncComponentStatus(update *informer.ComponentStatusUpdate) {
	if s.dataStore == nil || update == nil || update.ComponentID == 0 {
		return
	}
	appID := strings.TrimSpace(update.AppID)
	componentName := strings.TrimSpace(update.ComponentName)
	if appID == "" {
		return
	}
	defer func() {
		if s.cache == nil || s.cache.IsCacheDisabled() {
			return
		}
		cacheKey := cache.ApplicationComponentsKey(appID)
		if err := s.cache.Delete(cacheKey); err != nil {
			klog.V(4).Infof("Failed to invalidate component cache appID=%s: %v", appID, err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	component, err := s.findComponentForStatusSync(ctx, appID, update.ComponentID)
	if err != nil {
		klog.V(4).Infof("Failed to find component appID=%s componentID=%d name=%s: %v", appID, update.ComponentID, componentName, err)
		return
	}
	if component == nil {
		klog.V(4).Infof("Skip syncing status: component not found appID=%s componentID=%d name=%s", appID, update.ComponentID, componentName)
		return
	}
	expected := *component

	// 保护逻辑：清理态/未部署态不允许被 Informer 覆盖
	// 清理态在 Pod 完全消失后转换为 Not Deploy
	if component.Status == string(config.ComponentStatusNotDeploy) {
		klog.V(4).Infof("Skip updating component %s status from NotDeploy", componentName)
		return
	}
	if component.Status == string(config.ComponentStatusStopped) {
		klog.V(4).Infof("Skip updating component %s status from Stopped", componentName)
		return
	}
	if component.Status == string(config.ComponentStatusCleaning) {
		if update.Replicas != nil && *update.Replicas == 0 {
			status := config.ComponentStatusNotDeploy
			readyReplicas := int32(0)
			lastAbnormal := ""
			updated, err := repository.UpdateComponentRuntimeFieldsIfUnchanged(ctx, s.dataStore, &expected, map[string]interface{}{
				"status":         string(status),
				"ready_replicas": readyReplicas,
				"last_abnormal":  lastAbnormal,
			})
			if err != nil {
				klog.V(4).Infof("Failed to update component %d status to NotDeploy: %v", update.ComponentID, err)
				return
			}
			if !updated {
				klog.V(4).Infof("Skip stale cleanup status update appID=%s componentID=%d name=%s", appID, update.ComponentID, componentName)
				return
			}
			klog.V(4).Infof("Component %s (id=%d) cleanup completed, status set to NotDeploy", componentName, update.ComponentID)
			return
		}
		klog.V(4).Infof("Skip updating component %s status while Cleaning", componentName)
		return
	}

	// 更新状态字段
	var statusPatch *config.ComponentStatus
	if update.Status != nil {
		status := mapComponentStatusForSync(component.Status, *update.Status)
		component.Status = string(status)
		statusPatch = &status
	} else if update.ReadyReplicas != nil {
		status := resolveComponentStatus(component.Replicas, *update.ReadyReplicas)
		status = mapComponentStatusForSync(component.Status, status)
		component.Status = string(status)
		statusPatch = &status
	}
	if update.ReadyReplicas != nil {
		component.ReadyReplicas = *update.ReadyReplicas
	}
	if update.LastAbnormal != nil {
		component.LastAbnormal = *update.LastAbnormal
	}

	runtimeUpdates := make(map[string]interface{}, 3)
	if statusPatch != nil {
		runtimeUpdates["status"] = string(*statusPatch)
	}
	if update.ReadyReplicas != nil {
		runtimeUpdates["ready_replicas"] = *update.ReadyReplicas
	}
	if update.LastAbnormal != nil {
		runtimeUpdates["last_abnormal"] = *update.LastAbnormal
	}
	updated, err := repository.UpdateComponentRuntimeFieldsIfUnchanged(ctx, s.dataStore, &expected, runtimeUpdates)
	if err != nil {
		klog.V(4).Infof("Failed to update component %d status: %v", update.ComponentID, err)
		return
	}
	if !updated {
		klog.V(4).Infof("Skip stale component status update appID=%s componentID=%d name=%s", appID, update.ComponentID, componentName)
		return
	}

	status := ""
	if update.Status != nil {
		status = string(*update.Status)
	}
	readyReplicas := int32(0)
	if update.ReadyReplicas != nil {
		readyReplicas = *update.ReadyReplicas
	}
	replicas := int32(0)
	if update.Replicas != nil {
		replicas = *update.Replicas
	}
	if update.LastAbnormal != nil {
		klog.V(4).Infof("Component %s (id=%d) status synced: %s, ready=%d/%d, lastAbnormal=%s",
			componentName, update.ComponentID, status, readyReplicas, replicas, *update.LastAbnormal)
		return
	}
	klog.V(4).Infof("Component %s (id=%d) status synced: %s, ready=%d/%d",
		componentName, update.ComponentID, status, readyReplicas, replicas)
}

func mapComponentStatusForSync(current string, next config.ComponentStatus) config.ComponentStatus {
	switch current {
	case string(config.ComponentStatusStarting):
		return preserveInProgressComponentStatus(config.ComponentStatusStarting, next)
	case string(config.ComponentStatusDeploying):
		return preserveInProgressComponentStatus(config.ComponentStatusDeploying, next)
	default:
		return next
	}
}

func preserveInProgressComponentStatus(current config.ComponentStatus, next config.ComponentStatus) config.ComponentStatus {
	switch next {
	case config.ComponentStatusRunning, config.ComponentStatusFailed:
		return next
	case config.ComponentStatusPending, config.ComponentStatusUnknown:
		return current
	default:
		return next
	}
}

func resolveComponentStatus(desired, ready int32) config.ComponentStatus {
	if desired > 0 && ready >= desired {
		return config.ComponentStatusRunning
	}
	if ready > 0 || desired > 0 {
		return config.ComponentStatusPending
	}
	return config.ComponentStatusFailed
}

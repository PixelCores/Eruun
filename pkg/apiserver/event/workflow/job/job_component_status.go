package job

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func syncConfigComponentStatus(ctx context.Context, job *model.JobTask, store datastore.DataStore) (string, error) {
	target := findComponentForJob(ctx, job, store)
	if target == nil {
		return "", nil
	}
	if target.ComponentType != config.ConfJob && target.ComponentType != config.SecretJob {
		return "", nil
	}
	if target.Status == string(config.ComponentStatusCleaning) {
		klog.V(4).InfoS("skip updating component status from cleaning", "component", target.Name, "status", target.Status)
		return "", nil
	}
	if !isConfigComponentJobType(job.JobType) {
		return "", nil
	}
	status, ok := componentStatusForJob(job.Status)
	if !ok {
		if !isSharedDefaultSkippedConfigJob(job) {
			return "", nil
		}
		status = config.ComponentStatusRunning
	}

	lastAbnormal := ""
	target.Status = string(status)
	if status == config.ComponentStatusFailed {
		lastAbnormal = strings.TrimSpace(job.Error)
	}
	target.LastAbnormal = lastAbnormal

	if err := repository.UpdateComponentRuntimeFields(ctx, store, target, map[string]interface{}{
		"status":        string(status),
		"last_abnormal": lastAbnormal,
	}); err != nil {
		return "", fmt.Errorf("update component %s status to %s: %w", target.Name, status, err)
	}
	return target.AppID, nil
}

func syncConfigComponentStatusIfWorkflowOwned(
	ctx context.Context,
	job *model.JobTask,
	store datastore.DataStore,
	runtime *Runtime,
) error {
	if job == nil || store == nil || !isConfigComponentJobType(job.JobType) {
		return nil
	}
	var appID string
	err := jobinfo.WithJobInfoOwnership(ctx, store, job, func(tx datastore.DataStore) error {
		var syncErr error
		appID, syncErr = syncConfigComponentStatus(ctx, job, tx)
		return syncErr
	})
	if err != nil {
		return err
	}
	invalidateComponentsCache(ctx, runtime, appID, "config status sync")
	return nil
}

func syncComponentStatusOnJobStart(ctx context.Context, job *model.JobTask, store datastore.DataStore) (string, error) {
	target := findComponentForJob(ctx, job, store)
	if target == nil {
		return "", nil
	}
	if !shouldMarkComponentPendingOnJobStart(job.JobType) {
		return "", nil
	}
	status := config.ComponentStatusPending
	readyReplicas := int32(0)
	lastAbnormal := ""
	target.Status = string(status)
	target.ReadyReplicas = readyReplicas
	target.LastAbnormal = lastAbnormal
	if err := repository.UpdateComponentRuntimeFields(ctx, store, target, map[string]interface{}{
		"status":         string(status),
		"ready_replicas": readyReplicas,
		"last_abnormal":  lastAbnormal,
	}); err != nil {
		return "", fmt.Errorf("update component %s status to pending: %w", target.Name, err)
	}
	return target.AppID, nil
}

func invalidateComponentsCache(ctx context.Context, runtime *Runtime, appID string, reason string) {
	if runtime == nil || runtime.Cache == nil {
		return
	}
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return
	}
	cacheKey := cache.ApplicationComponentsKey(appID)
	if err := cache.InvalidateAfterWrite(ctx, runtime.Cache, cacheKey); err != nil {
		klog.V(4).InfoS("invalidate component cache failed", "reason", reason, "appID", appID, "err", err)
	}
}

func shouldMarkComponentPendingOnJobStart(jobType string) bool {
	switch config.JobType(jobType) {
	case config.JobDeploy, config.JobDeployStore, config.JobDeployInstant, config.JobDeployScheduled:
		return true
	default:
		return false
	}
}

func findComponentForJob(ctx context.Context, job *model.JobTask, store datastore.DataStore) *model.ApplicationComponent {
	if job == nil || store == nil {
		return nil
	}
	appID := strings.TrimSpace(job.AppID)
	if appID == "" {
		appID = strings.TrimSpace(job.WorkflowID)
	}
	name := strings.TrimSpace(job.Name)
	if appID == "" || name == "" {
		return nil
	}
	entities, err := store.List(ctx, &model.ApplicationComponent{AppID: appID}, &datastore.ListOptions{})
	if err != nil {
		if !errors.Is(err, datastore.ErrRecordNotExist) {
			klog.ErrorS(err, "list components for app failed", "appID", appID)
		}
		return nil
	}
	for _, entity := range entities {
		component, ok := entity.(*model.ApplicationComponent)
		if !ok || component == nil {
			continue
		}
		if component.Name == name {
			return component
		}
	}
	return nil
}

func isConfigComponentJobType(jobType string) bool {
	switch config.JobType(jobType) {
	case config.JobDeployConfigMap, config.JobDeploySecret:
		return true
	default:
		return false
	}
}

func componentStatusForJob(status config.Status) (config.ComponentStatus, bool) {
	switch status {
	case config.StatusCompleted, config.StatusPassed:
		return config.ComponentStatusRunning, true
	case config.StatusFailed, config.StatusTimeout, config.StatusReject, config.StatusCancelled:
		return config.ComponentStatusFailed, true
	default:
		return "", false
	}
}

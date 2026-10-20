package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// TerminalizePrecreatedVersionUpdateCleanupJobs marks version-update cleanup jobs
// that were persisted before execution when their workflow exits before running them.
func TerminalizePrecreatedVersionUpdateCleanupJobs(ctx context.Context, store datastore.DataStore, taskID string, targetStatus config.Status, reason string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || store == nil {
		return nil
	}
	targetStatus = config.Status(strings.TrimSpace(string(targetStatus)))
	if !shouldTerminalizePrecreatedCleanupTargetStatus(targetStatus) {
		return nil
	}
	opts := &datastore.ListOptions{}
	opts.FilterOptions.In = append(opts.FilterOptions.In, datastore.InQueryOption{
		Key:    "type",
		Values: []string{string(config.JobCleanupResources)},
	})
	entities, err := store.List(ctx, &model.JobInfo{TaskID: taskID}, opts)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil
		}
		return err
	}
	now := time.Now().Unix()
	for _, entity := range entities {
		jobInfo, ok := entity.(*model.JobInfo)
		if !ok || jobInfo == nil {
			return datastore.ErrEntityInvalid
		}
		marked, err := isVersionUpdateRemoveCleanupJobInfo(jobInfo)
		if err != nil {
			return err
		}
		if !marked || !shouldTerminalizePrecreatedCleanupStatus(config.Status(strings.TrimSpace(jobInfo.Status))) {
			continue
		}
		updates := map[string]interface{}{
			"status":   string(targetStatus),
			"end_time": now,
			"error":    strings.TrimSpace(reason),
		}
		updated, err := store.CompareAndSwap(ctx, jobInfo, "status", jobInfo.Status, updates)
		if err != nil {
			return err
		}
		if !updated {
			continue
		}
		jobInfo.Status = string(targetStatus)
		jobInfo.EndTime = now
		jobInfo.Error = strings.TrimSpace(reason)
	}
	return nil
}

func isVersionUpdateRemoveCleanupJobInfo(jobInfo *model.JobInfo) (bool, error) {
	if jobInfo == nil || strings.TrimSpace(jobInfo.Type) != string(config.JobCleanupResources) {
		return false, nil
	}
	raw := strings.TrimSpace(jobInfo.InternalInfo)
	if raw == "" {
		return false, nil
	}
	var marker struct {
		Source string `json:"source"`
	}
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		return false, nil
	}
	return marker.Source == config.JobInfoSourceVersionUpdateRemove, nil
}

func shouldTerminalizePrecreatedCleanupStatus(status config.Status) bool {
	switch status {
	case "", config.StatusCreated, config.StatusQueued, config.StatusWaiting, config.QueueItemPending:
		return true
	default:
		return false
	}
}

func shouldTerminalizePrecreatedCleanupTargetStatus(status config.Status) bool {
	switch status {
	case config.StatusCancelled, config.StatusFailed, config.StatusTimeout, config.StatusReject:
		return true
	default:
		return false
	}
}

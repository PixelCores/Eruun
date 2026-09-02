package workflow

import (
	"context"

	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"
)

func TestTerminalizePrecreatedVersionUpdateCleanupJobsSkipsRunningJobs(t *testing.T) {
	store := &statusDataStore{
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-running-cleanup",
				Status:       string(config.StatusRunning),
				InternalInfo: `{"source":"version_update_remove"}`,
				ServiceName:  "api",
			},
		},
	}

	err := TerminalizePrecreatedVersionUpdateCleanupJobs(context.Background(), store, "task-running-cleanup", config.StatusFailed, "deploy failed")
	require.NoError(t, err)
	require.Equal(t, string(config.StatusRunning), store.jobs[0].Status)
	require.Empty(t, store.jobs[0].Error)
	require.Zero(t, store.jobs[0].EndTime)
}

func TestTerminalizePrecreatedVersionUpdateCleanupJobsSkipsJobsThatStartDuringTerminalization(t *testing.T) {
	var store *statusDataStore
	store = &statusDataStore{
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-starting-cleanup",
				Status:       string(config.StatusQueued),
				InternalInfo: `{"source":"version_update_remove"}`,
				ServiceName:  "api",
			},
		},
		beforeCAS: func(*model.WorkflowQueue) {
			store.jobs[0].Status = string(config.StatusRunning)
		},
	}

	err := TerminalizePrecreatedVersionUpdateCleanupJobs(context.Background(), store, "task-starting-cleanup", config.StatusFailed, "deploy failed")
	require.NoError(t, err)
	require.Equal(t, string(config.StatusRunning), store.jobs[0].Status)
	require.Empty(t, store.jobs[0].Error)
	require.Zero(t, store.jobs[0].EndTime)
}

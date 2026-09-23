package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

type schedulingReadCounter struct {
	datastore.DataStore
	parentGets, parentLists int
}

func (s *schedulingReadCounter) Get(ctx context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.parentGets++
	}
	return s.DataStore.Get(ctx, entity)
}

func (s *schedulingReadCounter) List(ctx context.Context, entity datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.parentLists++
	}
	return s.DataStore.List(ctx, entity, options)
}

func TestScheduledJobsBatchParentOwnershipReads(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	lease := now.Add(time.Hour)
	for i := range 205 {
		id := fmt.Sprintf("task-%03d", i)
		if i != 0 { // A missing parent must fail closed without a fallback GET.
			require.NoError(t, store.Add(ctx, &model.WorkflowQueue{TaskID: id, WorkspaceID: "space",
				Status: config.StatusRunning, RunGeneration: 1, RunToken: "token", WorkerID: "worker",
				LeaseExpiresAt: &lease, JobSpec: "large declaration which must not survive the page"}))
		}
		key := "execution-" + id
		require.NoError(t, store.Add(ctx, &model.JobInfo{TaskID: id, WorkspaceID: "space", ExecutionKey: &key,
			Status: string(config.StatusRunning), RunGeneration: 1, SchedulingState: workflowconfig.JobSchedulingAdmitted,
			SchedulingOwnerStatus: config.StatusRunning, SchedulingGeneration: 1, SchedulingQueuedAt: &now}))
	}
	reads := &schedulingReadCounter{DataStore: store}
	parents := map[string]*model.WorkflowQueue{}
	valid := 0
	err := scanScheduledJobs(ctx, reads, []string{workflowconfig.JobSchedulingAdmitted}, parents, func(job *model.JobInfo) error {
		current, err := jobSchedulingCurrent(ctx, reads, job, now, parents, true)
		if current {
			valid++
		}
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 204, valid)
	require.Equal(t, 3, reads.parentLists)
	require.Zero(t, reads.parentGets)
	require.Contains(t, parents, "task-000")
	require.Nil(t, parents["task-000"])
	for _, parent := range parents {
		if parent != nil {
			require.Empty(t, parent.JobSpec)
		}
	}
}

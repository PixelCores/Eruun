package workflow

import (
	"context"

	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpsertWorkflowScheduleCreates(t *testing.T) {
	enabled := true
	store := &scheduleDataStore{
		app: &model.Applications{ID: "app-1"},
		workflows: []*model.Workflow{
			{ID: "wf-1", AppID: "app-1", Name: "deploy", Alias: "Deploy"},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewNoopLocker("test-app-schedule")}

	resp, err := svc.UpsertWorkflowSchedule(context.Background(), "app-1", apis.UpsertWorkflowScheduleRequest{
		WorkflowID: "wf-1",
		Cron:       "*/5 * * * *",
		Enabled:    &enabled,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, store.schedules, 1)
	require.Equal(t, "*/5 * * * *", store.schedules[0].Cron)
	require.True(t, store.schedules[0].Enabled)
	require.NotZero(t, store.schedules[0].NextRun)
	require.Equal(t, "deploy", resp.Schedule.WorkflowName)
}

func TestUpsertWorkflowScheduleDisables(t *testing.T) {
	enabled := false
	store := &scheduleDataStore{
		app: &model.Applications{ID: "app-1"},
		workflows: []*model.Workflow{
			{ID: "wf-1", AppID: "app-1", Name: "deploy"},
		},
		schedules: []*model.WorkflowSchedule{
			{
				ID:         "sch-1",
				AppID:      "app-1",
				WorkflowID: "wf-1",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    time.Now().Add(5 * time.Minute).Unix(),
			},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewNoopLocker("test-app-schedule")}

	resp, err := svc.UpsertWorkflowSchedule(context.Background(), "app-1", apis.UpsertWorkflowScheduleRequest{
		WorkflowID: "wf-1",
		Cron:       "*/5 * * * *",
		Enabled:    &enabled,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, store.schedules, 1)
	require.False(t, store.schedules[0].Enabled)
	require.Zero(t, store.schedules[0].NextRun)
}

func TestListWorkflowSchedules(t *testing.T) {
	store := &scheduleDataStore{
		app: &model.Applications{ID: "app-1"},
		workflows: []*model.Workflow{
			{ID: "wf-1", AppID: "app-1", Name: "deploy"},
		},
		schedules: []*model.WorkflowSchedule{
			{
				ID:         "sch-1",
				AppID:      "app-1",
				WorkflowID: "wf-1",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    time.Now().Add(5 * time.Minute).Unix(),
			},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewNoopLocker("test-app-schedule")}

	resp, err := svc.ListWorkflowSchedules(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, resp, 1)
	require.Equal(t, "wf-1", resp[0].WorkflowID)
	require.Equal(t, "deploy", resp[0].WorkflowName)
}

func TestDeleteWorkflowSchedule(t *testing.T) {
	store := &scheduleDataStore{
		schedules: []*model.WorkflowSchedule{
			{
				ID:         "sch-1",
				AppID:      "app-1",
				WorkflowID: "wf-1",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    time.Now().Add(5 * time.Minute).Unix(),
			},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewNoopLocker("test-app-schedule")}

	err := svc.DeleteWorkflowSchedule(context.Background(), "app-1", "wf-1")
	require.NoError(t, err)
	require.Len(t, store.schedules, 0)
}

func TestUpsertWorkflowScheduleFailsWhenLockUnavailable(t *testing.T) {
	enabled := true
	store := &scheduleDataStore{
		app: &model.Applications{ID: "app-1"},
		workflows: []*model.Workflow{
			{ID: "wf-1", AppID: "app-1", Name: "deploy"},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	_, err := svc.UpsertWorkflowSchedule(context.Background(), "app-1", apis.UpsertWorkflowScheduleRequest{
		WorkflowID: "wf-1",
		Cron:       "*/5 * * * *",
		Enabled:    &enabled,
	})
	require.ErrorIs(t, err, bcode.ErrDistributedLockUnavailable)
}

func TestDeleteWorkflowScheduleFailsWhenLockUnavailable(t *testing.T) {
	store := &scheduleDataStore{}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.DeleteWorkflowSchedule(context.Background(), "app-1", "wf-1")
	require.ErrorIs(t, err, bcode.ErrDistributedLockUnavailable)
}

func TestDispatchWorkflowSchedulesRequiresTransactionalStore(t *testing.T) {
	now := time.Now().UTC()
	initialNext := now.Add(-1 * time.Minute).Unix()
	store := &scheduleDataStore{
		schedules: []*model.WorkflowSchedule{
			{
				ID:         "sch-1",
				AppID:      "app-1",
				WorkflowID: "wf-1",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    initialNext,
			},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())
	require.ErrorContains(t, err, "workflow schedule dispatch requires transactional datastore")
	require.Equal(t, 0, processed)
	require.Len(t, store.queues, 0)
	require.Equal(t, int64(0), store.schedules[0].LastRun)
	require.Equal(t, initialNext, store.schedules[0].NextRun)
}

func TestDispatchWorkflowSchedulesDefersLockedAppAndContinuesOtherApps(t *testing.T) {
	now := time.Now().UTC()
	lockedNext := now.Add(-1 * time.Hour).Unix()
	otherNext := now.Add(-30 * time.Minute).Unix()
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{Name: "web", WorkflowType: config.JobDeploy}},
	})
	require.NoError(t, err)

	store := &scheduleDataStore{
		workflows: []*model.Workflow{
			{ID: "wf-locked", AppID: "app-locked", Name: "deploy", Steps: steps},
			{ID: "wf-other", AppID: "app-other", Name: "deploy", Steps: steps},
		},
		schedules: []*model.WorkflowSchedule{
			{ID: "sch-locked", AppID: "app-locked", WorkflowID: "wf-locked", Cron: "*/5 * * * *", Enabled: true, NextRun: lockedNext},
			{ID: "sch-other", AppID: "app-other", WorkflowID: "wf-other", Cron: "*/5 * * * *", Enabled: true, NextRun: otherNext},
		},
	}
	txStore := &transactionalScheduleDataStore{scheduleDataStore: store}
	lockProvider := locker.NewMemoryLocker("test-schedule-dispatch-contention")
	releaseLock := holdWorkflowTestAppScheduleLock(t, lockProvider, "app-locked")
	defer releaseLock()
	svc := &workflowServiceImpl{
		Store:          txStore,
		Cfg:            &config.Config{AllowPrivateURLTargets: true},
		ScheduleLocker: lockProvider,
	}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, store.queues, 1)
	require.Equal(t, "app-other", store.queues[0].AppID)
	require.Equal(t, lockedNext, store.schedules[0].NextRun)
	require.Zero(t, store.schedules[0].LastRun)
	require.Greater(t, store.schedules[1].NextRun, otherNext)
	require.NotZero(t, store.schedules[1].LastRun)

	releaseLock()
	processed, err = svc.DispatchWorkflowSchedules(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, store.queues, 2)
	require.Equal(t, "app-locked", store.queues[1].AppID)
	require.Greater(t, store.schedules[0].NextRun, lockedNext)
	require.NotZero(t, store.schedules[0].LastRun)
}

func TestDispatchWorkflowSchedulesSkipsActive(t *testing.T) {
	now := time.Now().UTC()
	initialNext := now.Add(-1 * time.Minute).Unix()
	store := &scheduleDataStore{
		workflows: []*model.Workflow{
			{ID: "wf-1", AppID: "app-1", Name: "deploy", Steps: &model.JSONStruct{}},
		},
		schedules: []*model.WorkflowSchedule{
			{
				ID:         "sch-1",
				AppID:      "app-1",
				WorkflowID: "wf-1",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    initialNext,
			},
		},
		tasks: []*model.WorkflowQueue{
			{TaskID: "t1", AppID: "app-1", Status: config.StatusRunning},
		},
	}
	txStore := &transactionalScheduleDataStore{scheduleDataStore: store}
	svc := &workflowServiceImpl{Store: txStore, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewMemoryLocker("test-schedule-dispatch")}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, processed)
	require.Len(t, store.queues, 0)
	require.Equal(t, int64(0), store.schedules[0].LastRun)
	require.Greater(t, store.schedules[0].NextRun, initialNext)
}

func TestDispatchWorkflowSchedulesEnqueues(t *testing.T) {
	now := time.Now().UTC()
	initialNext := now.Add(-1 * time.Hour).Unix()
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "web", WorkflowType: config.JobDeploy},
		},
	})
	require.NoError(t, err)

	store := &scheduleDataStore{
		workflows: []*model.Workflow{
			{ID: "wf-1", AppID: "app-1", Name: "deploy", Steps: steps},
		},
		schedules: []*model.WorkflowSchedule{
			{
				ID:         "sch-1",
				AppID:      "app-1",
				WorkflowID: "wf-1",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    initialNext,
			},
		},
	}
	txStore := &transactionalScheduleDataStore{scheduleDataStore: store}
	svc := &workflowServiceImpl{Store: txStore, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewMemoryLocker("test-schedule-dispatch")}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, store.queues, 1)
	require.Equal(t, config.StatusWaiting, store.queues[0].Status)
	require.NotNil(t, store.queues[0].IdempotencyKey)
	require.Equal(t, workflowScheduleIdempotencyKey("sch-1", initialNext), *store.queues[0].IdempotencyKey)
	require.NotZero(t, store.schedules[0].LastRun)
	require.Greater(t, store.schedules[0].NextRun, now.Unix())
}

func TestDispatchWorkflowSchedulesReusesExistingIdempotentTask(t *testing.T) {
	now := time.Now().UTC()
	initialNext := now.Add(-1 * time.Hour).Unix()
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "web", WorkflowType: config.JobDeploy},
		},
	})
	require.NoError(t, err)

	idempotencyKey := workflowScheduleIdempotencyKey("sch-1", initialNext)
	existingTask := &model.WorkflowQueue{
		TaskID:         "task-existing",
		AppID:          "app-1",
		WorkflowID:     "wf-1",
		Status:         config.StatusCompleted,
		IdempotencyKey: &idempotencyKey,
	}
	store := &scheduleDataStore{
		workflows: []*model.Workflow{
			{ID: "wf-1", AppID: "app-1", Name: "deploy", Steps: steps},
		},
		schedules: []*model.WorkflowSchedule{
			{
				ID:         "sch-1",
				AppID:      "app-1",
				WorkflowID: "wf-1",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    initialNext,
			},
		},
		queues: []*model.WorkflowQueue{existingTask},
		tasks:  []*model.WorkflowQueue{existingTask},
	}
	txStore := &transactionalScheduleDataStore{scheduleDataStore: store}
	svc := &workflowServiceImpl{Store: txStore, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewMemoryLocker("test-schedule-dispatch")}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, store.queues, 1)
	require.Equal(t, "task-existing", store.queues[0].TaskID)
	require.NotZero(t, store.schedules[0].LastRun)
	require.Greater(t, store.schedules[0].NextRun, now.Unix())
}

func TestEnqueueWorkflowTaskWithIdempotencyKeyReturnsOriginalDuplicateWhenUnresolved(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "web", WorkflowType: config.JobDeploy},
		},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{
		scheduleDataStore: scheduleDataStore{queueAddErr: datastore.ErrRecordExist},
		components: []*model.ApplicationComponent{
			{Name: "web", AppID: "app-1"},
		},
	}
	svc := &workflowServiceImpl{}

	_, err = svc.enqueueWorkflowTaskWithStoreAndIdempotencyKey(context.Background(), store, &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Name:  "deploy",
		Steps: steps,
	}, 0, "workflow-schedule:sch-1:100")
	require.ErrorIs(t, err, datastore.ErrRecordExist)
	require.Len(t, store.queues, 0)
}

func TestDispatchWorkflowSchedulesContinuesAfterDispatchFailure(t *testing.T) {
	now := time.Now().UTC()
	failedNext := now.Add(-1 * time.Hour).Unix()
	successfulNext := now.Add(-30 * time.Minute).Unix()
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "web", WorkflowType: config.JobDeploy},
		},
	})
	require.NoError(t, err)

	store := &scheduleDataStore{
		workflows: []*model.Workflow{
			{ID: "wf-ok", AppID: "app-ok", Name: "deploy", Steps: steps},
		},
		schedules: []*model.WorkflowSchedule{
			{
				ID:         "sch-fail",
				AppID:      "app-fail",
				WorkflowID: "wf-missing",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    failedNext,
			},
			{
				ID:         "sch-ok",
				AppID:      "app-ok",
				WorkflowID: "wf-ok",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    successfulNext,
			},
		},
	}
	txStore := &transactionalScheduleDataStore{scheduleDataStore: store}
	svc := &workflowServiceImpl{Store: txStore, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewMemoryLocker("test-schedule-dispatch")}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "dispatch workflow schedule sch-fail")
	require.Equal(t, 1, processed)
	require.Len(t, store.queues, 1)
	require.Equal(t, "app-ok", store.queues[0].AppID)
	require.Equal(t, config.StatusWaiting, store.queues[0].Status)
	require.Equal(t, failedNext, store.schedules[0].NextRun)
	require.Equal(t, int64(0), store.schedules[0].LastRun)
	require.Greater(t, store.schedules[1].NextRun, now.Unix())
	require.NotZero(t, store.schedules[1].LastRun)
}

func TestDispatchWorkflowSchedulesContinuesAfterActiveAppSkip(t *testing.T) {
	now := time.Now().UTC()
	activeNext := now.Add(-1 * time.Hour).Unix()
	successfulNext := now.Add(-30 * time.Minute).Unix()
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "web", WorkflowType: config.JobDeploy},
		},
	})
	require.NoError(t, err)

	store := &scheduleDataStore{
		workflows: []*model.Workflow{
			{ID: "wf-ok", AppID: "app-ok", Name: "deploy", Steps: steps},
		},
		schedules: []*model.WorkflowSchedule{
			{
				ID:         "sch-active",
				AppID:      "app-active",
				WorkflowID: "wf-active",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    activeNext,
			},
			{
				ID:         "sch-ok",
				AppID:      "app-ok",
				WorkflowID: "wf-ok",
				Cron:       "*/5 * * * *",
				Enabled:    true,
				NextRun:    successfulNext,
			},
		},
		tasks: []*model.WorkflowQueue{
			{TaskID: "t-active", AppID: "app-active", Status: config.StatusRunning},
		},
	}
	txStore := &transactionalScheduleDataStore{scheduleDataStore: store}
	svc := &workflowServiceImpl{Store: txStore, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewMemoryLocker("test-schedule-dispatch")}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, store.queues, 1)
	require.Equal(t, "app-ok", store.queues[0].AppID)
	require.Equal(t, int64(0), store.schedules[0].LastRun)
	require.Greater(t, store.schedules[0].NextRun, activeNext)
	require.NotZero(t, store.schedules[1].LastRun)
	require.Greater(t, store.schedules[1].NextRun, now.Unix())
}

func TestEnqueueWorkflowAllowsComponentlessApprovalOnlyWorkflow(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &model.WorkflowStepApproval{
					NotifyURL: "https://example.com/approval",
				},
			},
		},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{}
	svc := &workflowServiceImpl{}
	resp, err := svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
		ID:    "wf-approval-only",
		AppID: "app-without-components",
		Name:  "approval-only",
		Steps: steps,
	}, 0)
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Len(t, store.queues, 1)
}

func TestEnqueueWorkflowRejectsEmptyWorkflowWithoutQueueTask(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{}
	svc := &workflowServiceImpl{}
	resp, err := svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
		ID:    "wf-empty",
		AppID: "app-without-components",
		Name:  "empty",
		Steps: steps,
	}, 0)
	require.ErrorIs(t, err, bcode.ErrWorkflowEmpty)
	require.Nil(t, resp)
	require.Len(t, store.queues, 0)
}

func TestEnqueueWorkflowAllowsMissingStepJobTypeAsDeployDefault(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "deploy-web"},
		},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{
		components: []*model.ApplicationComponent{
			{Name: "deploy-web", AppID: "app-1"},
		},
	}
	svc := &workflowServiceImpl{}
	resp, err := svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
		ID:    "wf-missing-job-type",
		AppID: "app-1",
		Name:  "deploy",
		Steps: steps,
	}, 0)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.TaskID)
	require.Len(t, store.queues, 1)
}

func TestEnqueueWorkflowRejectsUnsupportedStepJobTypeWithoutQueueTask(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "deploy-web", WorkflowType: config.JobType("unsupported_job_type")},
		},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{}
	svc := &workflowServiceImpl{}
	resp, err := svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
		ID:    "wf-unsupported-job-type",
		AppID: "app-without-components",
		Name:  "deploy",
		Steps: steps,
	}, 0)
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Contains(t, err.Error(), "unsupported_job_type")
	require.Nil(t, resp)
	require.Len(t, store.queues, 0)
}

func TestEnqueueWorkflowAllowsMissingSubStepJobTypeAsDeployDefault(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name: "deploy-group",
				SubSteps: []*model.WorkflowSubStep{
					{Name: "deploy-web"},
				},
			},
		},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{
		components: []*model.ApplicationComponent{
			{Name: "deploy-web", AppID: "app-1"},
		},
	}
	svc := &workflowServiceImpl{}
	resp, err := svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
		ID:    "wf-missing-substep-job-type",
		AppID: "app-1",
		Name:  "deploy",
		Steps: steps,
	}, 0)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.TaskID)
	require.Len(t, store.queues, 1)
}

func TestEnqueueWorkflowRejectsUnsupportedSubStepJobTypeWithoutQueueTask(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name: "deploy-group",
				SubSteps: []*model.WorkflowSubStep{
					{Name: "deploy-web", WorkflowType: config.JobType("unsupported_job_type")},
				},
			},
		},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{}
	svc := &workflowServiceImpl{}
	resp, err := svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
		ID:    "wf-unsupported-substep-job-type",
		AppID: "app-without-components",
		Name:  "deploy",
		Steps: steps,
	}, 0)
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Contains(t, err.Error(), "unsupported_job_type")
	require.Nil(t, resp)
	require.Len(t, store.queues, 0)
}

func TestEnqueueWorkflowRejectsComponentWorkflowWithoutComponents(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "deploy-web",
				WorkflowType: config.JobDeploy,
			},
		},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{}
	svc := &workflowServiceImpl{}
	_, err = svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
		ID:    "wf-component",
		AppID: "app-without-components",
		Name:  "deploy",
		Steps: steps,
	}, 0)
	require.ErrorIs(t, err, bcode.ErrApplicationNoComponents)
	require.Len(t, store.queues, 0)
}

func TestEnqueueWorkflowRejectsMixedApprovalAndComponentWorkflowWithoutComponents(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &model.WorkflowStepApproval{
					NotifyURL: "https://example.com/approval",
				},
			},
			{
				Name:         "deploy-web",
				WorkflowType: config.JobDeploy,
			},
		},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{}
	svc := &workflowServiceImpl{}
	_, err = svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
		ID:    "wf-mixed",
		AppID: "app-without-components",
		Name:  "approval-then-deploy",
		Steps: steps,
	}, 0)
	require.ErrorIs(t, err, bcode.ErrApplicationNoComponents)
	require.Len(t, store.queues, 0)
}

func TestEnqueueWorkflowRejectsObserveApplication(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{Name: "deploy", WorkflowType: config.JobDeploy}},
	})
	require.NoError(t, err)

	store := &enqueueWorkflowDataStore{
		scheduleDataStore: scheduleDataStore{app: &model.Applications{
			ID:             "observe-app",
			ManagementMode: config.ManagementModeObserve,
		}},
		components: []*model.ApplicationComponent{{AppID: "observe-app", Name: "api"}},
	}
	svc := &workflowServiceImpl{}
	_, err = svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
		ID:    "wf-observe",
		AppID: "observe-app",
		Name:  "deploy",
		Steps: steps,
	}, 0)
	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.Empty(t, store.queues)
}

func TestEnqueueWorkflowRejectsUnsafeAdoptedJobs(t *testing.T) {
	tests := []struct {
		name string
		step *model.WorkflowStep
	}{
		{
			name: "database reset",
			step: &model.WorkflowStep{Name: "reset", WorkflowType: config.JobDatabaseReset},
		},
		{
			name: "nested cleanup",
			step: &model.WorkflowStep{
				Name:     "parallel",
				StepType: config.WorkflowStepTypeComponent,
				SubSteps: []*model.WorkflowSubStep{{Name: "cleanup", WorkflowType: config.JobCleanupResources}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
				Steps: []*model.WorkflowStep{tt.step},
			})
			require.NoError(t, err)

			store := &enqueueWorkflowDataStore{
				scheduleDataStore: scheduleDataStore{app: &model.Applications{
					ID:             "adopted-app",
					ManagementMode: config.ManagementModeAdopted,
				}},
				components: []*model.ApplicationComponent{{AppID: "adopted-app", Name: "api"}},
			}
			svc := &workflowServiceImpl{}
			_, err = svc.enqueueWorkflowTaskWithStore(context.Background(), store, &model.Workflow{
				ID:    "wf-adopted",
				AppID: "adopted-app",
				Name:  "unsafe",
				Steps: steps,
			}, 0)
			require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
			require.Empty(t, store.queues)
		})
	}
}

func TestExecWorkflowTaskForAppSerializesIdleCheckAndTaskInsert(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{
		Name:     "manual-check",
		StepType: config.WorkflowStepTypeApproval,
	}}})
	require.NoError(t, err)
	store := &manualExecTestStore{
		statusDataStore: &statusDataStore{workflow: &model.Workflow{
			ID:    "wf-1",
			AppID: "app-1",
			Steps: steps,
		}},
		taskAddStarted: make(chan struct{}, 1),
		releaseTaskAdd: make(chan struct{}),
	}
	svc := &workflowServiceImpl{
		Store:          store,
		ScheduleLocker: locker.NewMemoryLocker("manual-exec-test"),
	}

	firstResult := make(chan error, 1)
	go func() {
		_, execErr := svc.ExecWorkflowTaskForApp(context.Background(), "app-1", "wf-1", 0)
		firstResult <- execErr
	}()
	select {
	case <-store.taskAddStarted:
	case <-time.After(time.Second):
		t.Fatal("first execution did not reach task insert")
	}

	_, secondErr := svc.ExecWorkflowTaskForApp(context.Background(), "app-1", "wf-1", 0)
	require.ErrorIs(t, secondErr, bcode.ErrApplicationOperationLocked)
	close(store.releaseTaskAdd)
	require.NoError(t, <-firstResult)
	require.Len(t, store.tasks, 1)
}

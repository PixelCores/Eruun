package workflow

import (
	"bytes"
	"context"
	"encoding/base64"

	"fmt"

	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestNewWorkflowControllerRequiresURLSecurityPolicy(t *testing.T) {
	ctl, err := NewWorkflowController(&model.WorkflowQueue{TaskID: "task-1"}, nil, nil, &controllerTestStore{}, nil, nil, nil)
	require.Nil(t, ctl)
	require.Error(t, err)
	require.Contains(t, err.Error(), "url security policy is required")
}

func TestNewWorkflowControllerLoadsImportSecretKeyring(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
	cfg := &config.Config{
		ImportSecretKeyring: fmt.Sprintf(
			`{"activeKeyId":"active","keys":{"active":%q}}`,
			key,
		),
	}
	ctl, err := NewWorkflowController(
		&model.WorkflowQueue{TaskID: "task-1"},
		nil,
		nil,
		&controllerTestStore{},
		cfg,
		nil,
		&spec.URLSecurityPolicySpec{},
	)
	require.NoError(t, err)
	require.NotNil(t, ctl.importSecretKeyring)
	require.Equal(t, "active", ctl.importSecretKeyring.ActiveKeyID())
}

func TestNewWorkflowControllerRejectsInvalidImportSecretKeyring(t *testing.T) {
	ctl, err := NewWorkflowController(
		&model.WorkflowQueue{TaskID: "task-1"},
		nil,
		nil,
		&controllerTestStore{},
		&config.Config{ImportSecretKeyring: `{"activeKeyId":"missing","keys":{}}`},
		nil,
		&spec.URLSecurityPolicySpec{},
	)
	require.Nil(t, ctl)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load workflow import secret keyring")
}

func TestUpdateWorkflowStatusDoesNotOverwriteCancelledTask(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:       "task-complete-cancel-race",
		WorkflowName: "deploy",
		Status:       config.StatusRunning,
	}
	store := &controllerTestStore{task: cloneWorkflowQueue(task)}
	ctl := newTestWorkflowController(t, task, nil, store)

	store.mu.Lock()
	store.task.Status = config.StatusCancelled
	store.mu.Unlock()

	ctl.updateWorkflowStatus(context.Background())

	store.mu.Lock()
	persisted := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusCancelled, persisted.Status)
	require.Equal(t, config.StatusCancelled, ctl.snapshotTask().Status)
	_, _, stopped := ctl.snapshotTaskPersistence()
	require.True(t, stopped)
}

func TestUpdateWorkflowTaskTreatsMatchingCASMissAsNoopSuccess(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:       "task-noop-cas",
		WorkflowName: "deploy",
		Status:       config.StatusRunning,
	}
	store := &controlledWorkflowCASStore{
		controllerTestStore: &controllerTestStore{task: cloneWorkflowQueue(task)},
		falseCalls:          map[int]func(*model.WorkflowQueue){1: nil},
	}
	ctl := newTestWorkflowController(t, task, nil, store)

	ctl.updateWorkflowTask()

	_, _, stopped := ctl.snapshotTaskPersistence()
	require.False(t, stopped)

	ctl.setStatus(config.StatusCompleted)
	ctl.updateWorkflowTask()

	store.mu.Lock()
	persisted := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusCompleted, persisted.Status)
	_, _, stopped = ctl.snapshotTaskPersistence()
	require.False(t, stopped)
}

func TestWorkflowCtlSnapshotTask(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:       "task-123",
		WorkflowName: "test-workflow",
		Status:       config.StatusQueued,
	}

	ctl := &WorkflowCtl{
		workflowTask: task,
	}

	snapshot := ctl.snapshotTask()
	require.Equal(t, "task-123", snapshot.TaskID)
	require.Equal(t, "test-workflow", snapshot.WorkflowName)
	require.Equal(t, config.StatusQueued, snapshot.Status)
}

func TestWorkflowCtlSetStatus(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID: "task-123",
		Status: config.StatusQueued,
	}

	ctl := &WorkflowCtl{
		workflowTask: task,
	}

	ctl.setStatus(config.StatusRunning)
	require.Equal(t, config.StatusRunning, ctl.workflowTask.Status)

	ctl.setStatus(config.StatusCompleted)
	require.Equal(t, config.StatusCompleted, ctl.workflowTask.Status)
}

func TestWorkflowCtlMutateTask(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID: "task-123",
		Status: config.StatusQueued,
	}

	ctl := &WorkflowCtl{
		workflowTask: task,
	}

	ctl.mutateTask(func(t *model.WorkflowQueue) {
		t.Status = config.StatusRunning
		t.WorkflowName = "updated-workflow"
	})

	require.Equal(t, config.StatusRunning, ctl.workflowTask.Status)
	require.Equal(t, "updated-workflow", ctl.workflowTask.WorkflowName)
}

func TestIsWorkflowTerminal(t *testing.T) {
	testCases := []struct {
		status   config.Status
		expected bool
	}{
		{config.StatusPassed, true},
		{config.StatusFailed, true},
		{config.StatusTimeout, true},
		{config.StatusReject, true},
		{config.StatusCancelled, true},
		{config.StatusRunning, false},
		{config.StatusQueued, false},
		{config.StatusWaiting, false},
	}

	for _, tc := range testCases {
		t.Run(string(tc.status), func(t *testing.T) {
			result := isWorkflowTerminal(tc.status)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsJobSuccessStatus(t *testing.T) {
	testCases := []struct {
		name     string
		task     *model.JobTask
		expected bool
	}{
		{
			name:     "completed",
			task:     &model.JobTask{Status: config.StatusCompleted},
			expected: true,
		},
		{
			name:     "skipped",
			task:     &model.JobTask{Status: config.StatusSkipped},
			expected: true,
		},
		{
			name:     "passed",
			task:     &model.JobTask{Status: config.StatusPassed},
			expected: true,
		},
		{
			name:     "scheduled-distributed",
			task:     &model.JobTask{JobType: string(config.JobDeployScheduled), Status: config.StatusDistributed},
			expected: true,
		},
		{
			name:     "instant-distributed",
			task:     &model.JobTask{JobType: string(config.JobDeployInstant), Status: config.StatusDistributed},
			expected: true,
		},
		{
			name:     "scheduled-waiting",
			task:     &model.JobTask{JobType: string(config.JobDeployScheduled), Status: config.StatusWaiting},
			expected: true,
		},
		{
			name:     "distributed-non-scheduled",
			task:     &model.JobTask{JobType: string(config.JobDeploy), Status: config.StatusDistributed},
			expected: false,
		},
		{
			name:     "failed",
			task:     &model.JobTask{Status: config.StatusFailed},
			expected: false,
		},
		{
			name:     "running",
			task:     &model.JobTask{Status: config.StatusRunning},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, isJobSuccessStatus(tc.task))
		})
	}
}

package workflow

import (
	"context"

	"encoding/json"
	"errors"

	"net/http"
	"net/http/httptest"

	miniredis "github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestWorkflowRunStopsFailureCleanupWhenPersistenceStops(t *testing.T) {
	tests := []struct {
		name                       string
		configureStore             func(*controlledWorkflowCASStore)
		expectPersistenceUncertain bool
		expectedStatus             config.Status
		expectedCancelledCallbacks int32
	}{
		{
			name: "authoritative cancellation",
			configureStore: func(store *controlledWorkflowCASStore) {
				store.falseCalls = map[int]func(*model.WorkflowQueue){
					5: func(task *model.WorkflowQueue) {
						task.Status = config.StatusCancelled
						task.CancelSource = config.CancelSourceUser
					},
				}
			},
			expectedStatus:             config.StatusCancelled,
			expectedCancelledCallbacks: 1,
		},
		{
			name: "persistence reload failure",
			configureStore: func(store *controlledWorkflowCASStore) {
				store.falseCalls = map[int]func(*model.WorkflowQueue){5: nil}
				store.failTaskGetAfterCAS = 5
				store.taskGetErr = errors.New("injected cleanup persistence reload failure")
			},
			expectPersistenceUncertain: true,
			expectedStatus:             config.StatusRunning,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var failureCallbacks int32
			failureServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				atomic.AddInt32(&failureCallbacks, 1)
				w.WriteHeader(http.StatusOK)
			}))
			defer failureServer.Close()

			var cancelledCallbacks int32
			cancelledServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				atomic.AddInt32(&cancelledCallbacks, 1)
				w.WriteHeader(http.StatusOK)
			}))
			defer cancelledServer.Close()

			callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
				Failure:   failureServer.URL,
				Cancelled: cancelledServer.URL,
			})
			require.NoError(t, err)
			steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
				FailurePolicy: workflowconfig.WorkflowFailurePolicyCleanupAll,
				Steps: []*model.WorkflowStep{
					{
						Name:         "deploy-secret",
						WorkflowType: config.JobDeploy,
						Mode:         config.WorkflowModeStepByStep,
						Properties:   []model.Policies{{Policies: []string{"api-secret"}}},
					},
				},
			})
			require.NoError(t, err)
			secretProperties, err := model.NewJSONStructByStruct(model.Properties{
				Secret: map[string]string{"token": "value"},
			})
			require.NoError(t, err)

			task := &model.WorkflowQueue{
				TaskID:       "task-cleanup-cancel-race",
				WorkflowID:   "wf-cleanup-cancel-race",
				WorkflowName: "deploy-workflow",
				AppID:        "app-1",
				ProjectID:    "project-1",
				Type:         config.WorkflowTaskTypeWorkflow,
				Status:       config.StatusWaiting,
				Callback:     callback,
			}
			baseStore := &controllerTestStore{
				application: &model.Applications{ID: task.AppID, Name: "DemoApp"},
				workflow:    &model.Workflow{ID: task.WorkflowID, Steps: steps},
				task:        cloneWorkflowQueue(task),
				components: []*model.ApplicationComponent{
					{
						Name:          "api-secret",
						AppID:         task.AppID,
						Namespace:     "default",
						ComponentType: config.SecretJob,
						Properties:    secretProperties,
					},
				},
			}
			store := &controlledWorkflowCASStore{controllerTestStore: baseStore}
			tt.configureStore(store)

			client := kubefake.NewSimpleClientset()
			client.PrependReactor("create", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("injected secret create failure")
			})
			ctl := newTestWorkflowController(t, task, client, store)
			redisServer, err := miniredis.Run()
			require.NoError(t, err)
			defer redisServer.Close()
			redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
			defer redisClient.Close()
			ctl.Cache = cacheutil.NewMemCacheWithClient(false, redisClient)

			runErr := ctl.Run(context.Background(), 1)

			if tt.expectPersistenceUncertain {
				require.ErrorIs(t, runErr, errWorkflowTaskPersistenceUncertain)
				require.True(t, ctl.terminalCallbackSuppressed())
			} else {
				require.NoError(t, runErr)
			}
			require.Equal(t, tt.expectedStatus, ctl.snapshotTask().Status)
			baseStore.mu.Lock()
			persistedStatus := baseStore.task.Status
			baseStore.mu.Unlock()
			require.Equal(t, tt.expectedStatus, persistedStatus)
			require.Equal(t, int32(0), atomic.LoadInt32(&failureCallbacks))
			require.Equal(t, tt.expectedCancelledCallbacks, atomic.LoadInt32(&cancelledCallbacks))

			var deleteActions int
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" {
					deleteActions++
				}
			}
			require.Zero(t, deleteActions, "failure cleanup must not issue deletes after persistence stop")
		})
	}
}

func TestStopOnFailureLogicForStepModes(t *testing.T) {
	// This test verifies that stopOnFailure is correctly set based on step mode:
	// - StepByStep (sequential) mode: stopOnFailure = true (stop on first failure)
	// - DAG (parallel) mode: stopOnFailure = false (continue all jobs)

	t.Run("StepByStep mode should have stopOnFailure=true", func(t *testing.T) {
		mode := config.WorkflowModeStepByStep
		// The fix: stopOnFailure = !mode.IsParallel()
		stopOnFailure := !mode.IsParallel()
		require.True(t, stopOnFailure, "StepByStep mode should stop on first failure")
	})

	t.Run("DAG mode should have stopOnFailure=false", func(t *testing.T) {
		mode := config.WorkflowModeDAG
		stopOnFailure := !mode.IsParallel()
		require.False(t, stopOnFailure, "DAG mode should continue all jobs even if some fail")
	})
}

func TestShouldCleanupAllOnWorkflowFailure(t *testing.T) {
	tests := []struct {
		name     string
		policy   workflowconfig.WorkflowFailurePolicy
		task     *model.JobTask
		expected bool
	}{
		{
			name:   "default cleanup failed keeps existing per job cleanup behavior",
			policy: workflowconfig.WorkflowFailurePolicyCleanupFailed,
			task: &model.JobTask{
				JobType: string(config.JobDeploy),
				Status:  config.StatusFailed,
			},
			expected: false,
		},
		{
			name:   "empty policy defaults to cleanup all on failed deploy job",
			policy: "",
			task: &model.JobTask{
				JobType: string(config.JobDeploy),
				Status:  config.StatusFailed,
			},
			expected: true,
		},
		{
			name:   "cleanup all on failed deploy job",
			policy: workflowconfig.WorkflowFailurePolicyCleanupAll,
			task: &model.JobTask{
				JobType: string(config.JobDeployService),
				Status:  config.StatusFailed,
			},
			expected: true,
		},
		{
			name:   "cleanup all on timed out deploy job",
			policy: workflowconfig.WorkflowFailurePolicyCleanupAll,
			task: &model.JobTask{
				JobType: string(config.JobDeployStore),
				Status:  config.StatusTimeout,
			},
			expected: true,
		},
		{
			name:   "instant job override skips cleanup all on failure",
			policy: workflowconfig.WorkflowFailurePolicyCleanupAll,
			task: &model.JobTask{
				JobType:       string(config.JobDeployInstant),
				Status:        config.StatusFailed,
				FailurePolicy: workflowconfig.WorkflowFailurePolicyCleanupFailed,
			},
			expected: false,
		},
		{
			name:   "instant job override skips cleanup all on timeout",
			policy: workflowconfig.WorkflowFailurePolicyCleanupAll,
			task: &model.JobTask{
				JobType:       string(config.JobDeployInstant),
				Status:        config.StatusTimeout,
				FailurePolicy: workflowconfig.WorkflowFailurePolicyCleanupFailed,
			},
			expected: false,
		},
		{
			name:   "successful workflow job does not trigger cleanup all",
			policy: workflowconfig.WorkflowFailurePolicyCleanupAll,
			task: &model.JobTask{
				JobType: string(config.JobDeploy),
				Status:  config.StatusCompleted,
			},
			expected: false,
		},
		{
			name:   "callback failure does not trigger cleanup all",
			policy: workflowconfig.WorkflowFailurePolicyCleanupAll,
			task: &model.JobTask{
				JobType: string(config.JobDeployCallback),
				Status:  config.StatusFailed,
			},
			expected: false,
		},
		{
			name:   "manual cancellation does not trigger cleanup all",
			policy: workflowconfig.WorkflowFailurePolicyCleanupAll,
			task: &model.JobTask{
				JobType: string(config.JobDeploy),
				Status:  config.StatusCancelled,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, shouldCleanupAllOnWorkflowFailure(tt.policy, tt.task))
		})
	}
}

func TestWorkflowFailureCleanupTrigger(t *testing.T) {
	sqlJobFailure := &model.JobTask{
		Name:          "sql-job",
		JobType:       string(config.JobDeployInstant),
		Status:        config.StatusFailed,
		FailurePolicy: workflowconfig.WorkflowFailurePolicyCleanupFailed,
	}
	require.Nil(t, workflowFailureCleanupTrigger(workflowconfig.WorkflowFailurePolicyCleanupAll, []*model.JobTask{
		sqlJobFailure,
		{Name: "api", JobType: string(config.JobDeploy), Status: config.StatusCompleted},
	}))
	ordinaryFailure := &model.JobTask{Name: "api", JobType: string(config.JobDeploy), Status: config.StatusFailed}
	require.Same(t, ordinaryFailure, workflowFailureCleanupTrigger(workflowconfig.WorkflowFailurePolicyCleanupAll, []*model.JobTask{
		sqlJobFailure,
		ordinaryFailure,
	}))
}

func TestWorkflowFailureReasonIncludesDistinctCleanupAllTrigger(t *testing.T) {
	sqlJobFailure := &model.JobTask{Name: "sql-job", Status: config.StatusFailed}
	ordinaryFailure := &model.JobTask{Name: "api", Status: config.StatusTimeout}

	require.Equal(t,
		"workflow deploy-workflow failed at job sql-job (status=failed); cleanup_all triggered by job api (status=timeout)",
		workflowFailureReason("deploy-workflow", sqlJobFailure, ordinaryFailure),
	)
	require.Equal(t,
		"workflow deploy-workflow failed at job api (status=timeout)",
		workflowFailureReason("deploy-workflow", ordinaryFailure, ordinaryFailure),
	)
	require.Equal(t,
		"workflow deploy-workflow failed at job sql-job (status=failed)",
		workflowFailureReason("deploy-workflow", sqlJobFailure, nil),
	)
}

func TestWorkflowFailureCleanupErrorAggregatesFailures(t *testing.T) {
	err := workflowFailureCleanupError([]*model.JobTask{
		{Name: "api", Status: config.StatusCompleted},
		{Name: "worker", Status: config.StatusFailed, Error: "delete deployment: transient"},
		{Name: "mysql", Status: config.StatusTimeout, Error: "wait cleanup timeout"},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "cleanup job worker status=failed: delete deployment: transient")
	require.Contains(t, err.Error(), "cleanup job mysql status=timeout: wait cleanup timeout")
	require.NotContains(t, err.Error(), "api")
}

func TestWorkflowFailureCleanupErrorIgnoresSuccessfulJobs(t *testing.T) {
	err := workflowFailureCleanupError([]*model.JobTask{
		{Name: "api", Status: config.StatusCompleted},
		{Name: "worker", Status: config.StatusSkipped},
		{Name: "mysql", Status: config.StatusPassed},
	})

	require.NoError(t, err)
}

func TestTriggerWorkflowCallbackIncludesCleanupAllTriggerReason(t *testing.T) {
	type callbackResult struct {
		reason string
		err    error
	}
	resultReceived := make(chan callbackResult, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Reason string `json:"reason"`
		}
		decodeErr := json.NewDecoder(r.Body).Decode(&payload)
		resultReceived <- callbackResult{reason: payload.Reason, err: decodeErr}
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Failure: callbackServer.URL})
	require.NoError(t, err)
	task := &model.WorkflowQueue{
		TaskID:       "task-callback-reason",
		Status:       config.StatusFailed,
		AppID:        "app-1",
		WorkflowID:   "wf-callback-reason",
		WorkflowName: "deploy-workflow",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		Callback:     callback,
		BaseModel:    model.BaseModel{CreateTime: time.Now()},
	}
	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), &controllerTestStore{})
	reason := workflowFailureReason(
		"deploy-workflow",
		&model.JobTask{Name: "sql-job", Status: config.StatusFailed},
		&model.JobTask{Name: "api", Status: config.StatusTimeout},
	)

	ctl.triggerWorkflowCallback(context.Background(), config.StatusFailed, reason)

	select {
	case result := <-resultReceived:
		require.NoError(t, result.err)
		require.Equal(t, reason, result.reason)
	case <-time.After(2 * time.Second):
		t.Fatal("failure callback reason not received")
	}
}

func TestWorkflowRunFailureTerminalizesSkippedVersionUpdateCleanupJobs(t *testing.T) {
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "deploy-web",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeStepByStep,
				Properties:   []model.Policies{{Policies: []string{"web"}}},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-failed-before-cleanup",
		WorkflowID:   "wf-failed-before-cleanup",
		WorkflowName: "deploy-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
	}
	removed := &model.ApplicationComponent{
		Name:          "old",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	store := &controllerTestStore{
		application: &model.Applications{ID: "app-1", Name: "DemoApp"},
		workflow: &model.Workflow{
			ID:    "wf-failed-before-cleanup",
			Steps: stepsJSON,
		},
		task: cloneWorkflowQueue(task),
		components: []*model.ApplicationComponent{
			{
				Name:          "web",
				AppID:         "app-1",
				Namespace:     "default",
				Image:         "nginx:1.27",
				Replicas:      1,
				ComponentType: config.ServerJob,
			},
		},
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-failed-before-cleanup",
				Status:       string(config.StatusQueued),
				InternalInfo: mustVersionUpdateCleanupInternalInfo(t, removed, 1),
				ServiceName:  "old",
			},
		},
	}

	ctl := newTestWorkflowController(t, task, nil, store)
	err = ctl.Run(context.Background(), 1)
	require.Error(t, err)
	require.Equal(t, config.StatusFailed, task.Status)

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, config.StatusFailed, store.task.Status)
	require.Equal(t, string(config.StatusFailed), store.jobs[0].Status)
	require.Contains(t, store.jobs[0].Error, "deploy-workflow")
	require.NotZero(t, store.jobs[0].EndTime)
}

func TestWorkflowRunStartFailureTerminalizesPrecreatedCleanupJobs(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:        "task-start-failure-cleanup",
		WorkflowID:    "wf-start-failure-cleanup",
		WorkflowName:  "deploy-workflow",
		AppID:         "app-1",
		Status:        config.StatusRunning,
		RunGeneration: 1,
		RunToken:      "run-token-1",
		WorkerID:      "worker-1",
	}
	removed := &model.ApplicationComponent{
		Name:          "old",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	store := &controllerTestStore{
		task: cloneWorkflowQueue(task),
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-start-failure-cleanup",
				Status:       string(config.StatusQueued),
				InternalInfo: mustVersionUpdateCleanupInternalInfo(t, removed, 0),
				ServiceName:  "old",
			},
		},
	}
	runner := &Workflow{Store: store}
	ensureTestWorkflowExecutionIdentity(task)

	runner.markTaskRunStartFailure(context.Background(), task, errors.New("load url security policy"))

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, config.StatusFailed, store.task.Status)
	require.Equal(t, string(config.StatusFailed), store.jobs[0].Status)
	require.Equal(t, "load url security policy", store.jobs[0].Error)
	require.NotZero(t, store.jobs[0].EndTime)
}

package job

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type workflowOwnershipStore struct {
	*noopStore
	mu        sync.Mutex
	task      model.WorkflowQueue
	tasks     []model.WorkflowQueue
	tasksByID map[string]model.WorkflowQueue
	errs      []error
}

func (s *workflowOwnershipStore) Get(_ context.Context, entity datastore.Entity) error {
	task, ok := entity.(*model.WorkflowQueue)
	if !ok {
		return s.noopStore.Get(context.Background(), entity)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		if err != nil {
			return err
		}
	}
	if len(s.tasks) > 0 {
		*task = s.tasks[0]
		if len(s.tasks) > 1 {
			s.tasks = s.tasks[1:]
		}
		return nil
	}
	if stored, exists := s.tasksByID[task.TaskID]; exists {
		*task = stored
		return nil
	}
	*task = s.task
	return nil
}

func (s *workflowOwnershipStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func (s *workflowOwnershipStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, _ map[string]interface{}, _ map[string]interface{}) (bool, error) {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		return true, nil
	}
	return false, nil
}

func TestNewInstantAndScheduledCtlNilJob(t *testing.T) {
	require.Nil(t, NewInstantJobCtl(nil, nil, nil, nil))
	require.Nil(t, NewScheduledJobCtl(nil, nil, nil, nil))
}
func TestDelayedJobControllersPropagateWorkflowOwnership(t *testing.T) {
	newTask := func(jobType config.JobType) *model.JobTask {
		return &model.JobTask{
			Name:               "delayed-job",
			TaskID:             "task-delayed",
			Namespace:          "default",
			JobType:            string(jobType),
			ExecutionKey:       "execution-delayed",
			RunGeneration:      3,
			OwnerRunGeneration: 3,
			OwnerStatus:        config.StatusRunning,
			RunToken:           "run-3",
			WorkerID:           "worker-delayed",
			JobInfo: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name:      "delayed-job",
				Namespace: "default",
				Annotations: map[string]string{
					config.AnnotationJobStartTime: "4102444800",
				},
			}},
		}
	}
	assertPayload := func(t *testing.T, queue *enqueueCaptureQueue) {
		t.Helper()
		require.Len(t, queue.enqueued, 1)
		payload, err := (&DelayDispatcher{}).decodePayload(queue.enqueued[0])
		require.NoError(t, err)
		require.Equal(t, "execution-delayed", payload.ExecutionKey)
		require.Equal(t, uint64(3), payload.RunGeneration)
		require.Equal(t, "run-3", payload.RunToken)
	}

	t.Run("instant", func(t *testing.T) {
		queue := &enqueueCaptureQueue{enqueueID: "delay-instant"}
		task := newTask(config.JobDeployInstant)
		store := &workflowOwnershipStore{noopStore: &noopStore{}, task: model.WorkflowQueue{
			TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 3, RunToken: "run-3", WorkerID: "worker-delayed",
		}}
		ctl := NewInstantJobCtl(task, fake.NewSimpleClientset(), store, func() {})
		ctl.setRuntime(&jobRuntime{delayQueue: queue})

		require.NoError(t, ctl.run(context.Background()))
		assertPayload(t, queue)
		require.Equal(t, config.JobDelayStatePending, task.DelayState)
		require.NotEmpty(t, task.DelayPayload)
	})

	t.Run("scheduled", func(t *testing.T) {
		queue := &enqueueCaptureQueue{enqueueID: "delay-scheduled"}
		task := newTask(config.JobDeployScheduled)
		store := &workflowOwnershipStore{noopStore: &noopStore{}, task: model.WorkflowQueue{
			TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 3, RunToken: "run-3", WorkerID: "worker-delayed",
		}}
		ctl := NewScheduledJobCtl(task, fake.NewSimpleClientset(), store, func() {})
		ctl.setRuntime(&jobRuntime{delayQueue: queue})

		require.NoError(t, ctl.runOneTimeJob(context.Background(), task.JobInfo.(*batchv1.Job)))
		assertPayload(t, queue)
		require.Equal(t, config.StatusDistributed, task.Status)
		require.Equal(t, config.JobDelayStatePending, task.DelayState)
	})
}

func TestDelayedJobControllersCommitWithoutQueue(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobDeployInstant, config.JobDeployScheduled} {
		t.Run(string(jobType), func(t *testing.T) {
			store := &jobInfoSaveStore{}
			jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name:      "durable-delay",
				Namespace: "default",
				Annotations: map[string]string{
					config.AnnotationJobStartTime: "4102444800",
				},
			}}
			task := &model.JobTask{
				Name:          "durable-delay",
				TaskID:        "task-durable-delay",
				Namespace:     "default",
				JobType:       string(jobType),
				ExecutionKey:  "execution-durable-delay",
				RunGeneration: 2,
				JobInfo:       jobObj,
			}
			task.AppID = "app"
			jobObj.Spec.Template.Spec = workspacePodSpec()
			jobObj.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
			_, err := workspace.PrepareTask(task, "app", &model.Workspace{ID: "workspace", Namespace: "default"}, spec.WorkspaceConfig{})
			require.NoError(t, err)
			var ctl JobCtl
			if jobType == config.JobDeployInstant {
				ctl = NewInstantJobCtl(task, fake.NewSimpleClientset(), store, func() {})
			} else {
				ctl = NewScheduledJobCtl(task, fake.NewSimpleClientset(), store, func() {})
			}

			require.NoError(t, ctl.Run(context.Background()))
			require.Equal(t, config.StatusDistributed, task.Status)
			require.NotNil(t, store.added)
			require.Equal(t, config.JobDelayStatePending, store.added.DelayState)
			require.NotEmpty(t, store.added.DelayPayload)
			var persisted DelayJobPayload
			require.NoError(t, json.Unmarshal([]byte(store.added.DelayPayload), &persisted))
			requireWorkspacePodSecurity(t, persisted.Job.Spec.Template.Spec)
		})
	}
}

func TestImmediateJobControllersRejectStaleWorkflowOwnerBeforeKubernetesAccess(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobDeployInstant, config.JobDeployScheduled} {
		t.Run(string(jobType), func(t *testing.T) {
			jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "demo", Namespace: "default",
				Annotations: map[string]string{config.AnnotationJobRunPolicy: string(workflowconfig.JobRunPolicyRecreate)},
			}}
			jobTask := &model.JobTask{
				Name: "demo", Namespace: "default", TaskID: "task-1", JobType: string(jobType), JobInfo: jobObj,
				ExecutionKey: "execution-1", RunGeneration: 1, RunToken: "token-1",
				OwnerRunGeneration: 1, WorkerID: "worker-old",
			}
			store := &workflowOwnershipStore{noopStore: &noopStore{}, task: model.WorkflowQueue{
				TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 2, RunToken: "token-2", WorkerID: "worker-new",
			}}
			client := fake.NewSimpleClientset()

			var err error
			if jobType == config.JobDeployInstant {
				ctl := NewInstantJobCtl(jobTask, client, store, func() {})
				err = ctl.run(context.Background())
			} else {
				ctl := NewScheduledJobCtl(jobTask, client, store, func() {})
				err = ctl.runOneTimeJob(context.Background(), jobObj)
			}

			require.ErrorIs(t, err, errWorkflowJobOwnershipChanged)
			require.Empty(t, client.Actions())
		})
	}
}

func TestImmediateJobControllersDoNotDeleteNewerExecution(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobDeployInstant, config.JobDeployScheduled} {
		t.Run(string(jobType), func(t *testing.T) {
			desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "demo", Namespace: "default",
				Annotations: map[string]string{config.AnnotationJobRunPolicy: string(workflowconfig.JobRunPolicyRecreate)},
			}}
			jobTask := &model.JobTask{
				Name: "demo", Namespace: "default", TaskID: "task-1", JobType: string(jobType), JobInfo: desired,
				ExecutionKey: "execution-1", RunGeneration: 1, RunToken: "token-1",
				OwnerRunGeneration: 1, WorkerID: "worker-old",
			}
			store := &workflowOwnershipStore{noopStore: &noopStore{}, task: model.WorkflowQueue{
				TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 1, RunToken: "token-1", WorkerID: "worker-old",
			}}
			existing := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "demo", Namespace: "default", UID: "new-job-uid",
				Annotations: map[string]string{
					config.AnnotationJobExecutionKey:  "execution-2",
					config.AnnotationJobRunGeneration: "2",
				},
			}}
			client := fake.NewSimpleClientset(existing)

			var err error
			if jobType == config.JobDeployInstant {
				ctl := NewInstantJobCtl(jobTask, client, store, func() {})
				err = ctl.run(context.Background())
			} else {
				ctl := NewScheduledJobCtl(jobTask, client, store, func() {})
				err = ctl.runOneTimeJob(context.Background(), desired)
			}

			require.Error(t, err)
			require.True(t, errors.Is(err, errJobExecutionIdentityChanged))
			for _, action := range client.Actions() {
				require.False(t, action.Matches("delete", "jobs"), "newer Job must not be deleted: %#v", action)
			}
			live, getErr := client.BatchV1().Jobs("default").Get(context.Background(), "demo", metav1.GetOptions{})
			require.NoError(t, getErr)
			require.Equal(t, existing.UID, live.UID)
		})
	}
}

func TestExistingJobExecutionIdentityComparesGenerationsWithinOneTask(t *testing.T) {
	loadErr := errors.New("temporary workflow lookup failure")
	tests := []struct {
		name          string
		oldTaskStatus config.Status
		storeErr      error
		wantErr       error
		wantDelete    bool
	}{
		{
			name:          "terminal old task allows same generation replacement",
			oldTaskStatus: config.StatusCompleted,
			wantDelete:    true,
		},
		{
			name:          "active old task blocks replacement",
			oldTaskStatus: config.StatusRunning,
			wantErr:       errJobExecutionIdentityChanged,
		},
		{
			name:     "old task lookup failure stops infrastructure",
			storeErr: loadErr,
			wantErr:  signal.ErrInfrastructureStop,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "demo", Namespace: "default",
				Annotations: map[string]string{
					config.AnnotationJobRunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
					config.AnnotationJobTaskID:        "task-new",
					config.AnnotationJobExecutionKey:  "execution-new",
					config.AnnotationJobRunGeneration: "1",
				},
			}}
			existing := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "demo", Namespace: "default", UID: "old-job-uid",
				Annotations: map[string]string{
					config.AnnotationJobTaskID:        "task-old",
					config.AnnotationJobExecutionKey:  "execution-old",
					config.AnnotationJobRunGeneration: "1",
				},
			}}
			store := &workflowOwnershipStore{
				noopStore: &noopStore{},
				tasksByID: map[string]model.WorkflowQueue{
					"task-old": {TaskID: "task-old", Status: tc.oldTaskStatus},
				},
			}
			if tc.storeErr != nil {
				store.errs = []error{tc.storeErr}
			}
			client := fake.NewSimpleClientset(existing)

			action, err := applyJobRunPolicy(
				context.Background(),
				client,
				store,
				desired,
				config.JobDeployInstant,
				validateExistingJobExecutionIdentity(context.Background(), store, desired),
			)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, runPolicyActionCreate, action)
			}
			deleted := false
			for _, clientAction := range client.Actions() {
				if clientAction.Matches("delete", "jobs") {
					deleted = true
				}
			}
			require.Equal(t, tc.wantDelete, deleted)
		})
	}
}

func TestStampJobExecutionIdentityAlwaysIncludesTaskID(t *testing.T) {
	jobObj := &batchv1.Job{}

	stampJobExecutionIdentity(&model.JobTask{TaskID: "task-1"}, jobObj)

	require.Equal(t, "task-1", jobObj.Annotations[config.AnnotationJobTaskID])
}

func TestInstantJobCtlRunClientNil(t *testing.T) {
	ackCount := 0
	jobTask := &model.JobTask{
		Name:      "demo",
		Namespace: "default",
		JobType:   string(config.JobDeployInstant),
		JobInfo: &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		},
	}
	ctl := NewInstantJobCtl(jobTask, nil, &noopStore{}, func() { ackCount++ })
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusFailed, jobTask.Status)
}

func TestInstantJobCtlRunUnexpectedJobInfoType(t *testing.T) {
	jobTask := &model.JobTask{
		Name:      "demo",
		Namespace: "default",
		JobType:   string(config.JobDeployInstant),
		JobInfo:   "invalid",
	}
	ctl := NewInstantJobCtl(jobTask, fake.NewSimpleClientset(), &noopStore{}, func() {})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, config.StatusFailed, jobTask.Status)
}

func TestInstantJobCtlRunCompletesFromOwnedSucceededPodFallback(t *testing.T) {
	jobObj := jobForPodFallback("instant-job-pod-complete", nil)
	client := fake.NewSimpleClientset(succeededPodForJob(jobObj, "instant-job-pod-complete-abc", jobObj.UID))
	ackCount := 0
	jobTask := &model.JobTask{
		Name:      jobObj.Name,
		Namespace: jobObj.Namespace,
		JobType:   string(config.JobDeployInstant),
		JobInfo:   jobObj,
	}
	ctl := NewInstantJobCtl(jobTask, client, &noopStore{}, func() { ackCount++ })
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusCompleted, jobTask.Status)
	require.Equal(t, "", jobTask.Error)
}

func TestScheduledJobCtlRunUnexpectedJobInfoType(t *testing.T) {
	ackCount := 0
	jobTask := &model.JobTask{
		Name:      "demo",
		Namespace: "default",
		JobType:   string(config.JobDeployScheduled),
		JobInfo:   "invalid",
	}
	ctl := NewScheduledJobCtl(jobTask, fake.NewSimpleClientset(), &noopStore{}, func() { ackCount++ })
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusFailed, jobTask.Status)
}

func TestScheduledJobCtlRunOneTimeSkip(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{
					Type:   batchv1.JobComplete,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	_, err := client.BatchV1().Jobs("default").Create(ctx, existing, metav1.CreateOptions{})
	require.NoError(t, err)

	ackCount := 0
	jobTask := &model.JobTask{
		Name:      "demo",
		Namespace: "default",
		JobType:   string(config.JobDeployScheduled),
		JobInfo: &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo",
				Namespace: "default",
				Annotations: map[string]string{
					config.AnnotationJobRunPolicy: string(workflowconfig.JobRunPolicySkipIfCompleted),
				},
			},
		},
	}
	ctl := NewScheduledJobCtl(jobTask, client, &noopStore{}, func() { ackCount++ })
	require.NotNil(t, ctl)

	err = ctl.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, ackCount)
	require.Equal(t, config.StatusSkipped, jobTask.Status)
	require.Equal(t, "", jobTask.Error)
}

func TestScheduledJobCtlRunCronJobPath(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	jobTask := &model.JobTask{
		Name:      "demo-cron",
		Namespace: "default",
		JobType:   string(config.JobDeployScheduled),
		JobInfo: &batchv1.CronJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-cron",
				Namespace: "default",
			},
			Spec: batchv1.CronJobSpec{
				Schedule: "*/5 * * * *",
				JobTemplate: batchv1.JobTemplateSpec{
					Spec: batchv1.JobSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								RestartPolicy: corev1.RestartPolicyNever,
								Containers: []corev1.Container{{
									Name:  "demo",
									Image: "busybox",
								}},
							},
						},
					},
				},
			},
		},
	}
	ctl := NewScheduledJobCtl(jobTask, client, &noopStore{}, func() {})
	require.NotNil(t, ctl)

	err := ctl.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, config.StatusCompleted, jobTask.Status)
}

func TestInstantJobCtlHelpers(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := &jobInfoStore{}
	jobTask := &model.JobTask{
		Name:      "instant-job",
		TaskID:    "task-1",
		Namespace: "default",
		JobType:   string(config.JobDeployInstant),
		JobInfo: &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "instant-job", Namespace: "default"},
		},
	}
	ctl := NewInstantJobCtl(jobTask, client, store, func() {})
	require.NotNil(t, ctl)

	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Equal(t, 1, store.addCount)

	created, err := ctl.createJob(context.Background(), jobTask.JobInfo.(*batchv1.Job))
	require.NoError(t, err)
	require.True(t, created)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, _, err := ctl.wait(ctx)
	require.Error(t, err)
	require.Equal(t, config.StatusTimeout, status)

	ctl.Clean(context.Background())
}

func TestScheduledJobCtlHelpers(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := &jobInfoStore{}
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scheduled-job",
			Namespace: "default",
		},
	}
	jobTask := &model.JobTask{
		Name:      "scheduled-job",
		TaskID:    "task-2",
		Namespace: "default",
		JobType:   string(config.JobDeployScheduled),
		JobInfo:   jobObj,
	}
	ctl := NewScheduledJobCtl(jobTask, client, store, func() {})
	require.NotNil(t, ctl)

	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Equal(t, 1, store.addCount)

	created, err := ctl.createJob(context.Background(), jobObj)
	require.NoError(t, err)
	require.True(t, created)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, _, err := ctl.wait(ctx)
	require.Error(t, err)
	require.Equal(t, config.StatusTimeout, status)

	ctl.Clean(context.Background())
}

func TestGenerateOneTimeJob(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "demo",
		AppID:     "app-1",
		Namespace: "default",
		Image:     "busybox",
	}
	props := &model.Properties{}

	result := GenerateOneTimeJob(component, props, "", time.Now().Unix()+60)
	require.NotNil(t, result)
	jobObj, ok := result.Service.(*batchv1.Job)
	require.True(t, ok)
	require.NotNil(t, jobObj.Annotations)
	_, exists := jobObj.Annotations[config.AnnotationJobStartTime]
	require.True(t, exists)
}

func TestImmediateJobControllersRejectReplacementAfterRecreateWait(t *testing.T) {
	currentOwner := model.WorkflowQueue{
		TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 1, RunToken: "token-1", WorkerID: "worker-old",
	}
	newOwner := model.WorkflowQueue{
		TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 2, RunToken: "token-2", WorkerID: "worker-new",
	}
	for _, jobType := range []config.JobType{config.JobDeployInstant, config.JobDeployScheduled} {
		for _, tc := range []struct {
			name            string
			owners          []model.WorkflowQueue
			ownershipErrs   []error
			expectedErr     error
			expectedGetJobs int
		}{
			{
				name:            "ownership transferred",
				owners:          []model.WorkflowQueue{currentOwner, newOwner},
				expectedErr:     errWorkflowJobOwnershipChanged,
				expectedGetJobs: 2,
			},
			{
				name:            "ownership read failed",
				owners:          []model.WorkflowQueue{currentOwner, currentOwner},
				ownershipErrs:   []error{nil, errors.New("temporary database outage")},
				expectedErr:     signal.ErrInfrastructureStop,
				expectedGetJobs: 2,
			},
			{
				name:            "replacement identity changed",
				owners:          []model.WorkflowQueue{currentOwner, currentOwner},
				expectedErr:     errJobExecutionIdentityChanged,
				expectedGetJobs: 3,
			},
		} {
			t.Run(string(jobType)+"/"+tc.name, func(t *testing.T) {
				desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name: "demo", Namespace: "default",
					Annotations: map[string]string{config.AnnotationJobRunPolicy: string(workflowconfig.JobRunPolicyRecreate)},
				}}
				jobTask := &model.JobTask{
					Name: "demo", Namespace: "default", TaskID: "task-1", JobType: string(jobType), JobInfo: desired,
					ExecutionKey: "execution-1", RunGeneration: 1, RunToken: "token-1",
					OwnerRunGeneration: 1, WorkerID: "worker-old",
				}
				store := &workflowOwnershipStore{
					noopStore: &noopStore{},
					tasks:     append([]model.WorkflowQueue(nil), tc.owners...),
					errs:      append([]error(nil), tc.ownershipErrs...),
				}
				oldJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name: "demo", Namespace: "default", UID: "old-job-uid",
					Annotations: map[string]string{
						config.AnnotationJobExecutionKey:  "execution-1",
						config.AnnotationJobRunGeneration: "1",
					},
				}}
				replacement := oldJob.DeepCopy()
				replacement.UID = "new-job-uid"
				replacement.Annotations[config.AnnotationJobExecutionKey] = "execution-2"
				replacement.Annotations[config.AnnotationJobRunGeneration] = "2"
				client := fake.NewSimpleClientset()
				getJobs := 0
				client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
					getJobs++
					if getJobs == 1 {
						return true, oldJob.DeepCopy(), nil
					}
					return true, replacement.DeepCopy(), nil
				})
				client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, nil
				})

				var err error
				if jobType == config.JobDeployInstant {
					err = NewInstantJobCtl(jobTask, client, store, func() {}).run(context.Background())
				} else {
					err = NewScheduledJobCtl(jobTask, client, store, func() {}).runOneTimeJob(context.Background(), desired)
				}

				require.ErrorIs(t, err, tc.expectedErr)
				require.Equal(t, tc.expectedGetJobs, getJobs)
				for _, action := range client.Actions() {
					require.False(t, action.Matches("create", "jobs"), "stale worker must not create or adopt the replacement Job")
				}
			})
		}
	}
}

func TestRunJobsPreservesNonTerminalStateAfterRecreateOwnershipReadFailure(t *testing.T) {
	currentOwner := model.WorkflowQueue{
		TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 1, RunToken: "token-1", WorkerID: "worker-old",
	}
	for _, concurrencyCase := range []struct {
		name        string
		concurrency int
	}{
		{name: "serial", concurrency: 1},
		{name: "parallel", concurrency: 2},
	} {
		for _, jobType := range []config.JobType{config.JobDeployInstant, config.JobDeployScheduled} {
			t.Run(concurrencyCase.name+"/"+string(jobType), func(t *testing.T) {
				temporaryErr := errors.New("temporary database outage")
				desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name: "demo", Namespace: "default",
					Annotations: map[string]string{config.AnnotationJobRunPolicy: string(workflowconfig.JobRunPolicyRecreate)},
				}}
				jobTask := &model.JobTask{
					Name: "demo", Namespace: "default", TaskID: "task-1", JobType: string(jobType), JobInfo: desired,
					ExecutionKey: "execution-1", RunGeneration: 1, RunToken: "token-1",
					OwnerRunGeneration: 1, WorkerID: "worker-old",
				}
				store := &workflowOwnershipStore{
					noopStore: &noopStore{},
					task:      currentOwner,
					tasks:     []model.WorkflowQueue{currentOwner, currentOwner},
					errs:      []error{nil, temporaryErr},
				}
				oldJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name: "demo", Namespace: "default", UID: "old-job-uid",
					Annotations: map[string]string{
						config.AnnotationJobExecutionKey:  "execution-1",
						config.AnnotationJobRunGeneration: "1",
					},
				}}
				replacement := oldJob.DeepCopy()
				replacement.UID = "new-job-uid"
				client := fake.NewSimpleClientset()
				getJobs := 0
				client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
					getJobs++
					if getJobs == 1 {
						return true, oldJob.DeepCopy(), nil
					}
					return true, replacement.DeepCopy(), nil
				})
				client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, nil
				})
				ackCount := 0

				runErr := RunJobs(context.Background(), []*model.JobTask{jobTask}, concurrencyCase.concurrency, client, nil, store, func() {
					ackCount++
				}, true, nil, nil, nil, nil, nil)

				require.ErrorIs(t, runErr, signal.ErrInfrastructureStop)
				require.ErrorIs(t, runErr, temporaryErr)
				require.Equal(t, config.StatusPrepare, jobTask.Status)
				require.Empty(t, jobTask.Error)
				require.Zero(t, jobTask.EndTime)
				require.Equal(t, 2, ackCount)
				require.Equal(t, 2, getJobs)
				for _, action := range client.Actions() {
					require.False(t, action.Matches("create", "jobs"), "infrastructure recovery must not create or adopt the replacement Job")
				}
			})
		}
	}
}

func TestImmediateJobCreateRejectsAlreadyExistsReplacement(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobDeployInstant, config.JobDeployScheduled} {
		t.Run(string(jobType), func(t *testing.T) {
			desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "demo", Namespace: "default",
				Annotations: map[string]string{
					config.AnnotationJobExecutionKey:  "execution-1",
					config.AnnotationJobRunGeneration: "1",
				},
			}}
			replacement := desired.DeepCopy()
			replacement.UID = "new-job-uid"
			replacement.Annotations[config.AnnotationJobExecutionKey] = "execution-2"
			replacement.Annotations[config.AnnotationJobRunGeneration] = "2"
			jobTask := &model.JobTask{
				Name: desired.Name, Namespace: desired.Namespace, JobType: string(jobType), JobInfo: desired,
			}
			client := fake.NewSimpleClientset()
			getJobs := 0
			client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
				getJobs++
				if getJobs == 1 {
					return true, nil, k8serrors.NewNotFound(
						schema.GroupResource{Group: batchv1.GroupName, Resource: "jobs"},
						desired.Name,
					)
				}
				return true, replacement.DeepCopy(), nil
			})
			client.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, k8serrors.NewAlreadyExists(
					schema.GroupResource{Group: batchv1.GroupName, Resource: "jobs"},
					desired.Name,
				)
			})

			var created bool
			var err error
			if jobType == config.JobDeployInstant {
				created, err = NewInstantJobCtl(jobTask, client, &noopStore{}, func() {}).createJob(context.Background(), desired)
			} else {
				created, err = NewScheduledJobCtl(jobTask, client, &noopStore{}, func() {}).createJob(context.Background(), desired)
			}

			require.False(t, created)
			require.ErrorIs(t, err, errJobExecutionIdentityChanged)
			require.Equal(t, 2, getJobs)
		})
	}
}

package jobdelay

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

func TestDelayedJobControllersPropagateWorkflowOwnership(t *testing.T) {
	newTask := jobtest.NewDelayedJobTask
	assertPayload := func(t *testing.T, queue *jobtest.EnqueueCaptureQueue) {
		t.Helper()
		require.Len(t, queue.Enqueued, 1)
		payload, err := (&DelayDispatcher{}).decodeNotification(queue.Enqueued[0])
		require.NoError(t, err)
		require.Equal(t, "execution-delayed", payload.ExecutionKey)
		require.Equal(t, uint64(3), payload.RunGeneration)
		require.Equal(t, "run-3", payload.RunToken)
		require.Nil(t, payload.Job)
	}

	t.Run("scheduled", func(t *testing.T) {
		queue := &jobtest.EnqueueCaptureQueue{EnqueueID: "delay-scheduled"}
		task := newTask(config.JobDeployScheduled)
		store := &jobtest.WorkflowOwnershipStore{NoopStore: &jobtest.NoopStore{}, Task: model.WorkflowQueue{
			TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 3, RunToken: "run-3", WorkerID: "worker-delayed",
		}}
		ctl := NewScheduledJobCtl(task, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: store, Ack: func() {}, DelayQueue: queue})

		require.NoError(t, ctl.runOneTimeJob(context.Background(), task.JobInfo.(*batchv1.Job)))
		assertPayload(t, queue)
		require.Equal(t, config.StatusDistributed, task.Status)
		require.Equal(t, config.JobDelayStatePending, task.DelayState)
	})
}

func TestImmediateJobControllersRejectStaleWorkflowOwnerBeforeKubernetesAccess(t *testing.T) {
	jobType := config.JobDeployScheduled
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
		store := &jobtest.WorkflowOwnershipStore{NoopStore: &jobtest.NoopStore{}, Task: model.WorkflowQueue{
			TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 2, RunToken: "token-2", WorkerID: "worker-new",
		}}
		client := fake.NewSimpleClientset()

		var err error
		ctl := NewScheduledJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}})
		err = ctl.runOneTimeJob(context.Background(), jobObj)

		require.ErrorIs(t, err, jobexec.ErrWorkflowJobOwnershipChanged)
		require.Empty(t, client.Actions())
	})
}

func TestImmediateJobControllersDoNotDeleteNewerExecution(t *testing.T) {
	jobType := config.JobDeployScheduled
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
		store := &jobtest.WorkflowOwnershipStore{NoopStore: &jobtest.NoopStore{}, Task: model.WorkflowQueue{
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
		ctl := NewScheduledJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}})
		err = ctl.runOneTimeJob(context.Background(), desired)

		require.Error(t, err)
		require.True(t, errors.Is(err, jobinfo.ErrJobExecutionIdentityChanged))
		for _, action := range client.Actions() {
			require.False(t, action.Matches("delete", "jobs"), "newer Job must not be deleted: %#v", action)
		}
		live, getErr := client.BatchV1().Jobs("default").Get(context.Background(), "demo", metav1.GetOptions{})
		require.NoError(t, getErr)
		require.Equal(t, existing.UID, live.UID)
	})
}

func TestScheduledJobCtlRunUnexpectedJobInfoType(t *testing.T) {
	ackCount := 0
	jobTask := &model.JobTask{
		Name:      "demo",
		Namespace: "default",
		JobType:   string(config.JobDeployScheduled),
		JobInfo:   "invalid",
	}
	ctl := NewScheduledJobCtl(jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: &jobtest.NoopStore{}, Ack: func() { ackCount++ }})
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
	ctl := NewScheduledJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: &jobtest.NoopStore{}, Ack: func() { ackCount++ }})
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
	ctl := NewScheduledJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: &jobtest.NoopStore{}, Ack: func() {}})
	require.NotNil(t, ctl)

	err := ctl.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, config.StatusCompleted, jobTask.Status)
}

func TestScheduledJobCtlHelpers(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := &jobtest.JobInfoStore{}
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
	ctl := NewScheduledJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: jobtest.WithJobTestOwner(store, jobTask), Ack: func() {}})
	require.NotNil(t, ctl)

	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Equal(t, 1, store.AddCount)

	created, err := ctl.CreateBatchJob(context.Background(), jobObj)
	require.NoError(t, err)
	require.True(t, created)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, _, err := ctl.WaitBatchJob(ctx)
	require.Error(t, err)
	require.Equal(t, config.StatusTimeout, status)

	ctl.Clean(context.Background())
}

func TestImmediateJobControllersRejectReplacementAfterRecreateWait(t *testing.T) {
	currentOwner := model.WorkflowQueue{
		TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 1, RunToken: "token-1", WorkerID: "worker-old",
	}
	newOwner := model.WorkflowQueue{
		TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 2, RunToken: "token-2", WorkerID: "worker-new",
	}
	jobType := config.JobDeployScheduled
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
			expectedErr:     jobexec.ErrWorkflowJobOwnershipChanged,
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
			expectedErr:     jobinfo.ErrJobExecutionIdentityChanged,
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
			store := &jobtest.WorkflowOwnershipStore{
				NoopStore: &jobtest.NoopStore{},
				Tasks:     append([]model.WorkflowQueue(nil), tc.owners...),
				Errs:      append([]error(nil), tc.ownershipErrs...),
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
			err = NewScheduledJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}}).runOneTimeJob(context.Background(), desired)

			require.ErrorIs(t, err, tc.expectedErr)
			require.Equal(t, tc.expectedGetJobs, getJobs)
			for _, action := range client.Actions() {
				require.False(t, action.Matches("create", "jobs"), "stale worker must not create or adopt the replacement Job")
			}
		})
	}
}

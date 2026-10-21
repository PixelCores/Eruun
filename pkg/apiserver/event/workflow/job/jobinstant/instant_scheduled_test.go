package jobinstant

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
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
	workflowtraits "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func TestDelayedJobControllersPropagateWorkflowOwnership(t *testing.T) {
	newTask := jobtest.NewDelayedJobTask
	assertPayload := func(t *testing.T, queue *jobtest.EnqueueCaptureQueue) {
		t.Helper()
		require.Len(t, queue.Enqueued, 1)
		payload := &jobinfo.DelayJobPayload{}
		err := json.Unmarshal(queue.Enqueued[0], payload)
		require.NoError(t, err)
		require.NoError(t, jobinfo.ValidateDelayJobIdentity(payload))
		notification, err := json.Marshal(jobinfo.NotificationForDelayJob(payload))
		require.NoError(t, err)
		require.JSONEq(t, string(notification), string(queue.Enqueued[0]))
		require.Equal(t, "execution-delayed", payload.ExecutionKey)
		require.Equal(t, uint64(3), payload.RunGeneration)
		require.Equal(t, "run-3", payload.RunToken)
		require.Nil(t, payload.Job)
	}

	t.Run("instant", func(t *testing.T) {
		queue := &jobtest.EnqueueCaptureQueue{EnqueueID: "delay-instant"}
		task := newTask(config.JobDeployInstant)
		store := &jobtest.WorkflowOwnershipStore{NoopStore: &jobtest.NoopStore{}, Task: model.WorkflowQueue{
			TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 3, RunToken: "run-3", WorkerID: "worker-delayed",
		}}
		ctl := NewInstantJobCtl(task, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: store, Ack: func() {}, DelayQueue: queue})

		require.NoError(t, ctl.run(context.Background()))
		assertPayload(t, queue)
		require.Equal(t, config.JobDelayStatePending, task.DelayState)
		require.NotEmpty(t, task.DelayPayload)
	})

}

func TestImmediateJobControllersRejectStaleWorkflowOwnerBeforeKubernetesAccess(t *testing.T) {
	jobType := config.JobDeployInstant
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
		ctl := NewInstantJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}})
		err = ctl.run(context.Background())

		require.ErrorIs(t, err, jobexec.ErrWorkflowJobOwnershipChanged)
		require.Empty(t, client.Actions())
	})
}

func TestImmediateJobControllersDoNotDeleteNewerExecution(t *testing.T) {
	jobType := config.JobDeployInstant
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
		ctl := NewInstantJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}})
		err = ctl.run(context.Background())

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
	ctl := NewInstantJobCtl(jobTask, &jobexec.Runtime{Client: nil, Store: &jobtest.NoopStore{}, Ack: func() { ackCount++ }})
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
	ctl := NewInstantJobCtl(jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: &jobtest.NoopStore{}, Ack: func() {}})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, config.StatusFailed, jobTask.Status)
}

func TestInstantJobCtlRunCompletesFromOwnedSucceededPodFallback(t *testing.T) {
	jobObj := jobtest.JobForPodFallback("instant-job-pod-complete", nil)
	client := fake.NewSimpleClientset(jobtest.SucceededPodForJob(jobObj, "instant-job-pod-complete-abc", jobObj.UID))
	ackCount := 0
	jobTask := &model.JobTask{
		Timeout:   60,
		Name:      jobObj.Name,
		Namespace: jobObj.Namespace,
		JobType:   string(config.JobDeployInstant),
		JobInfo:   jobObj,
	}
	ctl := NewInstantJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: &jobtest.NoopStore{}, Ack: func() { ackCount++ }})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusCompleted, jobTask.Status)
	require.Equal(t, "", jobTask.Error)
}

func TestInstantJobCtlHelpers(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := &jobtest.JobInfoStore{}
	jobTask := &model.JobTask{
		Timeout:   60,
		Name:      "instant-job",
		TaskID:    "task-1",
		Namespace: "default",
		JobType:   string(config.JobDeployInstant),
		JobInfo: &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "instant-job", Namespace: "default"},
		},
	}
	ctl := NewInstantJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: jobtest.WithJobTestOwner(store, jobTask), Ack: func() {}})
	require.NotNil(t, ctl)

	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Equal(t, 1, store.AddCount)

	created, err := ctl.CreateBatchJob(context.Background(), jobTask.JobInfo.(*batchv1.Job))
	require.NoError(t, err)
	require.True(t, created)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, _, err := ctl.WaitBatchJob(ctx)
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

	result, err := GenerateOneTimeJob(component, props, "", time.Now().Unix()+60, workflowtraits.Profiles{})
	if err != nil {
		t.Fatal(err)
	}
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
	jobType := config.JobDeployInstant
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
			err = NewInstantJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}}).run(context.Background())

			require.ErrorIs(t, err, tc.expectedErr)
			require.Equal(t, tc.expectedGetJobs, getJobs)
			for _, action := range client.Actions() {
				require.False(t, action.Matches("create", "jobs"), "stale worker must not create or adopt the replacement Job")
			}
		})
	}
}

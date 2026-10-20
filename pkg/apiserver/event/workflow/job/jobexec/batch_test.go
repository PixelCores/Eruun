package jobexec

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	restfake "k8s.io/client-go/rest/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

func TestApplyJobRunPolicy(t *testing.T) {
	const (
		name      = "demo-job"
		namespace = "default"
	)

	newJob := func(policy workflowconfig.JobRunPolicy) *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Annotations: map[string]string{
					config.AnnotationJobRunPolicy:     string(policy),
					config.AnnotationJobTaskID:        "task-1",
					config.AnnotationJobExecutionKey:  "execution-2",
					config.AnnotationJobRunGeneration: "2",
				},
				Labels: map[string]string{
					config.LabelAppID:         "app-1",
					config.LabelComponentName: "demo",
				},
			},
		}
	}

	completedJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
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
	failedJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{
					Type:   batchv1.JobFailed,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	runningJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				config.AnnotationJobTaskID:        "task-1",
				config.AnnotationJobExecutionKey:  "execution-1",
				config.AnnotationJobRunGeneration: "1",
			},
		},
		Status: batchv1.JobStatus{
			Active: 1,
		},
	}

	tests := []struct {
		name          string
		policy        workflowconfig.JobRunPolicy
		existing      *batchv1.Job
		expectAction  runPolicyAction
		expectErr     bool
		expectStatus  config.Status
		expectDeleted bool
	}{
		{
			name:         "skip if completed",
			policy:       workflowconfig.JobRunPolicySkipIfCompleted,
			existing:     completedJob,
			expectAction: RunPolicyActionSkip,
		},
		{
			name:         "skip if running allows existing job",
			policy:       workflowconfig.JobRunPolicySkipIfCompleted,
			existing:     runningJob,
			expectAction: runPolicyActionCreate,
		},
		{
			name:         "skip if failed returns error",
			policy:       workflowconfig.JobRunPolicySkipIfCompleted,
			existing:     failedJob,
			expectErr:    true,
			expectStatus: config.StatusFailed,
		},
		{
			name:          "recreate deletes existing job",
			policy:        workflowconfig.JobRunPolicyRecreate,
			existing:      runningJob,
			expectAction:  runPolicyActionCreate,
			expectDeleted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := fake.NewSimpleClientset()
			if tc.existing != nil {
				existing := tc.existing.DeepCopy()
				existing.UID = "existing-job-uid"
				existing.ResourceVersion = "1"
				if _, err := client.BatchV1().Jobs(namespace).Create(ctx, existing, metav1.CreateOptions{}); err != nil {
					t.Fatalf("create existing job: %v", err)
				}
			}

			action, err := ApplyJobRunPolicy(ctx, client, nil, newJob(tc.policy), config.JobDeployInstant)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				var statusErr *jobinfo.StatusError
				if !errors.As(err, &statusErr) {
					t.Fatalf("expected StatusError, got %v", err)
				}
				if statusErr.Status != tc.expectStatus {
					t.Fatalf("expected status %s, got %s", tc.expectStatus, statusErr.Status)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if action != tc.expectAction {
				t.Fatalf("expected action %v, got %v", tc.expectAction, action)
			}
			if tc.expectDeleted {
				_, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
				if err == nil || !k8serrors.IsNotFound(err) {
					t.Fatalf("expected job to be deleted, got %v", err)
				}
			}
		})
	}
}

func TestAdoptReusableJobExecution(t *testing.T) {
	desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      "shared-job",
		Namespace: "default",
		Annotations: map[string]string{
			config.AnnotationJobTaskID:        "task-1",
			config.AnnotationJobExecutionKey:  "execution-2",
			config.AnnotationJobRunGeneration: "2",
		},
	}}

	t.Run("adopts an older generation of the same task", func(t *testing.T) {
		existing := desired.DeepCopy()
		existing.Annotations[config.AnnotationJobExecutionKey] = "execution-1"
		existing.Annotations[config.AnnotationJobRunGeneration] = "1"
		client := fake.NewSimpleClientset(existing)

		live, err := adoptReusableJobExecution(context.Background(), client, desired, existing)
		require.NoError(t, err)
		require.Equal(t, "task-1", live.Annotations[config.AnnotationJobTaskID])
		require.Equal(t, "execution-2", live.Annotations[config.AnnotationJobExecutionKey])
		require.Equal(t, "2", live.Annotations[config.AnnotationJobRunGeneration])
	})

	t.Run("keeps an exact identity unchanged", func(t *testing.T) {
		existing := desired.DeepCopy()
		client := fake.NewSimpleClientset(existing)

		live, err := adoptReusableJobExecution(context.Background(), client, desired, existing)
		require.NoError(t, err)
		require.Same(t, existing, live)
		require.Empty(t, client.Actions())
	})

	tests := []struct {
		name   string
		mutate func(*batchv1.Job)
	}{
		{
			name: "rejects a newer generation",
			mutate: func(existing *batchv1.Job) {
				existing.Annotations[config.AnnotationJobExecutionKey] = "execution-3"
				existing.Annotations[config.AnnotationJobRunGeneration] = "3"
			},
		},
		{
			name: "rejects a different execution in the same generation",
			mutate: func(existing *batchv1.Job) {
				existing.Annotations[config.AnnotationJobExecutionKey] = "execution-conflict"
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			existing := desired.DeepCopy()
			tc.mutate(existing)
			client := fake.NewSimpleClientset(existing)

			_, err := adoptReusableJobExecution(context.Background(), client, desired, existing)
			require.ErrorIs(t, err, jobinfo.ErrJobExecutionIdentityChanged)
			require.Empty(t, client.Actions())
		})
	}
}

func TestStartTimeFromJob(t *testing.T) {
	value, ok := StartTimeFromJob(nil)
	require.False(t, ok)
	require.Zero(t, value)

	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}}
	value, ok = StartTimeFromJob(job)
	require.False(t, ok)
	require.Zero(t, value)

	job.Annotations[config.AnnotationJobStartTime] = "invalid"
	value, ok = StartTimeFromJob(job)
	require.False(t, ok)
	require.Zero(t, value)

	job.Annotations[config.AnnotationJobStartTime] = "123"
	value, ok = StartTimeFromJob(job)
	require.True(t, ok)
	require.Equal(t, int64(123), value)
}

func TestWaitBatchJobUsesTaskTargetAndTimeout(t *testing.T) {
	tests := []struct {
		name        string
		jobInfo     interface{}
		timeout     int64
		wantTarget  string
		wantTimeout time.Duration
	}{
		{
			name: "task defaults", wantTarget: "task-namespace/jobs/task-name",
			wantTimeout: config.DefaultJobTaskTimeout,
		},
		{
			name: "empty job info and negative timeout", jobInfo: &batchv1.Job{}, timeout: -1,
			wantTarget: "task-namespace/jobs/task-name", wantTimeout: config.DefaultJobTaskTimeout,
		},
		{
			name: "job info namespace", jobInfo: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "job-namespace"}},
			timeout: 15, wantTarget: "job-namespace/jobs/task-name", wantTimeout: 15 * time.Second,
		},
		{
			name: "job info name", jobInfo: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job-name"}},
			timeout: 10, wantTarget: "task-namespace/jobs/job-name", wantTimeout: 10 * time.Second,
		},
		{
			name: "job info target", jobInfo: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "job-namespace", Name: "job-name"}},
			wantTarget: "job-namespace/jobs/job-name", wantTimeout: config.DefaultJobTaskTimeout,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requestPath string
			var deadline time.Time
			var hasDeadline bool
			httpClient := restfake.CreateHTTPClient(func(request *http.Request) (*http.Response, error) {
				requestPath = request.URL.Path
				deadline, hasDeadline = request.Context().Deadline()
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"apiVersion":"batch/v1","kind":"Job","status":{"conditions":[{"type":"Complete","status":"True"}]}}`)),
				}, nil
			})
			client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "https://kubernetes.example.test"}, httpClient)
			require.NoError(t, err)
			base := DeployNamespacedResourceJobBase{
				Namespace: "task-namespace",
				Client:    client,
				Job: &model.JobTask{
					Name: "task-name", Namespace: "task-namespace", Timeout: test.timeout, JobInfo: test.jobInfo,
				},
			}

			started := time.Now()
			status, message, err := base.WaitBatchJob(context.Background())
			finished := time.Now()
			require.NoError(t, err)
			require.Equal(t, config.StatusCompleted, status)
			require.Empty(t, message)
			require.Equal(t, "/apis/batch/v1/namespaces/"+test.wantTarget, requestPath)
			require.True(t, hasDeadline)
			require.False(t, deadline.Before(started.Add(test.wantTimeout)))
			require.False(t, deadline.After(finished.Add(test.wantTimeout)))
		})
	}
}

func TestWaitForJobCompletionBranches(t *testing.T) {
	status, msg, err := waitForJobCompletion(context.Background(), nil, "default", "job")
	require.Error(t, err)
	require.Equal(t, config.StatusFailed, status)
	require.Equal(t, "client is nil", msg)

	completedClient := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job-complete", Namespace: "default"},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	})
	status, msg, err = waitForJobCompletion(context.Background(), completedClient, "default", "job-complete")
	require.NoError(t, err)
	require.Equal(t, config.StatusCompleted, status)
	require.Equal(t, "", msg)

	failedClient := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job-failed", Namespace: "default"},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:    batchv1.JobFailed,
				Status:  corev1.ConditionTrue,
				Message: "boom",
			}},
		},
	})
	status, msg, err = waitForJobCompletion(context.Background(), failedClient, "default", "job-failed")
	require.Error(t, err)
	require.Equal(t, config.StatusFailed, status)
	require.Equal(t, "boom", msg)

	runningClient := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job-running", Namespace: "default"},
		Status: batchv1.JobStatus{
			Active: 1,
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	status, _, err = waitForJobCompletion(ctx, runningClient, "default", "job-running")
	require.Error(t, err)
	require.Equal(t, config.StatusTimeout, status)
}

func TestWaitForJobCompletionUsesOwnedSucceededPodFallback(t *testing.T) {
	jobObj := jobtest.JobForPodFallback("job-pod-complete", nil)
	client := fake.NewSimpleClientset(jobObj, jobtest.SucceededPodForJob(jobObj, "job-pod-complete-abc", jobObj.UID))

	status, msg, err := waitForJobCompletion(context.Background(), client, "default", jobObj.Name)
	require.NoError(t, err)
	require.Equal(t, config.StatusCompleted, status)
	require.Equal(t, "", msg)
}

func TestWaitForJobCompletionWaitsForRequiredSucceededPods(t *testing.T) {
	completions := int32(2)
	jobObj := jobtest.JobForPodFallback("job-two-completions", &completions)
	client := fake.NewSimpleClientset(jobObj, jobtest.SucceededPodForJob(jobObj, "job-two-completions-abc", jobObj.UID))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	status, _, err := waitForJobCompletion(ctx, client, "default", jobObj.Name)
	require.Error(t, err)
	require.Equal(t, config.StatusTimeout, status)
}

func TestWaitForJobCompletionIgnoresSucceededPodWithMismatchedOwner(t *testing.T) {
	jobObj := jobtest.JobForPodFallback("job-owner-mismatch", nil)
	client := fake.NewSimpleClientset(jobObj, jobtest.SucceededPodForJob(jobObj, "job-owner-mismatch-abc", types.UID("other-job-uid")))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	status, _, err := waitForJobCompletion(ctx, client, "default", jobObj.Name)
	require.Error(t, err)
	require.Equal(t, config.StatusTimeout, status)
}

type runPolicyStore struct {
	jobtest.NoopStore
	count int64
	err   error
}

func (s *runPolicyStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return s.count, s.err
}

func TestApplyJobRunPolicy_UsesStoreForSkipIfCompleted(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-job",
			Namespace: "default",
			Annotations: map[string]string{
				config.AnnotationJobRunPolicy: string(workflowconfig.JobRunPolicySkipIfCompleted),
			},
			Labels: map[string]string{
				config.LabelAppID:         "app-1",
				config.LabelComponentName: "demo",
			},
		},
	}

	store := &runPolicyStore{count: 1}
	action, err := ApplyJobRunPolicy(ctx, client, store, jobObj, config.JobDeployInstant)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action != RunPolicyActionSkip {
		t.Fatalf("expected skip action, got %v", action)
	}
}

func TestApplyJobRunPolicy_StoreErrorContinues(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-job",
			Namespace: "default",
			Annotations: map[string]string{
				config.AnnotationJobRunPolicy: string(workflowconfig.JobRunPolicySkipIfCompleted),
			},
			Labels: map[string]string{
				config.LabelAppID:         "app-1",
				config.LabelComponentName: "demo",
			},
		},
	}

	store := &runPolicyStore{err: errors.New("db error")}
	action, err := ApplyJobRunPolicy(ctx, client, store, jobObj, config.JobDeployInstant)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action != runPolicyActionCreate {
		t.Fatalf("expected create action, got %v", action)
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
			wantErr:       jobinfo.ErrJobExecutionIdentityChanged,
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
			store := &jobtest.WorkflowOwnershipStore{
				NoopStore: &jobtest.NoopStore{},
				TasksByID: map[string]model.WorkflowQueue{
					"task-old": {TaskID: "task-old", Status: tc.oldTaskStatus},
				},
			}
			if tc.storeErr != nil {
				store.Errs = []error{tc.storeErr}
			}
			client := fake.NewSimpleClientset(existing)

			action, err := ApplyJobRunPolicy(
				context.Background(),
				client,
				store,
				desired,
				config.JobDeployInstant,
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

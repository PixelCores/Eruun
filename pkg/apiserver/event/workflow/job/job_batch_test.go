package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestGenerateInstantJobSetsTTL(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "demo",
		AppID:     "app",
		Namespace: "default",
		Image:     "busybox",
	}
	props := &model.Properties{}

	result := GenerateInstantJob(component, props, "")
	require.NotNil(t, result)
	jobObj, ok := result.Service.(*batchv1.Job)
	require.True(t, ok)
	require.NotNil(t, jobObj.Spec.TTLSecondsAfterFinished)
	require.Equal(t, config.DefaultJobTTLSeconds, *jobObj.Spec.TTLSecondsAfterFinished)
	require.Equal(t, workflowconfig.DefaultWorkflowImagePullPolicy, jobObj.Spec.Template.Spec.Containers[0].ImagePullPolicy)
}

func TestGenerateScheduledCronJobSetsTTL(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "demo",
		AppID:     "app",
		Namespace: "default",
		Image:     "busybox",
	}
	props := &model.Properties{}

	result := GenerateScheduledCronJob(component, props, "0 * * * *")
	require.NotNil(t, result)
	cronObj, ok := result.Service.(*batchv1.CronJob)
	require.True(t, ok)
	require.NotNil(t, cronObj.Spec.JobTemplate.Spec.TTLSecondsAfterFinished)
	require.Equal(t, config.DefaultJobTTLSeconds, *cronObj.Spec.JobTemplate.Spec.TTLSecondsAfterFinished)
	require.NotNil(t, cronObj.Spec.SuccessfulJobsHistoryLimit)
	require.NotNil(t, cronObj.Spec.FailedJobsHistoryLimit)
	require.Equal(t, config.DefaultCronJobSuccessfulLimit, *cronObj.Spec.SuccessfulJobsHistoryLimit)
	require.Equal(t, config.DefaultCronJobFailedLimit, *cronObj.Spec.FailedJobsHistoryLimit)
	require.Equal(t, workflowconfig.DefaultWorkflowImagePullPolicy, cronObj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].ImagePullPolicy)
}

func TestGenerateScheduledCronJobOverridesHistoryLimit(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "demo",
		AppID:     "app",
		Namespace: "default",
		Image:     "busybox",
	}
	successLimit := int32(1)
	failedLimit := int32(2)
	props := &model.Properties{
		SuccessfulJobsHistoryLimit: &successLimit,
		FailedJobsHistoryLimit:     &failedLimit,
	}

	result := GenerateScheduledCronJob(component, props, "0 * * * *")
	require.NotNil(t, result)
	cronObj, ok := result.Service.(*batchv1.CronJob)
	require.True(t, ok)
	require.NotNil(t, cronObj.Spec.SuccessfulJobsHistoryLimit)
	require.NotNil(t, cronObj.Spec.FailedJobsHistoryLimit)
	require.Equal(t, successLimit, *cronObj.Spec.SuccessfulJobsHistoryLimit)
	require.Equal(t, failedLimit, *cronObj.Spec.FailedJobsHistoryLimit)
}

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
			expectAction: runPolicyActionSkip,
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
				if _, err := client.BatchV1().Jobs(namespace).Create(ctx, existing, metav1.CreateOptions{}); err != nil {
					t.Fatalf("create existing job: %v", err)
				}
			}

			action, err := applyJobRunPolicy(ctx, client, nil, newJob(tc.policy), config.JobDeployInstant)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				var statusErr *StatusError
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
			require.ErrorIs(t, err, errJobExecutionIdentityChanged)
			require.Empty(t, client.Actions())
		})
	}
}

func TestStartTimeFromJob(t *testing.T) {
	value, ok := startTimeFromJob(nil)
	require.False(t, ok)
	require.Zero(t, value)

	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}}
	value, ok = startTimeFromJob(job)
	require.False(t, ok)
	require.Zero(t, value)

	job.Annotations[config.AnnotationJobStartTime] = "invalid"
	value, ok = startTimeFromJob(job)
	require.False(t, ok)
	require.Zero(t, value)

	job.Annotations[config.AnnotationJobStartTime] = "123"
	value, ok = startTimeFromJob(job)
	require.True(t, ok)
	require.Equal(t, int64(123), value)
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
	jobObj := jobForPodFallback("job-pod-complete", nil)
	client := fake.NewSimpleClientset(jobObj, succeededPodForJob(jobObj, "job-pod-complete-abc", jobObj.UID))

	status, msg, err := waitForJobCompletion(context.Background(), client, "default", jobObj.Name)
	require.NoError(t, err)
	require.Equal(t, config.StatusCompleted, status)
	require.Equal(t, "", msg)
}

func TestWaitForJobCompletionWaitsForRequiredSucceededPods(t *testing.T) {
	completions := int32(2)
	jobObj := jobForPodFallback("job-two-completions", &completions)
	client := fake.NewSimpleClientset(jobObj, succeededPodForJob(jobObj, "job-two-completions-abc", jobObj.UID))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	status, _, err := waitForJobCompletion(ctx, client, "default", jobObj.Name)
	require.Error(t, err)
	require.Equal(t, config.StatusTimeout, status)
}

func TestWaitForJobCompletionIgnoresSucceededPodWithMismatchedOwner(t *testing.T) {
	jobObj := jobForPodFallback("job-owner-mismatch", nil)
	client := fake.NewSimpleClientset(jobObj, succeededPodForJob(jobObj, "job-owner-mismatch-abc", types.UID("other-job-uid")))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	status, _, err := waitForJobCompletion(ctx, client, "default", jobObj.Name)
	require.Error(t, err)
	require.Equal(t, config.StatusTimeout, status)
}

func jobForPodFallback(name string, completions *int32) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name + "-uid"),
		},
		Spec: batchv1.JobSpec{
			Completions: completions,
		},
	}
}

func succeededPodForJob(jobObj *batchv1.Job, name string, ownerUID types.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: jobObj.Namespace,
			Labels: map[string]string{
				batchv1.JobNameLabel: jobObj.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       jobObj.Name,
					UID:        ownerUID,
				},
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
}

type runPolicyStore struct {
	noopStore
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
	action, err := applyJobRunPolicy(ctx, client, store, jobObj, config.JobDeployInstant)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action != runPolicyActionSkip {
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
	action, err := applyJobRunPolicy(ctx, client, store, jobObj, config.JobDeployInstant)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action != runPolicyActionCreate {
		t.Fatalf("expected create action, got %v", action)
	}
}

type jobInfoStore struct {
	addCount  int
	addErr    error
	lastAdded datastore.Entity
}

func (s *jobInfoStore) Add(_ context.Context, entity datastore.Entity) error {
	s.addCount++
	s.lastAdded = entity
	return s.addErr
}

func (s *jobInfoStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (s *jobInfoStore) Put(context.Context, datastore.Entity) error        { return nil }
func (s *jobInfoStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (s *jobInfoStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (s *jobInfoStore) Get(context.Context, datastore.Entity) error { return nil }
func (s *jobInfoStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}
func (s *jobInfoStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (s *jobInfoStore) IsExist(context.Context, datastore.Entity) (bool, error) { return false, nil }
func (s *jobInfoStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *jobInfoStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

var _ datastore.DataStore = (*jobInfoStore)(nil)

func TestRunJob_SkippedWritesJobInfo(t *testing.T) {
	store := &jobInfoStore{}
	jobTask := &model.JobTask{
		Name:       "demo",
		Namespace:  "default",
		WorkflowID: "wf-1",
		ProjectID:  "proj-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDeploy),
		Status:     config.StatusSkipped,
	}

	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, nil)

	require.Equal(t, 1, store.addCount)
	info, ok := store.lastAdded.(*model.JobInfo)
	require.True(t, ok)
	require.Equal(t, string(config.StatusSkipped), info.Status)
	require.NotZero(t, info.StartTime)
	require.NotZero(t, info.EndTime)
}

type componentStatusStore struct {
	components     []*model.ApplicationComponent
	updated        *model.ApplicationComponent
	updates        []*model.ApplicationComponent
	statuses       []string
	jobInfos       []*model.JobInfo
	managementMode config.ManagementMode
	putErr         error
}

func (s *componentStatusStore) Add(_ context.Context, entity datastore.Entity) error {
	if jobInfo, ok := entity.(*model.JobInfo); ok {
		copied := *jobInfo
		s.jobInfos = append(s.jobInfos, &copied)
	}
	return nil
}
func (s *componentStatusStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (s *componentStatusStore) Put(_ context.Context, entity datastore.Entity) error {
	if s.putErr != nil {
		return s.putErr
	}
	if comp, ok := entity.(*model.ApplicationComponent); ok {
		s.updated = comp
		s.updates = append(s.updates, comp)
		s.statuses = append(s.statuses, comp.Status)
	}
	return nil
}
func (s *componentStatusStore) Delete(context.Context, datastore.Entity) error { return nil }
func (s *componentStatusStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (s *componentStatusStore) Get(_ context.Context, entity datastore.Entity) error {
	if app, ok := entity.(*model.Applications); ok {
		app.ManagementMode = s.managementMode
	}
	return nil
}
func (s *componentStatusStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	compQuery, ok := query.(*model.ApplicationComponent)
	if !ok {
		return nil, datastore.ErrRecordNotExist
	}
	var out []datastore.Entity
	for _, comp := range s.components {
		if compQuery.AppID != "" && comp.AppID != compQuery.AppID {
			continue
		}
		out = append(out, comp)
	}
	if len(out) == 0 {
		return nil, datastore.ErrRecordNotExist
	}
	return out, nil
}
func (s *componentStatusStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (s *componentStatusStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (s *componentStatusStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *componentStatusStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func (s *componentStatusStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if s.putErr != nil {
		return false, s.putErr
	}
	if _, ok := entity.(*model.ApplicationComponent); !ok {
		return false, nil
	}
	for _, comp := range s.components {
		if !componentMatchesConditions(comp, conditions) {
			continue
		}
		applyComponentRuntimeUpdateMap(comp, updates)
		copied := *comp
		s.updated = &copied
		s.updates = append(s.updates, &copied)
		s.statuses = append(s.statuses, copied.Status)
		return true, nil
	}
	return false, nil
}

func componentMatchesConditions(component *model.ApplicationComponent, conditions map[string]interface{}) bool {
	if component == nil {
		return false
	}
	if appID, ok := conditions["app_id"].(string); ok && component.AppID != appID {
		return false
	}
	if name, ok := conditions["name"].(string); ok && component.Name != name {
		return false
	}
	if id, ok := conditions["id"].(int); ok && component.ID != id {
		return false
	}
	return true
}

func applyComponentRuntimeUpdateMap(component *model.ApplicationComponent, updates map[string]interface{}) {
	if component == nil {
		return
	}
	for key, value := range updates {
		switch key {
		case "status":
			if status, ok := value.(string); ok {
				component.Status = status
			}
		case "ready_replicas":
			if readyReplicas, ok := value.(int32); ok {
				component.ReadyReplicas = readyReplicas
			}
		case "last_abnormal":
			if lastAbnormal, ok := value.(string); ok {
				component.LastAbnormal = lastAbnormal
			}
		}
	}
}

var _ datastore.DataStore = (*componentStatusStore)(nil)
var _ datastore.ConditionalCompareAndSwap = (*componentStatusStore)(nil)

func TestRunJob_ConfigMapUpdatesComponentStatus(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-1",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        "",
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, nil)

	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_ConfigMapSharedDefaultSkippedMapsRunning(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-1",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        string(config.ComponentStatusNotDeploy),
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		Status:    config.StatusSkipped,
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelShareName:     "shared-app-config",
					config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
				},
			},
		},
	}

	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, nil)

	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_ConfigMapSharedIgnoreSkippedDoesNotMapRunning(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-1",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        string(config.ComponentStatusNotDeploy),
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		Status:    config.StatusSkipped,
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelShareName:     "shared-app-config",
					config.LabelShareStrategy: string(domainspec.ShareStrategyIgnore),
				},
			},
		},
	}

	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, nil)

	require.Nil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.components[0].Status)
}

func TestRunJob_ConfigMapUpdatesComponentStatusWithWorkflowID(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "wf-1",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        "",
			},
		},
	}
	jobTask := &model.JobTask{
		Name:       "app-config",
		Namespace:  "default",
		WorkflowID: "wf-1",
		JobType:    string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, nil)

	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_ConfigMapDoesNotSetPending(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-2",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
				Status:        string(config.ComponentStatusNotDeploy),
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-2",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, nil)

	require.NotEmpty(t, store.statuses)
	for _, status := range store.statuses {
		require.NotEqual(t, string(config.ComponentStatusPending), status)
	}
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
}

func TestRunJob_ConfigMapInvalidatesComponentsCache(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-3",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
			},
		},
	}
	cacheStore := cacheutil.NewMemCache(false)
	cacheKey := cacheutil.ApplicationComponentsKey("app-3")
	require.NoError(t, cacheStore.Store(cacheKey, "stale"))
	require.True(t, cacheStore.Exists(cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-3",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runtime := newJobRuntime(cacheStore, nil, nil, nil, nil, nil)
	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, runtime)

	require.False(t, cacheStore.Exists(cacheKey))
	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
}

func TestRunJob_SecretFailureInvalidatesComponentsCache(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-4",
				Name:          "app-secret",
				Namespace:     "default",
				ComponentType: config.SecretJob,
			},
		},
	}
	cacheStore := cacheutil.NewMemCache(false)
	cacheKey := cacheutil.ApplicationComponentsKey("app-4")
	require.NoError(t, cacheStore.Store(cacheKey, "stale"))
	require.True(t, cacheStore.Exists(cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-secret",
		Namespace: "default",
		AppID:     "app-4",
		JobType:   string(config.JobDeploySecret),
		JobInfo:   "bad-job-info-type",
	}

	runtime := newJobRuntime(cacheStore, nil, nil, nil, nil, nil)
	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, runtime)

	require.False(t, cacheStore.Exists(cacheKey))
	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusFailed), store.updated.Status)
	require.NotEmpty(t, store.updated.LastAbnormal)
}

func TestRunJob_SecretSharedDefaultSkippedMapsRunning(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-4",
				Name:          "app-secret",
				Namespace:     "default",
				ComponentType: config.SecretJob,
				Status:        string(config.ComponentStatusNotDeploy),
			},
		},
	}
	jobTask := &model.JobTask{
		Name:      "app-secret",
		Namespace: "default",
		AppID:     "app-4",
		JobType:   string(config.JobDeploySecret),
		Status:    config.StatusSkipped,
		JobInfo: &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-secret",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelShareName:     "shared-app-secret",
					config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
				},
			},
		},
	}

	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, nil)

	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusRunning), store.updated.Status)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_ConfigMapDoesNotInvalidateCacheWhenStatusPersistFails(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-5",
				Name:          "app-config",
				Namespace:     "default",
				ComponentType: config.ConfJob,
			},
		},
		putErr: errors.New("persist failed"),
	}
	cacheStore := cacheutil.NewMemCache(false)
	cacheKey := cacheutil.ApplicationComponentsKey("app-5")
	require.NoError(t, cacheStore.Store(cacheKey, "stale"))
	require.True(t, cacheStore.Exists(cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-5",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}

	runtime := newJobRuntime(cacheStore, nil, nil, nil, nil, nil)
	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, runtime)

	require.True(t, cacheStore.Exists(cacheKey))
	require.Nil(t, store.updated)
}

func TestRunJob_DeployStartInvalidatesComponentsCache(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-6",
				Name:          "app-web",
				Namespace:     "default",
				ComponentType: config.ServerJob,
				Status:        string(config.ComponentStatusRunning),
				ReadyReplicas: 1,
			},
		},
	}
	cacheStore := cacheutil.NewMemCache(false)
	cacheKey := cacheutil.ApplicationComponentsKey("app-6")
	require.NoError(t, cacheStore.Store(cacheKey, "stale"))
	require.True(t, cacheStore.Exists(cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-web",
		Namespace: "default",
		AppID:     "app-6",
		JobType:   string(config.JobDeploy),
		JobInfo:   "bad-deploy-job-info",
	}

	runtime := newJobRuntime(cacheStore, nil, nil, nil, nil, nil)
	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, runtime)

	require.False(t, cacheStore.Exists(cacheKey))
	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusPending), store.updated.Status)
	require.Equal(t, int32(0), store.updated.ReadyReplicas)
	require.Equal(t, "", store.updated.LastAbnormal)
}

func TestRunJob_DeployStartDoesNotInvalidateCacheWhenStatusPersistFails(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{
			{
				AppID:         "app-7",
				Name:          "app-web",
				Namespace:     "default",
				ComponentType: config.ServerJob,
			},
		},
		putErr: errors.New("persist failed"),
	}
	cacheStore := cacheutil.NewMemCache(false)
	cacheKey := cacheutil.ApplicationComponentsKey("app-7")
	require.NoError(t, cacheStore.Store(cacheKey, "stale"))
	require.True(t, cacheStore.Exists(cacheKey))

	jobTask := &model.JobTask{
		Name:      "app-web",
		Namespace: "default",
		AppID:     "app-7",
		JobType:   string(config.JobDeploy),
		JobInfo:   "bad-deploy-job-info",
	}

	runtime := newJobRuntime(cacheStore, nil, nil, nil, nil, nil)
	runJob(context.Background(), jobTask, fake.NewSimpleClientset(), store, func() {}, runtime)

	require.True(t, cacheStore.Exists(cacheKey))
	require.Nil(t, store.updated)
}

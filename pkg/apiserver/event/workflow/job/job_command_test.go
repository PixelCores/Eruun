package job

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestWorkspaceJobTypesUseDurableStopCheckpoint(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobCommand, config.JobEval} {
		t.Run(string(jobType), func(t *testing.T) {
			task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
			task.JobType, task.WorkspaceID = string(jobType), "space"
			store := &retryCheckpointStore{}
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			installRetryJobReactor(t, client, 1, "OOMKilled", &created)
			ctl := newObservedInstantJobCtl(t, task, client, newRetryCreationBudgetStore(t, store), func() {})
			err := ctl.Run(WithCleanupTracker(context.Background()))
			require.ErrorContains(t, err, "job failed after attempt 1")
			require.Len(t, created, 1)
			require.Equal(t, string(jobType), store.record.Type)
			require.True(t, HasInstantJobRetryCheckpoint(store.record))
		})
	}
}

func TestWorkspaceJobCleanupWaitsForCommittedResult(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobCommand, config.JobEval} {
		t.Run(string(jobType), func(t *testing.T) {
			task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
			task.JobType, task.WorkspaceID = string(jobType), "space"
			live := task.JobInfo.(*batchv1.Job).DeepCopy()
			live.UID, live.ResourceVersion = "owned-job", "1"
			live.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			live.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			checkpoint := instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Attempt: 1,
				Deadline: time.Now().Add(time.Hour).UnixNano(), Job: live, CurrentUID: live.UID}
			if jobType == config.JobEval {
				checkpoint.Runner = json.RawMessage(`{"terminal":{"outcome":"succeeded","collectionComplete":true}}`)
			}
			raw, err := json.Marshal(checkpoint)
			require.NoError(t, err)
			task.InternalInfo, task.Status = string(raw), config.StatusCompleted
			task.JobInfo = live
			client := fake.NewSimpleClientset(live)
			finalizeCompletedJobIfNeeded(context.Background(), client, task)
			_, err = client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
			require.NoError(t, err, "finalization cannot delete before SaveInfo")
			store := &workspaceJobArtifactStore{}
			if jobType == config.JobEval {
				store.artifact = &model.JobArtifact{WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, ExecutionKey: task.ExecutionKey, Kind: "source", Summary: json.RawMessage(`{"collectionComplete":true}`)}
			}
			ctl := NewInstantJobCtl(task, client, store, func() {})
			require.NoError(t, ctl.SaveInfo(context.Background()))
			require.Equal(t, string(config.StatusCompleted), store.record.Status)
			_, err = client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
			require.Error(t, err)
		})
	}
}

type workspaceJobArtifactStore struct {
	retryCheckpointStore
	artifact    *model.JobArtifact
	artifactErr error
}

func (s *workspaceJobArtifactStore) List(ctx context.Context, entity datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	if _, ok := entity.(*model.JobArtifact); ok {
		if s.artifactErr != nil {
			return nil, s.artifactErr
		}
		if s.artifact != nil {
			return []datastore.Entity{s.artifact}, nil
		}
		return nil, nil
	}
	return s.retryCheckpointStore.List(ctx, entity, opts)
}

func TestEvaluationCleanupRetainsResultsUntilArchiveIsCommitted(t *testing.T) {
	for _, status := range []config.Status{config.StatusCompleted, config.StatusFailed, config.StatusCancelled, config.StatusTimeout} {
		t.Run(string(status), func(t *testing.T) {
			task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
			task.JobType, task.WorkspaceID, task.Status = string(config.JobEval), "space", status
			live := task.JobInfo.(*batchv1.Job).DeepCopy()
			live.UID, live.ResourceVersion = "owned-job", "1"
			live.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			outcome := "failed"
			if status == config.StatusCompleted {
				outcome = "succeeded"
			}
			raw, err := json.Marshal(instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Attempt: 1,
				Deadline: time.Now().Add(time.Hour).UnixNano(), Job: live, CurrentUID: live.UID,
				Runner: json.RawMessage(`{"terminal":{"outcome":"` + outcome + `","collectionComplete":true}}`)})
			require.NoError(t, err)
			task.InternalInfo, task.JobInfo = string(raw), live
			client := fake.NewSimpleClientset(live)
			store := &workspaceJobArtifactStore{}
			ctl := NewInstantJobCtl(task, client, store, func() {})
			ctl.Clean(context.Background())
			if status == config.StatusCancelled || status == config.StatusTimeout {
				require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
				return
			}
			require.Zero(t, countClientActions(client, "delete", "jobs"))
			store.artifact = &model.JobArtifact{WorkspaceID: "other", TaskID: task.TaskID, Kind: "source", Summary: json.RawMessage(`{"collectionComplete":true}`)}
			ctl.Clean(context.Background())
			require.Zero(t, countClientActions(client, "delete", "jobs"))
			store.artifact.WorkspaceID = task.WorkspaceID
			store.artifact.Summary = json.RawMessage(`{"collectionComplete":false}`)
			ctl.Clean(context.Background())
			require.Zero(t, countClientActions(client, "delete", "jobs"), "diagnostic-only archive cannot replace native outputs")
			store.artifact.Summary = json.RawMessage(`{"collectionComplete":true}`)
			ctl.Clean(context.Background())
			require.Zero(t, countClientActions(client, "delete", "jobs"), "complete outputs still require a committed execution result")
			require.NoError(t, ctl.SaveInfo(context.Background()))
			require.Zero(t, countClientActions(client, "delete", "jobs"), "another execution's archive cannot authorize cleanup")
			store.artifact.ExecutionKey = task.ExecutionKey
			require.NoError(t, ctl.SaveInfo(context.Background()))
			require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
		})
	}
}

func TestEvaluationExitWithoutArchiveIsFailedAndRetained(t *testing.T) {
	task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
	task.JobType, task.WorkspaceID = string(config.JobEval), "space"
	ttl := int32(90 * 86400)
	task.JobInfo.(*batchv1.Job).Spec.TTLSecondsAfterFinished = &ttl
	store := &workspaceJobArtifactStore{}
	client := fake.NewSimpleClientset()
	var created []*batchv1.Job
	installRetryJobReactor(t, client, 0, "", &created)
	ctl := newObservedInstantJobCtl(t, task, client, newRetryCreationBudgetStore(t, store), func() {})
	require.ErrorContains(t, ctl.Run(context.Background()), "without a collected result archive")
	require.Equal(t, config.StatusFailed, task.Status)
	require.Len(t, created, 1)
	require.Equal(t, ttl, *created[0].Spec.TTLSecondsAfterFinished)
	ctl.Clean(context.Background())
	require.Zero(t, countClientActions(client, "delete", "jobs"))
}

func TestWorkspaceJobPayloadCarriesRunnerExecutionIdentity(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobCommand, config.JobEval} {
		task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
		task.JobType = string(jobType)
		ApplyExecutionIdentity(task)
		payload := task.JobInfo.(*batchv1.Job)
		require.Equal(t, task.TaskID, payload.Spec.Template.Annotations[config.AnnotationJobTaskID])
		require.Equal(t, task.ExecutionKey, payload.Spec.Template.Annotations[config.AnnotationJobExecutionKey])
		require.Equal(t, "1", payload.Spec.Template.Annotations[config.AnnotationJobRunGeneration])
		stampJobExecutionIdentity(task, payload)
		require.Equal(t, payload.Annotations[config.AnnotationJobExecutionKey], payload.Spec.Template.Annotations[config.AnnotationJobExecutionKey])
	}
}

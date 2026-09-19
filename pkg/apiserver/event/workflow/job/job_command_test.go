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
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestBuildCommandJobUsesReferencesWithoutApplication(t *testing.T) {
	value := "hello"
	traits := spec.JobTraits{Resources: &spec.ResourceTraitsSpec{CPU: "1", Memory: "2Gi"},
		Envs: []spec.SimplifiedEnvSpec{{Name: "MESSAGE", ValueFrom: spec.ValueSource{Static: &value}},
			{Name: "TOKEN", ValueFrom: spec.ValueSource{Secret: &spec.SecretSelectorSpec{Name: "credentials", Key: "token"}}}},
		EnvFrom: []spec.EnvFromSourceSpec{{Type: "config", SourceName: "settings"}},
		Storage: []spec.StorageTraitSpec{{Name: "input", Type: "persistent", ClaimName: "uploaded-input", MountPath: "/input", ReadOnly: true}}}
	job, err := BuildCommandJob("command-task-1", "space", spec.CommandJobSpec{Image: "busybox:1.37.0", Command: []string{"echo"}, Args: []string{"hello"}, TimeoutSeconds: 60}, traits)
	require.NoError(t, err)
	require.Equal(t, "space", job.Namespace)
	require.Empty(t, job.Labels[config.LabelAppID])
	require.EqualValues(t, 0, *job.Spec.BackoffLimit)
	require.False(t, *job.Spec.Template.Spec.AutomountServiceAccountToken)
	require.Nil(t, job.Spec.TTLSecondsAfterFinished, "durable checkpoint establishes retention before creation")
	container := job.Spec.Template.Spec.Containers[0]
	require.Equal(t, "credentials", container.Env[1].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "settings", container.EnvFrom[0].ConfigMapRef.Name)
	require.Equal(t, "uploaded-input", job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName)
	require.Equal(t, "2Gi", container.Resources.Limits.Memory().String())
	policy, err := retryPolicyFromJob(job)
	require.NoError(t, err)
	require.Equal(t, "stop", policy.OnOOM)
	require.Zero(t, policy.MaxRetries)
}

// Application-only traits (sidecar, ingress, rollout, ...) are absent from
// spec.JobTraits, so they cannot reach this function at all. Their rejection is
// covered at the decode boundary in TestJobSpecRejectsApplicationOnlyTraits.
func TestBuildCommandJobRejectsAmbiguousOrUnsupportedTraits(t *testing.T) {
	literal := "value"
	for _, tc := range []struct {
		name   string
		traits spec.JobTraits
	}{
		{"dynamic storage", spec.JobTraits{Storage: []spec.StorageTraitSpec{{Name: "data", Type: "persistent", MountPath: "/data", TmpCreate: true}}}},
		{"host mount", spec.JobTraits{Storage: []spec.StorageTraitSpec{{Name: "host", Type: "host-mounted", MountPath: "/host"}}}},
		{"ambiguous env", spec.JobTraits{Envs: []spec.SimplifiedEnvSpec{{Name: "TOKEN", ValueFrom: spec.ValueSource{Static: &literal, Secret: &spec.SecretSelectorSpec{Name: "s", Key: "token"}}}}}},
		{"invalid secret", spec.JobTraits{EnvFrom: []spec.EnvFromSourceSpec{{Type: "secret", SourceName: "../other"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildCommandJob("command-task", "space", spec.CommandJobSpec{Image: "busybox:1.37.0", Command: []string{"true"}, TimeoutSeconds: 60}, tc.traits)
			require.Error(t, err)
		})
	}
}

func TestWorkspaceJobTypesUseDurableStopCheckpoint(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobCommand, config.JobEval} {
		t.Run(string(jobType), func(t *testing.T) {
			task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
			task.JobType, task.WorkspaceID = string(jobType), "space"
			store := &retryCheckpointStore{}
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			installRetryJobReactor(t, client, 1, "OOMKilled", &created)
			ctl := NewInstantJobCtl(task, client, store, func() {})
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
	ctl := NewInstantJobCtl(task, client, store, func() {})
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

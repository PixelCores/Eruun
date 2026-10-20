package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
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
			jobtest.InstallRetryJobReactor(t, client, 1, "OOMKilled", &created)
			ctl := newObservedInstantJobCtl(t, task, client, jobtest.NewRetryCreationBudgetStore(t, jobtest.WithJobTestOwner(store, task)), func() {})
			err := ctl.Run(jobexec.WithCleanupTracker(context.Background()))
			require.ErrorContains(t, err, "job failed after attempt 1")
			require.Len(t, created, 1)
			require.Equal(t, string(jobType), store.record.Type)
			require.True(t, jobinfo.HasInstantJobRetryCheckpoint(store.record))
		})
	}
}

func TestWorkspaceJobPayloadCarriesRunnerExecutionIdentity(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobCommand, config.JobEval} {
		task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
		task.JobType = string(jobType)
		jobinfo.ApplyExecutionIdentity(task)
		payload := task.JobInfo.(*batchv1.Job)
		require.Equal(t, task.TaskID, payload.Spec.Template.Annotations[config.AnnotationJobTaskID])
		require.Equal(t, task.ExecutionKey, payload.Spec.Template.Annotations[config.AnnotationJobExecutionKey])
		require.Equal(t, "1", payload.Spec.Template.Annotations[config.AnnotationJobRunGeneration])
		jobresult.StampJobExecutionIdentity(task, payload)
		require.Equal(t, payload.Annotations[config.AnnotationJobExecutionKey], payload.Spec.Template.Annotations[config.AnnotationJobExecutionKey])
	}
}

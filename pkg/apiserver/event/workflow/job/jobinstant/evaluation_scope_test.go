package jobinstant

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func evaluationScopeTask(t *testing.T, store *access.Store, appID string) *model.JobTask {
	t.Helper()
	lease := time.Now().Add(time.Hour)
	parent := &model.WorkflowQueue{TaskID: "parent", AppID: appID, WorkspaceID: "space", Status: config.StatusRunning,
		RunGeneration: 1, RunToken: "run-token", WorkerID: "worker", LeaseExpiresAt: &lease}
	if appID == "" {
		parent.Type = config.WorkflowTaskTypeJob
		parent.JobSpec = `{"name":"evaluation","stages":[{"name":"run","steps":[{"name":"evaluate","type":"job","traits":{"eval":{"env":"ack","agent":"oracle","taskPackageId":"11111111-1111-1111-1111-111111111111"}}}]}]}`
	} else {
		require.NoError(t, store.Add(context.Background(), &model.Applications{ID: appID, WorkspaceID: "space", Namespace: "space-ns"}))
	}
	require.NoError(t, store.Add(context.Background(), parent))
	backoff, automount := int32(0), false
	workload := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "evaluation-runner", Namespace: "space-ns", Annotations: map[string]string{
			config.AnnotationJobRunPolicy:           string(workflowconfig.JobRunPolicyRecreate),
			workflowconfig.AnnotationJobRetryPolicy: `{"onOOM":"stop"}`,
		}},
		Spec: batchv1.JobSpec{BackoffLimit: &backoff, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &automount,
			Containers: []corev1.Container{{Name: "job", Image: "example.com/runner:0.22.0", ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{"python", "/opt/eruun/runner.py"}}},
		}}},
	}
	if appID != "" {
		workload.Annotations[config.AnnotationComponentName] = "benchmark"
	}
	task := &model.JobTask{Name: workload.Name, Namespace: workload.Namespace, AppID: appID, WorkspaceID: "space",
		TaskID: parent.TaskID, JobType: string(config.JobEval), JobInfo: workload, Timeout: 60,
		ExecutionKey: "evaluation-execution", RunGeneration: 1, OwnerRunGeneration: 1, RunToken: parent.RunToken, WorkerID: parent.WorkerID,
		EvaluationInfo: `{"runnerToken":"private-evaluation-capability"}`}
	jobinfo.ApplyExecutionIdentity(task)
	return task
}

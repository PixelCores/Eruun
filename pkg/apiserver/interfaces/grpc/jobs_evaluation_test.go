package grpcapi

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestApplicationEvaluationResultsAreScopedToExecution(t *testing.T) {
	accounts, _ := grpcTestAccounts(t)
	store := accounts.Repo.Store
	artifactStore, err := artifacts.New(store, nil)
	require.NoError(t, err)
	server := &JobsServer{Jobs: &jobs.Service{Store: store, Artifacts: artifactStore}}
	ctx := account.WithScope(context.Background(), account.Scope{UserID: "user", WorkspaceID: "workspace", Namespace: "ns", Role: "owner"})
	task := &model.WorkflowQueue{TaskID: "workflow-task", WorkspaceID: "workspace", AppID: "app", Status: config.StatusCompleted}
	require.NoError(t, store.Add(ctx, task))
	for _, key := range []string{"eval-a", "eval-b"} {
		job := &model.JobTask{}
		require.NoError(t, jobs.SetEvaluationTraits(job, spec.JobTraits{Evaluation: &spec.EvaluationTraitSpec{
			Env: "ack", Agent: "oracle", TaskPackageID: "12345678-1234-1234-1234-123456789012",
		}}))
		require.NoError(t, store.Add(ctx, &model.JobInfo{
			TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, AppID: task.AppID,
			Type: string(config.JobEval), Status: string(config.StatusCompleted), ServiceName: key,
			ExecutionKey: &key, EvaluationInfo: job.EvaluationInfo,
		}))
		require.NoError(t, store.Add(ctx, &model.JobArtifact{
			ID: "artifact-" + key, WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, ExecutionKey: key,
			Kind: artifacts.KindSource, Summary: json.RawMessage(`{"collectionComplete":true}`),
		}))
		require.NoError(t, store.Add(ctx, &model.JobDelivery{
			ID: "delivery-" + key, WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, ExecutionKey: key,
			SourceID: "artifact-" + key, Target: "database", Mode: "full", State: artifacts.DeliverySucceeded,
		}))
	}

	for _, key := range []string{"eval-a", "eval-b"} {
		require.NoError(t, store.Add(ctx, &model.JobSandbox{ID: "sandbox-" + key, WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, ExecutionKey: key, TrialID: "trial-" + key, State: "retained", SandboxUID: "sandbox-uid-" + key, PodUID: "pod-uid-" + key, RunnerUID: "private-runner-uid", Image: "private-image:v1", RequestDigest: "private-digest"}))
	}
	for _, key := range []string{"eval-a", "eval-b"} {
		t.Run(key, func(t *testing.T) {
			req := &eruunv1.JobTaskRequest{TaskId: task.TaskID, ExecutionKey: key}
			result, err := server.GetJobResults(ctx, req)
			require.NoError(t, err)
			require.Equal(t, key, result.ExecutionKey)
			require.Equal(t, "collected", result.CollectionState)
			require.Len(t, result.Artifacts, 1)
			require.Equal(t, key, result.Artifacts[0].ExecutionKey)
			require.Len(t, result.Deliveries, 1)
			require.Equal(t, key, result.Deliveries[0].ExecutionKey)
			detail, err := server.GetJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, key, detail.ExecutionKey)
			require.Equal(t, spec.HarborVersion, detail.FrameworkVersion)
			require.Equal(t, key, detail.Job.Name)
			require.Equal(t, "job", detail.Job.Type)
			require.Equal(t, "oracle", detail.Job.Traits.Eval.Agent)
			require.Len(t, detail.Executions, 1)
			require.Len(t, detail.Sandboxes, 1)
			require.False(t, detail.SandboxesTruncated)
			require.Equal(t, "trial-"+key, detail.Sandboxes[0].TrialId)
			require.Equal(t, "sandbox-uid-"+key, detail.Sandboxes[0].SandboxUid)
			encoded, err := json.Marshal(detail)
			require.NoError(t, err)
			for _, private := range []string{"private-runner-uid", "private-image", "private-digest"} {
				require.NotContains(t, string(encoded), private)
			}
		})
	}

	for index := 0; index < 101; index++ {
		require.NoError(t, store.Add(ctx, &model.JobSandbox{ID: fmt.Sprintf("history-%03d", index), TrialID: fmt.Sprintf("history-%03d", index), WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, ExecutionKey: "eval-a", State: "released"}))
	}
	bounded, err := server.GetJob(ctx, &eruunv1.JobTaskRequest{TaskId: task.TaskID, ExecutionKey: "eval-a"})
	require.NoError(t, err)
	require.Len(t, bounded.Sandboxes, 100)
	require.True(t, bounded.SandboxesTruncated)
	unaffected, err := server.GetJob(ctx, &eruunv1.JobTaskRequest{TaskId: task.TaskID, ExecutionKey: "eval-b"})
	require.NoError(t, err)
	require.Len(t, unaffected.Sandboxes, 1)
	require.False(t, unaffected.SandboxesTruncated)

	for _, key := range []string{"", "unknown"} {
		_, err := server.GetJobResults(ctx, &eruunv1.JobTaskRequest{TaskId: task.TaskID, ExecutionKey: key})
		require.Equal(t, codes.NotFound, status.Code(err))
	}
	foreign := account.WithScope(context.Background(), account.Scope{WorkspaceID: "other", Namespace: "other-ns", Role: "owner"})
	_, err = server.GetJobResults(foreign, &eruunv1.JobTaskRequest{TaskId: task.TaskID, ExecutionKey: "eval-a"})
	require.Equal(t, codes.NotFound, status.Code(err))

	// A valid artifact from the same parent task must not escape its execution.
	download := &fakeDatasetDownloadStream{ctx: ctx}
	err = server.DownloadJobResult(&eruunv1.JobArtifactRequest{
		TaskId: task.TaskID, ExecutionKey: "eval-a", ArtifactId: "artifact-eval-b",
	}, download)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Empty(t, download.chunks)

	// Application cancellation stays in the workflow API even with an evaluation key.
	_, err = server.CancelJob(ctx, &eruunv1.JobTaskRequest{TaskId: task.TaskID})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = server.CancelJob(ctx, &eruunv1.JobTaskRequest{TaskId: task.TaskID, ExecutionKey: "eval-a"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

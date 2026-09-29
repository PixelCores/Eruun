package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type executionRecordingStore struct {
	artifacts.Backend
	taskReads        int
	executionQueries []*datastore.ListOptions
	returnedRows     []int
}

func (s *executionRecordingStore) Get(ctx context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.taskReads++
	}
	return s.Backend.Get(ctx, entity)
}

func (s *executionRecordingStore) List(ctx context.Context, entity datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	rows, err := s.Backend.List(ctx, entity, options)
	if _, ok := entity.(*model.JobInfo); ok {
		s.executionQueries = append(s.executionQueries, options)
		s.returnedRows = append(s.returnedRows, len(rows))
	}
	return rows, err
}

func TestResolveResultTaskQueriesOneAuthorizedExecutionSet(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		keys                         []*string
		requested, wantKey           string
		app, command, wrongWorkspace bool
		wantErr                      error
		wantQueries                  int
	}{
		{name: "none", wantQueries: 1},
		{name: "one", keys: []*string{ptr.To("execution")}, wantKey: "execution", wantQueries: 1},
		{name: "many need key", keys: []*string{ptr.To("first"), ptr.To("second")}, wantErr: bcode.ErrJobInput, wantQueries: 1},
		{name: "explicit recovered", keys: []*string{ptr.To("first"), ptr.To("recovered")}, requested: "recovered", wantKey: "recovered", wantQueries: 1},
		{name: "null identities ignored", keys: []*string{nil, nil, ptr.To("execution")}, wantKey: "execution", wantQueries: 1},
		{name: "historical empty identity", keys: []*string{nil, ptr.To("")}, wantQueries: 1},
		{name: "empty identity still counts", keys: []*string{ptr.To(""), ptr.To("execution")}, wantErr: bcode.ErrJobInput, wantQueries: 1},
		{name: "wrong key", keys: []*string{ptr.To("execution")}, requested: "missing", wantErr: bcode.ErrNotFound, wantQueries: 1},
		{name: "key is not trimmed", keys: []*string{ptr.To("execution ")}, requested: "execution ", wantKey: "execution ", wantQueries: 1},
		{name: "key comparison is literal", keys: []*string{ptr.To("execution ")}, requested: "execution", wantErr: bcode.ErrNotFound, wantQueries: 1},
		{name: "command", command: true, wantQueries: 1},
		{name: "command rejects execution key", command: true, requested: "command-0", wantErr: bcode.ErrNotFound, wantQueries: 1},
		{name: "application needs key", app: true, keys: []*string{ptr.To("execution")}, wantErr: bcode.ErrNotFound},
		{name: "application explicit key", app: true, keys: []*string{ptr.To("execution")}, requested: "execution", wantKey: "execution", wantQueries: 1},
		{name: "workspace rejected before execution query", wrongWorkspace: true, keys: []*string{ptr.To("execution")}, requested: "execution", wantErr: bcode.ErrForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, raw, ctx := testJobService(t)
			declaration := evaluationDeclaration("evaluation", "12345678-1234-1234-1234-123456789012", "oracle", "")
			if tc.command {
				declaration = commandRequest().JobSpec
			}
			snapshot, err := json.Marshal(declaration)
			require.NoError(t, err)
			task := &model.WorkflowQueue{TaskID: "task", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob, JobSpec: string(snapshot)}
			if tc.app {
				task.AppID = "app"
				task.Type = config.WorkflowTaskTypeWorkflow
				require.NoError(t, raw.Add(ctx, &model.Applications{ID: task.AppID, WorkspaceID: task.WorkspaceID, Namespace: "space-ns"}))
			}
			require.NoError(t, raw.Add(ctx, task))
			// Unrelated command rows come first and must be filtered before LIMIT.
			for i := 0; i < 8; i++ {
				require.NoError(t, raw.Add(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Type: string(config.JobCommand), ExecutionKey: ptr.To(fmt.Sprintf("command-%d", i))}))
			}
			for _, key := range tc.keys {
				require.NoError(t, raw.Add(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, AppID: task.AppID, Type: string(config.JobEval), ExecutionKey: key}))
			}
			if tc.wrongWorkspace {
				ctx = account.WithScope(ctx, account.Scope{WorkspaceID: "other", Namespace: "other-ns", Role: "member"})
			}
			recording := &executionRecordingStore{Backend: service.Store}
			service.Store = recording
			selected, key, err := service.ResolveResultTask(ctx, task.TaskID, tc.requested)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, selected)
			} else {
				require.NoError(t, err)
				require.Equal(t, task.TaskID, selected.TaskID)
				require.Equal(t, tc.wantKey, key)
			}
			require.Equal(t, 1, recording.taskReads)
			require.Len(t, recording.executionQueries, tc.wantQueries)
			if tc.wantQueries == 0 {
				return
			}
			query := recording.executionQueries[0]
			require.Contains(t, query.In, datastore.InQueryOption{Key: "type", Values: []string{string(config.JobEval)}})
			if tc.requested == "" {
				require.Equal(t, 2, query.PageSize)
				require.Contains(t, query.NotEqual, datastore.ComparisonQueryOption{Key: "execution_key", Value: nil})
				require.LessOrEqual(t, recording.returnedRows[0], 2)
			} else {
				require.Contains(t, query.In, datastore.InQueryOption{Key: "execution_key", Values: []string{tc.requested}})
			}
		})
	}
}

func TestGetEvaluationSelectsExecutionOnce(t *testing.T) {
	service, raw, ctx := testJobService(t)
	job := &model.JobTask{}
	require.NoError(t, SetEvaluationTraits(job, spec.JobTraits{Evaluation: &spec.EvaluationTraitSpec{Env: "ack", Agent: "oracle", TaskPackageID: "12345678-1234-1234-1234-123456789012"}}))
	declaration, err := json.Marshal(evaluationDeclaration("evaluation", "12345678-1234-1234-1234-123456789012", "oracle", ""))
	require.NoError(t, err)
	task := &model.WorkflowQueue{TaskID: "task", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob, JobSpec: string(declaration)}
	require.NoError(t, raw.Add(ctx, task))
	require.NoError(t, raw.Add(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Type: string(config.JobEval), ExecutionKey: ptr.To("execution"), EvaluationInfo: job.EvaluationInfo}))
	recording := &executionRecordingStore{Backend: service.Store}
	service.Store = recording
	detail, err := service.Get(ctx, task.TaskID, "execution")
	require.NoError(t, err)
	require.Equal(t, "execution", detail.ExecutionKey)
	require.Len(t, detail.Executions, 1)
	require.Equal(t, 1, recording.taskReads)
	require.Len(t, recording.executionQueries, 1)
}

package grpcapi

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"k8s.io/utils/ptr"
)

func TestJobDefinitionRPCsAndRunPagination(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "job-definitions.db")), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(&model.Workspace{}, &model.JobDefinition{}, &model.WorkflowQueue{}))
	store := &sqlstore.Driver{Client: *db}
	service, err := jobs.New(store, nil, &config.Config{Accounts: &spec.AccountConfig{}})
	require.NoError(t, err)
	server := &JobsServer{Jobs: service}
	ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", UserID: "member", Role: "member"})
	require.NoError(t, store.Add(ctx, &model.Workspace{ID: "space", Namespace: "space-ns"}))
	command, err := structpb.NewValue(map[string]any{"image": "busybox:1.37.0", "command": []any{"true"}})
	require.NoError(t, err)
	input := &eruunv1.JobSpec{Name: "daily", Stages: []*eruunv1.JobStage{{Name: "run", Steps: []*eruunv1.JobStep{{
		Name: "command", Type: "command", Spec: command, Retry: &eruunv1.JobStepRetry{BackoffLimit: 2},
	}}}}}
	schedule := &eruunv1.JobScheduleRequest{Cron: "0 2 * * *", Enabled: true}
	definition, err := server.CreateJobDefinition(ctx, &eruunv1.CreateJobDefinitionRequest{Name: input.Name, Volume: input.Volume, Stages: input.Stages, Schedule: schedule})
	require.NoError(t, err)
	require.Len(t, definition.Id, 24)
	require.EqualValues(t, 1, definition.Revision)
	require.Equal(t, "member", definition.Creator)
	require.NotNil(t, definition.CreateTime)
	require.Equal(t, "0 2 * * *", definition.Schedule.Cron)
	require.Equal(t, "Forbid", definition.Schedule.ConcurrencyPolicy)
	require.True(t, definition.Schedule.Enabled)
	require.Positive(t, definition.Schedule.NextRun)
	require.Equal(t, "busybox:1.37.0", definition.Spec.Stages[0].Steps[0].Spec.GetStructValue().Fields["image"].GetStringValue())
	require.Equal(t, "true", definition.Spec.Stages[0].Steps[0].Spec.GetStructValue().Fields["command"].GetListValue().Values[0].GetStringValue())
	require.EqualValues(t, 2, definition.Spec.Stages[0].Steps[0].Retry.BackoffLimit)
	_, err = server.CreateJobDefinition(ctx, &eruunv1.CreateJobDefinitionRequest{Name: input.Name, Volume: input.Volume, Stages: input.Stages})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	future := time.Now().Add(time.Hour).Unix()
	run, err := server.RunJobDefinition(ctx, &eruunv1.RunJobDefinitionRequest{DefinitionId: definition.Id, ExecuteAt: future})
	require.NoError(t, err)
	require.Equal(t, "job", run.Type)
	require.Equal(t, string(config.StatusWaiting), run.Status)
	_, err = server.RunJobDefinition(ctx, &eruunv1.RunJobDefinitionRequest{DefinitionId: definition.Id, ExecuteAt: -1})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	input.Name = "updated"
	updated, err := server.UpdateJobDefinition(ctx, &eruunv1.UpdateJobDefinitionRequest{DefinitionId: definition.Id, Spec: input})
	require.NoError(t, err)
	require.EqualValues(t, 2, updated.Revision)
	require.Equal(t, "updated", updated.Spec.Name)
	require.Nil(t, updated.Schedule, "replacement without schedule must clear it")
	schedule.ConcurrencyPolicy = "Replace"
	updated, err = server.UpdateJobDefinition(ctx, &eruunv1.UpdateJobDefinitionRequest{DefinitionId: definition.Id, Spec: input, Schedule: schedule})
	require.NoError(t, err)
	require.Equal(t, "Replace", updated.Schedule.ConcurrencyPolicy)
	require.True(t, updated.Schedule.Enabled)
	require.Positive(t, updated.Schedule.NextRun)
	read, err := server.GetJobDefinition(ctx, &eruunv1.JobDefinitionRequest{DefinitionId: definition.Id})
	require.NoError(t, err)
	require.True(t, proto.Equal(updated, read))
	definitions, err := server.ListJobDefinitions(ctx, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, definitions.Total)
	require.EqualValues(t, 1, definitions.Page)
	require.EqualValues(t, 20, definitions.PageSize)
	require.True(t, proto.Equal(updated, definitions.Items[0]))

	oneOff, err := server.SubmitJob(ctx, &eruunv1.SubmitJobRequest{Name: "one-off", Stages: input.Stages, ExecuteAt: future + 1})
	require.NoError(t, err)
	_, err = server.SubmitJob(ctx, &eruunv1.SubmitJobRequest{Name: "invalid", Stages: input.Stages, ExecuteAt: -1})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	filtered, err := server.ListJobs(ctx, &eruunv1.ListJobsRequest{DefinitionId: definition.Id})
	require.NoError(t, err)
	require.EqualValues(t, 1, filtered.Total)
	require.Len(t, filtered.Items, 1)
	require.Equal(t, run.TaskId, filtered.Items[0].Accepted.TaskId)
	require.Equal(t, "daily", filtered.Items[0].Name)
	require.Equal(t, definition.Id, filtered.Items[0].JobDefinitionId)
	require.EqualValues(t, 1, filtered.Items[0].JobDefinitionRevision)
	require.Equal(t, future, filtered.Items[0].ExecuteAt)
	require.Equal(t, "member", filtered.Items[0].Creator)
	require.NotNil(t, filtered.Items[0].CreateTime)
	page, err := server.ListJobs(ctx, &eruunv1.ListJobsRequest{Page: ptr.To(int32(1)), PageSize: ptr.To(int32(1))})
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Total)
	require.EqualValues(t, 1, page.PageSize)
	require.Len(t, page.Items, 1)
	require.Equal(t, oneOff.TaskId, page.Items[0].Accepted.TaskId)
	require.Equal(t, future+1, page.Items[0].ExecuteAt)
	page, err = server.ListJobs(ctx, &eruunv1.ListJobsRequest{Page: ptr.To(int32(2)), PageSize: ptr.To(int32(1))})
	require.NoError(t, err)
	require.Equal(t, run.TaskId, page.Items[0].Accepted.TaskId)
	_, err = server.ListJobs(ctx, &eruunv1.ListJobsRequest{Page: ptr.To(int32(0))})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = server.ListJobDefinitions(ctx, &eruunv1.ListJobDefinitionsRequest{PageSize: ptr.To(int32(101))})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	foreign := account.WithScope(context.Background(), account.Scope{WorkspaceID: "other", Namespace: "other-ns", Role: "member"})
	_, err = server.GetJobDefinition(foreign, &eruunv1.JobDefinitionRequest{DefinitionId: definition.Id})
	require.Equal(t, codes.NotFound, status.Code(err))
	viewer := account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "viewer"})
	_, err = server.RunJobDefinition(viewer, &eruunv1.RunJobDefinitionRequest{DefinitionId: definition.Id})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = server.DeleteJobDefinition(ctx, &eruunv1.JobDefinitionRequest{DefinitionId: definition.Id})
	require.NoError(t, err)
	_, err = server.GetJobDefinition(ctx, &eruunv1.JobDefinitionRequest{DefinitionId: definition.Id})
	require.Equal(t, codes.NotFound, status.Code(err))
	filtered, err = server.ListJobs(ctx, &eruunv1.ListJobsRequest{DefinitionId: definition.Id})
	require.NoError(t, err)
	require.EqualValues(t, 1, filtered.Total)
	require.Equal(t, definition.Id, filtered.Items[0].JobDefinitionId)
}

func TestJobDefinitionRPCsRejectMissingInput(t *testing.T) {
	server := &JobsServer{}
	ctx := context.Background()
	_, err := server.CreateJobDefinition(ctx, nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = server.GetJobDefinition(ctx, &eruunv1.JobDefinitionRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = server.UpdateJobDefinition(ctx, &eruunv1.UpdateJobDefinitionRequest{DefinitionId: "definition"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = server.DeleteJobDefinition(ctx, nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = server.RunJobDefinition(ctx, &eruunv1.RunJobDefinitionRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestJobRPCSchedulesRemainDefinitionOnly(t *testing.T) {
	server := &JobsServer{}
	ctx := context.Background()
	for _, request := range []proto.Message{&eruunv1.SubmitJobRequest{}, &eruunv1.RunJobDefinitionRequest{}} {
		require.Error(t, protojson.Unmarshal([]byte(`{"schedule":{"cron":"* * * * *","enabled":true}}`), request))
	}
	// Binary protobuf unknown fields also remain invalid instead of silently
	// accepting a future or forged schedule field on execution requests.
	submit := &eruunv1.SubmitJobRequest{}
	submit.ProtoReflect().SetUnknown([]byte{0x52, 0x00})
	_, err := server.SubmitJob(ctx, submit)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	run := &eruunv1.RunJobDefinitionRequest{DefinitionId: "definition"}
	run.ProtoReflect().SetUnknown([]byte{0x1a, 0x00})
	_, err = server.RunJobDefinition(ctx, run)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	schedule := &eruunv1.JobScheduleRequest{}
	schedule.ProtoReflect().SetUnknown([]byte{0x20, 0x01})
	_, err = server.CreateJobDefinition(ctx, &eruunv1.CreateJobDefinitionRequest{Schedule: schedule})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = server.UpdateJobDefinition(ctx, &eruunv1.UpdateJobDefinitionRequest{DefinitionId: "definition", Spec: &eruunv1.JobSpec{}, Schedule: schedule})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

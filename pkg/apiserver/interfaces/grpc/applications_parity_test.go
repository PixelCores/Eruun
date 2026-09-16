package grpcapi

import (
	"context"
	"errors"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type createExecApplicationFake struct {
	service.ApplicationsService
	created *apis.ApplicationBase
	marked  bool
	called  bool
}

func (f *createExecApplicationFake) CreateApplications(context.Context, apis.CreateApplicationsRequest) (*apis.ApplicationBase, error) {
	f.called = true
	return f.created, nil
}
func (f *createExecApplicationFake) MarkInitialDeployingWorkflowComponents(context.Context, string, string) error {
	f.marked = true
	return nil
}

type createExecWorkflowFake struct {
	service.WorkflowService
	result *apis.ExecWorkflowResponse
	err    error
}

func (f *createExecWorkflowFake) ExecWorkflowTaskForApp(context.Context, string, string, int64, string) (*apis.ExecWorkflowResponse, error) {
	return f.result, f.err
}

func TestGRPCCreateAndExecSharedOrchestration(t *testing.T) {
	t.Run("successful execution", func(t *testing.T) {
		app := &createExecApplicationFake{created: &apis.ApplicationBase{ID: "app-a", WorkflowID: "wf-a"}}
		workflow := &createExecWorkflowFake{result: &apis.ExecWorkflowResponse{TaskID: "task-a", Status: "queued"}}
		s := &ApplicationsServer{Applications: app, Workflow: workflow}
		resp, err := s.CreateAndExecApplications(context.Background(), &eruunv1.AppDTOCreateAndExecApplicationRequest{Name: "sample", WorkflowId: "wf-a"})
		require.NoError(t, err)
		require.Equal(t, "app-a", resp.Application.Id)
		require.Equal(t, "wf-a", resp.WorkflowId)
		require.Equal(t, "task-a", resp.TaskId)
		require.Equal(t, apis.CreateAndExecStatusQueued, resp.ExecStatus)
		require.Len(t, resp.AllowedActions, 1)
		require.True(t, app.marked)
	})
	t.Run("partial failure redacts domain error", func(t *testing.T) {
		app := &createExecApplicationFake{created: &apis.ApplicationBase{ID: "app-b", WorkflowID: "wf-b"}}
		workflow := &createExecWorkflowFake{err: errors.New("password=secret request body")}
		s := &ApplicationsServer{Applications: app, Workflow: workflow}
		resp, err := s.CreateAndExecApplications(context.Background(), &eruunv1.AppDTOCreateAndExecApplicationRequest{Name: "sample"})
		require.NoError(t, err)
		require.Equal(t, apis.CreateAndExecStatusFailed, resp.ExecStatus)
		require.Equal(t, "workflow execution failed", resp.ExecError)
		require.NotContains(t, resp.ExecError, "password")
		require.False(t, app.marked)
	})
}

func TestGRPCCreateAndExecRejectsInvalidRequestBeforeCreate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		appName    string
		workflowID string
	}{
		{name: "invalid application name", appName: "Invalid Name"},
		{name: "invalid workflow ID", appName: "sample", workflowID: "Bad ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := &createExecApplicationFake{created: &apis.ApplicationBase{ID: "app-a", WorkflowID: "wf-a"}}
			s := &ApplicationsServer{Applications: app, Workflow: &createExecWorkflowFake{}}
			_, err := s.CreateAndExecApplications(context.Background(), &eruunv1.AppDTOCreateAndExecApplicationRequest{
				Name: tc.appName, WorkflowId: tc.workflowID,
			})
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.False(t, app.called)
		})
	}
}

package service

import (
	"context"
	"errors"
	"testing"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/stretchr/testify/require"
)

type createExecAppStub struct {
	ApplicationsService
	created *apis.ApplicationBase
	err     error
	marked  bool
}

func (s *createExecAppStub) CreateApplications(context.Context, apis.CreateApplicationsRequest) (*apis.ApplicationBase, error) {
	return s.created, s.err
}
func (s *createExecAppStub) MarkInitialDeployingWorkflowComponents(context.Context, string, string) error {
	s.marked = true
	return nil
}

type createExecWorkflowStub struct {
	WorkflowService
	result *apis.ExecWorkflowResponse
	err    error
	called bool
}

func (s *createExecWorkflowStub) ExecWorkflowTaskForApp(context.Context, string, string, int64, string) (*apis.ExecWorkflowResponse, error) {
	s.called = true
	return s.result, s.err
}

func TestExecuteCreateAndExecApplicationSharedOutcomes(t *testing.T) {
	message := func(error) string { return "safe failure" }
	tests := []struct {
		name         string
		app          *createExecAppStub
		workflow     *createExecWorkflowStub
		input        apis.CreateAndExecApplicationRequest
		wantErr      bool
		wantStatus   string
		wantTask     string
		wantMarked   bool
		wantExecCall bool
	}{
		{
			name: "creation failure", app: &createExecAppStub{err: errors.New("create failed")},
			workflow: &createExecWorkflowStub{}, wantErr: true,
		},
		{
			name:       "missing workflow is partial success",
			app:        &createExecAppStub{created: &apis.ApplicationBase{ID: "app-a"}},
			workflow:   &createExecWorkflowStub{},
			wantStatus: apis.CreateAndExecStatusFailed,
		},
		{
			name:       "execution failure is partial success",
			app:        &createExecAppStub{created: &apis.ApplicationBase{ID: "app-b", WorkflowID: "wf-b"}},
			workflow:   &createExecWorkflowStub{err: errors.New("sensitive failure")},
			wantStatus: apis.CreateAndExecStatusFailed, wantExecCall: true,
		},
		{
			name:       "success marks initial deployment",
			app:        &createExecAppStub{created: &apis.ApplicationBase{ID: "app-c", WorkflowID: "wf-c"}},
			workflow:   &createExecWorkflowStub{result: &apis.ExecWorkflowResponse{TaskID: "task-c", Status: "queued"}},
			wantStatus: apis.CreateAndExecStatusQueued, wantTask: "task-c",
			wantMarked: true, wantExecCall: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := ExecuteCreateAndExecApplication(context.Background(), tc.app, tc.workflow, tc.input, "", message)
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, resp.ExecStatus)
			require.Equal(t, tc.wantTask, resp.TaskID)
			require.Equal(t, tc.wantMarked, tc.app.marked)
			require.Equal(t, tc.wantExecCall, tc.workflow.called)
			if tc.wantStatus == apis.CreateAndExecStatusFailed {
				require.Equal(t, "safe failure", resp.ExecError)
			}
		})
	}
}

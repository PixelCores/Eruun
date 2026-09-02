package api

import (
	"context"
	"encoding/json"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	"github.com/stretchr/testify/require"
	"testing"
)

type fakeWorkflowService struct {
	execResp            *apis.ExecWorkflowResponse
	execErr             error
	execForAppCalled    bool
	lastExecAppID       string
	lastExecWorkflowID  string
	lastExecExecuteAt   int64
	scheduleUpsertResp  *apis.UpsertWorkflowScheduleResponse
	scheduleUpsertErr   error
	scheduleUpsertReq   apis.UpsertWorkflowScheduleRequest
	scheduleUpsertApp   string
	scheduleListResp    []apis.WorkflowSchedule
	scheduleListErr     error
	scheduleListApp     string
	scheduleDeleteErr   error
	scheduleDeleteApp   string
	scheduleDeleteID    string
	cancelForAppCalled  bool
	cancelForAppErr     error
	cancelAllCalled     bool
	cancelAllResp       []string
	cancelAllErr        error
	cancelDelayedCalled bool
	lastCancelAppID     string
	lastCancelUser      string
	lastCancelReason    string
	lastCancelTaskID    string
	cancelDelayedErr    error
	cancelCalled        bool
	lastUser            string
	lastReason          string
	lastApprovalTaskID  string
	lastApprovalAction  string
	approveCalled       bool
	approveErr          error
	approveResp         *apis.TaskApprovalResponse
	taskStatusResp      *apis.TaskStatusResponse
	taskStagesResp      *apis.TaskStagesResponse
}

func (f *fakeWorkflowService) CreateWorkflowTask(context.Context, apis.CreateWorkflowRequest) (*apis.CreateWorkflowResponse, error) {
	return nil, nil
}

func (f *fakeWorkflowService) ExecWorkflowTask(context.Context, string, int64) (*apis.ExecWorkflowResponse, error) {
	return nil, nil
}

func (f *fakeWorkflowService) ExecWorkflowTaskForApp(_ context.Context, appID, workflowID string, executeAt int64) (*apis.ExecWorkflowResponse, error) {
	f.execForAppCalled = true
	f.lastExecAppID = appID
	f.lastExecWorkflowID = workflowID
	f.lastExecExecuteAt = executeAt
	if f.execErr != nil {
		return nil, f.execErr
	}
	if f.execResp == nil {
		f.execResp = &apis.ExecWorkflowResponse{TaskID: "test-task"}
	}
	return f.execResp, nil
}

func (f *fakeWorkflowService) WaitingTasks(context.Context) ([]*model.WorkflowQueue, error) {
	return nil, nil
}

func (f *fakeWorkflowService) UpdateTask(context.Context, *model.WorkflowQueue) bool { return true }

func (f *fakeWorkflowService) TaskRunning(context.Context) ([]*model.WorkflowQueue, error) {
	return nil, nil
}

func (f *fakeWorkflowService) CancelWorkflowTask(ctx context.Context, userName, taskID, reason string) error {
	f.cancelCalled = true
	f.lastUser = userName
	f.lastReason = reason
	f.lastCancelTaskID = taskID
	return nil
}

func (f *fakeWorkflowService) CancelWorkflowTaskForApp(_ context.Context, appID, userName, taskID, reason string) error {
	f.cancelForAppCalled = true
	f.lastCancelAppID = appID
	f.lastCancelUser = userName
	f.lastCancelTaskID = taskID
	f.lastCancelReason = reason
	return f.cancelForAppErr
}

func (f *fakeWorkflowService) CancelAllWorkflowTasksForApp(_ context.Context, appID, userName, reason string) ([]string, error) {
	f.cancelAllCalled = true
	f.lastCancelAppID = appID
	f.lastCancelUser = userName
	f.lastCancelReason = reason
	if f.cancelAllErr != nil {
		return nil, f.cancelAllErr
	}
	if f.cancelAllResp == nil {
		return []string{}, nil
	}
	return f.cancelAllResp, nil
}

func (f *fakeWorkflowService) CancelDelayedVersionTaskForApp(_ context.Context, appID, userName, taskID, reason string) error {
	f.cancelDelayedCalled = true
	f.lastCancelAppID = appID
	f.lastCancelUser = userName
	f.lastCancelTaskID = taskID
	f.lastCancelReason = reason
	return f.cancelDelayedErr
}

func (f *fakeWorkflowService) ApproveWorkflowTask(_ context.Context, taskID, action, userName, reason string) (*apis.TaskApprovalResponse, error) {
	f.approveCalled = true
	f.lastApprovalTaskID = taskID
	f.lastApprovalAction = action
	f.lastCancelUser = userName
	f.lastCancelReason = reason
	if f.approveErr != nil {
		return nil, f.approveErr
	}
	if f.approveResp != nil {
		return f.approveResp, nil
	}
	return &apis.TaskApprovalResponse{
		TaskID: taskID,
		Action: action,
		Status: string(config.StatusWaiting),
	}, nil
}

func (f *fakeWorkflowService) MarkTaskStatus(context.Context, string, config.Status, config.Status) (bool, error) {
	return false, nil
}

func (f *fakeWorkflowService) GetTaskStatus(context.Context, string) (*apis.TaskStatusResponse, error) {
	if f.taskStatusResp == nil {
		return &apis.TaskStatusResponse{TaskID: "task-123", Status: string(config.StatusRunning)}, nil
	}
	return f.taskStatusResp, nil
}

func (f *fakeWorkflowService) GetTaskStages(context.Context, string) (*apis.TaskStagesResponse, error) {
	if f.taskStagesResp == nil {
		return &apis.TaskStagesResponse{TaskID: "task-123", Status: string(config.StatusRunning)}, nil
	}
	return f.taskStagesResp, nil
}

func (f *fakeWorkflowService) UpsertWorkflowSchedule(_ context.Context, appID string, req apis.UpsertWorkflowScheduleRequest) (*apis.UpsertWorkflowScheduleResponse, error) {
	f.scheduleUpsertApp = appID
	f.scheduleUpsertReq = req
	if f.scheduleUpsertErr != nil {
		return nil, f.scheduleUpsertErr
	}
	if f.scheduleUpsertResp != nil {
		return f.scheduleUpsertResp, nil
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	return &apis.UpsertWorkflowScheduleResponse{
		Schedule: apis.WorkflowSchedule{
			AppID:      appID,
			WorkflowID: req.WorkflowID,
			Cron:       req.Cron,
			Enabled:    enabled,
		},
	}, nil
}

func (f *fakeWorkflowService) ListWorkflowSchedules(_ context.Context, appID string) ([]apis.WorkflowSchedule, error) {
	f.scheduleListApp = appID
	if f.scheduleListErr != nil {
		return nil, f.scheduleListErr
	}
	return f.scheduleListResp, nil
}

func (f *fakeWorkflowService) DeleteWorkflowSchedule(_ context.Context, appID, workflowID string) error {
	f.scheduleDeleteApp = appID
	f.scheduleDeleteID = workflowID
	return f.scheduleDeleteErr
}

func (f *fakeWorkflowService) DispatchWorkflowSchedules(context.Context) (int, error) {
	return 0, nil
}

type noopApplicationsService struct{}

func (noopApplicationsService) CreateApplications(context.Context, apis.CreateApplicationsRequest) (*apis.ApplicationBase, error) {
	return nil, nil
}

func (noopApplicationsService) CreateApplicationsWithMutation(context.Context, apis.CreateApplicationsRequest, service.ApplicationCreateMutation) (*apis.ApplicationBase, error) {
	return nil, nil
}

func (noopApplicationsService) MarkInitialDeployingWorkflowComponents(context.Context, string, string) error {
	return nil
}

func (noopApplicationsService) HasImmediateActiveVersionUpdateTask(context.Context, string, int64) (bool, error) {
	return false, nil
}

func (noopApplicationsService) GetApplication(context.Context, string) (*model.Applications, error) {
	return nil, nil
}

func (noopApplicationsService) ListApplications(context.Context, service.ListApplicationsOptions) ([]*apis.ApplicationBase, error) {
	return nil, nil
}

func (noopApplicationsService) ListTemplateApplications(context.Context, service.ListApplicationsOptions) ([]*apis.ApplicationBase, error) {
	return nil, nil
}

func (noopApplicationsService) BatchGetApplications(context.Context, []string) (*apis.BatchGetApplicationsResponse, error) {
	return nil, nil
}

func (noopApplicationsService) DeleteApplication(context.Context, *model.Applications) error {
	return nil
}

func (noopApplicationsService) DeleteApplicationCascade(context.Context, string, apis.DeleteApplicationRequest) (*apis.DeleteApplicationResponse, error) {
	return nil, nil
}

func (noopApplicationsService) CleanupApplicationResources(context.Context, string) (*apis.CleanupApplicationResourcesResponse, error) {
	return nil, nil
}

func (noopApplicationsService) PlanApplicationResourceCleanup(context.Context, string) (*apis.CleanupApplicationResourcesPlanResponse, error) {
	return nil, nil
}

func (noopApplicationsService) ApplyApplicationResourceCleanup(context.Context, string, apis.CleanupApplicationResourcesRequest) (*apis.CleanupApplicationResourcesResponse, error) {
	return nil, nil
}

func (noopApplicationsService) ResetApplicationDatabases(context.Context, string, apis.DatabaseResetRequest) (*apis.DatabaseResetResponse, error) {
	return nil, nil
}

func (noopApplicationsService) DownloadLogArchive(context.Context, string, apis.LogArchiveDownloadRequest) (*service.ComponentFileArchiveStream, error) {
	return nil, nil
}

func (noopApplicationsService) RestartApplicationWorkloads(context.Context, string, apis.ApplicationLifecycleRequest) (*apis.RestartApplicationWorkloadsResponse, error) {
	return nil, nil
}

func (noopApplicationsService) StopApplicationDeployments(context.Context, string, apis.ApplicationLifecycleRequest) (*apis.StopApplicationDeploymentsResponse, error) {
	return nil, nil
}

func (noopApplicationsService) StartApplicationDeployments(context.Context, string, apis.ApplicationLifecycleRequest) (*apis.StartApplicationDeploymentsResponse, error) {
	return nil, nil
}

func (noopApplicationsService) UpdateApplicationWorkflow(context.Context, string, apis.UpdateApplicationWorkflowRequest) (*apis.UpdateWorkflowResponse, error) {
	return nil, nil
}

func (noopApplicationsService) ListApplicationWorkflows(context.Context, string) ([]*model.Workflow, error) {
	return nil, nil
}

func (noopApplicationsService) ListApplicationComponents(context.Context, string) ([]*model.ApplicationComponent, error) {
	return nil, nil
}

func (noopApplicationsService) ListApplicationTasks(context.Context, string) ([]*model.WorkflowQueue, error) {
	return nil, nil
}

func (noopApplicationsService) ListCronJobs(context.Context) ([]*apis.CronJobInfo, error) {
	return nil, nil
}

func (noopApplicationsService) ListScheduledJobs(context.Context) ([]*apis.ScheduledJobInfo, error) {
	return nil, nil
}

func (noopApplicationsService) ListComponentContainers(context.Context, string, string) (*apis.ComponentContainersResponse, error) {
	return nil, nil
}

func (noopApplicationsService) StreamComponentLogs(context.Context, string, string, string) (*service.ComponentLogStream, error) {
	return nil, nil
}

func (noopApplicationsService) ExportComponentFilesZip(context.Context, string, string, apis.ExportComponentFilesRequest) (*service.ComponentFileArchiveStream, error) {
	return nil, nil
}

func (noopApplicationsService) ExecComponentShellScript(context.Context, string, string, apis.ExecComponentShellScriptRequest) (*apis.ExecComponentShellScriptResponse, error) {
	return nil, nil
}

func (noopApplicationsService) StreamComponentShellScript(context.Context, string, string, apis.ExecComponentShellScriptRequest) (*service.ComponentShellScriptStream, error) {
	return nil, nil
}

func (noopApplicationsService) UpdateVersion(context.Context, string, apis.UpdateVersionRequest) (*apis.UpdateVersionResponse, error) {
	return nil, nil
}

func (noopApplicationsService) DiffUpdateVersion(context.Context, string, apis.DiffUpdateVersionRequest) (*apis.DiffUpdateVersionResponse, error) {
	return nil, nil
}

var _ service.ApplicationsService = noopApplicationsService{}

type workflowListApplicationService struct {
	noopApplicationsService
	workflows []*model.Workflow
	err       error
}

func (s workflowListApplicationService) ListApplicationWorkflows(context.Context, string) ([]*model.Workflow, error) {
	return s.workflows, s.err
}

type templateApplicationService struct {
	noopApplicationsService
	templates []*apis.ApplicationBase
	err       error
	lastOpts  service.ListApplicationsOptions
}

type componentListApplicationService struct {
	noopApplicationsService
	components          []*model.ApplicationComponent
	activeVersionUpdate bool
	err                 error
	activeTaskErr       error
}

func (s componentListApplicationService) ListApplicationComponents(context.Context, string) ([]*model.ApplicationComponent, error) {
	return s.components, s.err
}

func (s componentListApplicationService) ListApplicationRuntimeComponents(context.Context, string) ([]*model.ApplicationComponent, error) {
	return s.components, s.err
}

func (s componentListApplicationService) ListApplicationTasks(context.Context, string) ([]*model.WorkflowQueue, error) {
	return nil, nil
}

func (s componentListApplicationService) HasImmediateActiveVersionUpdateTask(context.Context, string, int64) (bool, error) {
	return s.activeVersionUpdate, s.activeTaskErr
}

type batchComponentStatusApplicationService struct {
	noopApplicationsService
	componentsByAppID    map[string][]*model.ApplicationComponent
	activeTaskByAppID    map[string]bool
	errByAppID           map[string]error
	activeTaskErrByAppID map[string]error
}

func (s batchComponentStatusApplicationService) ListApplicationComponents(ctx context.Context, appID string) ([]*model.ApplicationComponent, error) {
	return s.ListApplicationRuntimeComponents(ctx, appID)
}

func (s batchComponentStatusApplicationService) ListApplicationRuntimeComponents(_ context.Context, appID string) ([]*model.ApplicationComponent, error) {
	if s.errByAppID != nil {
		if err, ok := s.errByAppID[appID]; ok {
			return nil, err
		}
	}
	components, ok := s.componentsByAppID[appID]
	if !ok {
		return nil, bcode.ErrApplicationNotExist
	}
	return components, nil
}

func (s batchComponentStatusApplicationService) ListApplicationTasks(_ context.Context, appID string) ([]*model.WorkflowQueue, error) {
	return nil, nil
}

func (s batchComponentStatusApplicationService) HasImmediateActiveVersionUpdateTask(_ context.Context, appID string, _ int64) (bool, error) {
	if s.activeTaskErrByAppID != nil {
		if err, ok := s.activeTaskErrByAppID[appID]; ok {
			return false, err
		}
	}
	if s.activeTaskByAppID == nil {
		return false, nil
	}
	return s.activeTaskByAppID[appID], nil
}

func testVersionUpdateTaskMarker(t *testing.T) string {
	t.Helper()
	payload, err := json.Marshal(model.VersionUpdateResourceActionInfo{
		Source:  config.JobInfoSourceVersionUpdateAction,
		Version: 1,
	})
	require.NoError(t, err)
	return string(payload)
}

func testVersionUpdateTask(t *testing.T, status config.Status) *model.WorkflowQueue {
	t.Helper()
	return &model.WorkflowQueue{
		TaskID:             "task-version",
		Status:             status,
		ResourceActionInfo: testVersionUpdateTaskMarker(t),
	}
}

type componentCacheSyncStore struct {
	component *model.ApplicationComponent
}

type recordingValidationService struct {
	called    bool
	appID     string
	workflow  apis.TryWorkflowRequest
	response  *apis.TryWorkflowResponse
	tryAppReq apis.CreateApplicationsRequest
}

func (s *recordingValidationService) TryApplication(_ context.Context, req apis.CreateApplicationsRequest) *apis.TryApplicationResponse {
	s.tryAppReq = req
	return &apis.TryApplicationResponse{Valid: true}
}

func (s *recordingValidationService) TryWorkflow(_ context.Context, appID string, req apis.TryWorkflowRequest) *apis.TryWorkflowResponse {
	s.called = true
	s.appID = appID
	s.workflow = req
	if s.response != nil {
		return s.response
	}
	return &apis.TryWorkflowResponse{Valid: true}
}

func (s *componentCacheSyncStore) Add(context.Context, datastore.Entity) error { return nil }

func (s *componentCacheSyncStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (s *componentCacheSyncStore) Put(_ context.Context, entity datastore.Entity) error {
	component, ok := entity.(*model.ApplicationComponent)
	if !ok {
		return nil
	}
	if s.component == nil {
		s.component = component
		return nil
	}
	*s.component = *component
	return nil
}

func (s *componentCacheSyncStore) Delete(context.Context, datastore.Entity) error { return nil }

func (s *componentCacheSyncStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *componentCacheSyncStore) Get(context.Context, datastore.Entity) error { return nil }

func (s *componentCacheSyncStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	componentQuery, ok := query.(*model.ApplicationComponent)
	if !ok || s.component == nil {
		return nil, datastore.ErrRecordNotExist
	}
	if componentQuery.AppID != "" && componentQuery.AppID != s.component.AppID {
		return nil, datastore.ErrRecordNotExist
	}
	return []datastore.Entity{s.component}, nil
}

func (s *componentCacheSyncStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *componentCacheSyncStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (s *componentCacheSyncStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *componentCacheSyncStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func (s *componentCacheSyncStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if _, ok := entity.(*model.ApplicationComponent); !ok || s.component == nil {
		return false, nil
	}
	if appID, ok := conditions["app_id"].(string); ok && s.component.AppID != appID {
		return false, nil
	}
	if name, ok := conditions["name"].(string); ok && s.component.Name != name {
		return false, nil
	}
	if id, ok := conditions["id"].(int); ok && s.component.ID != id {
		return false, nil
	}
	for key, value := range updates {
		switch key {
		case "status":
			if status, ok := value.(string); ok {
				s.component.Status = status
			}
		case "ready_replicas":
			if readyReplicas, ok := value.(int32); ok {
				s.component.ReadyReplicas = readyReplicas
			}
		case "last_abnormal":
			if lastAbnormal, ok := value.(string); ok {
				s.component.LastAbnormal = lastAbnormal
			}
		}
	}
	return true, nil
}

var _ datastore.DataStore = (*componentCacheSyncStore)(nil)

var _ datastore.ConditionalCompareAndSwap = (*componentCacheSyncStore)(nil)

type cacheBackedComponentListService struct {
	noopApplicationsService
	cache     cacheutil.ICache
	store     *componentCacheSyncStore
	listCalls int
}

type statusReadApplicationService struct {
	noopApplicationsService
	cachedComponents  []*model.ApplicationComponent
	runtimeComponents []*model.ApplicationComponent
	cachedCalls       int
	runtimeCalls      int
}

func (s *statusReadApplicationService) ListApplicationComponents(context.Context, string) ([]*model.ApplicationComponent, error) {
	s.cachedCalls++
	return s.cachedComponents, nil
}

func (s *statusReadApplicationService) ListApplicationRuntimeComponents(context.Context, string) ([]*model.ApplicationComponent, error) {
	s.runtimeCalls++
	return s.runtimeComponents, nil
}

func (s *cacheBackedComponentListService) ListApplicationComponents(_ context.Context, appID string) ([]*model.ApplicationComponent, error) {
	cacheKey := cacheutil.ApplicationComponentsKey(appID)
	if s.cache != nil {
		raw, err := s.cache.Load(cacheKey)
		if err == nil && raw != "" {
			var cached []*model.ApplicationComponent
			if err := json.Unmarshal([]byte(raw), &cached); err == nil {
				return cached, nil
			}
		}
	}

	s.listCalls++
	if s.store == nil || s.store.component == nil || s.store.component.AppID != appID {
		return nil, nil
	}
	componentCopy := *s.store.component
	result := []*model.ApplicationComponent{&componentCopy}
	if s.cache != nil {
		if bytes, err := json.Marshal(result); err == nil {
			_ = s.cache.Store(cacheKey, string(bytes))
		}
	}
	return result, nil
}

func (s *templateApplicationService) ListTemplateApplications(_ context.Context, opts service.ListApplicationsOptions) ([]*apis.ApplicationBase, error) {
	s.lastOpts = opts
	return s.templates, s.err
}

type listApplicationService struct {
	noopApplicationsService
	apps     []*apis.ApplicationBase
	err      error
	lastOpts service.ListApplicationsOptions
}

func (s *listApplicationService) ListApplications(_ context.Context, opts service.ListApplicationsOptions) ([]*apis.ApplicationBase, error) {
	s.lastOpts = opts
	return s.apps, s.err
}

type batchGetApplicationService struct {
	noopApplicationsService
	resp       *apis.BatchGetApplicationsResponse
	err        error
	lastAppIDs []string
}

func (s *batchGetApplicationService) BatchGetApplications(_ context.Context, appIDs []string) (*apis.BatchGetApplicationsResponse, error) {
	s.lastAppIDs = append([]string(nil), appIDs...)
	return s.resp, s.err
}

type taskListApplicationService struct {
	noopApplicationsService
	tasks []*model.WorkflowQueue
	err   error
}

func (s taskListApplicationService) ListApplicationTasks(context.Context, string) ([]*model.WorkflowQueue, error) {
	return s.tasks, s.err
}

type fakeCreateAndExecApplicationService struct {
	noopApplicationsService
	createResp       *apis.ApplicationBase
	createErr        error
	markErr          error
	lastCreate       apis.CreateApplicationsRequest
	lastMarkApp      string
	lastMarkWorkflow string
	createCalls      int
	markCalls        int
}

func (s *fakeCreateAndExecApplicationService) CreateApplications(_ context.Context, req apis.CreateApplicationsRequest) (*apis.ApplicationBase, error) {
	s.lastCreate = req
	s.createCalls++
	if s.createErr != nil {
		return nil, s.createErr
	}
	if s.createResp != nil {
		return s.createResp, nil
	}
	return &apis.ApplicationBase{
		ID:         "app-1",
		Name:       req.Name,
		WorkflowID: "wf-default",
	}, nil
}

func (s *fakeCreateAndExecApplicationService) MarkInitialDeployingWorkflowComponents(_ context.Context, appID, workflowID string) error {
	s.lastMarkApp = appID
	s.lastMarkWorkflow = workflowID
	s.markCalls++
	return s.markErr
}

type fakeUpdateVersionService struct {
	noopApplicationsService
	updateResp  *apis.UpdateVersionResponse
	updateErr   error
	diffResp    *apis.DiffUpdateVersionResponse
	diffErr     error
	lastAppID   string
	lastReq     apis.UpdateVersionRequest
	lastDiffID  string
	lastDiffReq apis.DiffUpdateVersionRequest
}

func (f *fakeUpdateVersionService) UpdateVersion(_ context.Context, appID string, req apis.UpdateVersionRequest) (*apis.UpdateVersionResponse, error) {
	f.lastAppID = appID
	f.lastReq = req
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	if f.updateResp != nil {
		return f.updateResp, nil
	}
	return &apis.UpdateVersionResponse{
		AppID:             appID,
		Version:           req.Version,
		PreviousVersion:   "1.0.0",
		Strategy:          req.Strategy,
		ExecutionScope:    req.ExecutionScope,
		UpdatedComponents: []string{"backend"},
	}, nil
}

func (f *fakeUpdateVersionService) DiffUpdateVersion(_ context.Context, targetAppID string, req apis.DiffUpdateVersionRequest) (*apis.DiffUpdateVersionResponse, error) {
	f.lastDiffID = targetAppID
	f.lastDiffReq = req
	if f.diffErr != nil {
		return nil, f.diffErr
	}
	if f.diffResp != nil {
		return f.diffResp, nil
	}
	return &apis.DiffUpdateVersionResponse{
		TargetAppID:           targetAppID,
		SourceAppID:           req.SourceAppID,
		TargetPreviousVersion: "1.0.0",
		TargetVersion:         "1.0.1",
		SourceVersion:         "1.0.1",
		DryRun:                req.DryRun,
		TargetOnlyStrategy:    req.TargetOnlyStrategy,
		VersionChanged:        true,
		HasChanges:            true,
		Executable:            true,
	}, nil
}

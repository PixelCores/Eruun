package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	wfcloudcontract "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type fakeCloudProvider struct {
	name       string
	ctx        context.Context
	runtime    CloudRuntime
	runtimeErr error
	req        *CloudJobRequest
	registry   *fakeCloudActionRegistry
	newRuntime func(context.Context, *CloudJobRequest) (CloudRuntime, error)
}

func (f *fakeCloudProvider) Name() string {
	return f.name
}

func (f *fakeCloudProvider) NewRuntime(ctx context.Context, req *CloudJobRequest) (CloudRuntime, error) {
	f.ctx = ctx
	f.req = req
	if f.newRuntime != nil {
		return f.newRuntime(ctx, req)
	}
	if f.runtimeErr != nil {
		return nil, f.runtimeErr
	}
	if f.runtime != nil {
		return f.runtime, nil
	}
	return &fakeCloudRuntime{}, nil
}

func TestCloudJobCtlRunInjectsDataStoreIntoProviderContext(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	store := &noopStore{}
	provider := &fakeCloudProvider{
		name:    "mock",
		runtime: &fakeCloudRuntime{result: &CloudJobResult{Message: "ok"}},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"create": newForwardCloudAction("create"),
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-store-context",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "create",
		},
	}
	ctl := NewCloudJobCtl(task, store)
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.NoError(t, err)
	require.Same(t, store, wfcloudcontract.DataStoreFromContext(provider.ctx))
}

func (f *fakeCloudProvider) ResolveAction(action string) (CloudAction, bool) {
	if f == nil || f.registry == nil {
		return nil, false
	}
	return f.registry.ResolveAction(action)
}

func (f *fakeCloudProvider) SupportedActions() []string {
	if f == nil || f.registry == nil {
		return nil
	}
	return f.registry.SupportedActions()
}

type fakeCloudActionRegistry struct {
	actions map[string]CloudAction
}

func (r *fakeCloudActionRegistry) ResolveAction(action string) (CloudAction, bool) {
	if len(r.actions) == 0 {
		return nil, false
	}
	a, ok := r.actions[action]
	return a, ok
}

func (r *fakeCloudActionRegistry) SupportedActions() []string {
	if len(r.actions) == 0 {
		return nil
	}
	out := make([]string, 0, len(r.actions))
	for action := range r.actions {
		out = append(out, action)
	}
	sort.Strings(out)
	return out
}

type fakeCloudRuntime struct {
	result *CloudJobResult
	err    error
	action string
	params map[string]interface{}
}

type cloudJobCheckpointStore struct {
	records map[int]*model.JobInfo
	nextID  int
}

func newCloudJobCheckpointStore() *cloudJobCheckpointStore {
	return &cloudJobCheckpointStore{
		records: map[int]*model.JobInfo{},
	}
}

func cloneJobInfo(info *model.JobInfo) *model.JobInfo {
	if info == nil {
		return nil
	}
	clone := *info
	return &clone
}

func matchesInFilters(opts *datastore.ListOptions, key, value string) bool {
	if opts == nil {
		return true
	}
	for _, item := range opts.FilterOptions.In {
		if item.Key != key {
			continue
		}
		for _, candidate := range item.Values {
			if candidate == value {
				return true
			}
		}
		return false
	}
	return true
}

func (s *cloudJobCheckpointStore) Add(_ context.Context, entity datastore.Entity) error {
	info, ok := entity.(*model.JobInfo)
	if !ok {
		return datastore.ErrEntityInvalid
	}
	s.nextID++
	clone := cloneJobInfo(info)
	clone.ID = s.nextID
	if clone.CreateTime.IsZero() {
		clone.CreateTime = time.Now()
	}
	s.records[clone.ID] = clone
	return nil
}

func (s *cloudJobCheckpointStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (s *cloudJobCheckpointStore) Put(_ context.Context, entity datastore.Entity) error {
	info, ok := entity.(*model.JobInfo)
	if !ok {
		return datastore.ErrEntityInvalid
	}
	clone := cloneJobInfo(info)
	if clone.ID == 0 {
		s.nextID++
		clone.ID = s.nextID
	}
	if clone.CreateTime.IsZero() {
		clone.CreateTime = time.Now()
	}
	clone.UpdateTime = time.Now()
	s.records[clone.ID] = clone
	return nil
}

func (s *cloudJobCheckpointStore) Delete(context.Context, datastore.Entity) error { return nil }
func (s *cloudJobCheckpointStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (s *cloudJobCheckpointStore) Get(context.Context, datastore.Entity) error { return nil }

func (s *cloudJobCheckpointStore) List(_ context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	jobInfo, ok := query.(*model.JobInfo)
	if !ok {
		return nil, datastore.ErrEntityInvalid
	}
	out := make([]datastore.Entity, 0, len(s.records))
	for _, item := range s.records {
		if jobInfo.TaskID != "" && item.TaskID != jobInfo.TaskID {
			continue
		}
		if !matchesInFilters(opts, "type", item.Type) {
			continue
		}
		if !matchesInFilters(opts, "service_name", item.ServiceName) {
			continue
		}
		out = append(out, cloneJobInfo(item))
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i].(*model.JobInfo)
		right := out[j].(*model.JobInfo)
		return left.CreateTime.After(right.CreateTime)
	})
	if opts != nil && opts.PageSize > 0 && len(out) > opts.PageSize {
		out = out[:opts.PageSize]
	}
	return out, nil
}

func (s *cloudJobCheckpointStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return int64(len(s.records)), nil
}

func (s *cloudJobCheckpointStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (s *cloudJobCheckpointStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *cloudJobCheckpointStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func findStoredJobInfoByExecutionKey(t *testing.T, store *cloudJobCheckpointStore, executionKey string) *model.JobInfo {
	t.Helper()
	for _, item := range store.records {
		record := parseCloudCheckpointFromJobInfo(item)
		if record == nil {
			continue
		}
		if cloudJobExecutionKeyFromRecord(record) == executionKey {
			return cloneJobInfo(item)
		}
	}
	t.Fatalf("job info with execution key %q not found", executionKey)
	return nil
}

func (f *fakeCloudRuntime) Call(_ context.Context, action string, params map[string]interface{}) (*CloudJobResult, error) {
	f.action = action
	f.params = params
	return f.result, f.err
}

type fakeCloudAction struct {
	validateErr error
	runErr      error
	run         func(ctx context.Context, runtime CloudRuntime, req *CloudJobRequest, state map[string]interface{}) (*CloudActionProgress, error)
}

func (f *fakeCloudAction) Validate(_ *CloudJobRequest) error {
	return f.validateErr
}

func (f *fakeCloudAction) Run(ctx context.Context, runtime CloudRuntime, req *CloudJobRequest, state map[string]interface{}) (*CloudActionProgress, error) {
	if f.runErr != nil {
		return nil, f.runErr
	}
	if f.run != nil {
		return f.run(ctx, runtime, req, state)
	}
	return nil, nil
}

func newForwardCloudAction(action string) CloudAction {
	return &fakeCloudAction{
		validateErr: nil,
		run: func(ctx context.Context, runtime CloudRuntime, req *CloudJobRequest, _ map[string]interface{}) (*CloudActionProgress, error) {
			if runtime == nil {
				return nil, fmt.Errorf("cloud runtime is nil")
			}
			if req == nil {
				return nil, fmt.Errorf("cloud job request is nil")
			}
			result, err := runtime.Call(ctx, action, req.Params)
			if err != nil {
				return nil, err
			}
			return &CloudActionProgress{
				Done:   true,
				Result: result,
			}, nil
		},
	}
}

func TestCloudJobCtlRunProviderNotRegistered(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	task := &model.JobTask{
		Name:    "cloud-create",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "missing",
			Action:   "create",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, errCloudProviderNotFound)
	require.NotEmpty(t, task.Info)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusFailed, record.Status)
	require.Equal(t, "missing", record.Provider)
	require.Equal(t, "create", record.Action)
}

func TestCloudJobCtlRunAliyunUnknownActionReturnsNotRegistered(t *testing.T) {
	restoreBuiltinCloudProvidersForTest()

	task := &model.JobTask{
		Name:    "cloud-aliyun",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "aliyun",
			Action:   "create-ecs",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, errCloudActionNotFound)
	require.Contains(t, err.Error(), "aliyun/create-ecs")
	require.NotEmpty(t, task.Info)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusFailed, record.Status)
	require.Equal(t, "aliyun", record.Provider)
	require.Equal(t, "create-ecs", record.Action)
}

func TestCloudJobCtlRunWithRegisteredProviderSuccess(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	runtime := &fakeCloudRuntime{
		result: &CloudJobResult{
			RequestID: "req-1",
			Message:   "ok",
			Output: map[string]interface{}{
				"instanceId": "i-123",
			},
		},
	}
	provider := &fakeCloudProvider{
		name:    "mock",
		runtime: runtime,
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"create": newForwardCloudAction("create"),
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:       "cloud-mock",
		Namespace:  "default",
		WorkflowID: "wf-1",
		ProjectID:  "proj-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "create",
			Params: map[string]interface{}{
				"zone": "cn-hangzhou-a",
			},
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.NoError(t, err)
	require.NotNil(t, provider.req)
	require.Equal(t, "mock", provider.req.Provider)
	require.Equal(t, "create", provider.req.Action)
	require.Equal(t, "cn-hangzhou-a", provider.req.Params["zone"])
	require.NoError(t, ctl.SaveInfo(context.Background()))

	require.Equal(t, "create", runtime.action)
	require.Equal(t, "cn-hangzhou-a", runtime.params["zone"])

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusCompleted, record.Status)
	require.NotNil(t, record.Result)
	require.Equal(t, "req-1", record.Result.RequestID)
	require.Equal(t, "ok", record.Result.Message)
	require.Equal(t, "i-123", record.Result.Output["instanceId"])
}

func TestCloudJobCtlRunKeepsRuntimeProviderSnapshotInContextOnly(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	provider := &fakeCloudProvider{
		name: "mock",
		newRuntime: func(_ context.Context, req *CloudJobRequest) (CloudRuntime, error) {
			req.RuntimeProviderSnapshot = map[string]string{
				"endpoint": "nas.cn-hangzhou.aliyuncs.com",
				"regionId": "cn-hangzhou",
			}
			return &fakeCloudRuntime{}, nil
		},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"create": &fakeCloudAction{
					run: func(ctx context.Context, _ CloudRuntime, req *CloudJobRequest, _ map[string]interface{}) (*CloudActionProgress, error) {
						cfg, ok := wfcloudcontract.RuntimeProviderSnapshotFromContext(ctx, "mock").(map[string]string)
						require.True(t, ok)
						require.Equal(t, "nas.cn-hangzhou.aliyuncs.com", cfg["endpoint"])
						require.Equal(t, "cn-hangzhou", cfg["regionId"])
						require.Nil(t, req.RuntimeProviderSnapshot)
						return &CloudActionProgress{
							Done:   true,
							Result: &CloudJobResult{Message: "done"},
						}, nil
					},
				},
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:      "cloud-public-safe",
		Namespace: "default",
		JobType:   string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider:     "mock",
			Action:       "create",
			ExecutionKey: "step:0/component:0",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "cloudjob: default/cloud-public-safe", task.Info)
	require.NotContains(t, task.Info, "accessKeySecret")
	require.NotContains(t, task.Info, `"sk"`)
	require.Nil(t, provider.req.RuntimeProviderSnapshot)
	require.NotContains(t, task.InternalInfo, "configSnapshot")
	require.NotContains(t, task.InternalInfo, "accessKeySecret")
	require.NotContains(t, task.InternalInfo, "nas.cn-hangzhou.aliyuncs.com")

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.NotNil(t, record.Request)
	require.Nil(t, record.Request.RuntimeProviderSnapshot)
}

func TestCloudJobCtlRunProviderReturnsError(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	expectedErr := errors.New("provider failed")
	provider := &fakeCloudProvider{
		name:       "broken",
		runtimeErr: expectedErr,
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"create": newForwardCloudAction("create"),
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-broken",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "broken",
			Action:   "create",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusFailed, record.Status)
	require.Equal(t, "provider failed", record.Error)
}

func TestCloudJobCtlRunSDKReturnsError(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	expectedErr := errors.New("runtime call failed")
	provider := &fakeCloudProvider{
		name: "mock",
		runtime: &fakeCloudRuntime{
			err: expectedErr,
		},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"create": newForwardCloudAction("create"),
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-runtime-error",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "create",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusFailed, record.Status)
	require.Equal(t, "runtime call failed", record.Error)
}

func TestCloudJobCtlRunActionNotRegistered(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	provider := &fakeCloudProvider{
		name:     "mock",
		runtime:  &fakeCloudRuntime{},
		registry: &fakeCloudActionRegistry{actions: map[string]CloudAction{}},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-no-action",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "create",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, errCloudActionNotFound)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusFailed, record.Status)
}

func TestCloudJobCtlRunActionValidateError(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	validateErr := errors.New("invalid params")
	provider := &fakeCloudProvider{
		name:    "mock",
		runtime: &fakeCloudRuntime{},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"create": &fakeCloudAction{validateErr: validateErr},
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-invalid",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "create",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, validateErr)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusFailed, record.Status)
	require.Equal(t, "invalid params", record.Error)
}

func TestCloudJobCtlRunMultiStepAction(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	runCount := 0
	action := &fakeCloudAction{
		run: func(_ context.Context, _ CloudRuntime, _ *CloudJobRequest, state map[string]interface{}) (*CloudActionProgress, error) {
			runCount++
			switch state["step"] {
			case nil:
				return &CloudActionProgress{
					Done:         false,
					RequeueAfter: time.Millisecond,
					State: map[string]interface{}{
						"step": "nas-ready",
					},
					Result: &CloudJobResult{Message: "nas ready"},
				}, nil
			case "nas-ready":
				return &CloudActionProgress{
					Done:         false,
					RequeueAfter: time.Millisecond,
					State: map[string]interface{}{
						"step": "ecs-ready",
					},
					Result: &CloudJobResult{Message: "ecs ready"},
				}, nil
			default:
				return &CloudActionProgress{
					Done: true,
					State: map[string]interface{}{
						"step": "completed",
					},
					Result: &CloudJobResult{
						RequestID: "req-final",
						Message:   "done",
						Output: map[string]interface{}{
							"publicIp": "1.2.3.4",
						},
					},
				}, nil
			}
		},
	}

	provider := &fakeCloudProvider{
		name:    "mock",
		runtime: &fakeCloudRuntime{},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"provision": action,
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-multi-step",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)
	ctl.waitFunc = func(_ context.Context, _ time.Duration) error { return nil }

	err := ctl.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, runCount)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusCompleted, record.Status)
	require.Equal(t, "completed", record.State["step"])
	require.NotNil(t, record.Result)
	require.Equal(t, "req-final", record.Result.RequestID)
	require.Equal(t, "1.2.3.4", record.Result.Output["publicIp"])
}

func TestCloudJobCtlRunFailsToResumePersistedStateWithoutRuntimeProviderSnapshot(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	store := newCloudJobCheckpointStore()
	seedTask := &model.JobTask{
		Name:       "cloud-restore",
		Namespace:  "default",
		WorkflowID: "wf-1",
		ProjectID:  "proj-1",
		AppID:      "app-1",
		TaskID:     "task-restore",
		JobType:    string(config.JobDeployCloud),
		Status:     config.StatusRunning,
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
			Params: map[string]interface{}{
				"zone": "cn-hangzhou-a",
			},
		},
		InternalInfo: recordCloudJobResult(&CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
			Params: map[string]interface{}{
				"zone": "cn-hangzhou-a",
			},
		}, &CloudJobRequest{
			Provider: "mock",
			Action:   "provision",
			Params: map[string]interface{}{
				"zone": "cn-hangzhou-a",
			},
		}, &CloudJobResult{Message: "checkpoint"}, map[string]interface{}{
			"step": "nas-ready",
		}, config.StatusRunning, nil),
	}
	require.NoError(t, saveOrUpdateJobInfo(context.Background(), store, seedTask))

	provider := &fakeCloudProvider{
		name: "mock",
		newRuntime: func(_ context.Context, req *CloudJobRequest) (CloudRuntime, error) {
			require.True(t, req.ResumeFromPersistedState)
			require.Equal(t, "cn-hangzhou-a", req.Params["zone"])
			require.Nil(t, req.RuntimeProviderSnapshot)
			return nil, errors.New("cloud job checkpoint cannot resume safely after process restart without runtime provider snapshot; rerun the workflow task")
		},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"provision": &fakeCloudAction{},
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:       "cloud-restore",
		Namespace:  "default",
		WorkflowID: "wf-1",
		ProjectID:  "proj-1",
		AppID:      "app-1",
		TaskID:     "task-restore",
		JobType:    string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
			Params: map[string]interface{}{
				"zone": "cn-shanghai-b",
			},
		},
	}
	ctl := NewCloudJobCtl(task, store)
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot resume safely after process restart")
	require.Equal(t, config.StatusFailed, task.Status)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusFailed, record.Status)
	require.Equal(t, "nas-ready", record.State["step"])
	require.NotNil(t, record.Request)
	require.Equal(t, "cn-hangzhou-a", record.Request.Params["zone"])
	require.NotContains(t, task.InternalInfo, "configSnapshot")
}

func TestCloudJobCtlRunDoesNotReuseLegacyCheckpointWithoutExecutionKey(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	store := newCloudJobCheckpointStore()
	legacyTask := &model.JobTask{
		Name:       "cloud-legacy-restore",
		Namespace:  "default",
		WorkflowID: "wf-legacy",
		ProjectID:  "proj-legacy",
		AppID:      "app-legacy",
		TaskID:     "task-legacy-restore",
		JobType:    string(config.JobDeployCloud),
		Status:     config.StatusRunning,
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
			Params: map[string]interface{}{
				"zone": "cn-hangzhou-a",
			},
		},
		Info: recordCloudJobResult(&CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
			Params: map[string]interface{}{
				"zone": "cn-hangzhou-a",
			},
		}, &CloudJobRequest{
			Provider: "mock",
			Action:   "provision",
			Params: map[string]interface{}{
				"zone": "cn-hangzhou-a",
			},
		}, nil, nil, config.StatusRunning, nil),
	}
	require.NoError(t, saveOrUpdateJobInfo(context.Background(), store, legacyTask))

	provider := &fakeCloudProvider{
		name: "mock",
		newRuntime: func(_ context.Context, req *CloudJobRequest) (CloudRuntime, error) {
			require.False(t, req.ResumeFromPersistedState)
			require.Equal(t, "cn-shanghai-b", req.Params["zone"])
			return &fakeCloudRuntime{}, nil
		},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"provision": &fakeCloudAction{
					run: func(_ context.Context, _ CloudRuntime, req *CloudJobRequest, state map[string]interface{}) (*CloudActionProgress, error) {
						require.Equal(t, "cn-shanghai-b", req.Params["zone"])
						require.Nil(t, state)
						return &CloudActionProgress{
							Done:   true,
							State:  map[string]interface{}{"step": "completed"},
							Result: &CloudJobResult{Message: "done"},
						}, nil
					},
				},
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:       "cloud-legacy-restore",
		Namespace:  "default",
		WorkflowID: "wf-legacy",
		ProjectID:  "proj-legacy",
		AppID:      "app-legacy",
		TaskID:     "task-legacy-restore",
		JobType:    string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider:     "mock",
			Action:       "provision",
			Params:       map[string]interface{}{"zone": "cn-shanghai-b"},
			ExecutionKey: "step:0/component:0",
		},
	}
	ctl := NewCloudJobCtl(task, store)
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, config.StatusCompleted, task.Status)
	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Len(t, store.records, 2)

	legacyRecord := parseCloudCheckpointFromJobInfo(findStoredJobInfoByExecutionKey(t, store, ""))
	require.NotNil(t, legacyRecord)
	require.Nil(t, legacyRecord.State)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, "completed", record.State["step"])
	require.NotNil(t, record.Request)
	require.Equal(t, "cn-shanghai-b", record.Request.Params["zone"])
	require.NotContains(t, task.InternalInfo, "configSnapshot")
}

func TestCloudJobCtlRunMatchesCheckpointByExecutionKey(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	store := newCloudJobCheckpointStore()
	firstExecutionKey := "step:0/component:0"
	secondExecutionKey := "step:1/component:0"
	firstGenerationKey := "generation-1-step-0"
	secondGenerationKey := "generation-1-step-1"

	firstTask := &model.JobTask{
		Name:          "cloud-repeat",
		Namespace:     "default",
		WorkflowID:    "wf-1",
		ProjectID:     "proj-1",
		AppID:         "app-1",
		TaskID:        "task-repeat",
		JobType:       string(config.JobDeployCloud),
		Status:        config.StatusRunning,
		ExecutionKey:  firstGenerationKey,
		RunGeneration: 1,
		JobInfo: &CloudJobInfo{
			Provider:     "mock",
			Action:       "provision",
			Params:       map[string]interface{}{"zone": "cn-hangzhou-a"},
			ExecutionKey: firstExecutionKey,
		},
		Info: "cloudjob: default/cloud-repeat",
		InternalInfo: recordCloudJobResult(&CloudJobInfo{
			Provider:     "mock",
			Action:       "provision",
			Params:       map[string]interface{}{"zone": "cn-hangzhou-a"},
			ExecutionKey: firstExecutionKey,
		}, &CloudJobRequest{
			Provider: "mock",
			Action:   "provision",
			Params:   map[string]interface{}{"zone": "cn-hangzhou-a"},
		}, nil, map[string]interface{}{"step": "first"}, config.StatusRunning, nil),
	}
	secondTask := &model.JobTask{
		Name:          "cloud-repeat",
		Namespace:     "default",
		WorkflowID:    "wf-1",
		ProjectID:     "proj-1",
		AppID:         "app-1",
		TaskID:        "task-repeat",
		JobType:       string(config.JobDeployCloud),
		Status:        config.StatusRunning,
		ExecutionKey:  secondGenerationKey,
		RunGeneration: 1,
		JobInfo: &CloudJobInfo{
			Provider:     "mock",
			Action:       "provision",
			Params:       map[string]interface{}{"zone": "cn-shanghai-b"},
			ExecutionKey: secondExecutionKey,
		},
		Info: "cloudjob: default/cloud-repeat",
		InternalInfo: recordCloudJobResult(&CloudJobInfo{
			Provider:     "mock",
			Action:       "provision",
			Params:       map[string]interface{}{"zone": "cn-shanghai-b"},
			ExecutionKey: secondExecutionKey,
		}, &CloudJobRequest{
			Provider: "mock",
			Action:   "provision",
			Params:   map[string]interface{}{"zone": "cn-shanghai-b"},
		}, nil, nil, config.StatusRunning, nil),
	}
	require.NoError(t, saveOrUpdateJobInfo(context.Background(), store, firstTask))
	require.NoError(t, saveOrUpdateJobInfo(context.Background(), store, secondTask))
	require.Len(t, store.records, 2)

	provider := &fakeCloudProvider{
		name: "mock",
		newRuntime: func(_ context.Context, req *CloudJobRequest) (CloudRuntime, error) {
			require.False(t, req.ResumeFromPersistedState)
			require.Equal(t, "cn-shanghai-b", req.Params["zone"])
			return &fakeCloudRuntime{}, nil
		},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"provision": &fakeCloudAction{
					run: func(_ context.Context, _ CloudRuntime, req *CloudJobRequest, state map[string]interface{}) (*CloudActionProgress, error) {
						require.Equal(t, "cn-shanghai-b", req.Params["zone"])
						require.Nil(t, state)
						return &CloudActionProgress{
							Done:   true,
							State:  map[string]interface{}{"step": "completed"},
							Result: &CloudJobResult{Message: "done"},
						}, nil
					},
				},
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:          "cloud-repeat",
		Namespace:     "default",
		WorkflowID:    "wf-1",
		ProjectID:     "proj-1",
		AppID:         "app-1",
		TaskID:        "task-repeat",
		JobType:       string(config.JobDeployCloud),
		Info:          "cloudjob: default/cloud-repeat",
		ExecutionKey:  secondGenerationKey,
		RunGeneration: 1,
		JobInfo: &CloudJobInfo{
			Provider:     "mock",
			Action:       "provision",
			Params:       map[string]interface{}{"zone": "cn-beijing-c"},
			ExecutionKey: secondExecutionKey,
		},
	}
	ctl := NewCloudJobCtl(task, store)
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.NoError(t, err)
	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Len(t, store.records, 2)

	firstStored := findStoredJobInfoByExecutionKey(t, store, firstExecutionKey)
	firstRecord := parseCloudCheckpointFromJobInfo(firstStored)
	require.NotNil(t, firstRecord)
	require.Equal(t, "first", firstRecord.State["step"])

	secondStored := findStoredJobInfoByExecutionKey(t, store, secondExecutionKey)
	secondRecord := parseCloudCheckpointFromJobInfo(secondStored)
	require.NotNil(t, secondRecord)
	require.Equal(t, "completed", secondRecord.State["step"])
	require.NotNil(t, secondRecord.Request)
	require.Equal(t, "cn-shanghai-b", secondRecord.Request.Params["zone"])
}

func TestCloudJobCtlRunInProgressWithoutRequeueDelay(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	provider := &fakeCloudProvider{
		name:    "mock",
		runtime: &fakeCloudRuntime{},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"provision": &fakeCloudAction{
					run: func(_ context.Context, _ CloudRuntime, _ *CloudJobRequest, _ map[string]interface{}) (*CloudActionProgress, error) {
						return &CloudActionProgress{
							Done:  false,
							State: map[string]interface{}{"step": "waiting"},
						}, nil
					},
				},
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-missing-requeue",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "did not provide requeue delay")

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusFailed, record.Status)
	require.Equal(t, "waiting", record.State["step"])
}

func TestCloudJobCtlRunReturnsStatusTimeoutWhenActionTimesOut(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	provider := &fakeCloudProvider{
		name:    "mock",
		runtime: &fakeCloudRuntime{},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"provision": &fakeCloudAction{
					runErr: context.DeadlineExceeded,
				},
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-timeout",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, statusErr.Status)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusTimeout, record.Status)
}

func TestCloudJobCtlRunReturnsStatusTimeoutWhenWaitTimesOut(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	provider := &fakeCloudProvider{
		name:    "mock",
		runtime: &fakeCloudRuntime{},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"provision": &fakeCloudAction{
					run: func(_ context.Context, _ CloudRuntime, _ *CloudJobRequest, _ map[string]interface{}) (*CloudActionProgress, error) {
						return &CloudActionProgress{
							Done:         false,
							RequeueAfter: time.Millisecond,
							State: map[string]interface{}{
								"step": "waiting",
							},
						}, nil
					},
				},
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-wait-timeout",
		JobType: string(config.JobDeployCloud),
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)
	ctl.waitFunc = func(_ context.Context, _ time.Duration) error {
		return context.DeadlineExceeded
	}

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, statusErr.Status)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusTimeout, record.Status)
	require.Equal(t, "waiting", record.State["step"])
}

func TestCloudJobCtlRunAppliesJobTimeoutContextToLoop(t *testing.T) {
	resetCloudProvidersForTest()
	defer restoreBuiltinCloudProvidersForTest()

	provider := &fakeCloudProvider{
		name:    "mock",
		runtime: &fakeCloudRuntime{},
		registry: &fakeCloudActionRegistry{
			actions: map[string]CloudAction{
				"provision": &fakeCloudAction{
					run: func(ctx context.Context, _ CloudRuntime, _ *CloudJobRequest, _ map[string]interface{}) (*CloudActionProgress, error) {
						if _, ok := ctx.Deadline(); !ok {
							return nil, fmt.Errorf("cloud job context missing deadline")
						}
						return &CloudActionProgress{
							Done:         false,
							RequeueAfter: time.Second,
							State: map[string]interface{}{
								"step": "waiting",
							},
						}, nil
					},
				},
			},
		},
	}
	RegisterCloudProvider(provider)

	task := &model.JobTask{
		Name:    "cloud-timeout-context",
		JobType: string(config.JobDeployCloud),
		Timeout: 1,
		JobInfo: &CloudJobInfo{
			Provider: "mock",
			Action:   "provision",
		},
	}
	ctl := NewCloudJobCtl(task, &noopStore{})
	require.NotNil(t, ctl)
	ctl.waitFunc = func(ctx context.Context, _ time.Duration) error {
		if _, ok := ctx.Deadline(); !ok {
			return fmt.Errorf("cloud wait context missing deadline")
		}
		return context.DeadlineExceeded
	}

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, statusErr.Status)

	var record CloudJobRecord
	require.NoError(t, json.Unmarshal([]byte(task.InternalInfo), &record))
	require.Equal(t, config.StatusTimeout, record.Status)
	require.Equal(t, "waiting", record.State["step"])
}

func TestBuildCloudJobRequestIncludesExecutionKey(t *testing.T) {
	request := buildCloudJobRequest(&model.JobTask{
		Name: "cloud", TaskID: "task-1", ExecutionKey: "execution-1",
	}, &CloudJobInfo{Provider: "mock", Action: "provision"}, nil)

	require.Equal(t, "execution-1", request.ExecutionKey)
}

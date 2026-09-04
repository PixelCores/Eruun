package aliyun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	aliyunnas "github.com/alibabacloud-go/nas-20170626/v2/client"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type fakeCloudRuntime struct {
	result *contracts.CloudJobResult
	err    error
	action string
	params map[string]interface{}
}

func (f *fakeCloudRuntime) Call(_ context.Context, action string, params map[string]interface{}) (*contracts.CloudJobResult, error) {
	f.action = action
	f.params = params
	return f.result, f.err
}

type cloudRuntimeExpectation struct {
	action       string
	result       *contracts.CloudJobResult
	err          error
	assertParams func(t *testing.T, params map[string]interface{})
}

type scriptedCloudRuntime struct {
	t            *testing.T
	expectations []cloudRuntimeExpectation
	callCount    int
}

func (s *scriptedCloudRuntime) Call(_ context.Context, action string, params map[string]interface{}) (*contracts.CloudJobResult, error) {
	s.callCount++
	if len(s.expectations) == 0 {
		return nil, fmt.Errorf("unexpected cloud runtime call #%d action=%s", s.callCount, action)
	}
	next := s.expectations[0]
	s.expectations = s.expectations[1:]
	if next.action != action {
		return nil, fmt.Errorf("unexpected cloud runtime action #%d got=%s want=%s", s.callCount, action, next.action)
	}
	if next.assertParams != nil {
		next.assertParams(s.t, params)
	}
	return next.result, next.err
}

func (s *scriptedCloudRuntime) assertExhausted(t *testing.T) {
	t.Helper()
	require.Len(t, s.expectations, 0)
}

type fakeAliyunNASClient struct {
	createFileSystemFn     func(request *aliyunnas.CreateFileSystemRequest) (*aliyunnas.CreateFileSystemResponse, error)
	createMountTargetFn    func(request *aliyunnas.CreateMountTargetRequest) (*aliyunnas.CreateMountTargetResponse, error)
	describeFileSystemsFn  func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error)
	describeMountTargetsFn func(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error)
	tagResourcesFn         func(request *aliyunnas.TagResourcesRequest) (*aliyunnas.TagResourcesResponse, error)
}

func (f *fakeAliyunNASClient) CreateFileSystem(request *aliyunnas.CreateFileSystemRequest) (*aliyunnas.CreateFileSystemResponse, error) {
	if f.createFileSystemFn == nil {
		return nil, fmt.Errorf("unexpected CreateFileSystem call")
	}
	return f.createFileSystemFn(request)
}

func (f *fakeAliyunNASClient) CreateMountTarget(request *aliyunnas.CreateMountTargetRequest) (*aliyunnas.CreateMountTargetResponse, error) {
	if f.createMountTargetFn == nil {
		return nil, fmt.Errorf("unexpected CreateMountTarget call")
	}
	return f.createMountTargetFn(request)
}

func (f *fakeAliyunNASClient) DescribeFileSystems(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
	if f.describeFileSystemsFn == nil {
		return nil, fmt.Errorf("unexpected DescribeFileSystems call")
	}
	return f.describeFileSystemsFn(request)
}

func (f *fakeAliyunNASClient) DescribeMountTargets(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error) {
	if f.describeMountTargetsFn == nil {
		return nil, fmt.Errorf("unexpected DescribeMountTargets call")
	}
	return f.describeMountTargetsFn(request)
}

func (f *fakeAliyunNASClient) TagResources(request *aliyunnas.TagResourcesRequest) (*aliyunnas.TagResourcesResponse, error) {
	if f.tagResourcesFn == nil {
		return nil, fmt.Errorf("unexpected TagResources call")
	}
	return f.tagResourcesFn(request)
}

type fakeSystemSettingStore struct {
	setting *model.SystemSetting
}

func (f *fakeSystemSettingStore) Add(context.Context, datastore.Entity) error        { return nil }
func (f *fakeSystemSettingStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (f *fakeSystemSettingStore) Put(context.Context, datastore.Entity) error        { return nil }
func (f *fakeSystemSettingStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (f *fakeSystemSettingStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (f *fakeSystemSettingStore) Get(_ context.Context, entity datastore.Entity) error {
	setting, ok := entity.(*model.SystemSetting)
	if !ok {
		return datastore.ErrEntityInvalid
	}
	if f.setting == nil || setting.Type != f.setting.Type {
		return datastore.ErrRecordNotExist
	}
	clone := *f.setting
	setting.Type = clone.Type
	setting.Value = append(json.RawMessage(nil), clone.Value...)
	return nil
}

func (f *fakeSystemSettingStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}

func (f *fakeSystemSettingStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (f *fakeSystemSettingStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (f *fakeSystemSettingStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (f *fakeSystemSettingStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return true, nil
}

func testAliyunCloudSetting() spec.AliyunCloudSettingSpec {
	return spec.AliyunCloudSettingSpec{
		AccessKeyID:     "test-ak",
		AccessKeySecret: "test-sk",
		RegionID:        "cn-hangzhou",
		ZoneID:          "cn-hangzhou-i",
		VpcID:           "vpc-001",
		VSwitchID:       "vsw-001",
	}
}

func mustMarshalAliyunCloudSetting(t *testing.T, setting spec.AliyunCloudSettingSpec) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(setting)
	require.NoError(t, err)
	return json.RawMessage(data)
}

func int64Ptr(value int64) *int64 {
	ptr := value
	return &ptr
}

func TestProviderResolveActionBuiltins(t *testing.T) {
	provider := NewProvider()

	for _, actionName := range []string{
		ActionNasEnsureFilesystem,
		ActionNasEnsureMountTarget,
		ActionK8sEnsureStorageClass,
	} {
		action, ok := provider.ResolveAction(actionName)
		require.True(t, ok)
		require.NotNil(t, action)
	}

	supported := provider.SupportedActions()
	require.ElementsMatch(t, []string{
		ActionNasEnsureFilesystem,
		ActionNasEnsureMountTarget,
		ActionK8sEnsureStorageClass,
	}, supported)
}

func TestProviderResolveActionUnknownStrictWhitelist(t *testing.T) {
	provider := NewProvider()

	action, ok := provider.ResolveAction("custom.action")
	require.False(t, ok)
	require.Nil(t, action)
}

func TestProviderValidateSystemSettingConnectivity(t *testing.T) {
	provider := NewProvider()
	provider.connectivityClientFactory = func(config spec.AliyunCloudSettingSpec) (nasClient, error) {
		require.Equal(t, testAliyunCloudSetting(), config)
		return &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				require.NotNil(t, request)
				require.NotNil(t, request.PageNumber)
				require.NotNil(t, request.PageSize)
				require.EqualValues(t, 1, *request.PageNumber)
				require.EqualValues(t, 1, *request.PageSize)
				return &aliyunnas.DescribeFileSystemsResponse{
					Body: &aliyunnas.DescribeFileSystemsResponseBody{
						RequestId: stringPtr("req-connectivity"),
					},
				}, nil
			},
		}, nil
	}

	err := provider.ValidateSystemSettingConnectivity(context.Background(), mustMarshalAliyunCloudSetting(t, testAliyunCloudSetting()))
	require.NoError(t, err)
	require.Equal(t, model.SystemSettingTypeAliyunCloud, provider.SystemSettingType())
}

func TestProviderValidateSystemSettingConnectivityRejectsInvalidConfig(t *testing.T) {
	provider := NewProvider()
	provider.connectivityClientFactory = func(spec.AliyunCloudSettingSpec) (nasClient, error) {
		t.Fatalf("connectivity client factory should not be called for invalid config")
		return nil, nil
	}

	err := provider.ValidateSystemSettingConnectivity(context.Background(), json.RawMessage(`{
		"accessKeyId":"test-ak",
		"accessKeySecret":"******",
		"regionId":"cn-hangzhou"
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "accessKeySecret")
}

func TestProviderValidateSystemSettingConnectivitySDKError(t *testing.T) {
	provider := NewProvider()
	provider.connectivityClientFactory = func(spec.AliyunCloudSettingSpec) (nasClient, error) {
		return &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				return nil, errors.New("sdk failed")
			},
		}, nil
	}

	err := provider.ValidateSystemSettingConnectivity(context.Background(), mustMarshalAliyunCloudSetting(t, testAliyunCloudSetting()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "sdk failed")
}

func TestProviderValidateSystemSettingConnectivityNilBody(t *testing.T) {
	provider := NewProvider()
	provider.connectivityClientFactory = func(spec.AliyunCloudSettingSpec) (nasClient, error) {
		return &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				return &aliyunnas.DescribeFileSystemsResponse{}, nil
			},
		}, nil
	}

	err := provider.ValidateSystemSettingConnectivity(context.Background(), mustMarshalAliyunCloudSetting(t, testAliyunCloudSetting()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil body")
}

func TestNasEnsureFilesystemActionValidateAndRun(t *testing.T) {
	action := &nasEnsureFilesystemAction{}

	err := action.Validate(&contracts.CloudJobRequest{Params: map[string]interface{}{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), ParamTenantID)

	require.NoError(t, action.Validate(&contracts.CloudJobRequest{
		Params: map[string]interface{}{
			ParamTenantID:     "tenant-1",
			ParamStorageType:  "Capacity",
			ParamProtocolType: "NFS",
		},
	}))

	runtime := &fakeCloudRuntime{
		result: &contracts.CloudJobResult{
			Output: map[string]interface{}{StateFileSystemIDKey: "fs-001"},
		},
	}
	progress, runErr := action.Run(context.Background(), runtime, &contracts.CloudJobRequest{
		Params: map[string]interface{}{
			ParamTenantID:     "tenant-1",
			ParamStorageType:  "Capacity",
			ParamProtocolType: "NFS",
		},
	}, nil)
	require.NoError(t, runErr)
	require.NotNil(t, progress)
	require.True(t, progress.Done)
	require.Equal(t, ActionNasEnsureFilesystem, runtime.action)
	require.Equal(t, StateStepFilesystemReady, progress.State[StateStepKey])
	require.Equal(t, "fs-001", progress.State[StateFileSystemIDKey])
}

func TestNasEnsureFilesystemActionRunSDKError(t *testing.T) {
	action := &nasEnsureFilesystemAction{}
	expectedErr := errors.New("runtime failed")
	runtime := &fakeCloudRuntime{err: expectedErr}
	_, err := action.Run(context.Background(), runtime, &contracts.CloudJobRequest{
		Params: map[string]interface{}{
			ParamTenantID:     "tenant-1",
			ParamStorageType:  "Capacity",
			ParamProtocolType: "NFS",
		},
	}, nil)
	require.ErrorIs(t, err, expectedErr)
}

func TestNasEnsureFilesystemActionRequeuesWhenTagPending(t *testing.T) {
	action := &nasEnsureFilesystemAction{}
	runtime := &fakeCloudRuntime{
		err: &fileSystemTagPendingError{
			fileSystemID: "fs-001",
			requestID:    "req-create-fs",
			cause:        errors.New("tag failed"),
		},
	}

	progress, err := action.Run(context.Background(), runtime, &contracts.CloudJobRequest{
		Params: map[string]interface{}{
			ParamTenantID:        "tenant-1",
			ParamStorageType:     "Capacity",
			ParamProtocolType:    "NFS",
			ParamPollIntervalSec: 2,
		},
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.False(t, progress.Done)
	require.Equal(t, 2*time.Second, progress.RequeueAfter)
	require.Equal(t, StateStepFilesystemTagPending, progress.State[StateStepKey])
	require.Equal(t, "fs-001", progress.State[StateFileSystemIDKey])
	require.EqualValues(t, 1, progress.State[StateFileSystemTagRetryCountKey])
}

func TestNasEnsureFilesystemActionFailsAfterMaxTagPendingRetries(t *testing.T) {
	action := &nasEnsureFilesystemAction{}
	runtime := &fakeCloudRuntime{
		err: &fileSystemTagPendingError{
			fileSystemID: "fs-001",
			requestID:    "req-create-fs",
			cause:        errors.New("not visible yet"),
		},
	}
	req := &contracts.CloudJobRequest{
		Params: map[string]interface{}{
			ParamTenantID:        "tenant-1",
			ParamStorageType:     "Capacity",
			ParamProtocolType:    "NFS",
			ParamPollIntervalSec: 2,
		},
	}

	progress, err := action.Run(context.Background(), runtime, req, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, progress.State[StateFileSystemTagRetryCountKey])

	progress, err = action.Run(context.Background(), runtime, req, progress.State)
	require.NoError(t, err)
	require.EqualValues(t, 2, progress.State[StateFileSystemTagRetryCountKey])

	progress, err = action.Run(context.Background(), runtime, req, progress.State)
	require.NoError(t, err)
	require.EqualValues(t, 3, progress.State[StateFileSystemTagRetryCountKey])

	_, err = action.Run(context.Background(), runtime, req, progress.State)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeded max retries")
	require.Contains(t, err.Error(), "fs-001")
}

func TestAliyunActionValidateRejectsManagedTopologyParams(t *testing.T) {
	tests := []struct {
		name   string
		action contracts.CloudAction
		req    *contracts.CloudJobRequest
		key    string
	}{
		{
			name:   "filesystem region",
			action: &nasEnsureFilesystemAction{},
			req: &contracts.CloudJobRequest{Params: map[string]interface{}{
				ParamTenantID:     "tenant-1",
				ParamStorageType:  "Capacity",
				ParamProtocolType: "NFS",
				ParamRegionID:     "cn-hangzhou",
			}},
			key: ParamRegionID,
		},
		{
			name:   "mount target vpc",
			action: &nasEnsureMountTargetAction{},
			req: &contracts.CloudJobRequest{Params: map[string]interface{}{
				ParamTenantID: "tenant-1",
				ParamVpcID:    "vpc-001",
			}},
			key: ParamVpcID,
		},
		{
			name:   "storageclass region",
			action: &k8sEnsureStorageClassAction{},
			req: &contracts.CloudJobRequest{Params: map[string]interface{}{
				ParamTenantID:         "tenant-1",
				ParamStorageClassName: "sc-tenant-1",
				ParamRegionID:         "cn-hangzhou",
			}},
			key: ParamRegionID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.action.Validate(tc.req)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.key)
			require.Contains(t, err.Error(), model.SystemSettingTypeAliyunCloud)
		})
	}
}

func TestAliyunActionValidateRejectsManagedStateParams(t *testing.T) {
	tests := []struct {
		name   string
		action contracts.CloudAction
		req    *contracts.CloudJobRequest
		key    string
	}{
		{
			name:   "filesystem fileSystemId",
			action: &nasEnsureFilesystemAction{},
			req: &contracts.CloudJobRequest{Params: map[string]interface{}{
				ParamTenantID:        "tenant-1",
				ParamStorageType:     "Capacity",
				ParamProtocolType:    "NFS",
				StateFileSystemIDKey: "fs-001",
			}},
			key: StateFileSystemIDKey,
		},
		{
			name:   "mount target fileSystemId",
			action: &nasEnsureMountTargetAction{},
			req: &contracts.CloudJobRequest{Params: map[string]interface{}{
				ParamTenantID:        "tenant-1",
				StateFileSystemIDKey: "fs-001",
			}},
			key: StateFileSystemIDKey,
		},
		{
			name:   "mount target mount domain",
			action: &nasEnsureMountTargetAction{},
			req: &contracts.CloudJobRequest{Params: map[string]interface{}{
				ParamTenantID:       "tenant-1",
				StateMountDomainKey: "d-001.nas.aliyuncs.com",
			}},
			key: StateMountDomainKey,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.action.Validate(tc.req)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.key)
			require.Contains(t, err.Error(), "workflow state")
		})
	}
}

func TestNasEnsureMountTargetActionStateMachine(t *testing.T) {
	action := &nasEnsureMountTargetAction{}
	runtime := &scriptedCloudRuntime{
		t: t,
		expectations: []cloudRuntimeExpectation{
			{
				action: ActionNasDescribeMountTarget,
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateFileSystemIDKey: "fs-001",
					},
				},
			},
			{
				action: ActionNasEnsureMountTarget,
				assertParams: func(t *testing.T, params map[string]interface{}) {
					require.Equal(t, "tenant-1", params[ParamTenantID])
					require.Equal(t, "fs-001", params[StateFileSystemIDKey])
				},
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateMountDomainKey: "d-001.nas.aliyuncs.com",
					},
				},
			},
			{
				action: ActionNasDescribeMountTarget,
				assertParams: func(t *testing.T, params map[string]interface{}) {
					require.Equal(t, "tenant-1", params[ParamTenantID])
					require.Equal(t, "fs-001", params[StateFileSystemIDKey])
				},
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateMountStatusKey:      "Active",
						StateMountDomainKey:      "d-001.nas.aliyuncs.com",
						StateMountConfirmInfoKey: "confirm-001",
					},
				},
			},
		},
	}

	req := &contracts.CloudJobRequest{
		Provider: ProviderName,
		Action:   ActionNasEnsureMountTarget,
		Params: map[string]interface{}{
			ParamTenantID:        "tenant-1",
			ParamPollIntervalSec: 2,
		},
	}

	require.NoError(t, action.Validate(req))

	progress, err := action.Run(context.Background(), runtime, req, nil)
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.True(t, progress.Done)
	require.Equal(t, StateStepMountTargetReady, progress.State[StateStepKey])
	require.Equal(t, "confirm-001", progress.State[StateMountConfirmInfoKey])
	require.Equal(t, "d-001.nas.aliyuncs.com", progress.State[StateMountDomainKey])

	runtime.assertExhausted(t)
}

func TestNasEnsureMountTargetActionWaitConfirmInfo(t *testing.T) {
	action := &nasEnsureMountTargetAction{}
	runtime := &scriptedCloudRuntime{
		t: t,
		expectations: []cloudRuntimeExpectation{
			{
				action: ActionNasDescribeMountTarget,
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateFileSystemIDKey: "fs-001",
					},
				},
			},
			{
				action: ActionNasEnsureMountTarget,
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateMountDomainKey: "d-001.nas.aliyuncs.com",
					},
				},
			},
			{
				action: ActionNasDescribeMountTarget,
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateMountStatusKey: "Active",
						StateMountDomainKey: "d-001.nas.aliyuncs.com",
					},
				},
			},
		},
	}

	req := &contracts.CloudJobRequest{
		Provider: ProviderName,
		Action:   ActionNasEnsureMountTarget,
		Params: map[string]interface{}{
			ParamTenantID:        "tenant-1",
			ParamPollIntervalSec: 4,
		},
	}

	progress, err := action.Run(context.Background(), runtime, req, nil)
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.False(t, progress.Done)
	require.Equal(t, 4*time.Second, progress.RequeueAfter)
	require.Equal(t, StateStepMountTargetPending, progress.State[StateStepKey])
	_, hasConfirm := progress.State[StateMountConfirmInfoKey]
	require.False(t, hasConfirm)

	runtime.assertExhausted(t)
}

func TestNasEnsureMountTargetActionPendingStateSkipsRecreate(t *testing.T) {
	action := &nasEnsureMountTargetAction{}
	runtime := &scriptedCloudRuntime{
		t: t,
		expectations: []cloudRuntimeExpectation{
			{
				action: ActionNasDescribeMountTarget,
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateFileSystemIDKey: "fs-001",
					},
				},
			},
			{
				action: ActionNasEnsureMountTarget,
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateMountDomainKey: "d-001.nas.aliyuncs.com",
					},
				},
			},
			{
				action: ActionNasDescribeMountTarget,
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateMountStatusKey: "Creating",
						StateMountDomainKey: "d-001.nas.aliyuncs.com",
					},
				},
			},
			{
				action: ActionNasDescribeMountTarget,
				assertParams: func(t *testing.T, params map[string]interface{}) {
					require.Equal(t, "tenant-1", params[ParamTenantID])
					require.Equal(t, "fs-001", params[StateFileSystemIDKey])
				},
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateMountStatusKey:      "Active",
						StateMountDomainKey:      "d-001.nas.aliyuncs.com",
						StateMountConfirmInfoKey: "confirm-001",
					},
				},
			},
		},
	}

	req := &contracts.CloudJobRequest{
		Provider: ProviderName,
		Action:   ActionNasEnsureMountTarget,
		Params: map[string]interface{}{
			ParamTenantID:        "tenant-1",
			ParamPollIntervalSec: 3,
		},
	}

	progress, err := action.Run(context.Background(), runtime, req, nil)
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.False(t, progress.Done)
	require.Equal(t, StateStepMountTargetPending, progress.State[StateStepKey])

	progress, err = action.Run(context.Background(), runtime, req, progress.State)
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.True(t, progress.Done)
	require.Equal(t, StateStepMountTargetReady, progress.State[StateStepKey])

	runtime.assertExhausted(t)
}

func TestK8sEnsureStorageClassActionWaitThenCreate(t *testing.T) {
	action := &k8sEnsureStorageClassAction{}
	runtime := &scriptedCloudRuntime{
		t: t,
		expectations: []cloudRuntimeExpectation{
			{
				action: ActionNasDescribeMountTarget,
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateFileSystemIDKey: "fs-001",
						StateMountStatusKey:  "Creating",
					},
				},
			},
			{
				action: ActionNasDescribeMountTarget,
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						StateFileSystemIDKey:     "fs-001",
						StateMountStatusKey:      "Active",
						StateMountDomainKey:      "d-001.nas.aliyuncs.com",
						StateMountConfirmInfoKey: "confirm-001",
					},
				},
			},
			{
				action: ActionK8sEnsureStorageClass,
				assertParams: func(t *testing.T, params map[string]interface{}) {
					require.Equal(t, "tenant-1", params[ParamTenantID])
					require.Equal(t, "fs-001", params[StateFileSystemIDKey])
					require.Equal(t, "d-001.nas.aliyuncs.com", params[StateMountDomainKey])
					require.Equal(t, "confirm-001", params[StateMountConfirmInfoKey])
					require.Equal(t, "sc-tenant-1", params[ParamStorageClassName])
				},
				result: &contracts.CloudJobResult{
					Output: map[string]interface{}{
						ParamStorageClassName: "sc-tenant-1",
					},
				},
			},
		},
	}

	req := &contracts.CloudJobRequest{
		Provider: ProviderName,
		Action:   ActionK8sEnsureStorageClass,
		Params: map[string]interface{}{
			ParamTenantID:         "tenant-1",
			ParamStorageClassName: "sc-tenant-1",
			ParamPollIntervalSec:  3,
		},
	}

	require.NoError(t, action.Validate(req))

	progress, err := action.Run(context.Background(), runtime, req, nil)
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.False(t, progress.Done)
	require.Equal(t, 3*time.Second, progress.RequeueAfter)
	require.Equal(t, StateStepStorageClassWaitMountTarget, progress.State[StateStepKey])

	progress, err = action.Run(context.Background(), runtime, req, progress.State)
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.True(t, progress.Done)
	require.Equal(t, StateStepStorageClassReady, progress.State[StateStepKey])
	require.Equal(t, "confirm-001", progress.State[StateMountConfirmInfoKey])
	require.NotNil(t, progress.Result)
	require.Equal(t, "sc-tenant-1", progress.Result.Output[ParamStorageClassName])

	runtime.assertExhausted(t)
}

func TestCloudPollIntervalDefaultAndParsing(t *testing.T) {
	require.Equal(t, DefaultPollInterval, cloudPollInterval(nil))
	require.Equal(t, DefaultPollInterval, cloudPollInterval(map[string]interface{}{ParamPollIntervalSec: "abc"}))
	require.Equal(t, 5*time.Second, cloudPollInterval(map[string]interface{}{ParamPollIntervalSec: "5"}))
	require.Equal(t, 7*time.Second, cloudPollInterval(map[string]interface{}{ParamPollIntervalSec: 7}))
}

func TestProviderNewRuntimeRequiresAliyunCloudSetting(t *testing.T) {
	provider := NewProvider()
	ctx := contracts.WithDataStore(context.Background(), &fakeSystemSettingStore{})
	_, err := provider.NewRuntime(ctx, &contracts.CloudJobRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), model.SystemSettingTypeAliyunCloud)
}

func TestProviderNewRuntimeLoadsAliyunCloudSettingFromContext(t *testing.T) {
	provider := NewProvider()
	config := testAliyunCloudSetting()
	ctx := contracts.WithDataStore(context.Background(), &fakeSystemSettingStore{
		setting: &model.SystemSetting{
			Type:  model.SystemSettingTypeAliyunCloud,
			Value: mustMarshalAliyunCloudSetting(t, config),
		},
	})
	req := &contracts.CloudJobRequest{}

	rawRuntime, err := provider.NewRuntime(ctx, req)
	require.NoError(t, err)
	typedRuntime, ok := rawRuntime.(*client)
	require.True(t, ok)
	require.Equal(t, config.RegionID, typedRuntime.config.RegionID)
	require.Equal(t, config.ZoneID, typedRuntime.config.ZoneID)
	require.Equal(t, config.VpcID, typedRuntime.config.VpcID)
	require.Equal(t, config.VSwitchID, typedRuntime.config.VSwitchID)

	runtimeSnapshot, ok := req.RuntimeProviderSnapshot.(*runtimeAliyunSnapshot)
	require.True(t, ok)
	require.Equal(t, config.RegionID, runtimeSnapshot.RegionID)
	require.Equal(t, config.ZoneID, runtimeSnapshot.ZoneID)
	require.Equal(t, config.VpcID, runtimeSnapshot.VpcID)
	require.Equal(t, config.VSwitchID, runtimeSnapshot.VSwitchID)
	require.Empty(t, runtimeSnapshot.Endpoint)

	rawReq, err := json.Marshal(req)
	require.NoError(t, err)
	require.NotContains(t, string(rawReq), "configSnapshot")
	require.NotContains(t, string(rawReq), "accessKeySecret")
	require.NotContains(t, string(rawReq), "test-sk")
}

func TestProviderNewRuntimeRejectsResumeWithoutRuntimeProviderSnapshot(t *testing.T) {
	provider := NewProvider()
	ctx := contracts.WithDataStore(context.Background(), &fakeSystemSettingStore{
		setting: &model.SystemSetting{
			Type:  model.SystemSettingTypeAliyunCloud,
			Value: mustMarshalAliyunCloudSetting(t, testAliyunCloudSetting()),
		},
	})
	req := &contracts.CloudJobRequest{
		ResumeFromPersistedState: true,
	}

	_, err := provider.NewRuntime(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot resume safely after process restart")
}

func TestProviderNewRuntimeUsesRuntimeProviderSnapshotOnResume(t *testing.T) {
	provider := NewProvider()
	currentConfig := testAliyunCloudSetting()
	currentConfig.AccessKeyID = "current-ak"
	currentConfig.AccessKeySecret = "current-sk"
	currentConfig.RegionID = "cn-shanghai"
	currentConfig.ZoneID = "cn-shanghai-b"
	currentConfig.VpcID = "vpc-current"
	currentConfig.VSwitchID = "vsw-current"

	ctx := contracts.WithDataStore(context.Background(), &fakeSystemSettingStore{
		setting: &model.SystemSetting{
			Type:  model.SystemSettingTypeAliyunCloud,
			Value: mustMarshalAliyunCloudSetting(t, currentConfig),
		},
	})
	req := &contracts.CloudJobRequest{
		ResumeFromPersistedState: true,
		RuntimeProviderSnapshot: &runtimeAliyunSnapshot{
			Endpoint:  "nas.cn-hangzhou.aliyuncs.com",
			RegionID:  "cn-hangzhou",
			ZoneID:    "cn-hangzhou-i",
			VpcID:     "vpc-stable",
			VSwitchID: "vsw-stable",
		},
	}

	rawRuntime, err := provider.NewRuntime(ctx, req)
	require.NoError(t, err)
	typedRuntime, ok := rawRuntime.(*client)
	require.True(t, ok)
	require.Equal(t, "current-ak", typedRuntime.config.AccessKeyID)
	require.Equal(t, "current-sk", typedRuntime.config.AccessKeySecret)
	require.Equal(t, "cn-hangzhou", typedRuntime.config.RegionID)
	require.Equal(t, "cn-hangzhou-i", typedRuntime.config.ZoneID)
	require.Equal(t, "vpc-stable", typedRuntime.config.VpcID)
	require.Equal(t, "vsw-stable", typedRuntime.config.VSwitchID)

	rawReq, err := json.Marshal(req)
	require.NoError(t, err)
	require.NotContains(t, string(rawReq), "configSnapshot")
	require.NotContains(t, string(rawReq), "current-sk")
}

func TestSDKCallEnsureFilesystemCreatesAndTagsTenant(t *testing.T) {
	var createdRequest *aliyunnas.CreateFileSystemRequest
	var tagRequest *aliyunnas.TagResourcesRequest
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				require.NotNil(t, request.Tag)
				require.Len(t, request.Tag, 1)
				require.Equal(t, AliyunTenantTagKey, stringValue(request.Tag[0].Key))
				require.Equal(t, "tenant-1", stringValue(request.Tag[0].Value))
				return &aliyunnas.DescribeFileSystemsResponse{
					Body: &aliyunnas.DescribeFileSystemsResponseBody{
						RequestId: stringPtr("req-describe"),
					},
				}, nil
			},
			createFileSystemFn: func(request *aliyunnas.CreateFileSystemRequest) (*aliyunnas.CreateFileSystemResponse, error) {
				createdRequest = request
				return &aliyunnas.CreateFileSystemResponse{
					Body: &aliyunnas.CreateFileSystemResponseBody{
						RequestId:    stringPtr("req-create-fs"),
						FileSystemId: stringPtr("fs-001"),
					},
				}, nil
			},
			tagResourcesFn: func(request *aliyunnas.TagResourcesRequest) (*aliyunnas.TagResourcesResponse, error) {
				tagRequest = request
				return &aliyunnas.TagResourcesResponse{
					Body: &aliyunnas.TagResourcesResponseBody{
						RequestId: stringPtr("req-tag"),
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureFilesystem, map[string]interface{}{
		ParamTenantID:     "tenant-1",
		ParamStorageType:  "Capacity",
		ParamProtocolType: "NFS",
		ParamCapacityGiB:  100,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "req-create-fs", result.RequestID)
	require.Equal(t, "fs-001", result.Output[StateFileSystemIDKey])
	require.NotNil(t, createdRequest)
	require.Equal(t, "Capacity", stringValue(createdRequest.StorageType))
	require.Equal(t, "NFS", stringValue(createdRequest.ProtocolType))
	require.Equal(t, "cn-hangzhou-i", stringValue(createdRequest.ZoneId))
	require.EqualValues(t, 100, *createdRequest.Capacity)
	require.NotNil(t, tagRequest)
	require.Equal(t, AliyunNASResourceTypeFileSystem, stringValue(tagRequest.ResourceType))
	require.Len(t, tagRequest.ResourceId, 1)
	require.Equal(t, "fs-001", stringValue(tagRequest.ResourceId[0]))
	require.Len(t, tagRequest.Tag, 1)
	require.Equal(t, AliyunTenantTagKey, stringValue(tagRequest.Tag[0].Key))
	require.Equal(t, "tenant-1", stringValue(tagRequest.Tag[0].Value))
}

func TestSDKCallEnsureFilesystemReusesExistingTaggedFilesystem(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				return &aliyunnas.DescribeFileSystemsResponse{
					Body: &aliyunnas.DescribeFileSystemsResponseBody{
						RequestId: stringPtr("req-describe"),
						FileSystems: &aliyunnas.DescribeFileSystemsResponseBodyFileSystems{
							FileSystem: []*aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem{
								{
									FileSystemId:   stringPtr("fs-existing"),
									StorageType:    stringPtr("Capacity"),
									ProtocolType:   stringPtr("NFS"),
									FileSystemType: stringPtr("standard"),
									Capacity:       int64Ptr(100),
								},
							},
						},
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureFilesystem, map[string]interface{}{
		ParamTenantID:       "tenant-1",
		ParamStorageType:    "Capacity",
		ParamProtocolType:   "NFS",
		ParamFileSystemType: "standard",
		ParamCapacityGiB:    100,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "fs-existing", result.Output[StateFileSystemIDKey])
}

func TestSDKCallEnsureFilesystemTagFailureReturnsPendingError(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				require.Nil(t, request.FileSystemId)
				return &aliyunnas.DescribeFileSystemsResponse{
					Body: &aliyunnas.DescribeFileSystemsResponseBody{
						RequestId: stringPtr("req-describe"),
					},
				}, nil
			},
			createFileSystemFn: func(request *aliyunnas.CreateFileSystemRequest) (*aliyunnas.CreateFileSystemResponse, error) {
				return &aliyunnas.CreateFileSystemResponse{
					Body: &aliyunnas.CreateFileSystemResponseBody{
						RequestId:    stringPtr("req-create-fs"),
						FileSystemId: stringPtr("fs-001"),
					},
				}, nil
			},
			tagResourcesFn: func(request *aliyunnas.TagResourcesRequest) (*aliyunnas.TagResourcesResponse, error) {
				return nil, errors.New("tag failed")
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureFilesystem, map[string]interface{}{
		ParamTenantID:     "tenant-1",
		ParamStorageType:  "Capacity",
		ParamProtocolType: "NFS",
	})
	require.Nil(t, result)
	var pendingErr *fileSystemTagPendingError
	require.ErrorAs(t, err, &pendingErr)
	require.Equal(t, "fs-001", pendingErr.fileSystemID)
	require.Equal(t, "req-create-fs", pendingErr.requestID)
}

func TestSDKCallEnsureFilesystemRetryTagsExplicitFileSystemID(t *testing.T) {
	tagCalls := 0
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				return &aliyunnas.DescribeFileSystemsResponse{
					Body: &aliyunnas.DescribeFileSystemsResponseBody{
						RequestId: stringPtr("req-describe"),
						FileSystems: &aliyunnas.DescribeFileSystemsResponseBodyFileSystems{
							FileSystem: []*aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem{
								{
									FileSystemId: stringPtr("fs-001"),
									StorageType:  stringPtr("Capacity"),
									ProtocolType: stringPtr("NFS"),
								},
							},
						},
					},
				}, nil
			},
			tagResourcesFn: func(request *aliyunnas.TagResourcesRequest) (*aliyunnas.TagResourcesResponse, error) {
				tagCalls++
				return &aliyunnas.TagResourcesResponse{
					Body: &aliyunnas.TagResourcesResponseBody{
						RequestId: stringPtr("req-tag"),
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureFilesystem, map[string]interface{}{
		ParamTenantID:        "tenant-1",
		ParamStorageType:     "Capacity",
		ParamProtocolType:    "NFS",
		StateFileSystemIDKey: "fs-001",
	})
	require.NoError(t, err)
	require.Equal(t, 1, tagCalls)
	require.Equal(t, "fs-001", result.Output[StateFileSystemIDKey])
}

func TestSDKCallEnsureFilesystemRetryNotVisibleReturnsPendingError(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				return &aliyunnas.DescribeFileSystemsResponse{
					Body: &aliyunnas.DescribeFileSystemsResponseBody{
						RequestId: stringPtr("req-describe"),
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureFilesystem, map[string]interface{}{
		ParamTenantID:        "tenant-1",
		ParamStorageType:     "Capacity",
		ParamProtocolType:    "NFS",
		StateFileSystemIDKey: "fs-001",
	})
	require.Nil(t, result)
	var pendingErr *fileSystemTagPendingError
	require.ErrorAs(t, err, &pendingErr)
	require.Equal(t, "fs-001", pendingErr.fileSystemID)
	require.Equal(t, "req-describe", pendingErr.requestID)
	require.Contains(t, pendingErr.Error(), "not visible yet")
}

func TestSDKCallEnsureFilesystemRejectsMismatchedExistingContract(t *testing.T) {
	tests := []struct {
		name       string
		existing   *aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem
		params     map[string]interface{}
		errorField string
	}{
		{
			name: "storage type",
			existing: &aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem{
				FileSystemId: stringPtr("fs-existing"),
				StorageType:  stringPtr("Performance"),
				ProtocolType: stringPtr("NFS"),
			},
			params: map[string]interface{}{
				ParamTenantID:     "tenant-1",
				ParamStorageType:  "Capacity",
				ParamProtocolType: "NFS",
			},
			errorField: ParamStorageType,
		},
		{
			name: "protocol type",
			existing: &aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem{
				FileSystemId: stringPtr("fs-existing"),
				StorageType:  stringPtr("Capacity"),
				ProtocolType: stringPtr("SMB"),
			},
			params: map[string]interface{}{
				ParamTenantID:     "tenant-1",
				ParamStorageType:  "Capacity",
				ParamProtocolType: "NFS",
			},
			errorField: ParamProtocolType,
		},
		{
			name: "file system type",
			existing: &aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem{
				FileSystemId:   stringPtr("fs-existing"),
				StorageType:    stringPtr("Capacity"),
				ProtocolType:   stringPtr("NFS"),
				FileSystemType: stringPtr("extreme"),
			},
			params: map[string]interface{}{
				ParamTenantID:       "tenant-1",
				ParamStorageType:    "Capacity",
				ParamProtocolType:   "NFS",
				ParamFileSystemType: "standard",
			},
			errorField: ParamFileSystemType,
		},
		{
			name: "capacity",
			existing: &aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem{
				FileSystemId: stringPtr("fs-existing"),
				StorageType:  stringPtr("Capacity"),
				ProtocolType: stringPtr("NFS"),
				Capacity:     int64Ptr(100),
			},
			params: map[string]interface{}{
				ParamTenantID:     "tenant-1",
				ParamStorageType:  "Capacity",
				ParamProtocolType: "NFS",
				ParamCapacityGiB:  200,
			},
			errorField: ParamCapacityGiB,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sdk := &client{
				config: testAliyunCloudSetting(),
				nas: &fakeAliyunNASClient{
					describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
						return &aliyunnas.DescribeFileSystemsResponse{
							Body: &aliyunnas.DescribeFileSystemsResponseBody{
								RequestId: stringPtr("req-describe"),
								FileSystems: &aliyunnas.DescribeFileSystemsResponseBodyFileSystems{
									FileSystem: []*aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem{tc.existing},
								},
							},
						}, nil
					},
				},
			}

			result, err := sdk.Call(context.Background(), ActionNasEnsureFilesystem, tc.params)
			require.Nil(t, result)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errorField)
		})
	}
}

func TestCloudMapInt64RejectsFractionalFloat(t *testing.T) {
	_, _, err := cloudMapInt64(map[string]interface{}{ParamCapacityGiB: 1.9}, ParamCapacityGiB)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fractional")
}

func TestCloudMapInt64AllowsWholeFloat(t *testing.T) {
	value, ok, err := cloudMapInt64(map[string]interface{}{ParamCapacityGiB: 2.0}, ParamCapacityGiB)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 2, value)
}

func TestSDKCallEnsureMountTargetCreatesWithVpcDefaults(t *testing.T) {
	var createRequest *aliyunnas.CreateMountTargetRequest
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeMountTargetsFn: func(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				return &aliyunnas.DescribeMountTargetsResponse{
					Body: &aliyunnas.DescribeMountTargetsResponseBody{
						RequestId: stringPtr("req-describe-mt"),
					},
				}, nil
			},
			createMountTargetFn: func(request *aliyunnas.CreateMountTargetRequest) (*aliyunnas.CreateMountTargetResponse, error) {
				createRequest = request
				return &aliyunnas.CreateMountTargetResponse{
					Body: &aliyunnas.CreateMountTargetResponseBody{
						RequestId:         stringPtr("req-create-mt"),
						MountTargetDomain: stringPtr("d-001.nas.aliyuncs.com"),
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureMountTarget, map[string]interface{}{
		ParamTenantID:        "tenant-1",
		StateFileSystemIDKey: "fs-001",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "d-001.nas.aliyuncs.com", result.Output[StateMountDomainKey])
	require.NotNil(t, createRequest)
	require.Equal(t, "fs-001", stringValue(createRequest.FileSystemId))
	require.Equal(t, "vpc-001", stringValue(createRequest.VpcId))
	require.Equal(t, "vsw-001", stringValue(createRequest.VSwitchId))
	require.Equal(t, AliyunNASNetworkTypeVpc, stringValue(createRequest.NetworkType))
	require.Equal(t, AliyunNASDefaultAccessGroupName, stringValue(createRequest.AccessGroupName))
}

func TestSDKCallEnsureMountTargetAllowsSecurityGroupReuseWithExplicitMountDomain(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeMountTargetsFn: func(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				require.Equal(t, "d-001.nas.aliyuncs.com", stringValue(request.MountTargetDomain))
				return &aliyunnas.DescribeMountTargetsResponse{
					Body: &aliyunnas.DescribeMountTargetsResponseBody{
						RequestId: stringPtr("req-describe-mt"),
						MountTargets: &aliyunnas.DescribeMountTargetsResponseBodyMountTargets{
							MountTarget: []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget{
								{
									MountTargetDomain: stringPtr("d-001.nas.aliyuncs.com"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-001"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
							},
						},
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureMountTarget, map[string]interface{}{
		ParamTenantID:        "tenant-1",
		StateFileSystemIDKey: "fs-001",
		StateMountDomainKey:  "d-001.nas.aliyuncs.com",
		ParamSecurityGroupID: "sg-001",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "fs-001", result.Output[StateFileSystemIDKey])
	require.Equal(t, "d-001.nas.aliyuncs.com", result.Output[StateMountDomainKey])
}

func TestSDKCallEnsureMountTargetRequiresMountDomainForSecurityGroupReuse(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeMountTargetsFn: func(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				return &aliyunnas.DescribeMountTargetsResponse{
					Body: &aliyunnas.DescribeMountTargetsResponseBody{
						RequestId: stringPtr("req-describe-mt"),
						MountTargets: &aliyunnas.DescribeMountTargetsResponseBodyMountTargets{
							MountTarget: []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget{
								{
									MountTargetDomain: stringPtr("d-001.nas.aliyuncs.com"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-001"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
							},
						},
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureMountTarget, map[string]interface{}{
		ParamTenantID:        "tenant-1",
		StateFileSystemIDKey: "fs-001",
		ParamSecurityGroupID: "sg-001",
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), ParamSecurityGroupID)
	require.Contains(t, err.Error(), StateMountDomainKey)
	require.Contains(t, err.Error(), "requires explicit")
}

func TestSDKCallEnsureMountTargetSelectsUniqueTargetByTopology(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeMountTargetsFn: func(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				require.Nil(t, request.MountTargetDomain)
				return &aliyunnas.DescribeMountTargetsResponse{
					Body: &aliyunnas.DescribeMountTargetsResponseBody{
						RequestId: stringPtr("req-describe-mt"),
						MountTargets: &aliyunnas.DescribeMountTargetsResponseBodyMountTargets{
							MountTarget: []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget{
								{
									MountTargetDomain: stringPtr("d-selected.nas.aliyuncs.com"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-001"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
								{
									MountTargetDomain: stringPtr("d-other.nas.aliyuncs.com"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-999"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
							},
						},
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureMountTarget, map[string]interface{}{
		ParamTenantID:        "tenant-1",
		StateFileSystemIDKey: "fs-001",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "d-selected.nas.aliyuncs.com", result.Output[StateMountDomainKey])
}

func TestSDKCallEnsureMountTargetReturnsErrorWhenTopologyHasNoMatch(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeMountTargetsFn: func(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				return &aliyunnas.DescribeMountTargetsResponse{
					Body: &aliyunnas.DescribeMountTargetsResponseBody{
						RequestId: stringPtr("req-describe-mt"),
						MountTargets: &aliyunnas.DescribeMountTargetsResponseBodyMountTargets{
							MountTarget: []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget{
								{
									MountTargetDomain: stringPtr("d-1.nas.aliyuncs.com"),
									VpcId:             stringPtr("vpc-999"),
									VswId:             stringPtr("vsw-001"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
								{
									MountTargetDomain: stringPtr("d-2.nas.aliyuncs.com"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-999"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
							},
						},
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureMountTarget, map[string]interface{}{
		ParamTenantID:        "tenant-1",
		StateFileSystemIDKey: "fs-001",
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no match for topology")
}

func TestSDKCallEnsureMountTargetReturnsErrorWhenTopologyIsStillAmbiguous(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeMountTargetsFn: func(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				return &aliyunnas.DescribeMountTargetsResponse{
					Body: &aliyunnas.DescribeMountTargetsResponseBody{
						RequestId: stringPtr("req-describe-mt"),
						MountTargets: &aliyunnas.DescribeMountTargetsResponseBodyMountTargets{
							MountTarget: []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget{
								{
									MountTargetDomain: stringPtr("d-1.nas.aliyuncs.com"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-001"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
								{
									MountTargetDomain: stringPtr("d-2.nas.aliyuncs.com"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-001"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
							},
						},
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasEnsureMountTarget, map[string]interface{}{
		ParamTenantID:        "tenant-1",
		StateFileSystemIDKey: "fs-001",
	})
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous for topology")
}

func TestSDKCallDescribeMountTargetByTenantReturnsConfirmInfo(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				require.Nil(t, request.FileSystemId)
				return &aliyunnas.DescribeFileSystemsResponse{
					Body: &aliyunnas.DescribeFileSystemsResponseBody{
						RequestId: stringPtr("req-describe-fs"),
						FileSystems: &aliyunnas.DescribeFileSystemsResponseBodyFileSystems{
							FileSystem: []*aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem{
								{FileSystemId: stringPtr("fs-001")},
							},
						},
					},
				}, nil
			},
			describeMountTargetsFn: func(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				return &aliyunnas.DescribeMountTargetsResponse{
					Body: &aliyunnas.DescribeMountTargetsResponseBody{
						RequestId: stringPtr("req-describe-mt"),
						MountTargets: &aliyunnas.DescribeMountTargetsResponseBodyMountTargets{
							MountTarget: []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget{
								{
									MountTargetDomain: stringPtr("d-001.nas.aliyuncs.com"),
									Status:            stringPtr("Active"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-001"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
									ClientMasterNodes: &aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTargetClientMasterNodes{
										ClientMasterNode: []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTargetClientMasterNodesClientMasterNode{
											{
												EcsId:         stringPtr("i-001"),
												EcsIp:         stringPtr("10.0.0.10"),
												DefaultPasswd: stringPtr("secret-should-not-leak"),
											},
										},
									},
								},
							},
						},
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasDescribeMountTarget, map[string]interface{}{
		ParamTenantID: "tenant-1",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "fs-001", result.Output[StateFileSystemIDKey])
	require.Equal(t, "d-001.nas.aliyuncs.com", result.Output[StateMountDomainKey])
	require.Equal(t, "Active", result.Output[StateMountStatusKey])
	confirmInfo, ok := result.Output[StateMountConfirmInfoKey].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "vpc-001", confirmInfo[ParamVpcID])
	require.Equal(t, "vsw-001", confirmInfo[ParamVSwitchID])
	clientMasterNodes, ok := confirmInfo["clientMasterNodes"].([]map[string]string)
	require.True(t, ok)
	require.Len(t, clientMasterNodes, 1)
	require.Equal(t, "i-001", clientMasterNodes[0]["ecsId"])
	require.Equal(t, "10.0.0.10", clientMasterNodes[0]["ecsIp"])
	_, leakedPassword := clientMasterNodes[0]["defaultPasswd"]
	require.False(t, leakedPassword)
}

func TestSDKCallDescribeMountTargetSelectsByTopologyWhenMultipleTargets(t *testing.T) {
	sdk := &client{
		config: testAliyunCloudSetting(),
		nas: &fakeAliyunNASClient{
			describeFileSystemsFn: func(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				require.Nil(t, request.FileSystemId)
				return &aliyunnas.DescribeFileSystemsResponse{
					Body: &aliyunnas.DescribeFileSystemsResponseBody{
						RequestId: stringPtr("req-describe-fs"),
						FileSystems: &aliyunnas.DescribeFileSystemsResponseBodyFileSystems{
							FileSystem: []*aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem{
								{FileSystemId: stringPtr("fs-001")},
							},
						},
					},
				}, nil
			},
			describeMountTargetsFn: func(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error) {
				require.Equal(t, "fs-001", stringValue(request.FileSystemId))
				return &aliyunnas.DescribeMountTargetsResponse{
					Body: &aliyunnas.DescribeMountTargetsResponseBody{
						RequestId: stringPtr("req-describe-mt"),
						MountTargets: &aliyunnas.DescribeMountTargetsResponseBodyMountTargets{
							MountTarget: []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget{
								{
									MountTargetDomain: stringPtr("d-other.nas.aliyuncs.com"),
									Status:            stringPtr("Inactive"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-999"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
								{
									MountTargetDomain: stringPtr("d-selected.nas.aliyuncs.com"),
									Status:            stringPtr("Active"),
									VpcId:             stringPtr("vpc-001"),
									VswId:             stringPtr("vsw-001"),
									AccessGroup:       stringPtr(AliyunNASDefaultAccessGroupName),
									NetworkType:       stringPtr(AliyunNASNetworkTypeVpc),
								},
							},
						},
					},
				}, nil
			},
		},
	}

	result, err := sdk.Call(context.Background(), ActionNasDescribeMountTarget, map[string]interface{}{
		ParamTenantID: "tenant-1",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "fs-001", result.Output[StateFileSystemIDKey])
	require.Equal(t, "d-selected.nas.aliyuncs.com", result.Output[StateMountDomainKey])
	require.Equal(t, "Active", result.Output[StateMountStatusKey])
}

func TestSDKCallEnsureStorageClassCreatesAndValidatesContract(t *testing.T) {
	clientset := kubefake.NewSimpleClientset()
	sdk := &client{
		config:         testAliyunCloudSetting(),
		nas:            &fakeAliyunNASClient{},
		storageClasses: clientset.StorageV1().StorageClasses(),
	}

	result, err := sdk.Call(context.Background(), ActionK8sEnsureStorageClass, map[string]interface{}{
		ParamTenantID:          "tenant-1",
		ParamStorageClassName:  "sc-tenant-1",
		StateFileSystemIDKey:   "fs-001",
		StateMountDomainKey:    "d-001.nas.aliyuncs.com",
		ParamReclaimPolicy:     "Retain",
		ParamVolumeBindingMode: "Immediate",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "sc-tenant-1", result.Output[ParamStorageClassName])

	storageClass, err := clientset.StorageV1().StorageClasses().Get(context.Background(), "sc-tenant-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, AliyunNASStorageProvisioner, storageClass.Provisioner)
	require.Equal(t, "d-001.nas.aliyuncs.com:/", storageClass.Parameters[StorageClassParamServer])
	require.Equal(t, StorageClassParamVolumeAsSubpath, storageClass.Parameters[StorageClassParamVolumeAs])
	require.NotNil(t, storageClass.ReclaimPolicy)
	require.Equal(t, corev1.PersistentVolumeReclaimRetain, *storageClass.ReclaimPolicy)
	require.NotNil(t, storageClass.VolumeBindingMode)
	require.Equal(t, storagev1.VolumeBindingImmediate, *storageClass.VolumeBindingMode)
}

func TestSDKCallEnsureStorageClassRejectsMismatchedExistingResource(t *testing.T) {
	deletePolicy := corev1.PersistentVolumeReclaimDelete
	immediate := storagev1.VolumeBindingImmediate
	clientset := kubefake.NewSimpleClientset(&storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "sc-tenant-1"},
		Provisioner: AliyunNASStorageProvisioner,
		Parameters: map[string]string{
			StorageClassParamServer:   "other.nas.aliyuncs.com:/",
			StorageClassParamVolumeAs: StorageClassParamVolumeAsSubpath,
		},
		ReclaimPolicy:     &deletePolicy,
		VolumeBindingMode: &immediate,
	})
	sdk := &client{
		config:         testAliyunCloudSetting(),
		nas:            &fakeAliyunNASClient{},
		storageClasses: clientset.StorageV1().StorageClasses(),
	}

	_, err := sdk.Call(context.Background(), ActionK8sEnsureStorageClass, map[string]interface{}{
		ParamTenantID:          "tenant-1",
		ParamStorageClassName:  "sc-tenant-1",
		StateFileSystemIDKey:   "fs-001",
		StateMountDomainKey:    "d-001.nas.aliyuncs.com",
		ParamReclaimPolicy:     "Delete",
		ParamVolumeBindingMode: "Immediate",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), StorageClassParamServer)
}

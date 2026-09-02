package aliyun

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type Provider struct {
	registry                  contracts.CloudActionRegistry
	connectivityClientFactory func(spec.AliyunCloudSettingSpec) (nasClient, error)
}

type runtimeAliyunSnapshot struct {
	Endpoint  string `json:"endpoint,omitempty"`
	RegionID  string `json:"regionId,omitempty"`
	ZoneID    string `json:"zoneId,omitempty"`
	VpcID     string `json:"vpcId,omitempty"`
	VSwitchID string `json:"vswId,omitempty"`
}

func NewProvider() *Provider {
	return &Provider{
		connectivityClientFactory: newConnectivityNASClient,
		registry: &actionRegistry{
			factories: map[string]contracts.CloudActionFactory{
				ActionNasEnsureFilesystem:   newNasEnsureFilesystemAction,
				ActionNasEnsureMountTarget:  newNasEnsureMountTargetAction,
				ActionK8sEnsureStorageClass: newK8sEnsureStorageClassAction,
			},
		},
	}
}

func (p *Provider) Name() string {
	return ProviderName
}

func (p *Provider) NewRuntime(ctx context.Context, req *contracts.CloudJobRequest) (contracts.CloudRuntime, error) {
	if req == nil {
		return nil, fmt.Errorf("cloud job request is nil")
	}
	runtimeSnapshot, _ := req.RuntimeProviderSnapshot.(*runtimeAliyunSnapshot)
	if runtimeSnapshot == nil {
		runtimeSnapshot, _ = contracts.RuntimeProviderSnapshotFromContext(ctx, ProviderName).(*runtimeAliyunSnapshot)
	}
	if req.ResumeFromPersistedState && runtimeSnapshot == nil {
		return nil, fmt.Errorf("cloud job checkpoint for provider %q cannot resume safely after process restart without runtime provider snapshot; rerun the workflow task", ProviderName)
	}

	store := contracts.DataStoreFromContext(ctx)
	if store == nil {
		return nil, fmt.Errorf("cloud job datastore is missing from context")
	}
	settingRepo := repository.NewSystemSettingRepositoryWithStore(store)
	setting, findErr := settingRepo.FindByType(ctx, model.SystemSettingTypeAliyunCloud)
	if findErr != nil {
		if findErr == datastore.ErrRecordNotExist {
			return nil, fmt.Errorf("system setting %q is not configured", model.SystemSettingTypeAliyunCloud)
		}
		return nil, findErr
	}

	var (
		config spec.AliyunCloudSettingSpec
		err    error
	)
	config, err = spec.ParseAliyunCloudSetting(setting.Value)
	if err != nil {
		return nil, fmt.Errorf("invalid system setting %q: %w", model.SystemSettingTypeAliyunCloud, err)
	}
	if runtimeSnapshot != nil {
		config = applyRuntimeAliyunSnapshot(config, runtimeSnapshot)
	} else {
		req.RuntimeProviderSnapshot = buildRuntimeAliyunSnapshot(config)
	}
	nasClient, err := newNASClient(config)
	if err != nil {
		return nil, err
	}
	return &client{
		config: config,
		nas:    nasClient,
	}, nil
}

func buildRuntimeAliyunSnapshot(config spec.AliyunCloudSettingSpec) *runtimeAliyunSnapshot {
	normalized := spec.NormalizeAliyunCloudSetting(config)
	return &runtimeAliyunSnapshot{
		Endpoint:  strings.TrimSpace(normalized.Endpoint),
		RegionID:  strings.TrimSpace(normalized.RegionID),
		ZoneID:    strings.TrimSpace(normalized.ZoneID),
		VpcID:     strings.TrimSpace(normalized.VpcID),
		VSwitchID: strings.TrimSpace(normalized.VSwitchID),
	}
}

func applyRuntimeAliyunSnapshot(config spec.AliyunCloudSettingSpec, snapshot *runtimeAliyunSnapshot) spec.AliyunCloudSettingSpec {
	if snapshot == nil {
		return config
	}
	config.Endpoint = strings.TrimSpace(snapshot.Endpoint)
	config.RegionID = strings.TrimSpace(snapshot.RegionID)
	config.ZoneID = strings.TrimSpace(snapshot.ZoneID)
	config.VpcID = strings.TrimSpace(snapshot.VpcID)
	config.VSwitchID = strings.TrimSpace(snapshot.VSwitchID)
	return config
}

func (p *Provider) ActionRegistry() contracts.CloudActionRegistry {
	if p == nil {
		return nil
	}
	return p.registry
}

type actionRegistry struct {
	factories map[string]contracts.CloudActionFactory
}

func (r *actionRegistry) ResolveAction(action string) (contracts.CloudAction, bool) {
	normalized := strings.TrimSpace(action)
	if normalized == "" || r == nil {
		return nil, false
	}
	factory, ok := r.factories[normalized]
	if !ok || factory == nil {
		return nil, false
	}
	return factory(), true
}

func (r *actionRegistry) SupportedActions() []string {
	if r == nil || len(r.factories) == 0 {
		return nil
	}
	actions := make([]string, 0, len(r.factories))
	for action := range r.factories {
		actions = append(actions, action)
	}
	sort.Strings(actions)
	return actions
}

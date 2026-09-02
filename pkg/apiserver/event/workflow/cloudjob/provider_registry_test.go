package cloudjob

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/aliyun"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

func TestRestoreBuiltinCloudProvidersForTestRegistersAliyun(t *testing.T) {
	ResetCloudProvidersForTest()
	defer RestoreBuiltinCloudProvidersForTest()

	_, ok := GetCloudProvider(aliyun.ProviderName)
	require.False(t, ok)

	RestoreBuiltinCloudProvidersForTest()
	provider, ok := GetCloudProvider(aliyun.ProviderName)
	require.True(t, ok)
	require.NotNil(t, provider)
	require.NotNil(t, provider.ActionRegistry())

	settingSupport, ok := GetCloudProviderSettingSupport(model.SystemSettingTypeAliyunCloud)
	require.True(t, ok)
	require.NotNil(t, settingSupport)
}

type registryTestProvider struct {
	name string
}

func (p *registryTestProvider) Name() string {
	return p.name
}

func (p *registryTestProvider) NewRuntime(context.Context, *contracts.CloudJobRequest) (contracts.CloudRuntime, error) {
	return nil, nil
}

func (p *registryTestProvider) ActionRegistry() contracts.CloudActionRegistry {
	return nil
}

type registryTestProviderWithSettingSupport struct {
	registryTestProvider
	settingType string
}

func (p *registryTestProviderWithSettingSupport) SystemSettingType() string {
	return p.settingType
}

func (p *registryTestProviderWithSettingSupport) NormalizeSystemSettingValue(value json.RawMessage) (json.RawMessage, error) {
	return value, nil
}

func (p *registryTestProviderWithSettingSupport) SanitizeSystemSettingValue(value json.RawMessage) json.RawMessage {
	return value
}

func (p *registryTestProviderWithSettingSupport) ValidateSystemSettingConnectivity(context.Context, json.RawMessage) error {
	return nil
}

func TestRegisterCloudProviderReplacesAndRemovesStaleSettingSupport(t *testing.T) {
	ResetCloudProvidersForTest()
	defer RestoreBuiltinCloudProvidersForTest()

	RegisterCloudProvider(&registryTestProviderWithSettingSupport{
		registryTestProvider: registryTestProvider{name: aliyun.ProviderName},
		settingType:          model.SystemSettingTypeAliyunCloud,
	})

	settingSupport, ok := GetCloudProviderSettingSupport(model.SystemSettingTypeAliyunCloud)
	require.True(t, ok)
	require.NotNil(t, settingSupport)

	RegisterCloudProvider(&registryTestProvider{name: aliyun.ProviderName})

	_, ok = GetCloudProviderSettingSupport(model.SystemSettingTypeAliyunCloud)
	require.False(t, ok)
}

func TestRegisterCloudProviderReplacesSettingSupportType(t *testing.T) {
	ResetCloudProvidersForTest()
	defer RestoreBuiltinCloudProvidersForTest()

	RegisterCloudProvider(&registryTestProviderWithSettingSupport{
		registryTestProvider: registryTestProvider{name: aliyun.ProviderName},
		settingType:          model.SystemSettingTypeAliyunCloud,
	})

	newSettingType := "mockCloud"
	RegisterCloudProvider(&registryTestProviderWithSettingSupport{
		registryTestProvider: registryTestProvider{name: aliyun.ProviderName},
		settingType:          newSettingType,
	})

	_, ok := GetCloudProviderSettingSupport(model.SystemSettingTypeAliyunCloud)
	require.False(t, ok)

	settingSupport, ok := GetCloudProviderSettingSupport(newSettingType)
	require.True(t, ok)
	require.NotNil(t, settingSupport)
}

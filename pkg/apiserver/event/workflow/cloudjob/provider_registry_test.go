package cloudjob

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

func TestRegisterCloudProvider(t *testing.T) {
	ResetCloudProvidersForTest()
	defer ResetCloudProvidersForTest()

	registered := &registryTestProvider{name: " TestCloud "}
	RegisterCloudProvider(registered)
	provider, ok := GetCloudProvider("testcloud")
	require.True(t, ok)
	require.Same(t, registered, provider)
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

func (p *registryTestProvider) ResolveAction(string) (contracts.CloudAction, bool) {
	return nil, false
}

func (p *registryTestProvider) SupportedActions() []string {
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
	defer ResetCloudProvidersForTest()

	RegisterCloudProvider(&registryTestProviderWithSettingSupport{
		registryTestProvider: registryTestProvider{name: "testcloud"},
		settingType:          model.SystemSettingTypeAliyunCloud,
	})

	settingSupport, ok := systemsetting.GetCloudProviderSettingSupport(model.SystemSettingTypeAliyunCloud)
	require.True(t, ok)
	require.NotNil(t, settingSupport)

	RegisterCloudProvider(&registryTestProvider{name: "testcloud"})

	_, ok = systemsetting.GetCloudProviderSettingSupport(model.SystemSettingTypeAliyunCloud)
	require.False(t, ok)
}

func TestRegisterCloudProviderReplacesSettingSupportType(t *testing.T) {
	ResetCloudProvidersForTest()
	defer ResetCloudProvidersForTest()

	RegisterCloudProvider(&registryTestProviderWithSettingSupport{
		registryTestProvider: registryTestProvider{name: "testcloud"},
		settingType:          model.SystemSettingTypeAliyunCloud,
	})

	newSettingType := "mockCloud"
	RegisterCloudProvider(&registryTestProviderWithSettingSupport{
		registryTestProvider: registryTestProvider{name: "testcloud"},
		settingType:          newSettingType,
	})

	_, ok := systemsetting.GetCloudProviderSettingSupport(model.SystemSettingTypeAliyunCloud)
	require.False(t, ok)

	settingSupport, ok := systemsetting.GetCloudProviderSettingSupport(newSettingType)
	require.True(t, ok)
	require.NotNil(t, settingSupport)
}

func TestRegisterCloudProviderSettingsRemainAvailableDuringReplacement(t *testing.T) {
	ResetCloudProvidersForTest()
	t.Cleanup(ResetCloudProvidersForTest)
	provider := &registryTestProviderWithSettingSupport{registryTestProvider: registryTestProvider{name: "testcloud"}, settingType: "mockCloud"}
	RegisterCloudProvider(provider)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			RegisterCloudProvider(provider)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if support, ok := systemsetting.GetCloudProviderSettingSupport(" mockCloud "); !ok || support != provider {
				t.Error("provider replacement exposed missing settings support")
				return
			}
		}
	}()
	wg.Wait()
	ResetCloudProvidersForTest()
	_, ok := systemsetting.GetCloudProviderSettingSupport("mockCloud")
	require.False(t, ok)
}

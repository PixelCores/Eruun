package cloudjob

import (
	"strings"
	"sync"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/aliyun"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

var (
	cloudProvidersMu      sync.RWMutex
	cloudProviders        = map[string]contracts.CloudProvider{}
	cloudProviderSettings = map[string]contracts.CloudProviderSettingSupport{}
)

func init() {
	registerBuiltinCloudProviders()
}

func registerBuiltinCloudProviders() {
	RegisterCloudProvider(aliyun.NewProvider())
}

// RegisterCloudProvider adds or replaces a cloud provider implementation by name.
func RegisterCloudProvider(provider contracts.CloudProvider) {
	if provider == nil {
		return
	}
	name := NormalizeProviderName(provider.Name())
	if name == "" {
		return
	}
	cloudProvidersMu.Lock()
	defer cloudProvidersMu.Unlock()
	if oldProvider, ok := cloudProviders[name]; ok {
		if oldSettingSupport, ok := oldProvider.(contracts.CloudProviderSettingSupport); ok {
			if oldSettingType := normalizeSystemSettingType(oldSettingSupport.SystemSettingType()); oldSettingType != "" {
				delete(cloudProviderSettings, oldSettingType)
			}
		}
	}
	cloudProviders[name] = provider
	if settingSupport, ok := provider.(contracts.CloudProviderSettingSupport); ok {
		if settingType := normalizeSystemSettingType(settingSupport.SystemSettingType()); settingType != "" {
			cloudProviderSettings[settingType] = settingSupport
		}
	}
}

func GetCloudProvider(name string) (contracts.CloudProvider, bool) {
	normalized := NormalizeProviderName(name)
	if normalized == "" {
		return nil, false
	}
	cloudProvidersMu.RLock()
	defer cloudProvidersMu.RUnlock()
	provider, ok := cloudProviders[normalized]
	return provider, ok
}

func GetCloudProviderSettingSupport(settingType string) (contracts.CloudProviderSettingSupport, bool) {
	normalized := normalizeSystemSettingType(settingType)
	if normalized == "" {
		return nil, false
	}
	cloudProvidersMu.RLock()
	defer cloudProvidersMu.RUnlock()
	settingSupport, ok := cloudProviderSettings[normalized]
	return settingSupport, ok
}

func NormalizeProviderName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func normalizeSystemSettingType(settingType string) string {
	return strings.TrimSpace(settingType)
}

func ResetCloudProvidersForTest() {
	cloudProvidersMu.Lock()
	defer cloudProvidersMu.Unlock()
	cloudProviders = map[string]contracts.CloudProvider{}
	cloudProviderSettings = map[string]contracts.CloudProviderSettingSupport{}
}

func RestoreBuiltinCloudProvidersForTest() {
	ResetCloudProvidersForTest()
	registerBuiltinCloudProviders()
}

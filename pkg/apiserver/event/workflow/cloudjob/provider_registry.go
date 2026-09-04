package cloudjob

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

// CloudProvider is the contract every cloud vendor implementation must satisfy.
// Action lookup belongs to the provider so callers do not depend on a second registry abstraction.
type CloudProvider interface {
	Name() string
	NewRuntime(ctx context.Context, req *contracts.CloudJobRequest) (contracts.CloudRuntime, error)
	ResolveAction(action string) (contracts.CloudAction, bool)
	SupportedActions() []string
}

// CloudProviderSettingSupport defines optional system_setting integration for a provider.
type CloudProviderSettingSupport interface {
	SystemSettingType() string
	NormalizeSystemSettingValue(value json.RawMessage) (json.RawMessage, error)
	SanitizeSystemSettingValue(value json.RawMessage) json.RawMessage
	ValidateSystemSettingConnectivity(ctx context.Context, value json.RawMessage) error
}

var (
	cloudProvidersMu      sync.RWMutex
	cloudProviders        = map[string]CloudProvider{}
	cloudProviderSettings = map[string]CloudProviderSettingSupport{}
)

// RegisterCloudProvider adds or replaces a cloud provider implementation by name.
func RegisterCloudProvider(provider CloudProvider) {
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
		if oldSettingSupport, ok := oldProvider.(CloudProviderSettingSupport); ok {
			if oldSettingType := normalizeSystemSettingType(oldSettingSupport.SystemSettingType()); oldSettingType != "" {
				delete(cloudProviderSettings, oldSettingType)
			}
		}
	}
	cloudProviders[name] = provider
	if settingSupport, ok := provider.(CloudProviderSettingSupport); ok {
		if settingType := normalizeSystemSettingType(settingSupport.SystemSettingType()); settingType != "" {
			cloudProviderSettings[settingType] = settingSupport
		}
	}
}

func GetCloudProvider(name string) (CloudProvider, bool) {
	normalized := NormalizeProviderName(name)
	if normalized == "" {
		return nil, false
	}
	cloudProvidersMu.RLock()
	defer cloudProvidersMu.RUnlock()
	provider, ok := cloudProviders[normalized]
	return provider, ok
}

func GetCloudProviderSettingSupport(settingType string) (CloudProviderSettingSupport, bool) {
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
	cloudProviders = map[string]CloudProvider{}
	cloudProviderSettings = map[string]CloudProviderSettingSupport{}
}

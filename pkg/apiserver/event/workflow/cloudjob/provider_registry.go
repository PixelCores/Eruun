package cloudjob

import (
	"context"
	"strings"
	"sync"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
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

var (
	cloudProvidersMu sync.RWMutex
	cloudProviders   = map[string]CloudProvider{}
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
	previous, _ := cloudProviders[name].(systemsetting.CloudProviderSettingSupport)
	next, _ := provider.(systemsetting.CloudProviderSettingSupport)
	systemsetting.ReplaceCloudProviderSettingSupport(previous, next)
	cloudProviders[name] = provider
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

func NormalizeProviderName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func ResetCloudProvidersForTest() {
	cloudProvidersMu.Lock()
	defer cloudProvidersMu.Unlock()
	cloudProviders = map[string]CloudProvider{}
	systemsetting.ResetCloudProviderSettingsForTest()
}

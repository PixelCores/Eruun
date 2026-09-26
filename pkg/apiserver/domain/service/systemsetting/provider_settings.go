package systemsetting

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
)

// CloudProviderSettingSupport defines optional system_setting integration for a provider.
type CloudProviderSettingSupport interface {
	SystemSettingType() string
	NormalizeSystemSettingValue(value json.RawMessage) (json.RawMessage, error)
	SanitizeSystemSettingValue(value json.RawMessage) json.RawMessage
	ValidateSystemSettingConnectivity(ctx context.Context, value json.RawMessage) error
}

var (
	cloudProviderSettingsMu sync.RWMutex
	cloudProviderSettings   = map[string]CloudProviderSettingSupport{}
)

// ReplaceCloudProviderSettingSupport replaces optional settings alongside a provider registration.
// Removal and insertion share a lock so readers cannot observe the intermediate state.
func ReplaceCloudProviderSettingSupport(previous, next CloudProviderSettingSupport) {
	cloudProviderSettingsMu.Lock()
	defer cloudProviderSettingsMu.Unlock()
	if previous != nil {
		if settingType := strings.TrimSpace(previous.SystemSettingType()); settingType != "" {
			delete(cloudProviderSettings, settingType)
		}
	}
	if next != nil {
		if settingType := strings.TrimSpace(next.SystemSettingType()); settingType != "" {
			cloudProviderSettings[settingType] = next
		}
	}
}

// GetCloudProviderSettingSupport looks up dynamically registered settings support.
func GetCloudProviderSettingSupport(settingType string) (CloudProviderSettingSupport, bool) {
	normalized := strings.TrimSpace(settingType)
	if normalized == "" {
		return nil, false
	}
	cloudProviderSettingsMu.RLock()
	defer cloudProviderSettingsMu.RUnlock()
	support, ok := cloudProviderSettings[normalized]
	return support, ok
}

func ResetCloudProviderSettingsForTest() {
	cloudProviderSettingsMu.Lock()
	defer cloudProviderSettingsMu.Unlock()
	cloudProviderSettings = map[string]CloudProviderSettingSupport{}
}

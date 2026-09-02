package spec

import (
	"encoding/json"
	"fmt"
)

const (
	DefaultPodRestartMonitorWindowSeconds = int64(1800)
	DefaultPodRestartMonitorThreshold     = 3
)

// PodRestartMonitorSettingSpec controls informer-driven Pod restart threshold detection.
type PodRestartMonitorSettingSpec struct {
	Enabled       bool  `json:"enabled"`
	WindowSeconds int64 `json:"windowSeconds"`
	Threshold     int   `json:"threshold"`
}

type podRestartMonitorSettingWire struct {
	Enabled       *bool  `json:"enabled"`
	WindowSeconds *int64 `json:"windowSeconds"`
	Threshold     *int   `json:"threshold"`
}

// DefaultPodRestartMonitorSetting returns the default Pod restart monitor policy.
func DefaultPodRestartMonitorSetting() PodRestartMonitorSettingSpec {
	return PodRestartMonitorSettingSpec{
		Enabled:       true,
		WindowSeconds: DefaultPodRestartMonitorWindowSeconds,
		Threshold:     DefaultPodRestartMonitorThreshold,
	}
}

// ParsePodRestartMonitorSetting decodes and validates a Pod restart monitor setting.
func ParsePodRestartMonitorSetting(value json.RawMessage) (PodRestartMonitorSettingSpec, error) {
	var wire podRestartMonitorSettingWire
	if err := json.Unmarshal(value, &wire); err != nil {
		return PodRestartMonitorSettingSpec{}, err
	}
	if wire.Enabled == nil {
		return PodRestartMonitorSettingSpec{}, fmt.Errorf("enabled is required")
	}
	if wire.WindowSeconds == nil {
		return PodRestartMonitorSettingSpec{}, fmt.Errorf("windowSeconds is required")
	}
	if wire.Threshold == nil {
		return PodRestartMonitorSettingSpec{}, fmt.Errorf("threshold is required")
	}
	setting := PodRestartMonitorSettingSpec{
		Enabled:       *wire.Enabled,
		WindowSeconds: *wire.WindowSeconds,
		Threshold:     *wire.Threshold,
	}
	if err := ValidatePodRestartMonitorSetting(setting); err != nil {
		return PodRestartMonitorSettingSpec{}, err
	}
	return setting, nil
}

// NormalizePodRestartMonitorSettingValue returns canonical JSON for the setting.
func NormalizePodRestartMonitorSettingValue(value json.RawMessage) (json.RawMessage, error) {
	setting, err := ParsePodRestartMonitorSetting(value)
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(setting)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(out), nil
}

// ValidatePodRestartMonitorSetting validates Pod restart monitor fields.
func ValidatePodRestartMonitorSetting(setting PodRestartMonitorSettingSpec) error {
	if setting.WindowSeconds <= 0 {
		return fmt.Errorf("windowSeconds must be greater than 0")
	}
	if setting.Threshold <= 0 {
		return fmt.Errorf("threshold must be greater than 0")
	}
	return nil
}

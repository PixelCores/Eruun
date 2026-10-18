package systemsetting

import (
	"context"
	"encoding/json"
)

// CloudProviderSettingSupport normalizes, masks and checks one cloud provider's system setting.
type CloudProviderSettingSupport interface {
	SystemSettingType() string
	NormalizeSystemSettingValue(value json.RawMessage) (json.RawMessage, error)
	SanitizeSystemSettingValue(value json.RawMessage) json.RawMessage
	ValidateSystemSettingConnectivity(ctx context.Context, value json.RawMessage) error
}

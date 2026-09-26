package systemsetting

import (
	"context"
	"encoding/json"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
)

// AliyunSettingSupport supplies settings behavior independently of workflow execution.
type AliyunSettingSupport struct{}

func (AliyunSettingSupport) SystemSettingType() string { return model.SystemSettingTypeAliyunCloud }
func (AliyunSettingSupport) NormalizeSystemSettingValue(value json.RawMessage) (json.RawMessage, error) {
	return spec.NormalizeAliyunCloudSettingValue(value)
}
func (AliyunSettingSupport) SanitizeSystemSettingValue(value json.RawMessage) json.RawMessage {
	return spec.SanitizeAliyunCloudSettingValue(value)
}
func (AliyunSettingSupport) ValidateSystemSettingConnectivity(ctx context.Context, value json.RawMessage) error {
	return clients.ValidateAliyunNASConnectivity(ctx, value)
}

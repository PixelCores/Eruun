package aliyun

import (
	"context"
	"encoding/json"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
)

func (p *Provider) SystemSettingType() string { return model.SystemSettingTypeAliyunCloud }
func (p *Provider) NormalizeSystemSettingValue(value json.RawMessage) (json.RawMessage, error) {
	return spec.NormalizeAliyunCloudSettingValue(value)
}
func (p *Provider) SanitizeSystemSettingValue(value json.RawMessage) json.RawMessage {
	return spec.SanitizeAliyunCloudSettingValue(value)
}
func (p *Provider) ValidateSystemSettingConnectivity(ctx context.Context, value json.RawMessage) error {
	return clients.ValidateAliyunNASConnectivity(ctx, value)
}

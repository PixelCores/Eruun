package aliyun

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	aliyunnas "github.com/alibabacloud-go/nas-20170626/v2/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

const (
	defaultConnectivityConnectTimeoutMilliseconds = 3000
	defaultConnectivityReadTimeoutMilliseconds    = 5000
)

func (p *Provider) SystemSettingType() string {
	return model.SystemSettingTypeAliyunCloud
}

func (p *Provider) NormalizeSystemSettingValue(value json.RawMessage) (json.RawMessage, error) {
	return spec.NormalizeAliyunCloudSettingValue(value)
}

func (p *Provider) SanitizeSystemSettingValue(value json.RawMessage) json.RawMessage {
	return sanitizeAliyunCloudSettingValue(value)
}

func (p *Provider) ValidateSystemSettingConnectivity(ctx context.Context, value json.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	config, err := spec.ParseAliyunCloudSetting(value)
	if err != nil {
		return err
	}

	factory := newConnectivityNASClient
	if p != nil && p.connectivityClientFactory != nil {
		factory = p.connectivityClientFactory
	}

	nasClient, err := factory(config)
	if err != nil {
		return err
	}

	request := new(aliyunnas.DescribeFileSystemsRequest).
		SetPageNumber(1).
		SetPageSize(1)

	response, err := nasClient.DescribeFileSystems(request)
	if err != nil {
		return fmt.Errorf("describe aliyun nas filesystems for connectivity check: %w", err)
	}
	if response == nil || response.Body == nil {
		return fmt.Errorf("aliyun nas connectivity check returned nil body")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func sanitizeAliyunCloudSettingValue(value json.RawMessage) json.RawMessage {
	parsed, err := spec.ParseAliyunCloudSetting(value)
	if err == nil {
		parsed.AccessKeySecret = spec.AliyunCloudSecretMaskedValue
		sanitized, marshalErr := json.Marshal(parsed)
		if marshalErr == nil {
			return json.RawMessage(sanitized)
		}
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(value, &obj); err != nil {
		return json.RawMessage(`{}`)
	}

	maskAliyunCloudSecret(obj)

	sanitized, err := json.Marshal(obj)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(sanitized)
}

func maskAliyunCloudSecret(obj map[string]interface{}) {
	if obj == nil {
		return
	}
	for key := range obj {
		if strings.EqualFold(strings.TrimSpace(key), "accessKeySecret") {
			obj[key] = spec.AliyunCloudSecretMaskedValue
		}
	}
}

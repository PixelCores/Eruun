package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	openapi "github.com/alibabacloud-go/darabonba-openapi/client"
	aliyunnas "github.com/alibabacloud-go/nas-20170626/v2/client"
)

// NewAliyunNASClient creates the SDK client used by cloud resource operations.
func NewAliyunNASClient(config spec.AliyunCloudSettingSpec) (*aliyunnas.Client, error) {
	return newNASClientWithTimeout(config, 0, 0)
}

func newNASClientWithTimeout(config spec.AliyunCloudSettingSpec, connectTimeoutMS, readTimeoutMS int) (*aliyunnas.Client, error) {
	normalizedConfig := spec.NormalizeAliyunCloudSetting(config)
	if err := spec.ValidateAliyunCloudSetting(normalizedConfig); err != nil {
		return nil, fmt.Errorf("invalid system setting %q: %w", model.SystemSettingTypeAliyunCloud, err)
	}
	openAPIConfig := new(openapi.Config).
		SetRegionId(normalizedConfig.RegionID).
		SetAccessKeyId(normalizedConfig.AccessKeyID).
		SetAccessKeySecret(normalizedConfig.AccessKeySecret)
	if connectTimeoutMS > 0 {
		openAPIConfig.SetConnectTimeout(connectTimeoutMS)
	}
	if readTimeoutMS > 0 {
		openAPIConfig.SetReadTimeout(readTimeoutMS)
	}
	if normalizedConfig.Endpoint != "" {
		openAPIConfig.SetEndpoint(normalizedConfig.Endpoint)
	}
	client, err := aliyunnas.NewClient(openAPIConfig)
	if err != nil {
		return nil, fmt.Errorf("create aliyun nas client for region %q: %w", normalizedConfig.RegionID, err)
	}
	return client, nil
}

// ValidateAliyunNASConnectivity checks the configured account with bounded SDK timeouts.
func ValidateAliyunNASConnectivity(ctx context.Context, value json.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	config, err := spec.ParseAliyunCloudSetting(value)
	if err != nil {
		return err
	}
	client, err := newNASClientWithTimeout(config, 3000, 5000)
	if err != nil {
		return err
	}
	return checkAliyunNASConnectivity(ctx, client)
}

type nasConnectivityClient interface {
	DescribeFileSystems(*aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error)
}

func checkAliyunNASConnectivity(ctx context.Context, client nasConnectivityClient) error {
	request := new(aliyunnas.DescribeFileSystemsRequest).SetPageNumber(1).SetPageSize(1)
	response, err := client.DescribeFileSystems(request)
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

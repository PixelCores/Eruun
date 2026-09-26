package aliyun

import (
	"context"
	"fmt"
	"strings"

	aliyunnas "github.com/alibabacloud-go/nas-20170626/v2/client"
	storagev1client "k8s.io/client-go/kubernetes/typed/storage/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
)

type nasClient interface {
	CreateFileSystem(request *aliyunnas.CreateFileSystemRequest) (*aliyunnas.CreateFileSystemResponse, error)
	CreateMountTarget(request *aliyunnas.CreateMountTargetRequest) (*aliyunnas.CreateMountTargetResponse, error)
	DescribeFileSystems(request *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error)
	DescribeMountTargets(request *aliyunnas.DescribeMountTargetsRequest) (*aliyunnas.DescribeMountTargetsResponse, error)
	TagResources(request *aliyunnas.TagResourcesRequest) (*aliyunnas.TagResourcesResponse, error)
}

type client struct {
	config         spec.AliyunCloudSettingSpec
	nas            nasClient
	storageClasses storagev1client.StorageClassInterface
}

type fileSystemTagPendingError struct {
	fileSystemID string
	requestID    string
	cause        error
}

func (e *fileSystemTagPendingError) Error() string {
	if e == nil || e.cause == nil {
		return "filesystem tag is pending"
	}
	return e.cause.Error()
}

func (e *fileSystemTagPendingError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (c *client) Call(ctx context.Context, action string, params map[string]interface{}) (*contracts.CloudJobResult, error) {
	if c == nil {
		return nil, fmt.Errorf("aliyun client is nil")
	}
	if c.nas == nil {
		return nil, fmt.Errorf("aliyun nas client is nil")
	}

	switch strings.TrimSpace(action) {
	case ActionNasEnsureFilesystem:
		return c.ensureFilesystem(params)
	case ActionNasEnsureMountTarget:
		return c.ensureMountTarget(params)
	case ActionNasDescribeMountTarget:
		return c.describeMountTarget(params)
	case ActionK8sEnsureStorageClass:
		return c.ensureStorageClass(ctx, params)
	default:
		return nil, fmt.Errorf("aliyun action %q is not supported", strings.TrimSpace(action))
	}
}

func (c *client) requireConfigValue(value, key, action string) (string, error) {
	normalizedValue := strings.TrimSpace(value)
	if normalizedValue == "" {
		return "", fmt.Errorf("system setting %s.%s is required for %s", model.SystemSettingTypeAliyunCloud, key, action)
	}
	return normalizedValue, nil
}

func (c *client) storageClassClient() (storagev1client.StorageClassInterface, error) {
	if c.storageClasses != nil {
		return c.storageClasses, nil
	}
	kubeClient, err := clients.GetKubeClient()
	if err != nil {
		return nil, fmt.Errorf("get kube client for storageclass: %w", err)
	}
	c.storageClasses = kubeClient.StorageV1().StorageClasses()
	return c.storageClasses, nil
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func stringPtr(value string) *string {
	ptr := strings.TrimSpace(value)
	return &ptr
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

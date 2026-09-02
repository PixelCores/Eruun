package aliyun

import (
	"fmt"
	"strings"

	aliyunnas "github.com/alibabacloud-go/nas-20170626/v2/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

func (c *client) ensureFilesystem(params map[string]interface{}) (*contracts.CloudJobResult, error) {
	tenantID, err := requireCloudMapString(params, ParamTenantID)
	if err != nil {
		return nil, err
	}
	storageType, err := requireCloudMapString(params, ParamStorageType)
	if err != nil {
		return nil, err
	}
	protocolType, err := requireCloudMapString(params, ParamProtocolType)
	if err != nil {
		return nil, err
	}
	fileSystemType := cloudMapString(params, ParamFileSystemType)
	capacityGiB, hasCapacity, err := cloudMapInt64(params, ParamCapacityGiB)
	if err != nil {
		return nil, fmt.Errorf("invalid params.%s: %w", ParamCapacityGiB, err)
	}
	if hasCapacity && capacityGiB <= 0 {
		return nil, fmt.Errorf("cloud job requires params.%s > 0", ParamCapacityGiB)
	}

	explicitFileSystemID := cloudMapString(params, StateFileSystemIDKey)
	existingFileSystem, requestID, err := c.describeFileSystem(tenantID, explicitFileSystemID)
	if err != nil {
		return nil, err
	}
	if existingFileSystem != nil {
		fileSystemID := stringValue(existingFileSystem.FileSystemId)
		if fileSystemID == "" {
			return nil, fmt.Errorf("aliyun nas describe filesystem returned empty %s", StateFileSystemIDKey)
		}
		if err := validateExistingFileSystem(existingFileSystem, storageType, protocolType, fileSystemType, capacityGiB, hasCapacity); err != nil {
			return nil, err
		}
		if explicitFileSystemID != "" {
			if tagErr := c.tagFileSystem(fileSystemID, tenantID); tagErr != nil {
				return nil, &fileSystemTagPendingError{
					fileSystemID: fileSystemID,
					requestID:    requestID,
					cause:        tagErr,
				}
			}
		}
		return &contracts.CloudJobResult{
			RequestID: requestID,
			Message:   "aliyun nas filesystem already exists",
			Output: map[string]interface{}{
				StateFileSystemIDKey: fileSystemID,
			},
		}, nil
	}
	if explicitFileSystemID != "" {
		return nil, &fileSystemTagPendingError{
			fileSystemID: explicitFileSystemID,
			requestID:    requestID,
			cause:        fmt.Errorf("aliyun nas filesystem %q is not visible yet", explicitFileSystemID),
		}
	}

	createRequest := new(aliyunnas.CreateFileSystemRequest).
		SetStorageType(storageType).
		SetProtocolType(protocolType)
	if zoneID := strings.TrimSpace(c.config.ZoneID); zoneID != "" {
		createRequest.SetZoneId(zoneID)
	}
	if fileSystemType != "" {
		createRequest.SetFileSystemType(fileSystemType)
	}
	if description := cloudMapString(params, ParamDescription); description != "" {
		createRequest.SetDescription(description)
	}
	if hasCapacity {
		createRequest.SetCapacity(capacityGiB)
	}

	createResponse, err := c.nas.CreateFileSystem(createRequest)
	if err != nil {
		return nil, fmt.Errorf("create aliyun nas filesystem for tenant %q: %w", tenantID, err)
	}
	if createResponse == nil || createResponse.Body == nil {
		return nil, fmt.Errorf("aliyun nas create filesystem returned nil body")
	}
	fileSystemID := stringValue(createResponse.Body.FileSystemId)
	if fileSystemID == "" {
		return nil, fmt.Errorf("aliyun nas create filesystem returned empty %s", StateFileSystemIDKey)
	}
	if err := c.tagFileSystem(fileSystemID, tenantID); err != nil {
		return nil, &fileSystemTagPendingError{
			fileSystemID: fileSystemID,
			requestID:    stringValue(createResponse.Body.RequestId),
			cause:        err,
		}
	}

	return &contracts.CloudJobResult{
		RequestID: stringValue(createResponse.Body.RequestId),
		Message:   "aliyun nas filesystem ensured",
		Output: map[string]interface{}{
			StateFileSystemIDKey: fileSystemID,
		},
	}, nil
}

func (c *client) describeFileSystem(tenantID, fileSystemID string) (*aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem, string, error) {
	request := new(aliyunnas.DescribeFileSystemsRequest).
		SetPageNumber(1).
		SetPageSize(10)
	if strings.TrimSpace(fileSystemID) != "" {
		request.SetFileSystemId(strings.TrimSpace(fileSystemID))
	} else {
		request.SetTag([]*aliyunnas.DescribeFileSystemsRequestTag{
			new(aliyunnas.DescribeFileSystemsRequestTag).
				SetKey(AliyunTenantTagKey).
				SetValue(strings.TrimSpace(tenantID)),
		})
	}

	response, err := c.nas.DescribeFileSystems(request)
	if err != nil {
		if fileSystemID != "" {
			return nil, "", fmt.Errorf("describe aliyun nas filesystem %q: %w", fileSystemID, err)
		}
		return nil, "", fmt.Errorf("describe aliyun nas filesystems for tenant %q: %w", tenantID, err)
	}
	if response == nil || response.Body == nil {
		return nil, "", fmt.Errorf("aliyun nas describe filesystems returned nil body")
	}

	fileSystems := response.Body.FileSystems
	if fileSystems == nil || len(fileSystems.FileSystem) == 0 {
		return nil, stringValue(response.Body.RequestId), nil
	}
	if len(fileSystems.FileSystem) > 1 {
		if fileSystemID != "" {
			return nil, "", fmt.Errorf("aliyun nas filesystem lookup for id %q returned %d results", fileSystemID, len(fileSystems.FileSystem))
		}
		return nil, "", fmt.Errorf("aliyun nas filesystem lookup for tenant %q is ambiguous: found %d file systems", tenantID, len(fileSystems.FileSystem))
	}
	return fileSystems.FileSystem[0], stringValue(response.Body.RequestId), nil
}

func (c *client) tagFileSystem(fileSystemID, tenantID string) error {
	request := new(aliyunnas.TagResourcesRequest).
		SetResourceType(AliyunNASResourceTypeFileSystem).
		SetResourceId([]*string{stringPtr(fileSystemID)}).
		SetTag([]*aliyunnas.TagResourcesRequestTag{
			new(aliyunnas.TagResourcesRequestTag).
				SetKey(AliyunTenantTagKey).
				SetValue(tenantID),
		})
	response, err := c.nas.TagResources(request)
	if err != nil {
		return fmt.Errorf("tag aliyun nas filesystem %q with %s=%q: %w", fileSystemID, AliyunTenantTagKey, tenantID, err)
	}
	if response == nil || response.Body == nil {
		return fmt.Errorf("aliyun nas tag resources returned nil body")
	}
	return nil
}

func validateExistingFileSystem(existing *aliyunnas.DescribeFileSystemsResponseBodyFileSystemsFileSystem, storageType, protocolType, fileSystemType string, capacityGiB int64, hasCapacity bool) error {
	if existing == nil {
		return fmt.Errorf("aliyun nas filesystem is nil")
	}
	existingStorageType := stringValue(existing.StorageType)
	if existingStorageType != storageType {
		return fmt.Errorf("aliyun nas filesystem already exists with %s=%q, want %q", ParamStorageType, existingStorageType, storageType)
	}
	existingProtocolType := stringValue(existing.ProtocolType)
	if existingProtocolType != protocolType {
		return fmt.Errorf("aliyun nas filesystem already exists with %s=%q, want %q", ParamProtocolType, existingProtocolType, protocolType)
	}
	if fileSystemType != "" {
		existingFileSystemType := stringValue(existing.FileSystemType)
		if existingFileSystemType != fileSystemType {
			return fmt.Errorf("aliyun nas filesystem already exists with %s=%q, want %q", ParamFileSystemType, existingFileSystemType, fileSystemType)
		}
	}
	if hasCapacity {
		existingCapacity := int64(0)
		if existing.Capacity != nil {
			existingCapacity = *existing.Capacity
		}
		if existing.Capacity == nil || existingCapacity != capacityGiB {
			return fmt.Errorf("aliyun nas filesystem already exists with %s=%d, want %d", ParamCapacityGiB, existingCapacity, capacityGiB)
		}
	}
	return nil
}

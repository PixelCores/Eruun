package aliyun

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

func (c *client) ensureStorageClass(ctx context.Context, params map[string]interface{}) (*contracts.CloudJobResult, error) {
	if _, err := requireCloudMapString(params, ParamTenantID); err != nil {
		return nil, err
	}
	storageClassName, err := requireCloudMapString(params, ParamStorageClassName)
	if err != nil {
		return nil, err
	}
	if _, err := requireCloudMapString(params, StateFileSystemIDKey); err != nil {
		return nil, err
	}
	mountDomain, err := requireCloudMapString(params, StateMountDomainKey)
	if err != nil {
		return nil, err
	}

	server := buildStorageClassServer(mountDomain, cloudMapString(params, ParamServerPath))
	reclaimPolicy, err := parseReclaimPolicy(cloudMapString(params, ParamReclaimPolicy))
	if err != nil {
		return nil, err
	}
	volumeBindingMode, err := parseVolumeBindingMode(cloudMapString(params, ParamVolumeBindingMode))
	if err != nil {
		return nil, err
	}

	storageClasses, err := c.storageClassClient()
	if err != nil {
		return nil, err
	}
	existingStorageClass, err := storageClasses.Get(ctx, storageClassName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get storageclass %q: %w", storageClassName, err)
	}
	if err == nil {
		if matchErr := validateExistingStorageClass(existingStorageClass, server, reclaimPolicy, volumeBindingMode); matchErr != nil {
			return nil, matchErr
		}
		return &contracts.CloudJobResult{
			Message: "storageclass already exists",
			Output: map[string]interface{}{
				ParamStorageClassName: storageClassName,
			},
		}, nil
	}

	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: storageClassName,
		},
		Provisioner: AliyunNASStorageProvisioner,
		Parameters: map[string]string{
			StorageClassParamServer:   server,
			StorageClassParamVolumeAs: StorageClassParamVolumeAsSubpath,
		},
	}
	if reclaimPolicy != nil {
		storageClass.ReclaimPolicy = reclaimPolicy
	}
	if volumeBindingMode != nil {
		storageClass.VolumeBindingMode = volumeBindingMode
	}

	createdStorageClass, err := storageClasses.Create(ctx, storageClass, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create storageclass %q: %w", storageClassName, err)
	}

	return &contracts.CloudJobResult{
		Message: "storageclass ensured",
		Output: map[string]interface{}{
			ParamStorageClassName: createdStorageClass.Name,
		},
	}, nil
}

func buildStorageClassServer(mountDomain, serverPath string) string {
	return fmt.Sprintf("%s:%s", strings.TrimSpace(mountDomain), normalizeServerPath(serverPath))
}

func normalizeServerPath(serverPath string) string {
	normalizedPath := strings.TrimSpace(serverPath)
	if normalizedPath == "" {
		return DefaultStorageClassServerPath
	}
	if !strings.HasPrefix(normalizedPath, "/") {
		return "/" + normalizedPath
	}
	return normalizedPath
}

func parseReclaimPolicy(value string) (*corev1.PersistentVolumeReclaimPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return nil, nil
	case strings.ToLower(string(corev1.PersistentVolumeReclaimDelete)):
		policy := corev1.PersistentVolumeReclaimDelete
		return &policy, nil
	case strings.ToLower(string(corev1.PersistentVolumeReclaimRetain)):
		policy := corev1.PersistentVolumeReclaimRetain
		return &policy, nil
	default:
		return nil, fmt.Errorf("unsupported params.%s=%q", ParamReclaimPolicy, value)
	}
}

func parseVolumeBindingMode(value string) (*storagev1.VolumeBindingMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return nil, nil
	case strings.ToLower(string(storagev1.VolumeBindingImmediate)):
		mode := storagev1.VolumeBindingImmediate
		return &mode, nil
	case strings.ToLower(string(storagev1.VolumeBindingWaitForFirstConsumer)):
		mode := storagev1.VolumeBindingWaitForFirstConsumer
		return &mode, nil
	default:
		return nil, fmt.Errorf("unsupported params.%s=%q", ParamVolumeBindingMode, value)
	}
}

func validateExistingStorageClass(existing *storagev1.StorageClass, server string, reclaimPolicy *corev1.PersistentVolumeReclaimPolicy, volumeBindingMode *storagev1.VolumeBindingMode) error {
	if existing == nil {
		return fmt.Errorf("storageclass is nil")
	}
	if existing.Provisioner != AliyunNASStorageProvisioner {
		return fmt.Errorf("storageclass %q already exists with provisioner %q, want %q", existing.Name, existing.Provisioner, AliyunNASStorageProvisioner)
	}
	if strings.TrimSpace(existing.Parameters[StorageClassParamServer]) != server {
		return fmt.Errorf("storageclass %q already exists with %s=%q, want %q", existing.Name, StorageClassParamServer, existing.Parameters[StorageClassParamServer], server)
	}
	if strings.TrimSpace(existing.Parameters[StorageClassParamVolumeAs]) != StorageClassParamVolumeAsSubpath {
		return fmt.Errorf("storageclass %q already exists with %s=%q, want %q", existing.Name, StorageClassParamVolumeAs, existing.Parameters[StorageClassParamVolumeAs], StorageClassParamVolumeAsSubpath)
	}
	if reclaimPolicy != nil {
		if existing.ReclaimPolicy == nil || *existing.ReclaimPolicy != *reclaimPolicy {
			return fmt.Errorf("storageclass %q already exists with %s=%q, want %q", existing.Name, ParamReclaimPolicy, valueOrEmptyReclaimPolicy(existing.ReclaimPolicy), *reclaimPolicy)
		}
	}
	if volumeBindingMode != nil {
		if existing.VolumeBindingMode == nil || *existing.VolumeBindingMode != *volumeBindingMode {
			return fmt.Errorf("storageclass %q already exists with %s=%q, want %q", existing.Name, ParamVolumeBindingMode, valueOrEmptyVolumeBindingMode(existing.VolumeBindingMode), *volumeBindingMode)
		}
	}
	return nil
}

func valueOrEmptyReclaimPolicy(value *corev1.PersistentVolumeReclaimPolicy) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

func valueOrEmptyVolumeBindingMode(value *storagev1.VolumeBindingMode) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

package aliyun

import (
	"context"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

func describeMountTarget(ctx context.Context, runtime contracts.CloudRuntime, tenantID, fileSystemID, mountDomainHint string) (describedFSID, mountDomain, mountStatus string, confirmInfo interface{}, err error) {
	query := map[string]interface{}{
		ParamTenantID: tenantID,
	}
	if strings.TrimSpace(fileSystemID) != "" {
		query[StateFileSystemIDKey] = strings.TrimSpace(fileSystemID)
	}
	if strings.TrimSpace(mountDomainHint) != "" {
		query[StateMountDomainKey] = strings.TrimSpace(mountDomainHint)
	}
	result, callErr := runtime.Call(ctx, ActionNasDescribeMountTarget, query)
	if callErr != nil {
		return "", "", "", nil, callErr
	}
	describedFSID = cloudResultString(result, StateFileSystemIDKey)
	mountDomain = cloudResultString(result, StateMountDomainKey)
	mountStatus = normalizeStatus(cloudResultString(result, StateMountStatusKey))
	confirmInfo, _ = cloudResultValue(result, StateMountConfirmInfoKey)
	return describedFSID, mountDomain, mountStatus, confirmInfo, nil
}

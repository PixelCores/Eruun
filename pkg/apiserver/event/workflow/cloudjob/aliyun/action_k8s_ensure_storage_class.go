package aliyun

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type k8sEnsureStorageClassAction struct{}

func newK8sEnsureStorageClassAction() contracts.CloudAction {
	return &k8sEnsureStorageClassAction{}
}

func (a *k8sEnsureStorageClassAction) Validate(req *contracts.CloudJobRequest) error {
	if err := requireCloudParamStrings(req,
		ParamTenantID,
		ParamStorageClassName,
	); err != nil {
		return err
	}
	return rejectCloudParamStrings(req, ParamRegionID, ParamZoneID, ParamVpcID, ParamVSwitchID)
}

func (a *k8sEnsureStorageClassAction) Run(ctx context.Context, runtime contracts.CloudRuntime, req *contracts.CloudJobRequest, state map[string]interface{}) (*contracts.CloudActionProgress, error) {
	if runtime == nil {
		return nil, fmt.Errorf("cloud runtime is nil")
	}
	if req == nil {
		return nil, fmt.Errorf("cloud job request is nil")
	}

	tenantID, err := requireCloudParamString(req, ParamTenantID)
	if err != nil {
		return nil, err
	}
	storageClassName, err := requireCloudParamString(req, ParamStorageClassName)
	if err != nil {
		return nil, err
	}

	nextState := contracts.CloneCloudParams(state)
	if nextState == nil {
		nextState = map[string]interface{}{}
	}
	nextState[StateStorageClassNameKey] = storageClassName

	fileSystemID := cloudMapString(nextState, StateFileSystemIDKey)
	mountDomain := cloudMapString(nextState, StateMountDomainKey)
	mountStatus := normalizeStatus(cloudMapString(nextState, StateMountStatusKey))
	confirmInfo, _ := cloudMapValue(nextState, StateMountConfirmInfoKey)

	if fileSystemID == "" || mountDomain == "" || mountStatus != StateMountStatusActive || !cloudValuePresent(confirmInfo) {
		fetchedFSID, fetchedDomain, fetchedStatus, fetchedConfirmInfo, fetchErr := describeMountTarget(ctx, runtime, tenantID, fileSystemID, mountDomain)
		if fetchErr != nil {
			return nil, fetchErr
		}
		if fileSystemID == "" && fetchedFSID != "" {
			fileSystemID = fetchedFSID
		}
		if fetchedDomain != "" {
			mountDomain = fetchedDomain
		}
		if fetchedStatus != "" {
			mountStatus = fetchedStatus
		}
		if cloudValuePresent(fetchedConfirmInfo) {
			confirmInfo = fetchedConfirmInfo
		}
		if fileSystemID != "" {
			nextState[StateFileSystemIDKey] = fileSystemID
		}
		if mountDomain != "" {
			nextState[StateMountDomainKey] = mountDomain
		}
		if mountStatus != "" {
			nextState[StateMountStatusKey] = mountStatus
		}
		if cloudValuePresent(confirmInfo) {
			nextState[StateMountConfirmInfoKey] = confirmInfo
		}
	}

	if fileSystemID == "" {
		return nil, fmt.Errorf("aliyun nas state missing %s for tenant %q", StateFileSystemIDKey, tenantID)
	}
	if mountDomain == "" || mountStatus != StateMountStatusActive || !cloudValuePresent(confirmInfo) {
		nextState[StateStepKey] = StateStepStorageClassWaitMountTarget
		return &contracts.CloudActionProgress{
			Done:         false,
			State:        nextState,
			RequeueAfter: cloudPollInterval(req.Params),
			Result: &contracts.CloudJobResult{
				Message: "mount target is not ready",
			},
		}, nil
	}

	params := contracts.CloneCloudParams(req.Params)
	if params == nil {
		params = map[string]interface{}{}
	}
	params[ParamTenantID] = tenantID
	params[StateFileSystemIDKey] = fileSystemID
	params[StateMountDomainKey] = mountDomain
	if cloudValuePresent(confirmInfo) {
		params[StateMountConfirmInfoKey] = confirmInfo
	}

	result, err := runtime.Call(ctx, ActionK8sEnsureStorageClass, params)
	if err != nil {
		return nil, err
	}

	nextState[StateStepKey] = StateStepStorageClassReady
	return &contracts.CloudActionProgress{
		Done:   true,
		State:  nextState,
		Result: result,
	}, nil
}

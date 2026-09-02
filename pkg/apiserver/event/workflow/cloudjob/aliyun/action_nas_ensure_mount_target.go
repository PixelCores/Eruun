package aliyun

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type nasEnsureMountTargetAction struct{}

func newNasEnsureMountTargetAction() contracts.CloudAction {
	return &nasEnsureMountTargetAction{}
}

func (a *nasEnsureMountTargetAction) Validate(req *contracts.CloudJobRequest) error {
	if err := requireCloudParamStrings(req, ParamTenantID); err != nil {
		return err
	}
	if err := rejectCloudParamStrings(req, ParamRegionID, ParamZoneID, ParamVpcID, ParamVSwitchID); err != nil {
		return err
	}
	return rejectCloudStateParams(req, StateFileSystemIDKey, StateMountDomainKey, StateMountStatusKey, StateMountConfirmInfoKey, StateStepKey)
}

func (a *nasEnsureMountTargetAction) Run(ctx context.Context, runtime contracts.CloudRuntime, req *contracts.CloudJobRequest, state map[string]interface{}) (*contracts.CloudActionProgress, error) {
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

	nextState := contracts.CloneCloudParams(state)
	if nextState == nil {
		nextState = map[string]interface{}{}
	}

	fileSystemID := cloudMapString(nextState, StateFileSystemIDKey)
	if fileSystemID == "" {
		fileSystemID, _, _, _, err = describeMountTarget(ctx, runtime, tenantID, "", "")
		if err != nil {
			return nil, err
		}
		if fileSystemID == "" {
			return nil, fmt.Errorf("aliyun nas state missing %s for tenant %q", StateFileSystemIDKey, tenantID)
		}
		nextState[StateFileSystemIDKey] = fileSystemID
	}

	step := cloudMapString(nextState, StateStepKey)
	mountDomain := cloudMapString(nextState, StateMountDomainKey)
	shouldEnsureMountTarget := true
	switch step {
	case StateStepMountTargetCreated, StateStepMountTargetPending, StateStepMountTargetReady:
		shouldEnsureMountTarget = false
	}
	if shouldEnsureMountTarget {
		ensureParams := contracts.CloneCloudParams(req.Params)
		if ensureParams == nil {
			ensureParams = map[string]interface{}{}
		}
		ensureParams[ParamTenantID] = tenantID
		ensureParams[StateFileSystemIDKey] = fileSystemID
		if mountDomain != "" {
			ensureParams[StateMountDomainKey] = mountDomain
		}
		ensureResult, ensureErr := runtime.Call(ctx, ActionNasEnsureMountTarget, ensureParams)
		if ensureErr != nil {
			return nil, ensureErr
		}

		mountDomain = cloudResultString(ensureResult, StateMountDomainKey)
		if mountDomain != "" {
			nextState[StateMountDomainKey] = mountDomain
		}
		nextState[StateStepKey] = StateStepMountTargetCreated
	}

	describedFSID, describedDomain, describedStatus, describedConfirmInfo, err := describeMountTarget(ctx, runtime, tenantID, fileSystemID, mountDomain)
	if err != nil {
		return nil, err
	}
	if describedFSID != "" {
		fileSystemID = describedFSID
		nextState[StateFileSystemIDKey] = fileSystemID
	}
	if describedDomain != "" {
		mountDomain = describedDomain
		nextState[StateMountDomainKey] = mountDomain
	}
	if describedStatus != "" {
		nextState[StateMountStatusKey] = describedStatus
	}
	if cloudValuePresent(describedConfirmInfo) {
		nextState[StateMountConfirmInfoKey] = describedConfirmInfo
	}

	mountStatus := normalizeStatus(cloudMapString(nextState, StateMountStatusKey))
	confirmInfo, _ := cloudMapValue(nextState, StateMountConfirmInfoKey)

	if mountDomain != "" && mountStatus == StateMountStatusActive && cloudValuePresent(confirmInfo) {
		nextState[StateStepKey] = StateStepMountTargetReady
		return &contracts.CloudActionProgress{
			Done:  true,
			State: nextState,
			Result: &contracts.CloudJobResult{
				Message: "mount target is active and confirmed",
				Output: map[string]interface{}{
					StateFileSystemIDKey:     fileSystemID,
					StateMountDomainKey:      mountDomain,
					StateMountStatusKey:      mountStatus,
					StateMountConfirmInfoKey: confirmInfo,
				},
			},
		}, nil
	}

	nextState[StateStepKey] = StateStepMountTargetPending
	return &contracts.CloudActionProgress{
		Done:  false,
		State: nextState,
		Result: &contracts.CloudJobResult{
			Message: "mount target is not ready",
			Output: map[string]interface{}{
				StateFileSystemIDKey: fileSystemID,
				StateMountDomainKey:  mountDomain,
				StateMountStatusKey:  mountStatus,
			},
		},
		RequeueAfter: cloudPollInterval(req.Params),
	}, nil
}

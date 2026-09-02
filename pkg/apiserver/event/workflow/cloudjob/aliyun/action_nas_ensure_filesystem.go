package aliyun

import (
	"context"
	"errors"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type nasEnsureFilesystemAction struct{}

func newNasEnsureFilesystemAction() contracts.CloudAction {
	return &nasEnsureFilesystemAction{}
}

func (a *nasEnsureFilesystemAction) Validate(req *contracts.CloudJobRequest) error {
	if err := requireCloudParamStrings(req,
		ParamTenantID,
		ParamStorageType,
		ParamProtocolType,
	); err != nil {
		return err
	}
	if err := rejectCloudParamStrings(req, ParamRegionID, ParamZoneID, ParamVpcID, ParamVSwitchID); err != nil {
		return err
	}
	return rejectCloudStateParams(req, StateFileSystemIDKey, StateFileSystemTagRetryCountKey, StateStepKey)
}

func (a *nasEnsureFilesystemAction) Run(ctx context.Context, runtime contracts.CloudRuntime, req *contracts.CloudJobRequest, state map[string]interface{}) (*contracts.CloudActionProgress, error) {
	if runtime == nil {
		return nil, fmt.Errorf("cloud runtime is nil")
	}
	if req == nil {
		return nil, fmt.Errorf("cloud job request is nil")
	}

	nextState := contracts.CloneCloudParams(state)
	if nextState == nil {
		nextState = map[string]interface{}{}
	}

	params := contracts.CloneCloudParams(req.Params)
	if params == nil {
		params = map[string]interface{}{}
	}
	if fsID := cloudMapString(nextState, StateFileSystemIDKey); fsID != "" {
		params[StateFileSystemIDKey] = fsID
	}
	result, err := runtime.Call(ctx, ActionNasEnsureFilesystem, params)
	if err != nil {
		var pendingErr *fileSystemTagPendingError
		if !errors.As(err, &pendingErr) {
			return nil, err
		}
		retryCount, _, retryErr := cloudMapInt64(nextState, StateFileSystemTagRetryCountKey)
		if retryErr != nil {
			return nil, fmt.Errorf("invalid state.%s: %w", StateFileSystemTagRetryCountKey, retryErr)
		}
		fileSystemID := pendingErr.fileSystemID
		if fileSystemID == "" {
			fileSystemID = cloudMapString(nextState, StateFileSystemIDKey)
		}
		if retryCount >= DefaultFileSystemTagPendingMaxRetries {
			return nil, fmt.Errorf("aliyun nas filesystem %q tag pending exceeded max retries %d: %w", fileSystemID, DefaultFileSystemTagPendingMaxRetries, pendingErr)
		}
		nextState[StateStepKey] = StateStepFilesystemTagPending
		if fileSystemID != "" {
			nextState[StateFileSystemIDKey] = fileSystemID
		}
		nextState[StateFileSystemTagRetryCountKey] = retryCount + 1
		return &contracts.CloudActionProgress{
			Done:         false,
			State:        nextState,
			RequeueAfter: cloudPollInterval(req.Params),
			Result: &contracts.CloudJobResult{
				RequestID: pendingErr.requestID,
				Message:   "filesystem created and waiting for tenant tag",
				Output: map[string]interface{}{
					StateFileSystemIDKey: fileSystemID,
				},
			},
		}, nil
	}
	nextState[StateStepKey] = StateStepFilesystemReady
	delete(nextState, StateFileSystemTagRetryCountKey)
	if fsID := cloudResultString(result, StateFileSystemIDKey); fsID != "" {
		nextState[StateFileSystemIDKey] = fsID
	}

	return &contracts.CloudActionProgress{
		Done:   true,
		State:  nextState,
		Result: result,
	}, nil
}

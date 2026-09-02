package custom

import (
	"context"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type echoAction struct{}

func newEchoAction() contracts.CloudAction {
	return &echoAction{}
}

func (a *echoAction) Validate(req *contracts.CloudJobRequest) error {
	_, err := requireMessage(req)
	return err
}

func (a *echoAction) Run(ctx context.Context, runtime contracts.CloudRuntime, req *contracts.CloudJobRequest, state map[string]interface{}) (*contracts.CloudActionProgress, error) {
	if runtime == nil {
		return nil, fmt.Errorf("cloud runtime is nil")
	}
	if req == nil {
		return nil, fmt.Errorf("cloud job request is nil")
	}
	message, err := requireMessage(req)
	if err != nil {
		return nil, err
	}

	params := contracts.CloneCloudParams(req.Params)
	if params == nil {
		params = map[string]interface{}{}
	}
	params[ParamMessage] = message

	result, err := runtime.Call(ctx, ActionEcho, params)
	if err != nil {
		return nil, err
	}
	if result == nil {
		result = &contracts.CloudJobResult{}
	}
	if strings.TrimSpace(result.Message) == "" {
		result.Message = "custom echo action completed"
	}
	if result.Output == nil {
		result.Output = map[string]interface{}{}
	}
	if _, exists := result.Output[ParamMessage]; !exists {
		result.Output[ParamMessage] = message
	}

	nextState := contracts.CloneCloudParams(state)
	if nextState == nil {
		nextState = map[string]interface{}{}
	}
	nextState[StateStepKey] = StateStepCompleted

	return &contracts.CloudActionProgress{
		Done:   true,
		State:  nextState,
		Result: result,
	}, nil
}

func requireMessage(req *contracts.CloudJobRequest) (string, error) {
	if req == nil {
		return "", fmt.Errorf("cloud job request is nil")
	}
	raw, ok := req.Params[ParamMessage]
	if !ok || raw == nil {
		return "", fmt.Errorf("cloud job requires params.%s", ParamMessage)
	}
	message := strings.TrimSpace(fmt.Sprintf("%v", raw))
	if message == "" {
		return "", fmt.Errorf("cloud job requires params.%s", ParamMessage)
	}
	return message, nil
}

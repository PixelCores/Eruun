package custom

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type fakeCloudRuntime struct {
	result *contracts.CloudJobResult
	err    error
	action string
	params map[string]interface{}
}

func (f *fakeCloudRuntime) Call(_ context.Context, action string, params map[string]interface{}) (*contracts.CloudJobResult, error) {
	f.action = action
	f.params = params
	return f.result, f.err
}

func TestProviderResolveActionAndWhitelist(t *testing.T) {
	provider := NewProvider()

	action, ok := provider.ResolveAction(ActionEcho)
	require.True(t, ok)
	require.NotNil(t, action)

	unknown, found := provider.ResolveAction("custom.unknown")
	require.False(t, found)
	require.Nil(t, unknown)

	require.ElementsMatch(t, []string{ActionEcho}, provider.SupportedActions())
}

func TestProviderRuntimeTemplateReturnsNotImplemented(t *testing.T) {
	provider := NewProvider()
	runtime, err := provider.NewRuntime(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, runtime)

	_, callErr := runtime.Call(context.Background(), ActionEcho, nil)
	require.Error(t, callErr)
	require.Contains(t, callErr.Error(), "not implemented")
}

func TestEchoActionValidateAndRun(t *testing.T) {
	action := &echoAction{}

	err := action.Validate(&contracts.CloudJobRequest{Params: map[string]interface{}{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), ParamMessage)

	require.NoError(t, action.Validate(&contracts.CloudJobRequest{
		Params: map[string]interface{}{ParamMessage: "hello"},
	}))

	runtime := &fakeCloudRuntime{
		result: &contracts.CloudJobResult{
			RequestID: "req-1",
		},
	}
	progress, runErr := action.Run(context.Background(), runtime, &contracts.CloudJobRequest{
		Params: map[string]interface{}{ParamMessage: "hello"},
	}, map[string]interface{}{"existing": "state"})
	require.NoError(t, runErr)
	require.NotNil(t, progress)
	require.True(t, progress.Done)
	require.Equal(t, ActionEcho, runtime.action)
	require.Equal(t, "hello", runtime.params[ParamMessage])
	require.Equal(t, StateStepCompleted, progress.State[StateStepKey])
	require.Equal(t, "state", progress.State["existing"])
	require.NotNil(t, progress.Result)
	require.Equal(t, "req-1", progress.Result.RequestID)
	require.Equal(t, "custom echo action completed", progress.Result.Message)
	require.Equal(t, "hello", progress.Result.Output[ParamMessage])
}

func TestEchoActionRunPropagatesRuntimeError(t *testing.T) {
	action := &echoAction{}
	expectedErr := errors.New("runtime failed")
	runtime := &fakeCloudRuntime{err: expectedErr}

	_, err := action.Run(context.Background(), runtime, &contracts.CloudJobRequest{
		Params: map[string]interface{}{ParamMessage: "hello"},
	}, nil)
	require.ErrorIs(t, err, expectedErr)
}

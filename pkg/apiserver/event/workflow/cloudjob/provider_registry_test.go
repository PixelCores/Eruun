package cloudjob

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

func TestRegisterCloudProvider(t *testing.T) {
	ResetCloudProvidersForTest()
	defer ResetCloudProvidersForTest()

	registered := &registryTestProvider{name: " TestCloud "}
	RegisterCloudProvider(registered)
	provider, ok := GetCloudProvider("testcloud")
	require.True(t, ok)
	require.Same(t, registered, provider)
}

type registryTestProvider struct {
	name string
}

func (p *registryTestProvider) Name() string {
	return p.name
}

func (p *registryTestProvider) NewRuntime(context.Context, *contracts.CloudJobRequest) (contracts.CloudRuntime, error) {
	return nil, nil
}

func (p *registryTestProvider) ResolveAction(string) (contracts.CloudAction, bool) {
	return nil, false
}

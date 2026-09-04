package custom

import (
	"context"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type Provider struct {
	actions map[string]contracts.CloudActionFactory
}

var _ cloudjob.CloudProvider = (*Provider)(nil)

func NewProvider() *Provider {
	return &Provider{
		actions: map[string]contracts.CloudActionFactory{
			ActionEcho: newEchoAction,
		},
	}
}

func (p *Provider) Name() string {
	return ProviderName
}

func (p *Provider) NewRuntime(_ context.Context, _ *contracts.CloudJobRequest) (contracts.CloudRuntime, error) {
	return &runtime{}, nil
}

func (p *Provider) ResolveAction(action string) (contracts.CloudAction, bool) {
	normalized := strings.TrimSpace(action)
	if normalized == "" || p == nil {
		return nil, false
	}
	factory, ok := p.actions[normalized]
	if !ok || factory == nil {
		return nil, false
	}
	return factory(), true
}

func (p *Provider) SupportedActions() []string {
	if p == nil || len(p.actions) == 0 {
		return nil
	}
	actions := make([]string, 0, len(p.actions))
	for action := range p.actions {
		actions = append(actions, action)
	}
	sort.Strings(actions)
	return actions
}

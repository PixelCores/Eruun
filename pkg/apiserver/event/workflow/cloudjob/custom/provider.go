package custom

import (
	"context"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type Provider struct {
	registry contracts.CloudActionRegistry
}

func NewProvider() *Provider {
	return &Provider{
		registry: &actionRegistry{
			factories: map[string]contracts.CloudActionFactory{
				ActionEcho: newEchoAction,
			},
		},
	}
}

func (p *Provider) Name() string {
	return ProviderName
}

func (p *Provider) NewRuntime(_ context.Context, _ *contracts.CloudJobRequest) (contracts.CloudRuntime, error) {
	return &runtime{}, nil
}

func (p *Provider) ActionRegistry() contracts.CloudActionRegistry {
	if p == nil {
		return nil
	}
	return p.registry
}

type actionRegistry struct {
	factories map[string]contracts.CloudActionFactory
}

func (r *actionRegistry) ResolveAction(action string) (contracts.CloudAction, bool) {
	normalized := strings.TrimSpace(action)
	if normalized == "" || r == nil {
		return nil, false
	}
	factory, ok := r.factories[normalized]
	if !ok || factory == nil {
		return nil, false
	}
	return factory(), true
}

func (r *actionRegistry) SupportedActions() []string {
	if r == nil || len(r.factories) == 0 {
		return nil
	}
	actions := make([]string, 0, len(r.factories))
	for action := range r.factories {
		actions = append(actions, action)
	}
	sort.Strings(actions)
	return actions
}

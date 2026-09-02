package traits

import (
	"fmt"
)

// TargetWorkEnvProcessor renders the user-facing targetWorkEnv trait into a pod nodeSelector.
type TargetWorkEnvProcessor struct{}

// Name returns the name of the trait.
func (p *TargetWorkEnvProcessor) Name() string {
	return "targetWorkEnv"
}

// Process maps the target work environment selectors onto pod nodeSelector labels.
func (p *TargetWorkEnvProcessor) Process(ctx *TraitContext) (*TraitResult, error) {
	targetWorkEnv, ok := ctx.TraitData.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("unexpected type for targetWorkEnv trait: %T", ctx.TraitData)
	}

	if len(targetWorkEnv) == 0 {
		return nil, nil
	}

	nodeSelector := make(map[string]string, len(targetWorkEnv))
	for key, value := range targetWorkEnv {
		nodeSelector[key] = value
	}

	return &TraitResult{
		NodeSelector: nodeSelector,
	}, nil
}

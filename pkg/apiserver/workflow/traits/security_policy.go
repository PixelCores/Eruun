package traits

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// SecurityPolicyProcessor applies container-level security settings.
type SecurityPolicyProcessor struct{}

// Name returns the name of the trait.
func (s *SecurityPolicyProcessor) Name() string {
	return "securityPolicy"
}

// Process converts a SecurityPolicySpec into a container SecurityContext.
func (s *SecurityPolicyProcessor) Process(ctx *TraitContext) (*TraitResult, error) {
	policy, ok := ctx.TraitData.(*spec.SecurityPolicySpec)
	if !ok {
		return nil, fmt.Errorf("unexpected type for securityPolicy trait: %T", ctx.TraitData)
	}
	if policy == nil {
		return nil, nil
	}
	return &TraitResult{SecurityContext: copySecurityContext(policy)}, nil
}

func copySecurityContext(source *corev1.SecurityContext) *corev1.SecurityContext {
	if source == nil {
		return nil
	}
	return source.DeepCopy()
}

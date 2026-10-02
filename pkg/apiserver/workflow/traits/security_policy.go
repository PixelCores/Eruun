package traits

import (
	corev1 "k8s.io/api/core/v1"

	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// processSecurityPolicy applies container-level security settings.
// Converts a SecurityPolicySpec into a container SecurityContext.
func processSecurityPolicy(policy *spec.SecurityPolicySpec) (*TraitResult, error) {
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

package traitvalidation

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

// ValidateProfiles checks references against the authoritative repositories.
func ValidateProfiles(ctx context.Context, nodePoolRepo repository.NodePoolRepository, traits spec.Traits, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if traits.NodePool != "" {
		var err error
		if nodePoolRepo == nil {
			err = fmt.Errorf("nodePool repository is unavailable")
		} else {
			_, err = nodePoolRepo.Get(ctx, traits.NodePool)
		}
		if err != nil {
			errors = append(errors, apisv1.ValidationError{Field: field + ".nodePool", Code: apisv1.ErrCodeInvalidTraitConfig, Message: fmt.Sprintf("resolve nodePool %q: %v", traits.NodePool, err)})
		}
	}
	return errors
}

// ValidateNestedProfiles rejects pod-wide references on nested containers.
func ValidateNestedProfiles(traits spec.Traits, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	check := func(nested spec.Traits, field string) {
		if nested.NodePool != "" {
			errors = append(errors, apisv1.ValidationError{Field: field + ".nodePool", Code: apisv1.ErrCodeInvalidTraitConfig, Message: "nodePool is a pod-level trait and only supports component-level traits"})
		}
		errors = append(errors, ValidateNestedProfiles(nested, field)...)
	}
	for i, init := range traits.Init {
		check(init.Traits, fmt.Sprintf("%s.init[%d].traits", field, i))
	}
	for i, sidecar := range traits.Sidecar {
		check(sidecar.Traits, fmt.Sprintf("%s.sidecar[%d].traits", field, i))
	}
	return errors
}

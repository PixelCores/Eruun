package validation

import (
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/traitvalidation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

// validateIngressTrait preserves Try's sorted, per-label errors before sharing
// the remaining ingress rules with writes.
func (v *validationServiceImpl) validateIngressTrait(ingress spec.IngressTraitsSpec, field string) []apisv1.ValidationError {
	errors := validateReservedLabelMap(ingress.Label, fmt.Sprintf("%s.label", field), "traits.ingress.label")
	ingress.Label = nil
	return append(errors, traitvalidation.ValidateIngressTraitSpec(ingress, field)...)
}

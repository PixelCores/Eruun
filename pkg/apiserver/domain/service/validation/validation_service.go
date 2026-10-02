package validation

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/traitvalidation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

// validateServiceTrait preserves Try's name-before-label error order and
// per-label error fields while sharing the service rules with writes.
func (v *validationServiceImpl) validateServiceTrait(service spec.ServiceTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if name := strings.TrimSpace(service.Name); name != "" {
		errors = append(errors, traitvalidation.ValidateKubeResourceName(name, fmt.Sprintf("%s.name", field))...)
	}
	errors = append(errors, validateReservedLabelMap(service.Labels, fmt.Sprintf("%s.labels", field), "traits.service.labels")...)
	// Only the local value is changed; normalizedSpec retains the original fields.
	service.Name = ""
	service.Labels = nil
	return append(errors, traitvalidation.ValidateServiceTraitSpec(service, field)...)
}

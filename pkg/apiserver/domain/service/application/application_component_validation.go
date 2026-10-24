package application

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/traitvalidation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

const (
	minNameLength = 2
	maxNameLength = 63 // DNS-1123 label
)

var (
	nameRegexp            = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	applicationNameRegexp = regexp.MustCompile(spec.APINamePattern)

	validComponentTypes = map[config.JobType]bool{
		config.ServerJob:    true,
		config.StoreJob:     true,
		config.ConfJob:      true,
		config.SecretJob:    true,
		config.InstantJob:   true,
		config.ScheduledJob: true,
	}
)

// ValidateApplicationName checks the dotted application name format.
func ValidateApplicationName(name, field string) []apisv1.ValidationError {
	return validateName(name, field, datastore.PrimaryKeyMaxLength, false, applicationNameRegexp)
}

// ValidateLabelName checks component, workflow and step names, which keep a
// case-insensitive DNS-1123 label identity.
func ValidateLabelName(name, field string) []apisv1.ValidationError {
	return validateName(name, field, maxNameLength, true, nameRegexp)
}

func validateName(name, field string, maxLength int, allowUppercase bool, pattern *regexp.Regexp) []apisv1.ValidationError {
	if name == "" {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: fmt.Sprintf("%s is required", field),
		}}
	}
	var errors []apisv1.ValidationError
	if len(name) < minNameLength {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeNameTooShort,
			Message: fmt.Sprintf("%s must be at least %d characters", field, minNameLength),
		})
	}
	if len(name) > maxLength {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeNameTooLong,
			Message: fmt.Sprintf("%s must be at most %d characters", field, maxLength),
		})
	}
	checkedName := name
	if allowUppercase {
		checkedName = strings.ToLower(name)
	}
	if !pattern.MatchString(checkedName) {
		format := "lowercase alphanumeric, may contain hyphens, must start and end with alphanumeric"
		if pattern == applicationNameRegexp {
			format = "lowercase alphanumeric labels separated by dots, may contain hyphens, each label must start and end with alphanumeric"
		}
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidNameFormat,
			Message: fmt.Sprintf("%s must match DNS-1123 subdomain (%s)", field, format),
		})
	}
	return errors
}

// ValidateComponent is the single rule set for a resolved component and the
// one place that normalizes its evaluation traits, in place. names collects
// component names to detect duplicates across calls.
func ValidateComponent(comp *apisv1.CreateComponentRequest, field string, names map[string]bool) []apisv1.ValidationError {
	nameField := field + ".name"
	errors := ValidateLabelName(comp.Name, nameField)
	lowerName := comp.Name
	if names[lowerName] {
		errors = append(errors, apisv1.ValidationError{
			Field:   nameField,
			Code:    apisv1.ErrCodeDuplicateComponent,
			Message: fmt.Sprintf("duplicate component name: %s", comp.Name),
		})
	} else {
		names[lowerName] = true
	}

	if !validComponentTypes[comp.ComponentType] {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".type",
			Code:    apisv1.ErrCodeInvalidComponentType,
			Message: fmt.Sprintf("invalid component type: %s, must be one of: webservice, store, config, secret, job, scheduledjob", comp.ComponentType),
		})
	}
	if comp.Traits.Evaluation != nil {
		if err := spec.NormalizeComponentEvaluation(string(comp.ComponentType), comp.Image, comp.Properties, &comp.Traits); err != nil {
			errors = append(errors, apisv1.ValidationError{Field: field + ".traits.eval", Code: apisv1.ErrCodeInvalidTraitConfig, Message: err.Error()})
		}
	}
	if (comp.ComponentType == config.ServerJob ||
		comp.ComponentType == config.StoreJob ||
		(comp.ComponentType == config.InstantJob && comp.Traits.Evaluation == nil) ||
		comp.ComponentType == config.ScheduledJob) && comp.Image == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".image",
			Code:    apisv1.ErrCodeMissingImage,
			Message: "image is required for webservice, store, job, and scheduledjob component types",
		})
	}
	for _, key := range spec.ReservedComponentLabelsIn(comp.Properties.Labels) {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.properties.labels.%s", field, key),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("properties.labels key %q is reserved and cannot be overridden", key),
		})
	}
	errors = append(errors, validateJobProperties(*comp, field)...)
	return append(errors, traitvalidation.ValidateTraits(comp.Traits, field+".traits", comp.ComponentType)...)
}

func validateJobProperties(comp apisv1.CreateComponentRequest, field string) []apisv1.ValidationError {
	props := comp.Properties
	schedule := strings.TrimSpace(props.Schedule)
	errors := validateJobPolicies(comp.ComponentType, props, field+".properties")
	if runPolicy := strings.TrimSpace(props.RunPolicy); runPolicy != "" {
		if _, ok := workflowconfig.NormalizeJobRunPolicy(runPolicy); !ok {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".properties.runPolicy",
				Code:    apisv1.ErrCodeInvalidJobRunPolicy,
				Message: fmt.Sprintf("invalid runPolicy: %s, must be one of: recreate, skip_if_completed", runPolicy),
			})
		}
	}
	switch comp.ComponentType {
	case config.InstantJob:
		if schedule != "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".properties.schedule",
				Code:    apisv1.ErrCodeInvalidJobSchedule,
				Message: "job does not allow schedule",
			})
		}
		if props.StartTime < 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".properties.startTime",
				Code:    apisv1.ErrCodeInvalidJobStartTime,
				Message: "startTime must be a unix second timestamp",
			})
		}
	case config.ScheduledJob:
		if schedule == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".properties.schedule",
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "scheduledjob requires schedule",
			})
		} else if _, err := utils.NormalizeCronSchedule(schedule); err != nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".properties.schedule",
				Code:    apisv1.ErrCodeInvalidJobSchedule,
				Message: err.Error(),
			})
		}
	}
	return errors
}

// validateJobPolicies checks the retry and failure policies that only
// immediate job components accept.
func validateJobPolicies(componentType config.JobType, props apisv1.Properties, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if props.JobRetryPolicy != nil {
		var policyErr error
		if componentType != config.InstantJob || props.StartTime != 0 {
			policyErr = fmt.Errorf("jobRetryPolicy is only supported for immediate job components without startTime")
		} else {
			policyErr = props.JobRetryPolicy.Validate()
		}
		if policyErr != nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".jobRetryPolicy",
				Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
				Message: policyErr.Error(),
			})
		}
	}
	if props.FailurePolicy != nil {
		if componentType != config.InstantJob {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".failurePolicy",
				Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
				Message: "failurePolicy is only supported for job components",
			})
		} else if _, ok := workflowconfig.NormalizeJobFailurePolicy(*props.FailurePolicy); !ok {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".failurePolicy",
				Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
				Message: fmt.Sprintf("invalid failurePolicy: %s, only cleanup_failed is supported for job components", strings.TrimSpace(string(*props.FailurePolicy))),
			})
		}
	}
	return errors
}

// normalizeJobFailurePolicy stores the canonical failure policy of a
// validated component.
func normalizeJobFailurePolicy(props *apisv1.Properties) {
	if props.FailurePolicy == nil {
		return
	}
	policy, _ := workflowconfig.NormalizeJobFailurePolicy(*props.FailurePolicy)
	if policy == "" {
		props.FailurePolicy = nil
		return
	}
	props.FailurePolicy = &policy
}

// ValidateTemplateOverrides checks the trait overrides a request applies to
// template components; template expansion can otherwise discard their paths.
func ValidateTemplateOverrides(components []apisv1.CreateComponentRequest) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	for i, component := range components {
		if component.Template == nil || strings.TrimSpace(component.Template.ID) == "" {
			continue
		}
		field := fmt.Sprintf("component[%d].traits", i)
		if err := spec.ValidateNestedEvaluationTraits(&component.Traits); err != nil {
			errors = append(errors, apisv1.ValidationError{Field: field, Code: apisv1.ErrCodeInvalidTraitConfig, Message: err.Error()})
		}
		errors = append(errors, traitvalidation.ValidateNestedJobFailurePolicies(component.Traits, field)...)
		// Overrides merge only supported fields; reject nested gateways instead
		// of dropping them.
		errors = append(errors, traitvalidation.ValidateNestedGateway(component.Traits, field)...)
	}
	return errors
}

// firstValidationError turns the shared rule results into a write error.
func firstValidationError(base error, errors []apisv1.ValidationError) error {
	if len(errors) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s: %s", base, errors[0].Field, errors[0].Message)
}

// componentValidationError keeps the properties error code for rules on
// component or init container properties. Gateway errors keep their field and
// reason for the client, as on the other gateway write paths.
func componentValidationError(errors []apisv1.ValidationError) error {
	if len(errors) > 0 && errors[0].Code == apisv1.ErrCodeGatewayNotAllowed {
		return gatewayWriteError(errors)
	}
	if len(errors) > 0 && strings.Contains(errors[0].Field, ".properties.") {
		return firstValidationError(bcode.ErrInvalidProperties, errors)
	}
	return firstValidationError(bcode.ErrApplicationConfig, errors)
}

// validateGatewayForWrite applies the gateway rules to resolved components of a
// workspace write. The first error is returned with a client-safe message.
func validateGatewayForWrite(ctx context.Context, cfg *config.Config, components []apisv1.CreateComponentRequest) error {
	scope, scoped := access.FromContext(ctx)
	if !scoped {
		return nil
	}
	if cfg == nil {
		return bcode.ErrServiceUnavailable
	}
	for i := range components {
		if err := gatewayWriteError(traitvalidation.ValidateGateway(components[i], scope.Namespace, cfg.Gateway, fmt.Sprintf("component[%d].traits.gateway", i))); err != nil {
			return err
		}
	}
	return nil
}

func gatewayWriteError(errors []apisv1.ValidationError) error {
	if len(errors) == 0 {
		return nil
	}
	return bcode.WithSafeClientMessage(bcode.ErrApplicationConfig, errors[0].Field+": "+errors[0].Message)
}

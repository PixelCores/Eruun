package validation

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

var (
	// DNS-1123 subdomain pattern: lowercase alphanumeric, may contain hyphens
	// Must start and end with alphanumeric character
	nameRegexp            = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	applicationNameRegexp = regexp.MustCompile(spec.APINamePattern)

	// Kubernetes resource quantity pattern for storage size validation
	storageQuantityRegexp = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?$`)

	// Valid storage types
	validStorageTypes = map[string]bool{
		spec.StorageTypePersistent: true,
		spec.StorageTypeEphemeral:  true,
		spec.StorageTypeConfig:     true,
		spec.StorageTypeSecret:     true,
	}

	// Valid probe types
	validProbeTypes = map[string]bool{
		"liveness":  true,
		"readiness": true,
		"startup":   true,
	}

	// Valid component types
	validComponentTypes = map[config.JobType]bool{
		config.ServerJob:  true,
		config.StoreJob:   true,
		config.ConfJob:    true,
		config.SecretJob:  true,
		config.InstantJob: true,
	}

	validWorkflowStepTypes = map[config.WorkflowStepType]bool{
		config.WorkflowStepTypeComponent: true,
		config.WorkflowStepTypeApproval:  true,
	}

	validApprovalMethods = map[string]bool{
		"GET":    true,
		"POST":   true,
		"PUT":    true,
		"DELETE": true,
	}

	// Valid envFrom types
	validEnvFromTypes = map[string]bool{
		"secret":    true,
		"configMap": true,
	}
)

const (
	minNameLength = 2
	maxNameLength = 63 // DNS-1123 subdomain max length
)

// ValidationService provides validation capabilities for applications and workflows
type ValidationService interface {
	// TryApplication validates an application creation request without actually creating it
	TryApplication(ctx context.Context, req apisv1.CreateApplicationsRequest) *apisv1.TryApplicationResponse

	// TryWorkflow validates a workflow update request against existing components
	TryWorkflow(ctx context.Context, appID string, req apisv1.TryWorkflowRequest) *apisv1.TryWorkflowResponse
}

type validationServiceImpl struct {
	Cfg                       *config.Config
	URLSecurityPolicyProvider *urlpolicy.Provider
	AppRepo                   repository.ApplicationRepository
	ComponentRepo             repository.ComponentRepository
}

// NewValidationService creates a validator with its application dependencies.
func NewValidationService(cfg *config.Config, policy *urlpolicy.Provider, appRepo repository.ApplicationRepository, componentRepo repository.ComponentRepository) ValidationService {
	return &validationServiceImpl{
		Cfg:                       cfg,
		URLSecurityPolicyProvider: policy,
		AppRepo:                   appRepo,
		ComponentRepo:             componentRepo,
	}
}

// TryApplication validates an application creation request
func (v *validationServiceImpl) TryApplication(ctx context.Context, req apisv1.CreateApplicationsRequest) *apisv1.TryApplicationResponse {
	if req.Components == nil {
		req.Components = []apisv1.CreateComponentRequest{}
	}
	var errors []apisv1.ValidationError
	effectiveReq, err := v.effectiveTryApplicationRequest(ctx, req)
	if err != nil {
		errors = append(errors, apisv1.ValidationError{
			Field:   "id",
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: err.Error(),
		})
		return &apisv1.TryApplicationResponse{
			Valid:          false,
			Errors:         apisv1.FinalizeValidationErrors(errors, apisv1.ValidationPathApplication),
			NormalizedSpec: &req,
			Plan:           apisv1.NewApplicationExecutionPlan(req),
		}
	}

	// 1. Validate application name
	errors = append(errors, v.validateName(effectiveReq.Name, "name", datastore.PrimaryKeyMaxLength, false, applicationNameRegexp)...)
	if _, ok := workflowconfig.NormalizeWorkflowFailurePolicy(effectiveReq.FailurePolicy); !ok {
		errors = append(errors, apisv1.ValidationError{
			Field:   "failurePolicy",
			Code:    apisv1.ErrCodeInvalidWorkflowFailurePolicy,
			Message: fmt.Sprintf("unsupported workflow failurePolicy: %s", effectiveReq.FailurePolicy),
		})
	}
	resolvedComponents := effectiveReq.Components
	componentNames := make(map[string]bool)
	for i, comp := range effectiveReq.Components {
		fieldPrefix := fmt.Sprintf("component[%d]", i)
		if scope, ok := access.FromContext(ctx); ok {
			var err error
			if v.Cfg == nil || v.Cfg.Accounts == nil || (comp.Namespace != "" && comp.Namespace != scope.Namespace) {
				err = bcode.ErrForbidden
			} else {
				err = workspace.ValidateTraits(scope.Namespace, comp.Name, &comp.Traits, &comp.Properties, v.Cfg.Accounts.Workspace)
			}
			if err != nil {
				errors = append(errors, apisv1.ValidationError{Field: fieldPrefix, Code: apisv1.ErrCodeInvalidTraitConfig, Message: err.Error()})
			}
		}
		if err := spec.NormalizeComponentEvaluation(string(comp.ComponentType), comp.Image, comp.Properties, &comp.Traits); err == nil {
			effectiveReq.Components[i] = comp
		}
		errors = append(errors, v.validateComponent(comp, fieldPrefix, componentNames)...)
	}

	// Validate workflow steps and component references against resolved names.
	errors = append(errors, v.validateWorkflowSteps(effectiveReq.Workflow, workflowComponentIndexFromCreateComponents(resolvedComponents), "workflow")...)
	if resourceErr := v.validateTryApplicationResourceNames(ctx, effectiveReq, resolvedComponents); resourceErr != nil {
		errors = append(errors, apisv1.ValidationError{
			Field:   "component",
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: resourceErr.Error(),
		})
	}

	errors = append(errors, v.validateApplicationCallback(ctx, effectiveReq)...)

	return &apisv1.TryApplicationResponse{
		Valid:          len(errors) == 0,
		Errors:         apisv1.FinalizeValidationErrors(errors, apisv1.ValidationPathApplication),
		NormalizedSpec: &effectiveReq,
		Plan:           apisv1.NewApplicationExecutionPlan(effectiveReq),
	}
}

func (v *validationServiceImpl) validateApplicationCallback(ctx context.Context, req apisv1.CreateApplicationsRequest) []apisv1.ValidationError {
	if err := applicationservice.ValidateCreateApplicationCallback(ctx, v.Cfg, v.URLSecurityPolicyProvider, req); err != nil {
		return []apisv1.ValidationError{
			{
				Field:   applicationCallbackValidationField(req),
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: err.Error(),
			},
		}
	}
	return nil
}

func applicationCallbackValidationField(req apisv1.CreateApplicationsRequest) string {
	return "callback"
}

func (v *validationServiceImpl) effectiveTryApplicationRequest(ctx context.Context, req apisv1.CreateApplicationsRequest) (apisv1.CreateApplicationsRequest, error) {
	effective := req
	if scope, ok := access.FromContext(ctx); ok {
		if effective.Namespace != "" && effective.Namespace != scope.Namespace {
			return effective, bcode.ErrForbidden
		}
		effective.Namespace = scope.Namespace
	}
	appID := strings.TrimSpace(req.ID)
	if appID == "" {
		return effective, nil
	}
	if v.AppRepo == nil {
		return effective, fmt.Errorf("application repository is required to validate application update")
	}
	app, err := v.AppRepo.FindByID(ctx, appID)
	if err != nil {
		return effective, fmt.Errorf("find application %q: %w", appID, err)
	}
	effective.ID = app.ID
	if strings.TrimSpace(effective.Name) == "" {
		effective.Name = app.Name
	}
	if strings.TrimSpace(effective.Namespace) == "" {
		effective.Namespace = app.Namespace
	}
	if strings.TrimSpace(effective.Version) == "" {
		effective.Version = app.Version
	}
	return effective, nil
}

func (v *validationServiceImpl) validateTryApplicationResourceNames(ctx context.Context, req apisv1.CreateApplicationsRequest, components []apisv1.CreateComponentRequest) error {
	return applicationservice.ValidateTryApplicationResourceNames(ctx, v.AppRepo, v.ComponentRepo, req, components)
}

// validateName validates a name against the requested length and case rules.
func (v *validationServiceImpl) validateName(name, field string, maxLength int, allowUppercase bool, pattern *regexp.Regexp) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	if name == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: fmt.Sprintf("%s is required", field),
		})
		return errors
	}

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

// validateComponent validates a single component configuration
func (v *validationServiceImpl) validateComponent(comp apisv1.CreateComponentRequest, fieldPrefix string, componentNames map[string]bool) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	// Validate component name
	nameField := fmt.Sprintf("%s.name", fieldPrefix)
	errors = append(errors, v.validateName(comp.Name, nameField, maxNameLength, true, nameRegexp)...)

	// Check for duplicate component names
	lowerName := strings.ToLower(comp.Name)
	if componentNames[lowerName] {
		errors = append(errors, apisv1.ValidationError{
			Field:   nameField,
			Code:    apisv1.ErrCodeDuplicateComponent,
			Message: fmt.Sprintf("duplicate component name: %s", comp.Name),
		})
	} else {
		componentNames[lowerName] = true
	}

	// Validate component type
	if !validComponentTypes[comp.ComponentType] {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", fieldPrefix),
			Code:    apisv1.ErrCodeInvalidComponentType,
			Message: fmt.Sprintf("invalid component type: %s, must be one of: webservice, store, config, secret, job", comp.ComponentType),
		})
	}

	if comp.Traits.Evaluation != nil {
		if err := spec.NormalizeComponentEvaluation(string(comp.ComponentType), comp.Image, comp.Properties, &comp.Traits); err != nil {
			errors = append(errors, apisv1.ValidationError{Field: fieldPrefix + ".traits.eval", Code: apisv1.ErrCodeInvalidTraitConfig, Message: err.Error()})
		}
	}

	// Validate image requirement for webservice and store types
	if (comp.ComponentType == config.ServerJob ||
		comp.ComponentType == config.StoreJob ||
		(comp.ComponentType == config.InstantJob && comp.Traits.Evaluation == nil)) && comp.Image == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.image", fieldPrefix),
			Code:    apisv1.ErrCodeMissingImage,
			Message: "image is required for webservice, store, and job component types",
		})
	}

	// Validate reserved component labels are not overridden by user input.
	errors = append(errors, v.validateReservedPropertiesLabels(comp.Properties.Labels, fmt.Sprintf("%s.properties.labels", fieldPrefix))...)

	// Validate job-specific properties
	errors = append(errors, v.validateJobProperties(comp, fieldPrefix)...)

	// Validate traits
	traitsField := fmt.Sprintf("%s.traits", fieldPrefix)
	errors = append(errors, v.validateTraits(comp.Traits, traitsField, false, comp.ComponentType)...)

	return errors
}

func (v *validationServiceImpl) validateReservedPropertiesLabels(labels map[string]string, fieldPrefix string) []apisv1.ValidationError {
	return validateReservedLabelMap(labels, fieldPrefix, "properties.labels")
}

func validateReservedLabelMap(labels map[string]string, fieldPrefix, source string) []apisv1.ValidationError {
	keys := reservedComponentLabelsIn(labels)
	if len(keys) == 0 {
		return nil
	}

	errors := make([]apisv1.ValidationError, 0, len(keys))
	for _, key := range keys {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.%s", fieldPrefix, key),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("%s key %q is reserved and cannot be overridden", source, key),
		})
	}
	return errors
}

func (v *validationServiceImpl) validateJobProperties(comp apisv1.CreateComponentRequest, fieldPrefix string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	props := comp.Properties
	runPolicy := strings.TrimSpace(props.RunPolicy)
	startTime := props.StartTime
	if props.JobRetryPolicy != nil {
		var policyErr error
		if comp.ComponentType != config.InstantJob || startTime != 0 {
			policyErr = fmt.Errorf("jobRetryPolicy is only supported for immediate job components without startTime")
		} else {
			policyErr = props.JobRetryPolicy.Validate()
		}
		if policyErr != nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.jobRetryPolicy", fieldPrefix),
				Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
				Message: policyErr.Error(),
			})
		}
	}

	if props.FailurePolicy != nil {
		failurePolicy := strings.TrimSpace(string(*props.FailurePolicy))
		if comp.ComponentType != config.InstantJob {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.failurePolicy", fieldPrefix),
				Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
				Message: "failurePolicy is only supported for job components",
			})
		} else if _, ok := workflowconfig.NormalizeJobFailurePolicy(*props.FailurePolicy); !ok {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.failurePolicy", fieldPrefix),
				Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
				Message: fmt.Sprintf("invalid failurePolicy: %s, only cleanup_failed is supported for job components", failurePolicy),
			})
		}
	}

	if runPolicy != "" {
		if _, ok := workflowconfig.NormalizeJobRunPolicy(runPolicy); !ok {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.runPolicy", fieldPrefix),
				Code:    apisv1.ErrCodeInvalidJobRunPolicy,
				Message: fmt.Sprintf("invalid runPolicy: %s, must be one of: recreate, skip_if_completed", runPolicy),
			})
		}
	}

	switch comp.ComponentType {
	case config.InstantJob:
		if startTime < 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.startTime", fieldPrefix),
				Code:    apisv1.ErrCodeInvalidJobStartTime,
				Message: "startTime must be a unix second timestamp",
			})
		}
	}
	return errors
}

package validation

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/urlpolicy"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

var (
	// DNS-1123 subdomain pattern: lowercase alphanumeric, may contain hyphens
	// Must start and end with alphanumeric character
	nameRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

	// Kubernetes resource quantity pattern for storage size validation
	storageQuantityRegexp = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?$`)

	// Valid storage types
	validStorageTypes = map[string]bool{
		config.StorageTypePersistent: true,
		config.StorageTypeEphemeral:  true,
		config.StorageTypeConfig:     true,
		config.StorageTypeSecret:     true,
	}

	// Valid probe types
	validProbeTypes = map[string]bool{
		"liveness":  true,
		"readiness": true,
		"startup":   true,
	}

	// Valid component types
	validComponentTypes = map[config.JobType]bool{
		config.ServerJob:    true,
		config.StoreJob:     true,
		config.ConfJob:      true,
		config.SecretJob:    true,
		config.CloudJob:     true,
		config.InstantJob:   true,
		config.ScheduledJob: true,
	}

	// Valid workflow modes
	validWorkflowModes = map[string]bool{
		string(config.WorkflowModeStepByStep): true,
		string(config.WorkflowModeDAG):        true,
		"":                                    true, // Empty is allowed (defaults to StepByStep)
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

	// Valid protocols for service ports.
	validServiceProtocols = map[string]bool{
		"":     true,
		"TCP":  true,
		"UDP":  true,
		"SCTP": true,
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
	Cfg                       *config.Config                   `inject:""`
	URLSecurityPolicyProvider *urlpolicy.Provider              `inject:""`
	AppRepo                   repository.ApplicationRepository `inject:""`
	ComponentRepo             repository.ComponentRepository   `inject:""`
}

// NewValidationService creates a new ValidationService instance
func NewValidationService() ValidationService {
	return &validationServiceImpl{}
}

func (v *validationServiceImpl) WithRepositories(appRepo repository.ApplicationRepository, componentRepo repository.ComponentRepository) ValidationService {
	copy := *v
	if copy.AppRepo == nil {
		copy.AppRepo = appRepo
	}
	if copy.ComponentRepo == nil {
		copy.ComponentRepo = componentRepo
	}
	return &copy
}

// TryApplication validates an application creation request
func (v *validationServiceImpl) TryApplication(ctx context.Context, req apisv1.CreateApplicationsRequest) *apisv1.TryApplicationResponse {
	var errors []apisv1.ValidationError
	effectiveReq, err := v.effectiveTryApplicationRequest(ctx, req)
	if err != nil {
		errors = append(errors, apisv1.ValidationError{
			Field:   "id",
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: err.Error(),
		})
		return &apisv1.TryApplicationResponse{
			Valid:  false,
			Errors: errors,
		}
	}

	// 1. Validate application name
	errors = append(errors, v.validateName(effectiveReq.Name, "name")...)
	if _, ok := workflowconfig.NormalizeWorkflowFailurePolicy(effectiveReq.WorkflowFailurePolicy); !ok {
		errors = append(errors, apisv1.ValidationError{
			Field:   "workflow.failurePolicy",
			Code:    apisv1.ErrCodeInvalidWorkflowFailurePolicy,
			Message: fmt.Sprintf("unsupported workflow failurePolicy: %s", effectiveReq.WorkflowFailurePolicy),
		})
	}
	errors = append(errors, validateTemplateRequestNestedJobFailurePolicies(effectiveReq.Component)...)

	resolvedComponents := effectiveReq.Component
	resolvedComponentSourceIndexes := make([]int, len(resolvedComponents))
	for i := range resolvedComponentSourceIndexes {
		resolvedComponentSourceIndexes[i] = i
	}
	componentsResolved := true
	if requestUsesTemplate(effectiveReq.Component) {
		if v.AppRepo == nil || v.ComponentRepo == nil {
			componentsResolved = false
			errors = append(errors, apisv1.ValidationError{
				Field:   "component",
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: "template repositories are required to validate template components",
			})
		} else {
			components, sourceIndexes, err := applicationservice.ResolveComponentsWithSourceIndexes(ctx, v.AppRepo, v.ComponentRepo, applicationservice.ServiceNamespaceOrDefault(effectiveReq.Namespace), effectiveReq.Name, effectiveReq.Component)
			if err != nil {
				componentsResolved = false
				errors = append(errors, apisv1.ValidationError{
					Field:   "component",
					Code:    apisv1.ErrCodeInvalidTraitConfig,
					Message: err.Error(),
				})
			} else {
				resolvedComponents = components
				resolvedComponentSourceIndexes = sourceIndexes
			}
		}
	}

	if componentsResolved {
		// 2. Validate resolved components. Template requests are overrides until
		// resolution, so type/image/traits must be checked on the cloned output.
		componentNames := make(map[string]bool)
		for i, comp := range resolvedComponents {
			fieldIndex := i
			if i < len(resolvedComponentSourceIndexes) && resolvedComponentSourceIndexes[i] >= 0 {
				fieldIndex = resolvedComponentSourceIndexes[i]
			}
			fieldPrefix := fmt.Sprintf("component[%d]", fieldIndex)
			errors = append(errors, v.validateComponent(comp, fieldPrefix, componentNames)...)
		}

		// 3. Validate workflow steps and component references against resolved names.
		errors = append(errors, v.validateWorkflowSteps(effectiveReq.WorkflowSteps, workflowComponentIndexFromCreateComponents(resolvedComponents), "workflow")...)
		if resourceErr := v.validateTryApplicationResourceNames(ctx, effectiveReq, resolvedComponents); resourceErr != nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   "component",
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: resourceErr.Error(),
			})
		}
	}

	errors = append(errors, v.validateApplicationCallback(ctx, effectiveReq)...)

	return &apisv1.TryApplicationResponse{
		Valid:  len(errors) == 0,
		Errors: errors,
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
	if strings.TrimSpace(req.ID) != "" && req.Callback != nil {
		return "callback"
	}
	if len(req.WorkflowSteps) > 0 && !applicationservice.WorkflowCallbackIsEmpty(req.WorkflowCallback) {
		return "workflow.callback"
	}
	return "callback"
}

func (v *validationServiceImpl) effectiveTryApplicationRequest(ctx context.Context, req apisv1.CreateApplicationsRequest) (apisv1.CreateApplicationsRequest, error) {
	effective := req
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
	if effective.TemplateEnabled == nil {
		templateEnabled := app.TemplateEnabled
		effective.TemplateEnabled = &templateEnabled
	}
	return effective, nil
}

func requestUsesTemplate(components []apisv1.CreateComponentRequest) bool {
	for _, component := range components {
		if component.Template != nil && strings.TrimSpace(component.Template.ID) != "" {
			return true
		}
	}
	return false
}

func (v *validationServiceImpl) validateTryApplicationResourceNames(ctx context.Context, req apisv1.CreateApplicationsRequest, components []apisv1.CreateComponentRequest) error {
	return applicationservice.ValidateTryApplicationResourceNames(ctx, v.AppRepo, v.ComponentRepo, req, components)
}

// validateName validates a name against DNS-1123 subdomain rules
func (v *validationServiceImpl) validateName(name, field string) []apisv1.ValidationError {
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

	if len(name) > maxNameLength {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeNameTooLong,
			Message: fmt.Sprintf("%s must be at most %d characters", field, maxNameLength),
		})
	}

	// Convert to lowercase for validation (names should be lowercase)
	lowerName := strings.ToLower(name)
	if !nameRegexp.MatchString(lowerName) {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidNameFormat,
			Message: fmt.Sprintf("%s must match DNS-1123 subdomain (lowercase alphanumeric, may contain hyphens, must start and end with alphanumeric)", field),
		})
	}

	return errors
}

// validateComponent validates a single component configuration
func (v *validationServiceImpl) validateComponent(comp apisv1.CreateComponentRequest, fieldPrefix string, componentNames map[string]bool) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	// Validate component name
	nameField := fmt.Sprintf("%s.name", fieldPrefix)
	errors = append(errors, v.validateName(comp.Name, nameField)...)

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
			Message: fmt.Sprintf("invalid component type: %s, must be one of: webservice, store, config, secret, cloudjob, job, scheduledjob", comp.ComponentType),
		})
	}

	// Validate image requirement for webservice and store types
	if (comp.ComponentType == config.ServerJob ||
		comp.ComponentType == config.StoreJob ||
		comp.ComponentType == config.InstantJob ||
		comp.ComponentType == config.ScheduledJob) && comp.Image == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.image", fieldPrefix),
			Code:    apisv1.ErrCodeMissingImage,
			Message: "image is required for webservice, store, job, and scheduledjob component types",
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
	schedule := strings.TrimSpace(props.Schedule)
	runPolicy := strings.TrimSpace(props.RunPolicy)
	startTime := props.StartTime

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
	case config.CloudJob:
		if props.Cloud == nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.cloud", fieldPrefix),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "cloudjob requires properties.cloud",
			})
			return errors
		}
		if strings.TrimSpace(props.Cloud.Provider) == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.cloud.provider", fieldPrefix),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "cloudjob requires properties.cloud.provider",
			})
		}
		if strings.TrimSpace(props.Cloud.Action) == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.cloud.action", fieldPrefix),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "cloudjob requires properties.cloud.action",
			})
		}
	case config.InstantJob:
		if schedule != "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.schedule", fieldPrefix),
				Code:    apisv1.ErrCodeInvalidJobSchedule,
				Message: "job does not allow schedule",
			})
		}
		if startTime < 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.startTime", fieldPrefix),
				Code:    apisv1.ErrCodeInvalidJobStartTime,
				Message: "startTime must be a unix second timestamp",
			})
		}
	case config.ScheduledJob:
		if schedule == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.schedule", fieldPrefix),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "scheduledjob requires schedule",
			})
			return errors
		}
		if schedule != "" {
			if _, err := utils.NormalizeCronSchedule(schedule); err != nil {
				errors = append(errors, apisv1.ValidationError{
					Field:   fmt.Sprintf("%s.properties.schedule", fieldPrefix),
					Code:    apisv1.ErrCodeInvalidJobSchedule,
					Message: err.Error(),
				})
			}
		}
	}
	return errors
}

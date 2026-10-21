package validation

import (
	"context"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/traitvalidation"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
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
	NodePoolRepo              repository.NodePoolRepository
}

// NewValidationService creates a validator with its application dependencies.
func NewValidationService(cfg *config.Config, policy *urlpolicy.Provider, appRepo repository.ApplicationRepository, componentRepo repository.ComponentRepository, nodePoolRepo repository.NodePoolRepository) ValidationService {
	return &validationServiceImpl{
		Cfg:                       cfg,
		URLSecurityPolicyProvider: policy,
		AppRepo:                   appRepo,
		ComponentRepo:             componentRepo,
		NodePoolRepo:              nodePoolRepo,
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
	errors = append(errors, applicationservice.ValidateApplicationName(effectiveReq.Name, "name")...)
	if _, ok := workflowconfig.NormalizeWorkflowFailurePolicy(effectiveReq.FailurePolicy); !ok {
		errors = append(errors, apisv1.ValidationError{
			Field:   "failurePolicy",
			Code:    apisv1.ErrCodeInvalidWorkflowFailurePolicy,
			Message: fmt.Sprintf("unsupported workflow failurePolicy: %s", effectiveReq.FailurePolicy),
		})
	}
	templateInputErrors := applicationservice.ValidateTemplateOverrides(effectiveReq.Components)
	errors = append(errors, templateInputErrors...)

	resolvedComponents := effectiveReq.Components
	// Invalid template overrides can be discarded during expansion. Keep the
	// source components in normalizedSpec so their error paths remain locatable.
	componentsResolved := len(templateInputErrors) == 0
	if componentsResolved && requestUsesTemplate(effectiveReq.Components) {
		if v.AppRepo == nil || v.ComponentRepo == nil {
			componentsResolved = false
			errors = append(errors, apisv1.ValidationError{
				Field:   "component",
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: "template repositories are required to validate template components",
			})
		} else {
			components, _, err := applicationservice.ResolveComponentsWithSourceIndexes(ctx, v.AppRepo, v.ComponentRepo, effectiveReq.Namespace, effectiveReq.Name, effectiveReq.Components, v.Cfg)
			if err != nil {
				componentsResolved = false
				errors = append(errors, apisv1.ValidationError{
					Field:   "component",
					Code:    apisv1.ErrCodeInvalidTraitConfig,
					Message: err.Error(),
				})
			} else {
				resolvedComponents = components
			}
		}
	}

	if componentsResolved {
		effectiveReq.Components = resolvedComponents
	}
	if componentsResolved || len(templateInputErrors) > 0 {
		// Validate resolved components, or just the direct components when an
		// invalid template override requires retaining the source indexes.
		componentNames := make(map[string]bool)
		for i, comp := range effectiveReq.Components {
			if !componentsResolved && comp.Template != nil && strings.TrimSpace(comp.Template.ID) != "" {
				continue
			}
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
			errors = append(errors, applicationservice.ValidateComponent(comp, fieldPrefix, componentNames)...)
			errors = append(errors, traitvalidation.ValidateProfiles(ctx, v.NodePoolRepo, comp.Traits, fieldPrefix+".traits")...)
		}
	}

	if componentsResolved {
		// Validate workflow steps and component references against resolved names.
		errors = append(errors, applicationservice.ValidateWorkflowSteps(effectiveReq.Workflow, workflowComponentIndexFromCreateComponents(resolvedComponents), "workflow")...)
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

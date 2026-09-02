package application

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/urlpolicy"
)

func ResolveComponents(ctx context.Context, appRepo repository.ApplicationRepository, componentRepo repository.ComponentRepository, namespace, appName string, reqComponents []apisv1.CreateComponentRequest) ([]apisv1.CreateComponentRequest, error) {
	resolver := &applicationsServiceImpl{AppRepo: appRepo, ComponentRepo: componentRepo}
	return resolver.resolveComponents(ctx, namespace, appName, reqComponents)
}

func ResolveComponentsWithSourceIndexes(ctx context.Context, appRepo repository.ApplicationRepository, componentRepo repository.ComponentRepository, namespace, appName string, reqComponents []apisv1.CreateComponentRequest) ([]apisv1.CreateComponentRequest, []int, error) {
	resolver := &applicationsServiceImpl{AppRepo: appRepo, ComponentRepo: componentRepo}
	return resolver.resolveComponentsWithSourceIndexes(ctx, namespace, appName, reqComponents)
}

func ValidateTryApplicationResourceNames(ctx context.Context, appRepo repository.ApplicationRepository, componentRepo repository.ComponentRepository, req apisv1.CreateApplicationsRequest, components []apisv1.CreateComponentRequest) error {
	resourceApp := &model.Applications{
		ID:              req.ID,
		Name:            req.Name,
		Namespace:       req.Namespace,
		TemplateEnabled: req.TemplateEnabled != nil && *req.TemplateEnabled,
	}
	validator := &applicationsServiceImpl{AppRepo: appRepo, ComponentRepo: componentRepo}
	if resourceApp.ID == "" {
		if appRepo != nil && componentRepo != nil {
			return validator.validateApplicationResourceNames(ctx, resourceApp, components)
		}
		return validateResolvedResourceNames(applicationResourceNameKey(resourceApp), ServiceNamespaceOrDefault(req.Namespace), components)
	}
	if resourceApp.Name == "" || resourceApp.Namespace == "" {
		app, err := appRepo.FindByID(ctx, resourceApp.ID)
		if err != nil {
			return err
		}
		if resourceApp.Name == "" {
			resourceApp.Name = app.Name
		}
		if resourceApp.Namespace == "" {
			resourceApp.Namespace = app.Namespace
		}
		if req.TemplateEnabled == nil {
			resourceApp.TemplateEnabled = app.TemplateEnabled
		}
	}
	return validator.validateApplicationResourceNames(ctx, resourceApp, components)
}

func ValidateComponentTraitsForWrite(componentType config.JobType, traits apisv1.Traits, fieldPrefix string) error {
	return validateComponentTraitsForWrite(componentType, traits, fieldPrefix)
}

func ValidateCreateApplicationCallback(ctx context.Context, cfg *config.Config, provider *urlpolicy.Provider, req apisv1.CreateApplicationsRequest) error {
	validator := &applicationsServiceImpl{Cfg: cfg, URLSecurityPolicyProvider: provider}
	_, err := validator.resolveCreateApplicationCallback(ctx, req)
	return err
}

func ValidateWorkflowCallback(ctx context.Context, cfg *config.Config, provider *urlpolicy.Provider, callback *apisv1.WorkflowCallback) error {
	validator := &applicationsServiceImpl{Cfg: cfg, URLSecurityPolicyProvider: provider}
	_, err := validator.normalizeWorkflowCallbackForWrite(ctx, callback)
	return err
}

func WorkflowCallbackIsEmpty(callback *apisv1.WorkflowCallback) bool {
	return callbackIsEmpty(callback)
}

func LoadURLSecurityPolicy(ctx context.Context, provider *urlpolicy.Provider) (*spec.URLSecurityPolicySpec, error) {
	return loadURLSecurityPolicy(ctx, provider)
}

func CopyStringMap(source map[string]string) map[string]string {
	return copyStringMap(source)
}

func ServiceNamespaceOrDefault(namespace string) string {
	return serviceNamespaceOrDefault(namespace)
}

func PickNamespace(candidate, fallback string) string {
	return pickNamespace(candidate, fallback)
}

func CreateComponentShareEnabled(component apisv1.CreateComponentRequest) bool {
	return createComponentShareEnabled(component)
}

func ResolvedServiceResourceNames(component apisv1.CreateComponentRequest, resourceAppName string) []string {
	return resolvedServiceResourceNames(component, resourceAppName)
}

func ResolvedIngressResourceNames(component apisv1.CreateComponentRequest, resourceAppName string) []string {
	return resolvedIngressResourceNames(component, resourceAppName)
}

func ResolvedPVCResourceNames(component apisv1.CreateComponentRequest) []string {
	resolved := resolvedPVCResourceNames(component)
	names := make([]string, 0, len(resolved))
	for _, item := range resolved {
		names = append(names, item.name)
	}
	return names
}

func ReservedComponentLabelKeys() map[string]struct{} {
	keys := make(map[string]struct{}, len(reservedComponentLabelKeys))
	for key := range reservedComponentLabelKeys {
		keys[key] = struct{}{}
	}
	return keys
}

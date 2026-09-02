package application

import (
	"context"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/traitvalidation"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type resolvedResourceRef struct {
	kind                config.ResourceKind
	namespace           string
	name                string
	duplicateSafeShared bool
	componentIndex      int
	componentName       string
	source              string
	appID               string
	appName             string
}

type resolvedComponentLabelRef struct {
	componentIndex int
	componentName  string
	labelValue     string
}

type resolvedPVCResourceName struct {
	name   string
	source string
}

func validateResolvedResourceNames(resourceAppName, namespace string, components []apisv1.CreateComponentRequest) error {
	seenResources := make(map[string]resolvedResourceRef)
	seenComponentLabels := make(map[string]resolvedComponentLabelRef)
	return trackResolvedApplicationResources(seenResources, seenComponentLabels, resourceAppName, namespace, "", "", components, true)
}

func (c *applicationsServiceImpl) validateApplicationResourceNames(ctx context.Context, app *model.Applications, components []apisv1.CreateComponentRequest) error {
	if app == nil {
		return fmt.Errorf("%w: application is nil", bcode.ErrApplicationConfig)
	}
	if c.AppRepo == nil {
		return fmt.Errorf("application repository is not initialized")
	}
	if c.ComponentRepo == nil {
		return fmt.Errorf("component repository is not initialized")
	}

	namespace := serviceNamespaceOrDefault(app.Namespace)
	seenResources := make(map[string]resolvedResourceRef)
	seenComponentLabels := make(map[string]resolvedComponentLabelRef)
	if app.TemplateEnabled {
		return trackResolvedApplicationResources(seenResources, seenComponentLabels, applicationResourceNameKey(app), namespace, app.ID, app.Name, components, true)
	}

	apps, err := c.AppRepo.List(ctx, datastore.ListOptions{})
	if err != nil {
		return err
	}
	for _, existingApp := range apps {
		if existingApp == nil || existingApp.TemplateEnabled || existingApp.ID == app.ID || serviceNamespaceOrDefault(existingApp.Namespace) != namespace {
			continue
		}
		existingComponents, err := c.ComponentRepo.FindByAppID(ctx, existingApp.ID)
		if err != nil {
			return err
		}
		existingRequests, err := convertComponentModelsToCreateRequests(existingComponents)
		if err != nil {
			return err
		}
		if err := trackResolvedApplicationResources(seenResources, nil, applicationResourceNameKey(existingApp), namespace, existingApp.ID, existingApp.Name, existingRequests, false); err != nil {
			return err
		}
	}

	return trackResolvedApplicationResources(seenResources, seenComponentLabels, applicationResourceNameKey(app), namespace, app.ID, app.Name, components, true)
}

func convertComponentModelsToCreateRequests(components []*model.ApplicationComponent) ([]apisv1.CreateComponentRequest, error) {
	requests := make([]apisv1.CreateComponentRequest, 0, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		request, err := convertComponentModelToCreateRequest(component)
		if err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, nil
}

func trackResolvedApplicationResources(
	seenResources map[string]resolvedResourceRef,
	seenComponentLabels map[string]resolvedComponentLabelRef,
	resourceAppName string,
	namespace string,
	appID string,
	appName string,
	components []apisv1.CreateComponentRequest,
	failOnDuplicate bool,
) error {
	namespace = serviceNamespaceOrDefault(namespace)
	for componentIndex, component := range components {
		componentName := strings.TrimSpace(component.Name)
		componentNamespace := namespace
		usesSharedResourceName := createComponentShareEnabled(component)
		duplicateSafeShared := createComponentDuplicateSafeShare(component)
		componentResourceAppName := resourceAppName
		if usesSharedResourceName {
			componentResourceAppName = componentName
		}

		if seenComponentLabels != nil {
			if err := trackComponentLabelValue(seenComponentLabels, resolvedComponentLabelRef{
				componentIndex: componentIndex,
				componentName:  componentName,
				labelValue:     naming.BoundedLabelValue(componentName),
			}); err != nil {
				return err
			}
		}

		track := func(kind config.ResourceKind, name, source string) error {
			ref := resolvedResourceRef{
				kind:                kind,
				namespace:           componentNamespace,
				name:                name,
				duplicateSafeShared: duplicateSafeShared,
				componentIndex:      componentIndex,
				componentName:       componentName,
				source:              source,
				appID:               appID,
				appName:             appName,
			}
			if failOnDuplicate {
				return trackResolvedResource(seenResources, ref)
			}
			seedResolvedResource(seenResources, ref)
			return nil
		}

		switch component.ComponentType {
		case config.ConfJob:
			if err := track(config.ResourceConfigMap, componentName, "component.name"); err != nil {
				return err
			}
		case config.SecretJob:
			if err := track(config.ResourceSecret, componentName, "component.name"); err != nil {
				return err
			}
		case config.ServerJob:
			if err := track(config.ResourceDeployment, naming.WebServiceName(componentName, componentResourceAppName), "generated deployment"); err != nil {
				return err
			}
		case config.StoreJob:
			if err := track(config.ResourceStatefulSet, naming.StoreServerName(componentName, componentResourceAppName), "generated statefulset"); err != nil {
				return err
			}
		case config.InstantJob:
			if err := track(config.ResourceJob, naming.JobName(componentName, componentResourceAppName), "generated job"); err != nil {
				return err
			}
		case config.ScheduledJob:
			if strings.TrimSpace(component.Properties.Schedule) != "" {
				if err := track(config.ResourceCronJob, naming.CronJobName(componentName, componentResourceAppName), "generated cronjob"); err != nil {
					return err
				}
			}
		}

		for _, pvc := range resolvedPVCResourceNames(component) {
			if err := validateResolvedPVCResourceName(pvc); err != nil {
				return err
			}
		}
		for serviceIndex, serviceName := range resolvedServiceResourceNames(component, componentResourceAppName) {
			if err := track(config.ResourceService, serviceName, fmt.Sprintf("traits.service[%d]", serviceIndex)); err != nil {
				return err
			}
		}
		for ingressIndex, ingressName := range resolvedIngressResourceNames(component, componentResourceAppName) {
			if err := track(config.ResourceIngress, ingressName, fmt.Sprintf("traits.ingress[%d]", ingressIndex)); err != nil {
				return err
			}
		}
	}
	return nil
}

func trackResolvedResource(seen map[string]resolvedResourceRef, current resolvedResourceRef) error {
	current.namespace = serviceNamespaceOrDefault(current.namespace)
	current.name = strings.TrimSpace(current.name)
	if current.name == "" {
		return nil
	}
	if validationErrors := traitvalidation.ValidateKubeResourceName(current.name, current.source); len(validationErrors) > 0 {
		return fmt.Errorf("%w: %s", bcode.ErrApplicationConfig, validationErrors[0].Message)
	}
	key := string(current.kind) + "\x00" + current.namespace + "\x00" + current.name
	previous, ok := seen[key]
	if !ok {
		seen[key] = current
		return nil
	}
	if previous.duplicateSafeShared && current.duplicateSafeShared {
		return nil
	}
	resourceNameKind := string(current.kind)
	switch current.kind {
	case config.ResourceService:
		resourceNameKind = "service name"
	case config.ResourcePVC:
		resourceNameKind = "pvc name"
	}
	return fmt.Errorf("%w: component[%d] %q%s %s resolves to duplicate %s %q in namespace %s already used by component[%d] %q%s %s",
		bcode.ErrApplicationConfig,
		current.componentIndex, current.componentName, resourceRefAppSuffix(current), current.source, resourceNameKind, current.name, current.namespace,
		previous.componentIndex, previous.componentName, resourceRefAppSuffix(previous), previous.source)
}

func validateResolvedPVCResourceName(current resolvedPVCResourceName) error {
	name := strings.TrimSpace(current.name)
	if name == "" {
		return nil
	}
	if validationErrors := traitvalidation.ValidateKubeResourceName(name, current.source); len(validationErrors) > 0 {
		return fmt.Errorf("%w: %s", bcode.ErrApplicationConfig, validationErrors[0].Message)
	}
	return nil
}

func seedResolvedResource(seen map[string]resolvedResourceRef, current resolvedResourceRef) {
	current.namespace = serviceNamespaceOrDefault(current.namespace)
	current.name = strings.TrimSpace(current.name)
	if current.name == "" {
		return
	}
	key := string(current.kind) + "\x00" + current.namespace + "\x00" + current.name
	if previous, ok := seen[key]; ok {
		if previous.duplicateSafeShared && !current.duplicateSafeShared {
			seen[key] = current
		}
		return
	}
	seen[key] = current
}

func createComponentShareEnabled(component apisv1.CreateComponentRequest) bool {
	return component.Traits.Share != nil
}

func createComponentDuplicateSafeShare(component apisv1.CreateComponentRequest) bool {
	if component.Traits.Share == nil {
		return false
	}
	strategy, _ := config.NormalizeShareStrategy(component.Traits.Share.Strategy)
	return strategy == config.ShareStrategyDefault || strategy == config.ShareStrategyIgnore
}

func resourceRefAppSuffix(ref resolvedResourceRef) string {
	appName := strings.TrimSpace(ref.appName)
	appID := strings.TrimSpace(ref.appID)
	switch {
	case appName != "" && appID != "":
		return fmt.Sprintf(" in app %q (%s)", appName, appID)
	case appName != "":
		return fmt.Sprintf(" in app %q", appName)
	case appID != "":
		return fmt.Sprintf(" in app %s", appID)
	default:
		return ""
	}
}

func trackComponentLabelValue(seen map[string]resolvedComponentLabelRef, current resolvedComponentLabelRef) error {
	current.labelValue = strings.TrimSpace(current.labelValue)
	if current.labelValue == "" {
		return nil
	}
	previous, ok := seen[current.labelValue]
	if !ok {
		seen[current.labelValue] = current
		return nil
	}
	return fmt.Errorf("%w: component[%d] %q and component[%d] %q resolve to the same pod selector label value %q",
		bcode.ErrApplicationConfig,
		current.componentIndex, current.componentName,
		previous.componentIndex, previous.componentName,
		current.labelValue)
}

func resolvedServiceResourceNames(component apisv1.CreateComponentRequest, resourceAppName string) []string {
	switch component.ComponentType {
	case config.InstantJob, config.ScheduledJob, config.CloudJob:
		return nil
	}
	componentName := strings.TrimSpace(component.Name)
	if len(component.Traits.Service) > 0 {
		names := make([]string, 0, len(component.Traits.Service))
		for _, serviceTrait := range component.Traits.Service {
			serviceName := strings.TrimSpace(serviceTrait.Name)
			if serviceName == "" {
				serviceName = naming.ServiceName(componentName, resourceAppName)
			}
			names = append(names, serviceName)
		}
		return names
	}
	if len(component.Properties.Ports) == 0 {
		return nil
	}
	return []string{naming.ServiceName(componentName, resourceAppName)}
}

func resolvedPVCResourceNames(component apisv1.CreateComponentRequest) []resolvedPVCResourceName {
	var candidates []resolvedPVCResourceName
	collectStandalonePVCResourceNames(&candidates, component.Traits.Storage, "traits.storage")
	for initIndex, init := range component.Traits.Init {
		collectStandalonePVCResourceNames(&candidates, init.Traits.Storage, fmt.Sprintf("traits.init[%d].traits.storage", initIndex))
	}
	for sidecarIndex, sidecar := range component.Traits.Sidecar {
		collectStandalonePVCResourceNames(&candidates, sidecar.Traits.Storage, fmt.Sprintf("traits.sidecar[%d].traits.storage", sidecarIndex))
	}

	names := make([]resolvedPVCResourceName, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		name := strings.TrimSpace(candidate.name)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		candidate.name = name
		names = append(names, candidate)
	}
	return names
}

func collectStandalonePVCResourceNames(out *[]resolvedPVCResourceName, storages []spec.StorageTraitSpec, sourcePrefix string) {
	processedVolumes := make(map[string]struct{}, len(storages))
	for storageIndex, storage := range storages {
		if config.StorageTypeMapping[storage.Type] != config.VolumeTypePVC || storage.TmpCreate {
			continue
		}
		volumeName := utils.NormalizeLowerStrip(storage.Name)
		if volumeName == "" {
			continue
		}
		if _, exists := processedVolumes[volumeName]; exists {
			continue
		}
		processedVolumes[volumeName] = struct{}{}

		claimName := strings.TrimSpace(storage.ClaimName)
		if claimName == "" {
			claimName = strings.TrimSpace(storage.Name)
		}
		if claimName == "" {
			continue
		}
		*out = append(*out, resolvedPVCResourceName{
			name:   claimName,
			source: fmt.Sprintf("%s[%d]", sourcePrefix, storageIndex),
		})
	}
}

func resolvedIngressResourceNames(component apisv1.CreateComponentRequest, resourceAppName string) []string {
	if len(component.Traits.Ingress) == 0 {
		return nil
	}
	componentName := strings.TrimSpace(component.Name)
	names := make([]string, 0, len(component.Traits.Ingress))
	for i, ingressTrait := range component.Traits.Ingress {
		ingressName := strings.TrimSpace(ingressTrait.Name)
		if ingressName == "" {
			suffix := "ingress"
			if i > 0 {
				suffix = fmt.Sprintf("ingress-%d", i+1)
			}
			ingressName = fmt.Sprintf("%s-%s", strings.ToLower(componentName), suffix)
		}
		names = append(names, naming.IngressName(ingressName, resourceAppName))
	}
	return names
}

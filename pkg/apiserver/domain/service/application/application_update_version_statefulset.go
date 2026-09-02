package application

import (
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowtraits "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func preflightVersionUpdateStatefulSets(componentMap map[string]*model.ApplicationComponent, specs []apisv1.ComponentUpdateSpec, fullRecreate bool) ([]apisv1.ComponentUpdateSpec, error) {
	normalized, err := preserveVersionUpdatePVCIdentities(componentMap, specs, fullRecreate)
	if err != nil {
		return nil, err
	}
	if err := validateVersionUpdateStatefulSetImmutableFields(componentMap, normalized, fullRecreate); err != nil {
		return nil, err
	}
	return normalized, nil
}

func validateVersionUpdateStatefulSetImmutableFields(componentMap map[string]*model.ApplicationComponent, specs []apisv1.ComponentUpdateSpec, fullRecreate bool) error {
	for _, update := range specs {
		action, err := parseVersionUpdateComponentAction(update)
		if err != nil {
			return err
		}
		if action != config.ComponentActionUpdate {
			continue
		}
		component := componentMap[strings.ToLower(strings.TrimSpace(update.Name))]
		if component == nil || component.ComponentType != config.StoreJob {
			continue
		}
		currentStatefulSet, desiredStatefulSet, err := renderVersionUpdateStatefulSetTransition(component, update)
		if err != nil {
			return err
		}
		if versionUpdateRecreatesStatefulSet(component, fullRecreate) {
			if err := validateStatefulSetIdentityTransition(component.Name, currentStatefulSet, desiredStatefulSet); err != nil {
				return err
			}
			continue
		}
		if err := validateStatefulSetImmutableTransition(component.Name, currentStatefulSet, desiredStatefulSet); err != nil {
			return err
		}
	}
	return nil
}

func renderVersionUpdateStatefulSetTransition(component *model.ApplicationComponent, update apisv1.ComponentUpdateSpec) (*appsv1.StatefulSet, *appsv1.StatefulSet, error) {
	currentRequest, err := convertComponentModelToCreateRequest(component)
	if err != nil {
		return nil, nil, err
	}
	desiredRequest, err := applyComponentUpdateSpecToResolvedComponent(currentRequest, update)
	if err != nil {
		return nil, nil, err
	}
	desiredComponent, err := versionUpdateComponentSnapshot(component, desiredRequest)
	if err != nil {
		return nil, nil, err
	}

	currentStatefulSet, err := renderVersionUpdateStatefulSet(component)
	if err != nil {
		return nil, nil, err
	}
	desiredStatefulSet, err := renderVersionUpdateStatefulSet(desiredComponent)
	if err != nil {
		return nil, nil, err
	}
	return currentStatefulSet, desiredStatefulSet, nil
}

func versionUpdateStatefulSetPVCTemplatesToDelete(component *model.ApplicationComponent, update apisv1.ComponentUpdateSpec) ([]string, error) {
	current, desired, err := renderVersionUpdateStatefulSetTransition(component, update)
	if err != nil {
		return nil, err
	}
	if err := validateStatefulSetIdentityTransition(component.Name, current, desired); err != nil {
		return nil, err
	}
	return statefulSetPVCTemplatesToDelete(current, desired), nil
}

func versionUpdateStatefulSetRequiresDeletion(component *model.ApplicationComponent, update apisv1.ComponentUpdateSpec) (bool, error) {
	current, desired, err := renderVersionUpdateStatefulSetTransition(component, update)
	if err != nil {
		return false, err
	}
	if err := validateStatefulSetIdentityTransition(component.Name, current, desired); err != nil {
		return false, err
	}
	if current == nil || desired == nil {
		return false, nil
	}
	return current.Spec.ServiceName != desired.Spec.ServiceName ||
		!apiequality.Semantic.DeepEqual(current.Spec.Selector, desired.Spec.Selector) ||
		!apiequality.Semantic.DeepEqual(current.Spec.VolumeClaimTemplates, desired.Spec.VolumeClaimTemplates), nil
}

func statefulSetPVCTemplatesToDelete(current, desired *appsv1.StatefulSet) []string {
	if current == nil || desired == nil {
		return nil
	}
	currentByName := make(map[string]corev1.PersistentVolumeClaim, len(current.Spec.VolumeClaimTemplates))
	for _, pvc := range current.Spec.VolumeClaimTemplates {
		name := strings.TrimSpace(pvc.Name)
		if name != "" {
			currentByName[name] = pvc
		}
	}
	desiredByName := make(map[string]corev1.PersistentVolumeClaim, len(desired.Spec.VolumeClaimTemplates))
	for _, pvc := range desired.Spec.VolumeClaimTemplates {
		name := strings.TrimSpace(pvc.Name)
		if name != "" {
			desiredByName[name] = pvc
		}
	}

	changed := make(map[string]struct{})
	for name, currentPVC := range currentByName {
		desiredPVC, exists := desiredByName[name]
		if !exists || !apiequality.Semantic.DeepEqual(currentPVC.Spec, desiredPVC.Spec) {
			changed[name] = struct{}{}
		}
	}
	for name, desiredPVC := range desiredByName {
		currentPVC, exists := currentByName[name]
		if !exists || !apiequality.Semantic.DeepEqual(currentPVC.Spec, desiredPVC.Spec) {
			changed[name] = struct{}{}
		}
	}
	if len(changed) == 0 {
		return nil
	}
	names := make([]string, 0, len(changed))
	for name := range changed {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func versionUpdateRecreatesStatefulSet(component *model.ApplicationComponent, fullRecreate bool) bool {
	if !fullRecreate || component == nil || component.ComponentType != config.StoreJob {
		return false
	}
	_, shared := SharedLifecycleStrategyForComponent(component)
	return !shared
}

func versionUpdateComponentSnapshot(current *model.ApplicationComponent, desired apisv1.CreateComponentRequest) (*model.ApplicationComponent, error) {
	properties, err := model.NewJSONStructByStruct(desired.Properties)
	if err != nil {
		return nil, fmt.Errorf("marshal desired StatefulSet properties: %w", err)
	}
	traits, err := model.NewJSONStructByStruct(desired.Traits)
	if err != nil {
		return nil, fmt.Errorf("marshal desired StatefulSet traits: %w", err)
	}
	snapshot := *current
	snapshot.Image = desired.Image
	snapshot.Replicas = desired.Replicas
	snapshot.Properties = properties
	snapshot.Traits = traits
	return &snapshot, nil
}

func renderVersionUpdateStatefulSet(component *model.ApplicationComponent) (*appsv1.StatefulSet, error) {
	workflowtraits.RegisterAllProcessors()
	snapshot := *component
	result := workflowjob.GenerateStoreService(&snapshot)
	if result == nil {
		return nil, fmt.Errorf("%w: component %s cannot render desired StatefulSet", bcode.ErrApplicationConfig, component.Name)
	}
	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	if !ok || statefulSet == nil {
		return nil, fmt.Errorf("%w: component %s rendered unexpected StatefulSet type %T", bcode.ErrApplicationConfig, component.Name, result.Service)
	}
	return statefulSet, nil
}

func validateStatefulSetImmutableTransition(componentName string, current, desired *appsv1.StatefulSet) error {
	if current == nil || desired == nil {
		return nil
	}
	if err := validateStatefulSetIdentityTransition(componentName, current, desired); err != nil {
		return err
	}
	fail := func(field string, before, after any) error {
		internalErr := fmt.Errorf("%w: component %s changes StatefulSet immutable field %s from %v to %v; explicit StatefulSet/PVC migration or recreation is required",
			bcode.ErrApplicationConfig, componentName, field, before, after)
		return bcode.WithSafeClientMessage(internalErr, fmt.Sprintf(
			"component %s changes StatefulSet immutable field %s; explicit StatefulSet/PVC migration or recreation is required",
			componentName, field))
	}
	if current.Spec.ServiceName != desired.Spec.ServiceName {
		return fail("serviceName", current.Spec.ServiceName, desired.Spec.ServiceName)
	}
	if !apiequality.Semantic.DeepEqual(current.Spec.Selector, desired.Spec.Selector) {
		return fail("selector", current.Spec.Selector, desired.Spec.Selector)
	}
	if len(current.Spec.VolumeClaimTemplates) != len(desired.Spec.VolumeClaimTemplates) {
		return fail("volumeClaimTemplates", len(current.Spec.VolumeClaimTemplates), len(desired.Spec.VolumeClaimTemplates))
	}
	for index := range current.Spec.VolumeClaimTemplates {
		currentPVC := current.Spec.VolumeClaimTemplates[index]
		desiredPVC := desired.Spec.VolumeClaimTemplates[index]
		fieldPrefix := fmt.Sprintf("volumeClaimTemplates[%d]", index)
		if currentPVC.Name != desiredPVC.Name {
			return fail(fieldPrefix+".name", currentPVC.Name, desiredPVC.Name)
		}
		if !apiequality.Semantic.DeepEqual(currentPVC.Spec.StorageClassName, desiredPVC.Spec.StorageClassName) {
			return fail(fieldPrefix+".storageClass", storageClassName(currentPVC), storageClassName(desiredPVC))
		}
		currentSize := currentPVC.Spec.Resources.Requests[corev1.ResourceStorage]
		desiredSize := desiredPVC.Spec.Resources.Requests[corev1.ResourceStorage]
		if !currentSize.Equal(desiredSize) {
			return fail(fieldPrefix+".size", currentSize.String(), desiredSize.String())
		}
		if !apiequality.Semantic.DeepEqual(currentPVC.Spec, desiredPVC.Spec) {
			return fail(fieldPrefix+".spec", currentPVC.Spec, desiredPVC.Spec)
		}
	}
	return nil
}

func validateStatefulSetIdentityTransition(componentName string, current, desired *appsv1.StatefulSet) error {
	if current == nil || desired == nil {
		return nil
	}
	currentNamespace := strings.TrimSpace(current.Namespace)
	if currentNamespace == "" {
		currentNamespace = config.DefaultNamespace
	}
	desiredNamespace := strings.TrimSpace(desired.Namespace)
	if desiredNamespace == "" {
		desiredNamespace = config.DefaultNamespace
	}
	currentName := strings.TrimSpace(current.Name)
	desiredName := strings.TrimSpace(desired.Name)
	if currentNamespace == desiredNamespace && currentName == desiredName {
		return nil
	}
	internalErr := fmt.Errorf(
		"%w: component %s changes StatefulSet identity from %s/%s to %s/%s; StatefulSet identity migration is not supported by version update",
		bcode.ErrApplicationConfig,
		componentName,
		currentNamespace,
		currentName,
		desiredNamespace,
		desiredName,
	)
	return bcode.WithSafeClientMessage(internalErr, fmt.Sprintf(
		"component %s changes StatefulSet identity; migrate the StatefulSet/PVC separately before updating the version",
		componentName,
	))
}

func storageClassName(pvc corev1.PersistentVolumeClaim) string {
	if pvc.Spec.StorageClassName == nil {
		return ""
	}
	return *pvc.Spec.StorageClassName
}

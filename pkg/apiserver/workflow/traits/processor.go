package traits

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

// TraitContext provides the inputs a Processor needs to render its changes.
// It is read-only with respect to the source component and workload; mutations
// must be returned through TraitResult and applied by the framework.
type TraitContext struct {
	Component *model.ApplicationComponent
	Workload  runtime.Object
	TraitData interface{}
}

// TraitResult is the unit of changes emitted by a Processor. The framework
// aggregates multiple results and applies them onto the target workload.
type TraitResult struct {
	// Pod-level modifications
	InitContainers    []corev1.Container
	Containers        []corev1.Container
	Volumes           []corev1.Volume
	NodeSelector      map[string]string
	VolumeMounts      map[string][]corev1.VolumeMount   // Keyed by container name
	EnvVars           map[string][]corev1.EnvVar        // Keyed by container name
	EnvFromSources    map[string][]corev1.EnvFromSource // Keyed by container name
	AdditionalObjects []client.Object

	// Service account binding
	ServiceAccountName           string
	AutomountServiceAccountToken *bool

	// Container-level modifications
	LivenessProbe        *corev1.Probe
	ReadinessProbe       *corev1.Probe
	StartupProbe         *corev1.Probe
	ResourceRequirements *corev1.ResourceRequirements
	SecurityContext      *corev1.SecurityContext

	// Workload-level modifications
	DeploymentStrategy        *appsv1.DeploymentStrategy
	StatefulSetUpdateStrategy *appsv1.StatefulSetUpdateStrategy
}

// TraitProcessor defines a pluggable transformer that consumes a trait's data
// and emits a TraitResult. Implementations must be stateless and side-effect free.
type TraitProcessor interface {
	Name() string
	Process(ctx *TraitContext) (*TraitResult, error)
}

// NewTraitContext creates a new trait context.
func NewTraitContext(component *model.ApplicationComponent, workload runtime.Object, traitData interface{}) *TraitContext {
	return &TraitContext{
		Component: component,
		Workload:  workload,
		TraitData: traitData,
	}
}

var (
	// registeredTraitProcessors stores the registered trait processors in the desired execution order.
	registeredTraitProcessors []TraitProcessor
	registeredTraitNames      = map[string]struct{}{}
	registeredTraitMu         sync.Mutex
	registerAllProcessorsOnce sync.Once
)

// Register adds a Processor to the global ordered registry.
// Duplicate names are ignored to keep registration idempotent.
func Register(p TraitProcessor) {
	if p == nil {
		klog.Warningf("skip registering nil trait processor")
		return
	}
	name := p.Name()
	if strings.TrimSpace(name) == "" {
		klog.Warningf("skip registering unnamed trait processor: %T", p)
		return
	}
	registeredTraitMu.Lock()
	defer registeredTraitMu.Unlock()
	if _, exists := registeredTraitNames[name]; exists {
		klog.Warningf("trait processor already registered, skip duplicate: %s", name)
		return
	}
	klog.V(4).Infof("Registering trait processor: %s", name)
	registeredTraitProcessors = append(registeredTraitProcessors, p)
	registeredTraitNames[name] = struct{}{}
}

// RegisterAllProcessors defines the execution order for all built-in processors.
func RegisterAllProcessors() {
	registerAllProcessorsOnce.Do(registerBuiltinProcessors)
}

func registerBuiltinProcessors() {
	// 1. Register traits that define core resources first.
	Register(&StorageProcessor{})
	Register(&EnvFromProcessor{})
	Register(&EnvsProcessor{})
	Register(&TargetWorkEnvProcessor{})
	Register(&ResourcesProcessor{})
	Register(&SecurityPolicyProcessor{})
	Register(&ProbeProcessor{}) // Added ProbeProcessor
	Register(&RBACProcessor{})
	Register(&RolloutProcessor{})

	// 2. Register traits that add containers or recursively process other traits.
	Register(&InitProcessor{})
	Register(&SidecarProcessor{})
	// Register other processors here as they are added.
	Register(&IngressProcessor{})
}

// ResetTraitProcessorsForTest clears trait processor registry globals.
// Intended for tests and controlled re-initialization scenarios.
func ResetTraitProcessorsForTest() {
	registeredTraitMu.Lock()
	defer registeredTraitMu.Unlock()
	registeredTraitProcessors = []TraitProcessor{}
	registeredTraitNames = map[string]struct{}{}
	registerAllProcessorsOnce = sync.Once{}
}

// ApplyTraits is the public entrypoint. It dispatches traits to processors,
// aggregates their outputs, and applies changes onto the workload.
func ApplyTraits(component *model.ApplicationComponent, workload runtime.Object) ([]client.Object, error) {
	if component.Traits == nil {
		klog.V(4).Infof("Component %s has no traits to apply.", component.Name)
		return nil, nil
	}

	traitBytes, err := json.Marshal(component.Traits)
	if err != nil {
		return nil, fmt.Errorf("failed to re-marshal traits for component %s: %w", component.Name, err)
	}

	if string(traitBytes) == "{}" || string(traitBytes) == "null" {
		return nil, nil
	}

	var traits spec.Traits
	if err := json.Unmarshal(traitBytes, &traits); err != nil {
		return nil, fmt.Errorf("failed to unmarshal traits into concrete type for component %s: %w", component.Name, err)
	}

	// Start the recursive application of traits, with no exclusions at the top level.
	finalResult, err := applyTraitsRecursive(component, workload, &traits, nil)
	if err != nil {
		return nil, err
	}

	// Apply the aggregated result to the final workload.
	// Use normalized container name to match the actual container name in the workload.
	mainContainerName := utils.NormalizeLowerStrip(component.Name)
	if err := applyTraitResultToWorkload(finalResult, workload, mainContainerName); err != nil {
		return nil, err
	}

	klog.V(2).Infof("Successfully applied traits for component: %s", component.Name)
	return finalResult.AdditionalObjects, nil
}

// applyTraitsRecursive evaluates all traits found in spec.Traits and collects
// TraitResults. It supports recursion for nested traits and uses an exclusion
// list to prevent infinite loops (e.g., sidecar within sidecar).
func applyTraitsRecursive(component *model.ApplicationComponent, workload runtime.Object, traits *spec.Traits, excludeTraits []string) (*TraitResult, error) {
	traitsVal := reflect.ValueOf(traits).Elem()
	var allResults []*TraitResult

	// TmpCreate a map for quick lookup of excluded traits.
	excludeMap := make(map[string]bool)
	for _, t := range excludeTraits {
		excludeMap[t] = true
	}

	// Process each trait and collect results.
	for _, p := range registeredTraitProcessors {
		traitName := p.Name()
		if excludeMap[traitName] {
			continue // Skip excluded traits.
		}

		field, ok := traitFieldByName(traitsVal, traitName)
		if !ok || !field.IsValid() {
			continue
		}

		// Handle both slice and pointer types
		var shouldProcess bool
		var traitData interface{}

		switch field.Kind() {
		case reflect.Slice:
			if field.IsNil() {
				continue
			}
			if field.Len() > 0 {
				shouldProcess = true
				traitData = field.Interface()
			}
		case reflect.Ptr:
			if field.IsNil() {
				continue
			}
			shouldProcess = true
			traitData = field.Interface()
		case reflect.String:
			if strings.TrimSpace(field.String()) == "" {
				continue
			}
			shouldProcess = true
			traitData = field.String()
		case reflect.Map:
			if field.IsNil() || field.Len() == 0 {
				continue
			}
			shouldProcess = true
			traitData = field.Interface()
		default:
			continue
		}

		if shouldProcess {
			klog.V(3).Infof("Applying trait '%s' for component %s.", traitName, component.Name)
			ctx := NewTraitContext(component, workload, traitData)
			result, err := p.Process(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to process trait '%s': %w", traitName, err)
			}
			if result != nil {
				allResults = append(allResults, result)
			}
		}
	}

	// Merge all results from this level of recursion into a single result.
	return aggregateTraitResults(allResults)
}

// aggregateTraitResults merges multiple TraitResults into one, de-duplicating
// volumes/objects and concatenating per-container mounts/envs. For singleton
// fields (probes/resources) the last non-nil value wins by design.
func aggregateTraitResults(results []*TraitResult) (*TraitResult, error) {
	finalResult := &TraitResult{
		VolumeMounts:   make(map[string][]corev1.VolumeMount),
		EnvVars:        make(map[string][]corev1.EnvVar),
		EnvFromSources: make(map[string][]corev1.EnvFromSource),
	}
	// Use maps to track the names of added volumes and objects to prevent duplicates.
	volumeNameSet := make(map[string]bool)
	objectsByIdentity := make(map[string]client.Object)
	volumeMountSet := make(map[string]map[string]bool) // containerName -> mountPath -> exists

	for _, traitResult := range results {
		finalResult.InitContainers = append(finalResult.InitContainers, traitResult.InitContainers...)
		finalResult.Containers = append(finalResult.Containers, traitResult.Containers...)

		// De-duplicate Volumes
		for _, vol := range traitResult.Volumes {
			if !volumeNameSet[vol.Name] {
				finalResult.Volumes = append(finalResult.Volumes, vol)
				volumeNameSet[vol.Name] = true
			}
		}

		// De-duplicate AdditionalObjects
		for _, obj := range traitResult.AdditionalObjects {
			if obj == nil {
				return nil, fmt.Errorf("additional object is nil")
			}
			gvk, err := apiutil.GVKForObject(obj, kubernetesscheme.Scheme)
			if err != nil {
				return nil, fmt.Errorf("resolve additional object %T group kind: %w", obj, err)
			}
			key := fmt.Sprintf("%s/%s/%s", gvk.GroupKind().String(), obj.GetNamespace(), obj.GetName())
			existing, found := objectsByIdentity[key]
			if !found {
				finalResult.AdditionalObjects = append(finalResult.AdditionalObjects, obj)
				objectsByIdentity[key] = obj
				continue
			}
			if !apiequality.Semantic.DeepEqual(existing, obj) {
				return nil, fmt.Errorf("conflicting additional object %s", key)
			}
		}

		// Merge and de-duplicate VolumeMounts by container name and mount path.
		for containerName, mounts := range traitResult.VolumeMounts {
			if _, ok := volumeMountSet[containerName]; !ok {
				volumeMountSet[containerName] = make(map[string]bool)
			}
			for _, mount := range mounts {
				if !volumeMountSet[containerName][mount.MountPath] {
					finalResult.VolumeMounts[containerName] = append(finalResult.VolumeMounts[containerName], mount)
					volumeMountSet[containerName][mount.MountPath] = true
				}
			}
		}

		// Merge EnvVars by container name.
		for containerName, envs := range traitResult.EnvVars {
			finalResult.EnvVars[containerName] = append(finalResult.EnvVars[containerName], envs...)
		}

		// Merge EnvFromSources by container name.
		for containerName, envs := range traitResult.EnvFromSources {
			finalResult.EnvFromSources[containerName] = append(finalResult.EnvFromSources[containerName], envs...)
		}

		if len(traitResult.NodeSelector) > 0 {
			if finalResult.NodeSelector == nil {
				finalResult.NodeSelector = make(map[string]string, len(traitResult.NodeSelector))
			}
			for key, value := range traitResult.NodeSelector {
				finalResult.NodeSelector[key] = value
			}
		}

		// Merge Probes (last one wins)
		if traitResult.LivenessProbe != nil {
			finalResult.LivenessProbe = traitResult.LivenessProbe
		}
		if traitResult.ReadinessProbe != nil {
			finalResult.ReadinessProbe = traitResult.ReadinessProbe
		}
		if traitResult.StartupProbe != nil {
			finalResult.StartupProbe = traitResult.StartupProbe
		}

		// Merge ResourceRequirements (last one wins)
		if traitResult.ResourceRequirements != nil {
			finalResult.ResourceRequirements = traitResult.ResourceRequirements
		}
		if traitResult.SecurityContext != nil {
			finalResult.SecurityContext = traitResult.SecurityContext
		}
		if traitResult.DeploymentStrategy != nil {
			finalResult.DeploymentStrategy = traitResult.DeploymentStrategy
		}
		if traitResult.StatefulSetUpdateStrategy != nil {
			finalResult.StatefulSetUpdateStrategy = traitResult.StatefulSetUpdateStrategy
		}

		if traitResult.ServiceAccountName != "" {
			finalResult.ServiceAccountName = traitResult.ServiceAccountName
		}
		if traitResult.AutomountServiceAccountToken != nil {
			finalResult.AutomountServiceAccountToken = traitResult.AutomountServiceAccountToken
		}
	}
	return finalResult, nil
}

func traitFieldByName(traitsVal reflect.Value, traitName string) (reflect.Value, bool) {
	t := traitsVal.Type()
	lower := strings.ToLower(traitName)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if strings.ToLower(field.Name) == lower {
			return traitsVal.Field(i), true
		}
		if tag := field.Tag.Get("json"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				tag = tag[:idx]
			}
			if tag == traitName {
				return traitsVal.Field(i), true
			}
		}
	}
	return reflect.Value{}, false
}

// applyTraitResultToWorkload mutates the provided workload's PodTemplateSpec by
// appending containers/volumes and wiring mounts/envs to the correct targets.
// For StatefulSets, dynamically requested PVCs are moved into VolumeClaimTemplates.
func applyTraitResultToWorkload(result *TraitResult, workload runtime.Object, mainContainerName string) error {
	if err := applyWorkloadTraitResult(result, workload); err != nil {
		return err
	}

	podTemplate, err := getPodTemplateFromWorkload(workload)
	if err != nil {
		return err
	}

	podTemplate.Spec.InitContainers = append(podTemplate.Spec.InitContainers, result.InitContainers...)
	podTemplate.Spec.Containers = append(podTemplate.Spec.Containers, result.Containers...)
	podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes, result.Volumes...)
	if len(result.NodeSelector) > 0 {
		if podTemplate.Spec.NodeSelector == nil {
			podTemplate.Spec.NodeSelector = make(map[string]string, len(result.NodeSelector))
		}
		for key, value := range result.NodeSelector {
			podTemplate.Spec.NodeSelector[key] = value
		}
	}

	if result.ServiceAccountName != "" {
		if podTemplate.Spec.ServiceAccountName == "" {
			podTemplate.Spec.ServiceAccountName = result.ServiceAccountName
		} else if podTemplate.Spec.ServiceAccountName != result.ServiceAccountName {
			klog.Warningf("Trait attempted to set serviceAccountName=%s but workload already specifies %s; keeping existing value", result.ServiceAccountName, podTemplate.Spec.ServiceAccountName)
		}
	}
	if result.AutomountServiceAccountToken != nil {
		if podTemplate.Spec.AutomountServiceAccountToken == nil {
			podTemplate.Spec.AutomountServiceAccountToken = result.AutomountServiceAccountToken
		} else if *podTemplate.Spec.AutomountServiceAccountToken != *result.AutomountServiceAccountToken {
			klog.Warningf("Trait attempted to set automountServiceAccountToken=%t but workload already specifies %t; keeping existing value", *result.AutomountServiceAccountToken, *podTemplate.Spec.AutomountServiceAccountToken)
		}
	}

	// TmpCreate a map of all containers (main, init, sidecar) for easy lookup.
	containerMap := make(map[string]*corev1.Container)
	for i := range podTemplate.Spec.Containers {
		containerMap[podTemplate.Spec.Containers[i].Name] = &podTemplate.Spec.Containers[i]
	}
	for i := range podTemplate.Spec.InitContainers {
		containerMap[podTemplate.Spec.InitContainers[i].Name] = &podTemplate.Spec.InitContainers[i]
	}

	// Apply Probes to the main container
	mainContainer, ok := containerMap[mainContainerName]
	if !ok {
		return fmt.Errorf("main container %s not found in workload", mainContainerName)
	}
	// Apply Resources to the main container
	if result.ResourceRequirements != nil {
		mainContainer.Resources = *result.ResourceRequirements
	}
	if result.SecurityContext != nil {
		mainContainer.SecurityContext = result.SecurityContext
	}
	if result.LivenessProbe != nil {
		mainContainer.LivenessProbe = result.LivenessProbe
	}
	if result.ReadinessProbe != nil {
		mainContainer.ReadinessProbe = result.ReadinessProbe
	}
	if result.StartupProbe != nil {
		mainContainer.StartupProbe = result.StartupProbe
	}

	// Apply VolumeMounts to the correct containers.
	for containerName, mounts := range result.VolumeMounts {
		if container, ok := containerMap[containerName]; ok {
			container.VolumeMounts = append(container.VolumeMounts, mounts...)
		} else {
			klog.Warningf("Could not find container '%s' to apply volume mounts.", containerName)
		}
	}

	// Apply EnvVars to the correct containers.
	for containerName, envs := range result.EnvVars {
		if container, ok := containerMap[containerName]; ok {
			container.Env = append(container.Env, envs...)
		} else {
			klog.Warningf("Could not find container '%s' to apply env vars.", containerName)
		}
	}

	// Apply EnvFromSources to the correct containers.
	for containerName, envs := range result.EnvFromSources {
		if container, ok := containerMap[containerName]; ok {
			container.EnvFrom = append(container.EnvFrom, envs...)
		} else {
			klog.Warningf("Could not find container '%s' to apply env from sources.", containerName)
		}
	}

	// Handle AdditionalObjects, with special logic for PVCs.
	var remainingObjects []client.Object
	// Track PVC names that are converted to volumeClaimTemplates (for StatefulSets)
	// These should be removed from podTemplate.Spec.Volumes since volumeClaimTemplates
	// automatically create volumes with the template name.
	pvcTemplateNames := make(map[string]bool)

	for _, obj := range result.AdditionalObjects {
		// Check if the object is a PVC.
		pvc, isPvc := obj.(*corev1.PersistentVolumeClaim)
		if !isPvc {
			// Not a PVC, so we keep it.
			remainingObjects = append(remainingObjects, obj)
			continue
		}

		// Check if the PVC has the template annotation.
		anno := pvc.GetAnnotations()
		if anno != nil && anno[config.LabelStorageRole] == "template" {
			// This is a PVC template. It should be applied to a StatefulSet.
			if sts, isSts := workload.(*appsv1.StatefulSet); isSts {
				// It's a template for our StatefulSet. Add it to the templates list.
				// The namespace MUST be removed from the template's metadata.
				pvc.Namespace = ""
				sts.Spec.VolumeClaimTemplates = append(sts.Spec.VolumeClaimTemplates, *pvc)
				// Track this PVC name so we can remove the corresponding volume entry
				pvcTemplateNames[pvc.Name] = true
				// The template is now part of the StatefulSet, so we don't keep it as a standalone object.
			} else {
				// This is an error case: a template was requested for a non-StatefulSet workload.
				klog.Warningf("Component %s requested a PVC template for a workload of type %T, which is not supported. The PVC will be ignored.", mainContainerName, workload)
			}
		} else {
			// This is a regular, standalone PVC. We keep it.
			remainingObjects = append(remainingObjects, obj)
		}
	}
	// The list of additional objects now only contains the standalone objects.
	result.AdditionalObjects = remainingObjects

	// For StatefulSets with volumeClaimTemplates, remove the corresponding volumes from
	// podTemplate.Spec.Volumes. StatefulSets automatically create volumes from volumeClaimTemplates,
	// so explicit volume entries referencing the PVC would be incorrect.
	if len(pvcTemplateNames) > 0 {
		var filteredVolumes []corev1.Volume
		for _, vol := range podTemplate.Spec.Volumes {
			// Check if this volume references a PVC that was converted to a volumeClaimTemplate
			if vol.PersistentVolumeClaim != nil && pvcTemplateNames[vol.PersistentVolumeClaim.ClaimName] {
				klog.V(3).Infof("Removing volume %q from pod spec as it's now a volumeClaimTemplate", vol.Name)
				continue
			}
			filteredVolumes = append(filteredVolumes, vol)
		}
		podTemplate.Spec.Volumes = filteredVolumes
	}

	return nil
}

func applyWorkloadTraitResult(result *TraitResult, workload runtime.Object) error {
	if result == nil {
		return nil
	}
	if result.DeploymentStrategy != nil {
		deploy, ok := workload.(*appsv1.Deployment)
		if !ok {
			return fmt.Errorf("deployment rollout result cannot be applied to workload type %T", workload)
		}
		deploy.Spec.Strategy = *result.DeploymentStrategy
	}
	if result.StatefulSetUpdateStrategy != nil {
		statefulSet, ok := workload.(*appsv1.StatefulSet)
		if !ok {
			return fmt.Errorf("statefulset rollout result cannot be applied to workload type %T", workload)
		}
		statefulSet.Spec.UpdateStrategy = *result.StatefulSetUpdateStrategy
	}
	return nil
}

// getPodTemplateFromWorkload extracts the PodTemplateSpec from a supported workload.
func getPodTemplateFromWorkload(workload runtime.Object) (*corev1.PodTemplateSpec, error) {
	switch w := workload.(type) {
	case *appsv1.Deployment:
		return &w.Spec.Template, nil
	case *appsv1.StatefulSet:
		return &w.Spec.Template, nil
	case *appsv1.DaemonSet:
		return &w.Spec.Template, nil
	case *batchv1.Job:
		return &w.Spec.Template, nil
	case *batchv1.CronJob:
		return &w.Spec.JobTemplate.Spec.Template, nil
	default:
		return nil, fmt.Errorf("unsupported workload type: %T", workload)
	}
}

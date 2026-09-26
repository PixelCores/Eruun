package traits

import (
	"encoding/json"
	"fmt"

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
	finalResult, err := applyTraitsRecursive(component, workload, &traits, false)
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

// applyTraitsRecursive keeps the built-in order explicit and passes typed specs.
// Evaluation, Service, and Share are owned by the workflow/job builders.
// Nested containers exclude init, sidecar, targetWorkEnv, and rollout.
func applyTraitsRecursive(component *model.ApplicationComponent, workload runtime.Object, traits *spec.Traits, nested bool) (*TraitResult, error) {
	ctx := &TraitContext{Component: component, Workload: workload}
	var results []*TraitResult
	collect := func(result *TraitResult, err error) error {
		if err != nil {
			return err
		}
		if result != nil {
			results = append(results, result)
		}
		return nil
	}
	if len(traits.Storage) > 0 {
		if err := collect((&StorageProcessor{}).Process(ctx, traits.Storage)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'storage': %w", err)
		}
	}
	if len(traits.EnvFrom) > 0 {
		if err := collect((&EnvFromProcessor{}).Process(ctx, traits.EnvFrom)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'envFrom': %w", err)
		}
	}
	if len(traits.Envs) > 0 {
		if err := collect((&EnvsProcessor{}).Process(ctx, traits.Envs)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'envs': %w", err)
		}
	}
	if !nested && len(traits.TargetWorkEnv) > 0 {
		if err := collect((&TargetWorkEnvProcessor{}).Process(traits.TargetWorkEnv)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'targetWorkEnv': %w", err)
		}
	}
	if traits.Resources != nil {
		if err := collect((&ResourcesProcessor{}).Process(traits.Resources)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'resources': %w", err)
		}
	}
	if traits.SecurityPolicy != nil {
		if err := collect((&SecurityPolicyProcessor{}).Process(traits.SecurityPolicy)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'securityPolicy': %w", err)
		}
	}
	if len(traits.Probes) > 0 {
		if err := collect((&ProbeProcessor{}).Process(ctx, traits.Probes)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'probes': %w", err)
		}
	}
	if len(traits.RBAC) > 0 {
		if err := collect((&RBACProcessor{}).Process(ctx, traits.RBAC)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'rbac': %w", err)
		}
	}
	if !nested && traits.Rollout != nil {
		if err := collect((&RolloutProcessor{}).Process(ctx, traits.Rollout)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'rollout': %w", err)
		}
	}
	if !nested && len(traits.Init) > 0 {
		if err := collect((&InitProcessor{}).Process(ctx, traits.Init)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'init': %w", err)
		}
	}
	if !nested && len(traits.Sidecar) > 0 {
		if err := collect((&SidecarProcessor{}).Process(ctx, traits.Sidecar)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'sidecar': %w", err)
		}
	}
	if len(traits.Ingress) > 0 {
		if err := collect((&IngressProcessor{}).Process(ctx, traits.Ingress)); err != nil {
			return nil, fmt.Errorf("failed to process trait 'ingress': %w", err)
		}
	}
	return aggregateTraitResults(results)
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

// applyTraitResultToWorkload mutates the provided workload's PodTemplateSpec by
// appending containers/volumes and wiring mounts/envs to the correct targets.
// For StatefulSets, dynamically requested PVCs are moved into VolumeClaimTemplates.
func applyTraitResultToWorkload(result *TraitResult, workload runtime.Object, mainContainerName string) error {
	podTemplate, err := getPodTemplateFromWorkload(workload)
	if err != nil {
		return err
	}
	// Container names share one Pod namespace across init and regular containers.
	// Reject explicit/generated collisions before mutating the workload.
	names := make(map[string]struct{})
	for _, containers := range [][]corev1.Container{
		podTemplate.Spec.Containers, podTemplate.Spec.InitContainers,
		result.Containers, result.InitContainers,
	} {
		for _, container := range containers {
			if _, exists := names[container.Name]; exists {
				return fmt.Errorf("duplicate container name %q", container.Name)
			}
			names[container.Name] = struct{}{}
		}
	}
	if err := applyWorkloadTraitResult(result, workload); err != nil {
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

	return applyVolumeClaimTemplates(result, workload, podTemplate, mainContainerName)
}

func applyVolumeClaimTemplates(result *TraitResult, workload runtime.Object, podTemplate *corev1.PodTemplateSpec, mainContainerName string) error {
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

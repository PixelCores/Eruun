package traits

import (
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

// processInit creates init containers (pre-main) and applies nested traits
// to them (e.g., storage/env/probes/resources), excluding further init recursion.
func processInit(ctx *TraitContext, initTraits []spec.InitTraitSpec) (*TraitResult, error) {
	// This is the final result that will be returned, aggregating all outcomes.
	finalResult := &TraitResult{}

	for index, initTrait := range initTraits {
		if initTrait.Image == "" {
			return nil, fmt.Errorf("init container for component %s must have an image", ctx.Component.Name)
		}
		if initTrait.Traits.Rollout != nil {
			return nil, fmt.Errorf("init container %s must not contain rollout trait", initTrait.Name)
		}

		initContainerName := initTrait.Name
		if initContainerName == "" {
			initContainerName = naming.BoundedLabelValue(fmt.Sprintf("%s-init-%d", ctx.Component.Name, index+1))
		}

		// Convert env map to env vars
		var envVars []corev1.EnvVar
		for _, name := range slices.Sorted(maps.Keys(initTrait.Properties.Env)) {
			envVars = append(envVars, corev1.EnvVar{Name: name, Value: initTrait.Properties.Env[name]})
		}

		// Recursively apply nested traits, excluding pod-level traits and recursive container traits.
		// and semantically meaningless nesting (init containers cannot have sidecars).
		nestedResult, err := applyTraitsRecursive(ctx, &initTrait.Traits, true)
		if err != nil {
			return nil, fmt.Errorf("failed to process nested traits for init container %s: %w", initContainerName, err)
		}

		envVars = append(envVars, nestedResult.EnvVars...)

		initContainer := corev1.Container{
			Name:            initContainerName,
			Image:           initTrait.Image,
			Command:         initTrait.Properties.Command,
			Env:             envVars, // Now contains envs from both properties and traits
			EnvFrom:         nestedResult.EnvFromSources,
			VolumeMounts:    nestedResult.VolumeMounts,
			ImagePullPolicy: workflowconfig.DefaultWorkflowImagePullPolicy,
		}

		// Apply nested resource requirements to the init container if present
		if nestedResult.ResourceRequirements != nil {
			initContainer.Resources = *nestedResult.ResourceRequirements
		}
		if nestedResult.SecurityContext != nil {
			initContainer.SecurityContext = nestedResult.SecurityContext
		}

		// Add the created container to the final result.
		finalResult.InitContainers = append(finalResult.InitContainers, initContainer)

		// Merge volumes and additional objects from the nested traits into the final result.
		finalResult.Volumes = append(finalResult.Volumes, nestedResult.Volumes...)
		finalResult.AdditionalObjects = append(finalResult.AdditionalObjects, nestedResult.AdditionalObjects...)

		klog.V(3).Infof("Constructed init container %s for component %s", initContainerName, ctx.Component.Name)
	}

	return finalResult, nil
}

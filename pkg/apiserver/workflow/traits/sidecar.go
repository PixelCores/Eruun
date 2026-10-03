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

// processSidecar materializes additional containers attached to the Pod.
// It also supports nested traits (except nested sidecars) applied to the sidecar itself.
func processSidecar(ctx *TraitContext, sidecarTraits []spec.SidecarTraitsSpec) (*TraitResult, error) {
	finalResult := &TraitResult{}

	for index, sidecarSpec := range sidecarTraits {
		if sidecarSpec.Image == "" {
			return nil, fmt.Errorf("sidecar for component %s must have an image", ctx.Component.Name)
		}
		// As per the design, sidecars cannot have nested sidecars.
		if len(sidecarSpec.Traits.Sidecar) > 0 {
			return nil, fmt.Errorf("sidecar '%s' must not contain nested sidecars", sidecarSpec.Name)
		}
		if sidecarSpec.Traits.Rollout != nil {
			return nil, fmt.Errorf("sidecar '%s' must not contain rollout trait", sidecarSpec.Name)
		}

		sidecarName := sidecarSpec.Name
		if sidecarName == "" {
			sidecarName = naming.BoundedLabelValue(fmt.Sprintf("%s-sidecar-%d", ctx.Component.Name, index+1))
		}

		// Convert env map to env vars
		var envVars []corev1.EnvVar
		for _, name := range slices.Sorted(maps.Keys(sidecarSpec.Env)) {
			envVars = append(envVars, corev1.EnvVar{Name: name, Value: sidecarSpec.Env[name]})
		}

		// Recursively apply nested traits, excluding pod-level traits and recursive container traits.
		nestedResult, err := applyTraitsRecursive(ctx, &sidecarSpec.Traits, true)
		if err != nil {
			return nil, fmt.Errorf("failed to process nested traits for sidecar %s: %w", sidecarName, err)
		}

		envVars = append(envVars, nestedResult.EnvVars...)

		sidecarContainer := corev1.Container{
			Name:            sidecarName,
			Image:           sidecarSpec.Image,
			Command:         sidecarSpec.Command,
			Args:            sidecarSpec.Args,
			Env:             envVars,
			EnvFrom:         nestedResult.EnvFromSources,
			VolumeMounts:    nestedResult.VolumeMounts,
			ImagePullPolicy: workflowconfig.DefaultWorkflowImagePullPolicy,
			LivenessProbe:   nestedResult.LivenessProbe,
			ReadinessProbe:  nestedResult.ReadinessProbe,
			StartupProbe:    nestedResult.StartupProbe,
			SecurityContext: nestedResult.SecurityContext,
		}

		// Apply nested resource requirements to the sidecar if present
		if nestedResult.ResourceRequirements != nil {
			sidecarContainer.Resources = *nestedResult.ResourceRequirements
		}

		// Add the created container to the final result.
		finalResult.Containers = append(finalResult.Containers, sidecarContainer)

		// Merge volumes and additional objects from the nested traits into the final result.
		finalResult.Volumes = append(finalResult.Volumes, nestedResult.Volumes...)
		finalResult.AdditionalObjects = append(finalResult.AdditionalObjects, nestedResult.AdditionalObjects...)

		klog.V(3).Infof("Constructed sidecar container %s for component %s", sidecarName, ctx.Component.Name)
	}

	return finalResult, nil
}

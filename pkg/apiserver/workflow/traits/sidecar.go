package traits

import (
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

// SidecarProcessor materializes additional containers attached to the Pod.
// It also supports nested traits (except nested sidecars) applied to the sidecar itself.
type SidecarProcessor struct{}

// Process adds sidecar containers to the workload, recursively applying any nested traits.
func (s *SidecarProcessor) Process(ctx *TraitContext, sidecarTraits []spec.SidecarTraitsSpec) (*TraitResult, error) {
	finalResult := &TraitResult{
		VolumeMounts:   make(map[string][]corev1.VolumeMount),
		EnvFromSources: make(map[string][]corev1.EnvFromSource),
		EnvVars:        make(map[string][]corev1.EnvVar),
	}

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
		nestedResult, err := applyTraitsRecursive(ctx.Component, ctx.Workload, &sidecarSpec.Traits, true)
		if err != nil {
			return nil, fmt.Errorf("failed to process nested traits for sidecar %s: %w", sidecarName, err)
		}

		// The sidecar container gets the volume mounts from its nested traits.
		// Use normalized component name to match the key used in storage trait.
		normalizedName := utils.NormalizeLowerStrip(ctx.Component.Name)
		var volumeMounts []corev1.VolumeMount
		if nestedResult != nil {
			if mounts, ok := nestedResult.VolumeMounts[normalizedName]; ok {
				volumeMounts = mounts
			}
		}

		// The sidecar container also gets the EnvFrom and EnvVars from its nested traits.
		var envFromSources []corev1.EnvFromSource
		if nestedResult != nil {
			if envFrom, ok := nestedResult.EnvFromSources[normalizedName]; ok {
				envFromSources = envFrom
			}
			if nestedEnvVars, ok := nestedResult.EnvVars[normalizedName]; ok {
				envVars = append(envVars, nestedEnvVars...)
			}
		}

		sidecarContainer := corev1.Container{
			Name:            sidecarName,
			Image:           sidecarSpec.Image,
			Command:         sidecarSpec.Command,
			Args:            sidecarSpec.Args,
			Env:             envVars,
			EnvFrom:         envFromSources,
			VolumeMounts:    volumeMounts,
			ImagePullPolicy: workflowconfig.DefaultWorkflowImagePullPolicy,
		}

		// Apply probes if present
		if nestedResult != nil {
			if nestedResult.LivenessProbe != nil {
				sidecarContainer.LivenessProbe = nestedResult.LivenessProbe
			}
			if nestedResult.ReadinessProbe != nil {
				sidecarContainer.ReadinessProbe = nestedResult.ReadinessProbe
			}
			if nestedResult.StartupProbe != nil {
				sidecarContainer.StartupProbe = nestedResult.StartupProbe
			}
		}

		// Apply nested resource requirements to the sidecar if present
		if nestedResult != nil && nestedResult.ResourceRequirements != nil {
			sidecarContainer.Resources = *nestedResult.ResourceRequirements
		}
		if nestedResult != nil && nestedResult.SecurityContext != nil {
			sidecarContainer.SecurityContext = nestedResult.SecurityContext
		}

		// Add the created container to the final result.
		finalResult.Containers = append(finalResult.Containers, sidecarContainer)

		// Merge volumes and additional objects from the nested traits into the final result.
		if nestedResult != nil {
			finalResult.Volumes = append(finalResult.Volumes, nestedResult.Volumes...)
			finalResult.AdditionalObjects = append(finalResult.AdditionalObjects, nestedResult.AdditionalObjects...)
		}

		klog.V(3).Infof("Constructed sidecar container %s for component %s", sidecarName, ctx.Component.Name)
	}

	return finalResult, nil
}

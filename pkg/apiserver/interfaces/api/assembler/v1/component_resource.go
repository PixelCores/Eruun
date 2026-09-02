package v1

import (
	"strings"

	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func buildComponentResourceConfigs(component *apisv1.ApplicationComponent) []apisv1.ComponentResourceConfig {
	if component == nil {
		return nil
	}
	var resources []apisv1.ComponentResourceConfig
	resources = appendResourceConfig(resources, "main", component.Name, component.Traits.Resources)
	for _, init := range component.Traits.Init {
		resources = appendResourceConfig(resources, "init", init.Name, init.Traits.Resources)
	}
	for _, sidecar := range component.Traits.Sidecar {
		resources = appendResourceConfig(resources, "sidecar", sidecar.Name, sidecar.Traits.Resources)
	}
	if len(resources) == 0 {
		return nil
	}
	return resources
}

func appendResourceConfig(resources []apisv1.ComponentResourceConfig, scope, name string, resource *spec.ResourceTraitsSpec) []apisv1.ComponentResourceConfig {
	if resource == nil {
		return resources
	}
	if strings.TrimSpace(resource.CPU) == "" &&
		strings.TrimSpace(resource.Memory) == "" &&
		strings.TrimSpace(resource.CPULimit) == "" &&
		strings.TrimSpace(resource.MemoryLimit) == "" &&
		strings.TrimSpace(resource.GPU) == "" {
		return resources
	}
	return append(resources, apisv1.ComponentResourceConfig{
		Scope:       scope,
		Name:        strings.TrimSpace(name),
		CPU:         strings.TrimSpace(resource.CPU),
		Memory:      strings.TrimSpace(resource.Memory),
		CPULimit:    strings.TrimSpace(resource.CPULimit),
		MemoryLimit: strings.TrimSpace(resource.MemoryLimit),
		GPU:         strings.TrimSpace(resource.GPU),
	})
}

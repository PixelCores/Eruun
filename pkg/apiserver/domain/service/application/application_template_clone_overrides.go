package application

import (
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func applyPropertyOverrides(props *apisv1.Properties, override apisv1.Properties, compType config.JobType) {
	if len(override.Env) > 0 {
		if props.Env == nil {
			props.Env = make(map[string]string, len(override.Env))
		}
		for k, v := range override.Env {
			props.Env[k] = v
		}
	}
	if len(override.Secret) > 0 && compType == config.SecretJob {
		if props.Secret == nil {
			props.Secret = make(map[string]string, len(override.Secret))
		}
		for k, v := range override.Secret {
			props.Secret[k] = v
		}
	}
	if override.FailurePolicy != nil {
		failurePolicy := *override.FailurePolicy
		props.FailurePolicy = &failurePolicy
	}
}

func applyTraitOverrides(traits *apisv1.Traits, override apisv1.Traits) {
	if traits == nil {
		return
	}
	if override.Resources != nil && hasResourceOverride(override.Resources) {
		if traits.Resources == nil {
			resources := *override.Resources
			traits.Resources = &resources
		} else {
			if override.Resources.CPU != "" {
				traits.Resources.CPU = override.Resources.CPU
			}
			if override.Resources.Memory != "" {
				traits.Resources.Memory = override.Resources.Memory
			}
			if override.Resources.CPULimit != "" {
				traits.Resources.CPULimit = override.Resources.CPULimit
			}
			if override.Resources.MemoryLimit != "" {
				traits.Resources.MemoryLimit = override.Resources.MemoryLimit
			}
			if override.Resources.GPU != "" {
				traits.Resources.GPU = override.Resources.GPU
			}
		}
	}
	if len(override.TargetWorkEnv) > 0 {
		if traits.TargetWorkEnv == nil {
			traits.TargetWorkEnv = make(map[string]string, len(override.TargetWorkEnv))
		}
		for key, value := range override.TargetWorkEnv {
			traits.TargetWorkEnv[key] = value
		}
	}
}

func hasResourceOverride(resources *spec.ResourceTraitsSpec) bool {
	return resources.CPU != "" ||
		resources.Memory != "" ||
		resources.CPULimit != "" ||
		resources.MemoryLimit != "" ||
		resources.GPU != ""
}

func applyDefaultStorageClass(traits *apisv1.Traits, defaultStorageClass string) {
	if traits == nil {
		return
	}
	defaultStorageClass = strings.TrimSpace(defaultStorageClass)
	if defaultStorageClass == "" {
		return
	}
	for i := range traits.Storage {
		storage := &traits.Storage[i]
		if storage.Type == config.StorageTypePersistent && strings.TrimSpace(storage.StorageClass) == "" {
			storage.StorageClass = defaultStorageClass
		}
	}
	for i := range traits.Init {
		applyDefaultStorageClass(&traits.Init[i].Traits, defaultStorageClass)
	}
	for i := range traits.Sidecar {
		applyDefaultStorageClass(&traits.Sidecar[i].Traits, defaultStorageClass)
	}
}

func applyInitEnvOverrides(traits *apisv1.Traits, override apisv1.Traits) map[int]map[string]struct{} {
	if traits == nil || len(traits.Init) == 0 || len(override.Init) == 0 {
		return nil
	}
	var overrideKeys map[int]map[string]struct{}
	applyOverride := func(index int, env map[string]string) {
		if traits.Init[index].Properties.Env == nil {
			traits.Init[index].Properties.Env = make(map[string]string, len(env))
		}
		if overrideKeys == nil {
			overrideKeys = make(map[int]map[string]struct{})
		}
		if overrideKeys[index] == nil {
			overrideKeys[index] = make(map[string]struct{}, len(env))
		}
		for k, v := range env {
			traits.Init[index].Properties.Env[k] = v
			overrideKeys[index][k] = struct{}{}
		}
	}
	for _, initOverride := range override.Init {
		overrideName := strings.TrimSpace(initOverride.Name)
		if len(initOverride.Properties.Env) == 0 {
			continue
		}
		if overrideName == "" {
			applyOverride(0, initOverride.Properties.Env)
			continue
		}
		for i := range traits.Init {
			if traits.Init[i].Name != overrideName {
				continue
			}
			applyOverride(i, initOverride.Properties.Env)
			break
		}
	}
	return overrideKeys
}

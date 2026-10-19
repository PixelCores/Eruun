package application

import (
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func summarizeApplicationResourcesFromCreateComponents(components []apisv1.CreateComponentRequest) apisv1.ApplicationResources {
	for _, component := range components {
		if !isApplicationResourceSummaryComponentType(component.ComponentType) {
			continue
		}
		return applicationResourcesFromTraitSpec(component.Replicas, component.Traits.Resources)
	}
	return apisv1.ApplicationResources{}
}

func applicationResourcesFromTraitSpec(replicas int32, resources *spec.ResourceTraitsSpec) apisv1.ApplicationResources {
	if resources == nil {
		return apisv1.ApplicationResources{}
	}
	cpu := strings.TrimSpace(resources.CPU)
	memory := strings.TrimSpace(resources.Memory)
	return apisv1.ApplicationResources{
		CPUReq:   cpu,
		CPULimit: applicationResourceLimitOrRequest(resources.CPULimit, cpu),
		MemReq:   memory,
		MemLimit: applicationResourceLimitOrRequest(resources.MemoryLimit, memory),
		Replicas: replicas,
	}
}

func applicationResourceLimitOrRequest(limit, request string) string {
	limit = strings.TrimSpace(limit)
	if limit != "" {
		return limit
	}
	return strings.TrimSpace(request)
}

func isApplicationResourceSummaryComponentType(componentType config.JobType) bool {
	switch componentType {
	case config.ServerJob, config.StoreJob, config.InstantJob:
		return true
	default:
		return false
	}
}

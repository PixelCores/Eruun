package application

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func convertComponentModelToCreateRequest(comp *model.ApplicationComponent) (apisv1.CreateComponentRequest, error) {
	dto, err := assembler.ConvertComponentModelToDTO(comp)
	if err != nil {
		return apisv1.CreateComponentRequest{}, err
	}
	if dto == nil {
		return apisv1.CreateComponentRequest{}, nil
	}
	return apisv1.CreateComponentRequest{
		Name:          dto.Name,
		ComponentType: dto.ComponentType,
		Image:         dto.Image,
		Namespace:     dto.Namespace,
		Replicas:      dto.Replicas,
		Properties:    dto.Properties,
		Traits:        dto.Traits,
	}, nil
}

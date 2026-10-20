package v1

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/nodepool"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func CreateNodePoolCommand(request apis.CreateNodePoolRequest) nodepool.CreateNodePoolCommand {
	return nodepool.CreateNodePoolCommand{Name: request.Name, UpdateNodePoolCommand: UpdateNodePoolCommand(request.UpdateNodePoolRequest)}
}

func UpdateNodePoolCommand(request apis.UpdateNodePoolRequest) nodepool.UpdateNodePoolCommand {
	return nodepool.UpdateNodePoolCommand{Description: request.Description, NodeSelector: request.NodeSelector, Tolerations: request.Tolerations}
}

func NodePoolModelToDTO(item *model.NodePool) *apis.NodePool {
	if item == nil {
		return nil
	}
	return &apis.NodePool{Name: item.Name, Description: item.Description, NodeSelector: item.NodeSelector, Tolerations: item.Tolerations, CreateTime: item.CreateTime, UpdateTime: item.UpdateTime}
}

func NodePoolModelsToDTO(items []*model.NodePool) []*apis.NodePool {
	result := make([]*apis.NodePool, 0, len(items))
	for _, item := range items {
		result = append(result, NodePoolModelToDTO(item))
	}
	return result
}

func NodePoolModelsToOptions(items []*model.NodePool) []*apis.NodePoolOption {
	result := make([]*apis.NodePoolOption, 0, len(items))
	for _, item := range items {
		result = append(result, &apis.NodePoolOption{Name: item.Name, Description: item.Description})
	}
	return result
}

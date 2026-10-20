package v1

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/rbacprofile"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func CreateRBACProfileCommand(request apis.CreateRBACProfileRequest) rbacprofile.CreateRBACProfileCommand {
	return rbacprofile.CreateRBACProfileCommand{Name: request.Name, UpdateRBACProfileCommand: UpdateRBACProfileCommand(request.UpdateRBACProfileRequest)}
}

func UpdateRBACProfileCommand(request apis.UpdateRBACProfileRequest) rbacprofile.UpdateRBACProfileCommand {
	return rbacprofile.UpdateRBACProfileCommand{Description: request.Description, Rules: request.Rules}
}

func RBACProfileModelToDTO(item *model.RBACProfile) *apis.RBACProfile {
	if item == nil {
		return nil
	}
	return &apis.RBACProfile{Name: item.Name, Description: item.Description, Rules: item.Rules, CreateTime: item.CreateTime, UpdateTime: item.UpdateTime}
}

func RBACProfileModelsToDTO(items []*model.RBACProfile) []*apis.RBACProfile {
	result := make([]*apis.RBACProfile, 0, len(items))
	for _, item := range items {
		result = append(result, RBACProfileModelToDTO(item))
	}
	return result
}

func RBACProfileModelsToOptions(items []*model.RBACProfile) []*apis.RBACProfileOption {
	result := make([]*apis.RBACProfileOption, 0, len(items))
	for _, item := range items {
		result = append(result, &apis.RBACProfileOption{Name: item.Name, Description: item.Description})
	}
	return result
}

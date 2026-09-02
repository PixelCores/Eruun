package v1

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/programminglanguage"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

// CreateProgrammingLanguageCommand maps transport input to a domain command.
func CreateProgrammingLanguageCommand(request apisv1.CreateProgrammingLanguageRequest) programminglanguage.CreateProgrammingLanguageCommand {
	return programminglanguage.CreateProgrammingLanguageCommand{
		Name:    request.Name,
		Version: request.Version,
		Enabled: request.Enabled,
		CPUReq:  request.CPUReq,
		MemReq:  request.MemReq,
	}
}

// UpdateProgrammingLanguageCommand maps transport input to a domain command.
func UpdateProgrammingLanguageCommand(request apisv1.UpdateProgrammingLanguageRequest) programminglanguage.UpdateProgrammingLanguageCommand {
	return programminglanguage.UpdateProgrammingLanguageCommand{
		Name:    request.Name,
		Version: request.Version,
		Enabled: request.Enabled,
		CPUReq:  request.CPUReq,
		MemReq:  request.MemReq,
	}
}

// ProgrammingLanguageModelToDTO maps a domain model to its transport response.
func ProgrammingLanguageModelToDTO(language *model.ProgrammingLanguage) *apisv1.ProgrammingLanguage {
	if language == nil {
		return nil
	}
	return &apisv1.ProgrammingLanguage{
		ID:         language.ID,
		Code:       language.Code,
		Name:       language.Name,
		Version:    language.Version,
		Enabled:    language.Enabled != nil && *language.Enabled,
		CPUReq:     language.CPUReq,
		MemReq:     language.MemReq,
		CreateTime: language.CreateTime,
		UpdateTime: language.UpdateTime,
	}
}

// ProgrammingLanguageModelsToDTO maps a domain model collection to transport responses.
func ProgrammingLanguageModelsToDTO(languages []*model.ProgrammingLanguage) []*apisv1.ProgrammingLanguage {
	result := make([]*apisv1.ProgrammingLanguage, 0, len(languages))
	for _, language := range languages {
		result = append(result, ProgrammingLanguageModelToDTO(language))
	}
	return result
}

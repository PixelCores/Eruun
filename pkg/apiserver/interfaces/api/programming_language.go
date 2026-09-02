package api

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type programmingLanguages struct {
	ProgrammingLanguageService service.ProgrammingLanguageService `inject:""`
}

// NewProgrammingLanguages creates a programming language API handler.
func NewProgrammingLanguages() Interface {
	return &programmingLanguages{}
}

func (p *programmingLanguages) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/programming-languages", p.listProgrammingLanguages)
	group.GET("/programming-languages/:id", p.getProgrammingLanguage)
	group.POST("/programming-languages", p.createProgrammingLanguage)
	group.PUT("/programming-languages/:id", p.updateProgrammingLanguage)
	group.DELETE("/programming-languages/:id", p.deleteProgrammingLanguage)
}

func (p *programmingLanguages) listProgrammingLanguages(c *gin.Context) {
	handleContextResult(c, func(ctx context.Context) (apis.ListProgrammingLanguagesResponse, error) {
		items, err := p.ProgrammingLanguageService.List(ctx)
		return apis.ListProgrammingLanguagesResponse{Languages: assembler.ProgrammingLanguageModelsToDTO(items)}, err
	})
}

func (p *programmingLanguages) getProgrammingLanguage(c *gin.Context) {
	handlePathResult(c, programmingLanguageIDPathParam, func(ctx context.Context, id string) (*apis.ProgrammingLanguage, error) {
		language, err := p.ProgrammingLanguageService.Get(ctx, id)
		return assembler.ProgrammingLanguageModelToDTO(language), err
	})
}

func (p *programmingLanguages) createProgrammingLanguage(c *gin.Context) {
	handleBoundResult(
		c,
		validatedStrictJSONBody[apis.CreateProgrammingLanguageRequest](bcode.ErrProgrammingLanguageInvalid, true),
		func(ctx context.Context, req *apis.CreateProgrammingLanguageRequest) (*apis.ProgrammingLanguage, error) {
			language, err := p.ProgrammingLanguageService.Create(ctx, assembler.CreateProgrammingLanguageCommand(*req))
			return assembler.ProgrammingLanguageModelToDTO(language), err
		},
	)
}

func (p *programmingLanguages) updateProgrammingLanguage(c *gin.Context) {
	handlePathBoundResult(
		c,
		programmingLanguageIDPathParam,
		validatedStrictJSONBody[apis.UpdateProgrammingLanguageRequest](bcode.ErrProgrammingLanguageInvalid, true),
		func(ctx context.Context, id string, req *apis.UpdateProgrammingLanguageRequest) (*apis.ProgrammingLanguage, error) {
			language, err := p.ProgrammingLanguageService.Update(ctx, id, assembler.UpdateProgrammingLanguageCommand(*req))
			return assembler.ProgrammingLanguageModelToDTO(language), err
		},
	)
}

func (p *programmingLanguages) deleteProgrammingLanguage(c *gin.Context) {
	handlePathResult(c, programmingLanguageIDPathParam, func(ctx context.Context, id string) (apis.DeleteProgrammingLanguageResponse, error) {
		if err := p.ProgrammingLanguageService.Delete(ctx, id); err != nil {
			return apis.DeleteProgrammingLanguageResponse{}, err
		}
		return apis.DeleteProgrammingLanguageResponse{ID: id}, nil
	})
}

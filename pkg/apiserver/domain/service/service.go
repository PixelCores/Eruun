package service

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/conversion"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/programminglanguage"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/validation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type ApplicationsService = application.ApplicationsService
type ListApplicationsOptions = application.ListApplicationsOptions
type ComponentLogStream = application.ComponentLogStream
type ComponentFileArchiveStream = application.ComponentFileArchiveStream
type ComponentShellScriptStream = application.ComponentShellScriptStream

type WorkflowService = workflow.WorkflowService
type ValidationService = validation.ValidationService
type ConversionService = conversion.ConversionService
type SystemSettingService = systemsetting.SystemSettingService
type ProgrammingLanguageService = programminglanguage.ProgrammingLanguageService
type CreateProgrammingLanguageCommand = programminglanguage.CreateProgrammingLanguageCommand
type UpdateProgrammingLanguageCommand = programminglanguage.UpdateProgrammingLanguageCommand

func NewApplicationService() ApplicationsService {
	return application.NewApplicationService()
}

func NewWorkflowService() WorkflowService {
	return workflow.NewWorkflowService()
}

func NewConversionService() ConversionService {
	return conversion.NewConversionService()
}

func NewSystemSettingService() SystemSettingService {
	return systemsetting.NewSystemSettingService()
}

func NewProgrammingLanguageServiceWithRepository(repo repository.ProgrammingLanguageRepository) (ProgrammingLanguageService, error) {
	return programminglanguage.NewProgrammingLanguageServiceWithRepository(repo)
}

func TerminalizePrecreatedVersionUpdateCleanupJobs(ctx context.Context, store datastore.DataStore, taskID string, targetStatus config.Status, reason string) error {
	return workflow.TerminalizePrecreatedVersionUpdateCleanupJobs(ctx, store, taskID, targetStatus, reason)
}

// InitServiceBean initializes the remaining container-managed services.
func InitServiceBean(validationService ValidationService) []interface{} {
	applicationService := NewApplicationService()
	workflowService := NewWorkflowService()
	conversionService := NewConversionService()
	systemSettingService := NewSystemSettingService()
	return []interface{}{
		applicationService,
		workflowService,
		validationService,
		conversionService,
		systemSettingService,
	}
}

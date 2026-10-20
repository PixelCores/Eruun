package service

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/conversion"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/validation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/workflow"
)

// InitServiceBean initializes the remaining container-managed services.
func InitServiceBean(validationService validation.ValidationService) []interface{} {
	applicationService := application.NewApplicationService()
	workflowService := workflow.NewWorkflowService()
	conversionService := conversion.NewConversionService()
	systemSettingService := systemsetting.NewSystemSettingService()
	return []interface{}{
		applicationService,
		workflowService,
		validationService,
		conversionService,
		systemSettingService,
	}
}

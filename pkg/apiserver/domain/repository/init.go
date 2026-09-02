package repository

// InitRepositoryBean initializes all repository instances.
// Dependencies are injected via struct tags by the IoC container.
func InitRepositoryBean(programmingLanguageOverrides ...ProgrammingLanguageRepository) []interface{} {
	programmingLanguageRepository := NewProgrammingLanguageRepository()
	if len(programmingLanguageOverrides) > 0 && programmingLanguageOverrides[0] != nil {
		programmingLanguageRepository = programmingLanguageOverrides[0]
	}
	return []interface{}{
		NewApplicationRepository(),
		NewWorkflowRepository(),
		NewComponentRepository(),
		NewWorkflowQueueRepository(),
		NewSystemSettingRepository(),
		programmingLanguageRepository,
	}
}

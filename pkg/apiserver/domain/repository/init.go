package repository

// InitRepositoryBean initializes the repositories built by the container.
// Dependencies are injected via struct tags by the IoC container.
func InitRepositoryBean() []interface{} {
	return []interface{}{
		NewApplicationRepository(),
		NewWorkflowRepository(),
		NewComponentRepository(),
		NewSystemSettingRepository(),
	}
}

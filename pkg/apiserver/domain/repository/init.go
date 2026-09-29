package repository

// InitRepositoryBean initializes the remaining container-managed repositories.
// Application and component repositories are constructed by the server.
func InitRepositoryBean(appRepo ApplicationRepository, componentRepo ComponentRepository) []interface{} {
	return []interface{}{
		appRepo,
		NewWorkflowRepository(),
		componentRepo,
		NewSystemSettingRepository(),
	}
}

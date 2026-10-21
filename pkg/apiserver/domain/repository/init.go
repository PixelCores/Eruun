package repository

import "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"

// InitRepositoryBean initializes the remaining container-managed repositories.
// Application and component repositories are constructed by the server.
func InitRepositoryBean(store datastore.DataStore, appRepo ApplicationRepository, componentRepo ComponentRepository) []interface{} {
	return []interface{}{
		appRepo,
		NewWorkflowRepository(),
		componentRepo,
		NewSystemSettingRepository(store),
	}
}

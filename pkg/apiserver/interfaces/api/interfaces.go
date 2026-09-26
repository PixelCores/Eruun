package api

import "github.com/gin-gonic/gin"

var versionPrefix = "/api/v1"

// GetAPIPrefix returns the prefix of the API route path.
func GetAPIPrefix() []string {
	return []string{versionPrefix}
}

// Interface defines HTTP route registration.
type Interface interface {
	RegisterRoutes(group *gin.RouterGroup)
}

// NewHandlers returns fresh handlers owned by one server instance.
func NewHandlers() []Interface {
	return []Interface{
		NewApplications(),
		NewResourceImports(),
		NewWorkspaceJobs(),
		NewSettings(),
		NewProgrammingLanguages(),
		NewAccounts(),
		&health{},
	}
}

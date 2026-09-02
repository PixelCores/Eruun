package api

import (
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func requiredPathParam(c *gin.Context, name string, invalidErr error) (string, bool) {
	value := strings.TrimSpace(c.Param(name))
	if value == "" {
		bcode.ReturnError(c, invalidErr)
		return "", false
	}
	return value, true
}

func appIDPathParam(c *gin.Context) (string, bool) {
	return requiredPathParam(c, "appID", bcode.ErrApplicationNotExist)
}

func taskIDPathParam(c *gin.Context) (string, bool) {
	return requiredPathParam(c, "taskID", bcode.ErrWorkflowTaskNotExist)
}

func workflowIDPathParam(c *gin.Context) (string, bool) {
	return requiredPathParam(c, "workflowID", bcode.ErrWorkflowConfig)
}

func settingTypePathParam(c *gin.Context) (string, bool) {
	return requiredPathParam(c, "type", bcode.ErrSystemSettingValueInvalid)
}

func programmingLanguageIDPathParam(c *gin.Context) (string, bool) {
	return requiredPathParam(c, "id", bcode.ErrProgrammingLanguageNotFound)
}

func componentRouteParams(c *gin.Context) (string, string, bool) {
	appID := strings.TrimSpace(c.Param("appID"))
	componentName := strings.TrimSpace(c.Param("componentName"))
	if appID == "" || componentName == "" {
		bcode.ReturnError(c, bcode.ErrComponentNotFound)
		return "", "", false
	}
	return appID, componentName, true
}

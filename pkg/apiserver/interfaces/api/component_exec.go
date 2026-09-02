package api

import (
	"context"
	"strings"

	"github.com/gin-gonic/gin"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (app *applications) execComponentShellScript(c *gin.Context) {
	handleComponentBoundResult(
		c,
		validatedComponentShellScriptRequest,
		func(ctx context.Context, appID, componentName string, req *apis.ExecComponentShellScriptRequest) (*apis.ExecComponentShellScriptResponse, error) {
			return app.ApplicationService.ExecComponentShellScript(ctx, appID, componentName, *req)
		},
	)
}

func validatedComponentShellScriptRequest(c *gin.Context) (*apis.ExecComponentShellScriptRequest, bool) {
	req, ok := bindRequest[apis.ExecComponentShellScriptRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return nil, false
	}
	if strings.TrimSpace(req.Script) == "" {
		bcode.ReturnError(c, bcode.ErrComponentShellScriptInvalid)
		return nil, false
	}
	return req, true
}

package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"

	apiresponse "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/response"
)

func (app *applications) getCanonicalJSONSchema(c *gin.Context) {
	schema, err := apis.CanonicalJSONSchema()
	if err != nil {
		apiresponse.ReturnError(c, err)
		return
	}
	c.Data(http.StatusOK, "application/schema+json", schema)
}

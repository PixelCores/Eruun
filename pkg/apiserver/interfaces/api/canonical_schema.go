package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (app *applications) getCanonicalJSONSchema(c *gin.Context) {
	schema, err := apis.CanonicalJSONSchema()
	if err != nil {
		bcode.ReturnError(c, err)
		return
	}
	c.Data(http.StatusOK, "application/schema+json", schema)
}

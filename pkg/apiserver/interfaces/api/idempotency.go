package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/idempotencykey"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const idempotencyKeyHeader = "Idempotency-Key"

func bindIdempotencyKey(c *gin.Context, bindErr *bcode.Bcode) (string, bool) {
	values, provided := c.Request.Header[http.CanonicalHeaderKey(idempotencyKeyHeader)]
	if !provided {
		return "", true
	}
	if len(values) != 1 {
		bcode.ReturnErrorWithMessage(c, bindErr, "Idempotency-Key must be one non-empty value of at most 128 characters without surrounding whitespace")
		return "", false
	}
	if err := idempotencykey.Validate(values[0]); err != nil {
		bcode.ReturnErrorWithMessage(c, bindErr, err.Error())
		return "", false
	}
	return values[0], true
}

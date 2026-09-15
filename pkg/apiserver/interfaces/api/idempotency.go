package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const idempotencyKeyHeader = "Idempotency-Key"
const maxIdempotencyKeyLength = 128

func bindIdempotencyKey(c *gin.Context, bindErr *bcode.Bcode) (string, bool) {
	values, provided := c.Request.Header[http.CanonicalHeaderKey(idempotencyKeyHeader)]
	if !provided {
		return "", true
	}
	if len(values) != 1 || values[0] == "" || values[0] != strings.TrimSpace(values[0]) || len(values[0]) > maxIdempotencyKeyLength {
		bcode.ReturnErrorWithMessage(c, bindErr, "Idempotency-Key must be one non-empty value of at most 128 characters without surrounding whitespace")
		return "", false
	}
	for _, char := range values[0] {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("._:-", char) {
			continue
		}
		bcode.ReturnErrorWithMessage(c, bindErr, "Idempotency-Key may contain only letters, numbers, dot, underscore, colon, and hyphen")
		return "", false
	}
	return values[0], true
}

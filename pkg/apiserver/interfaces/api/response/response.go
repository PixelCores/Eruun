package response

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// SuccessCode indicates a successful response.
const SuccessCode int32 = 0

// Response defines the unified API response envelope.
type Response struct {
	Code    int32       `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// ReturnResponse writes the unified response envelope.
func ReturnResponse(c *gin.Context, httpCode int, code int32, message string, data interface{}) {
	c.JSON(httpCode, Response{Code: code, Message: message, Data: data})
}

// ReturnSuccess writes a successful response envelope.
func ReturnSuccess(c *gin.Context, data interface{}) {
	ReturnResponse(c, http.StatusOK, SuccessCode, "", data)
}

// ReturnErrorWithMessage writes an error response envelope with a custom message.
func ReturnErrorWithMessage(c *gin.Context, b *bcode.Bcode, message string) {
	if b == nil {
		if message == "" {
			message = bcode.ErrServer.Message
		}
		ReturnResponse(c, http.StatusInternalServerError, bcode.ErrServer.BusinessCode, message, nil)
		return
	}
	if message == "" {
		message = b.Message
	}
	ReturnResponse(c, int(b.HTTPCode), b.BusinessCode, message, nil)
}

// ReturnError Unified handling of all types of errors, generating a standard return structure.
func ReturnError(c *gin.Context, err error) {
	if err == nil {
		return
	}

	var bc *bcode.Bcode
	if errors.As(err, &bc) {
		ReturnErrorWithMessage(c, bc, bcode.SafeClientMessage(err))
		return
	}

	if errors.Is(err, datastore.ErrRecordNotExist) {
		ReturnErrorWithMessage(c, bcode.ErrNotFound, "")
		return
	}

	var validErr validator.ValidationErrors
	if errors.As(err, &validErr) {
		ReturnErrorWithMessage(c, bcode.ErrApplicationConfig, "")
		return
	}

	klog.ErrorS(errors.New("generic server error response"), "returning generic server error response", "errorType", fmt.Sprintf("%T", err))
	ReturnErrorWithMessage(c, bcode.ErrServer, "")
}

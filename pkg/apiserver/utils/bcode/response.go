package bcode

import (
	"net/http"

	"github.com/gin-gonic/gin"
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
func ReturnErrorWithMessage(c *gin.Context, b *Bcode, message string) {
	if b == nil {
		if message == "" {
			message = ErrServer.Message
		}
		ReturnResponse(c, http.StatusInternalServerError, ErrServer.BusinessCode, message, nil)
		return
	}
	if message == "" {
		message = b.Message
	}
	ReturnResponse(c, int(b.HTTPCode), b.BusinessCode, message, nil)
}

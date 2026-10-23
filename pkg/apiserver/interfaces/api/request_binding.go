package api

import (
	"errors"
	"io"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	apiresponse "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/response"
)

func bindStrictJSON[T any](c *gin.Context, invalidErr error, logBindErr bool) (*T, bool) {
	return bindStrictJSONBody[T](c, invalidErr, logBindErr, false)
}

func bindStrictJSONAllowEOF[T any](c *gin.Context, invalidErr error, logBindErr bool) (*T, bool) {
	return bindStrictJSONBody[T](c, invalidErr, logBindErr, true)
}

func bindStrictJSONBody[T any](c *gin.Context, invalidErr error, logBindErr, allowEOF bool) (*T, bool) {
	var req T
	data, err := io.ReadAll(c.Request.Body)
	if err == nil {
		err = apis.DecodeStrictJSON(data, &req)
	}
	if err != nil {
		if allowEOF && errors.Is(err, io.EOF) {
			return &req, true
		}
		if logBindErr {
			logRequestError(c, err, "decode strict json request failed")
		}
		apiresponse.ReturnError(c, invalidErr)
		return nil, false
	}
	return &req, true
}

func logRequestError(c *gin.Context, err error, message string) {
	if c == nil || c.Request == nil {
		klog.ErrorS(err, message)
		return
	}
	klog.ErrorS(err, message, "method", c.Request.Method, "path", c.FullPath())
}

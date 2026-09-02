package api

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func bindRequest[T any](c *gin.Context, invalidErr error, logBindErr bool) (*T, bool) {
	var req T
	if err := c.Bind(&req); err != nil {
		if logBindErr {
			logRequestError(c, err, "bind request failed")
		}
		bcode.ReturnError(c, invalidErr)
		return nil, false
	}
	return &req, true
}

func bindAndValidate[T any](c *gin.Context, invalidErr error, logBindErr bool) (*T, bool) {
	req, ok := bindRequest[T](c, invalidErr, logBindErr)
	if !ok {
		return nil, false
	}
	if err := validate.Struct(*req); err != nil {
		bcode.ReturnError(c, invalidErr)
		return nil, false
	}
	return req, true
}

func bindStrictJSON[T any](c *gin.Context, invalidErr error, logBindErr bool) (*T, bool) {
	var req T
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		if logBindErr {
			logRequestError(c, err, "decode strict json request failed")
		}
		bcode.ReturnError(c, invalidErr)
		return nil, false
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if logBindErr {
			if err == nil {
				logRequestError(c, errors.New("request body contains multiple JSON values"), "decode strict json request failed")
			} else {
				logRequestError(c, err, "decode strict json request failed")
			}
		}
		bcode.ReturnError(c, invalidErr)
		return nil, false
	}
	return &req, true
}

func bindStrictJSONAllowEOF[T any](c *gin.Context, invalidErr error, logBindErr bool) (*T, bool) {
	var req T
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		if errors.Is(err, io.EOF) {
			return &req, true
		}
		if logBindErr {
			logRequestError(c, err, "decode strict json request failed")
		}
		bcode.ReturnError(c, invalidErr)
		return nil, false
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if logBindErr {
			if err == nil {
				logRequestError(c, errors.New("request body contains multiple JSON values"), "decode strict json request failed")
			} else {
				logRequestError(c, err, "decode strict json request failed")
			}
		}
		bcode.ReturnError(c, invalidErr)
		return nil, false
	}
	return &req, true
}

func bindAndValidateStrictJSON[T any](c *gin.Context, invalidErr error, logBindErr bool) (*T, bool) {
	req, ok := bindStrictJSON[T](c, invalidErr, logBindErr)
	if !ok {
		return nil, false
	}
	if err := validate.Struct(*req); err != nil {
		bcode.ReturnError(c, invalidErr)
		return nil, false
	}
	return req, true
}

func bindJSONAllowEOF[T any](c *gin.Context, invalidErr error, logBindErr bool) (*T, bool) {
	var req T
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		if logBindErr {
			logRequestError(c, err, "bind json request failed")
		}
		bcode.ReturnError(c, invalidErr)
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

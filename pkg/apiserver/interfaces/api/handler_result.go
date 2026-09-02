package api

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type pathParamBinder func(*gin.Context) (string, bool)
type requestBinder[T any] func(*gin.Context) (*T, bool)

func validatedRequestBody[T any](invalidErr error, logBindErr bool) requestBinder[T] {
	return func(c *gin.Context) (*T, bool) {
		return bindAndValidate[T](c, invalidErr, logBindErr)
	}
}

func validatedStrictJSONBody[T any](invalidErr error, logBindErr bool) requestBinder[T] {
	return func(c *gin.Context) (*T, bool) {
		return bindAndValidateStrictJSON[T](c, invalidErr, logBindErr)
	}
}

func respondWithResult[T any](c *gin.Context, resp T, err error) bool {
	if err != nil {
		bcode.ReturnError(c, err)
		return false
	}
	bcode.ReturnSuccess(c, resp)
	return true
}

func handleContextResult[T any](c *gin.Context, run func(context.Context) (T, error)) {
	resp, err := run(c.Request.Context())
	respondWithResult(c, resp, err)
}

func handlePathResult[T any](
	c *gin.Context,
	pathParam pathParamBinder,
	run func(context.Context, string) (T, error),
) {
	value, ok := pathParam(c)
	if !ok {
		return
	}
	resp, err := run(c.Request.Context(), value)
	respondWithResult(c, resp, err)
}

func handleComponentResult[T any](
	c *gin.Context,
	run func(context.Context, string, string) (T, error),
) {
	appID, componentName, ok := componentRouteParams(c)
	if !ok {
		return
	}
	resp, err := run(c.Request.Context(), appID, componentName)
	respondWithResult(c, resp, err)
}

func handleBoundResult[Req any, Resp any](
	c *gin.Context,
	bind requestBinder[Req],
	run func(context.Context, *Req) (Resp, error),
) {
	req, ok := bind(c)
	if !ok {
		return
	}
	resp, err := run(c.Request.Context(), req)
	respondWithResult(c, resp, err)
}

func handleComponentBoundResult[Req any, Resp any](
	c *gin.Context,
	bind requestBinder[Req],
	run func(context.Context, string, string, *Req) (Resp, error),
) {
	appID, componentName, ok := componentRouteParams(c)
	if !ok {
		return
	}
	req, ok := bind(c)
	if !ok {
		return
	}
	resp, err := run(c.Request.Context(), appID, componentName, req)
	respondWithResult(c, resp, err)
}

func handlePathBoundResult[Req any, Resp any](
	c *gin.Context,
	pathParam pathParamBinder,
	bind requestBinder[Req],
	run func(context.Context, string, *Req) (Resp, error),
) {
	value, ok := pathParam(c)
	if !ok {
		return
	}
	req, ok := bind(c)
	if !ok {
		return
	}
	resp, err := run(c.Request.Context(), value, req)
	respondWithResult(c, resp, err)
}

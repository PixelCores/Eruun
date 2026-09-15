package grpcapi

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/idempotencykey"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"google.golang.org/grpc/metadata"
)

func requestIdempotencyKey(ctx context.Context, inputError *bcode.Bcode) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", nil
	}
	values := md.Get("idempotency-key")
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 {
		return "", bcode.WithSafeClientMessage(inputError, "Idempotency-Key must be one non-empty value")
	}
	if err := idempotencykey.Validate(values[0]); err != nil {
		return "", bcode.WithSafeClientMessage(inputError, err.Error())
	}
	return values[0], nil
}

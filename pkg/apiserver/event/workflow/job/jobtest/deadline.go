package jobtest

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
)

type RetryDeadlineObserver struct {
	informer.ComponentReadyObserver
	Observe func(context.Context) error
}

func (o RetryDeadlineObserver) WaitForJob(ctx context.Context, _, _ string, _ func(*batchv1.Job) (bool, error)) error {
	return o.Observe(ctx)
}

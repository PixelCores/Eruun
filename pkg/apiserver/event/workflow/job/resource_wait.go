package job

import (
	"context"
	"time"
)

type pollResourceReadyFunc func(context.Context) (bool, error)
type pollErrorMapper func(error) error
type pollTimeoutFunc func() error

type pollWaitOptions struct {
	timeout   time.Duration
	interval  time.Duration
	poll      pollResourceReadyFunc
	onCancel  pollErrorMapper
	onError   pollErrorMapper
	onTimeout pollTimeoutFunc
}

func waitForPolledResource(ctx context.Context, opts pollWaitOptions) error {
	timeout := time.After(opts.timeout)
	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if opts.onCancel != nil {
				return opts.onCancel(ctx.Err())
			}
			return ctx.Err()
		case <-timeout:
			if opts.onTimeout != nil {
				return opts.onTimeout()
			}
			return nil
		case <-ticker.C:
			ready, err := opts.poll(ctx)
			if err != nil {
				if opts.onError != nil {
					return opts.onError(err)
				}
				return err
			}
			if ready {
				return nil
			}
		}
	}
}

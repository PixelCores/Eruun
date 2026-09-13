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
	waitCtx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()
	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			if opts.onCancel != nil {
				return opts.onCancel(err)
			}
			return err
		}
		if waitCtx.Err() != nil {
			if opts.onTimeout != nil {
				return opts.onTimeout()
			}
			return waitCtx.Err()
		}
		select {
		case <-waitCtx.Done():
			continue
		case <-ticker.C:
			ready, err := opts.poll(waitCtx)
			if waitCtx.Err() != nil {
				continue
			}
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

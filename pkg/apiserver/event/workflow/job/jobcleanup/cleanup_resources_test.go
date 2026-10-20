package jobcleanup

import (
	"context"
)

type cleanupMutableErrorContext struct {
	context.Context
	err error
}

func (c *cleanupMutableErrorContext) Err() error { return c.err }

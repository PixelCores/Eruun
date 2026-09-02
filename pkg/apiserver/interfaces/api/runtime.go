package api

import (
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// InitRuntime initializes API runtime dependencies and returns non-recoverable
// initialization errors to caller.
func InitRuntime() error {
	if err := InitValidator(); err != nil {
		return fmt.Errorf("init validator: %w", err)
	}
	if err := bcode.Init(); err != nil {
		return fmt.Errorf("init bcode registry: %w", err)
	}
	return nil
}

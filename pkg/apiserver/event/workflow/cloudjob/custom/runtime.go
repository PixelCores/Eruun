package custom

import (
	"context"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type runtime struct{}

func (r *runtime) Call(_ context.Context, action string, _ map[string]interface{}) (*contracts.CloudJobResult, error) {
	action = strings.TrimSpace(action)
	return nil, fmt.Errorf("custom action %q is not implemented", action)
}

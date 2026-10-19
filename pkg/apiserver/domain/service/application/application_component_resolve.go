package application

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// resolveComponents applies the workspace boundary to requested components
// before resource-name validation and persistence.
func resolveComponents(ctx context.Context, namespace string, reqComponents []apisv1.CreateComponentRequest, cfg *config.Config) ([]apisv1.CreateComponentRequest, error) {
	components := make([]apisv1.CreateComponentRequest, 0, len(reqComponents))
	for index, comp := range reqComponents {
		if err := spec.ValidateNestedEvaluationTraits(&comp.Traits); err != nil {
			return nil, fmt.Errorf("%w: component[%d]: %v", bcode.ErrApplicationConfig, index, err)
		}
		components = append(components, comp)
	}
	scope, ok := access.FromContext(ctx)
	if !ok {
		return components, nil
	}
	if namespace != scope.Namespace || cfg == nil || cfg.Accounts == nil {
		return nil, bcode.ErrForbidden
	}
	for i := range components {
		comp := &components[i]
		if comp.Namespace != "" && comp.Namespace != scope.Namespace {
			return nil, bcode.ErrForbidden
		}
		comp.Namespace = scope.Namespace
		if err := workspace.ValidateTraits(namespace, comp.Name, &comp.Traits, &comp.Properties, cfg.Accounts.Workspace); err != nil {
			return nil, err
		}
	}
	return components, nil
}

func decodeJSONStruct(raw *model.JSONStruct, target interface{}) error {
	if raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	if string(data) == "null" {
		return nil
	}
	return json.Unmarshal(data, target)
}

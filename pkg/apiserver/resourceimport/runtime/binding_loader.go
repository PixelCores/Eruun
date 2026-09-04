package runtime

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// NewDataStoreBindingLoader reads component label claims and adopted source
// workload bindings directly from application records. Keeping this in the
// infrastructure layer avoids routing the leader-only coordinator through
// request-scoped application services.
func NewDataStoreBindingLoader(store datastore.DataStore) BindingLoader {
	return func(ctx context.Context) ([]SourceBinding, error) {
		if store == nil {
			return nil, nil
		}
		entities, err := store.List(ctx, &model.Applications{}, &datastore.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list applications for adopted source bindings: %w", err)
		}
		componentEntities, err := store.List(ctx, &model.ApplicationComponent{}, &datastore.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list components for adopted source bindings: %w", err)
		}
		componentsByAppID := make(map[string][]*model.ApplicationComponent)
		for _, entity := range componentEntities {
			component, ok := entity.(*model.ApplicationComponent)
			if !ok || component == nil {
				continue
			}
			appID := strings.TrimSpace(component.AppID)
			if appID == "" {
				continue
			}
			componentsByAppID[appID] = append(componentsByAppID[appID], component)
		}

		bindings := make([]SourceBinding, 0)
		for _, entity := range entities {
			app, ok := entity.(*model.Applications)
			if !ok || app == nil {
				continue
			}
			mode := app.EffectiveManagementMode()
			for _, component := range componentsByAppID[app.ID] {
				namespace := strings.TrimSpace(component.Namespace)
				if namespace == "" {
					namespace = strings.TrimSpace(app.Namespace)
				}
				binding := SourceBinding{
					Namespace:      namespace,
					AppID:          app.ID,
					ComponentID:    component.ID,
					ComponentName:  component.Name,
					ManagementMode: mode,
				}
				if mode == config.ManagementModeAdopted && component.HasSourceWorkload() {
					binding.WorkloadAPIVersion = component.SourceWorkloadAPIVersion
					binding.WorkloadKind = component.SourceWorkloadKind
					binding.WorkloadName = component.SourceWorkloadName
					binding.WorkloadUID = types.UID(*component.SourceWorkloadUID)
				}
				bindings = append(bindings, binding)
			}
		}
		return bindings, nil
	}
}

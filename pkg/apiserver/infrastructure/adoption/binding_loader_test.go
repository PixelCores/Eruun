package adoption

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type bindingLoaderStore struct {
	datastore.DataStore
	apps               []*model.Applications
	components         map[string][]*model.ApplicationComponent
	appListCalls       int
	componentListCalls int
}

func (s *bindingLoaderStore) List(
	_ context.Context,
	query datastore.Entity,
	_ *datastore.ListOptions,
) ([]datastore.Entity, error) {
	switch typed := query.(type) {
	case *model.Applications:
		s.appListCalls++
		result := make([]datastore.Entity, 0, len(s.apps))
		for _, app := range s.apps {
			result = append(result, app)
		}
		return result, nil
	case *model.ApplicationComponent:
		s.componentListCalls++
		components := s.components[typed.AppID]
		if typed.AppID == "" {
			for _, app := range s.apps {
				components = append(components, s.components[app.ID]...)
			}
		}
		result := make([]datastore.Entity, 0, len(components))
		for _, component := range components {
			result = append(result, component)
		}
		return result, nil
	default:
		return nil, nil
	}
}

func TestDataStoreBindingLoaderReturnsAllClaimsAndOnlyAdoptedSources(t *testing.T) {
	adoptedUID := "deployment-uid"
	observeUID := "observe-deployment-uid"
	store := &bindingLoaderStore{
		apps: []*model.Applications{
			{ID: "native", Namespace: "production", ManagementMode: config.ManagementModeNative},
			{ID: "observe", Namespace: "production", ManagementMode: config.ManagementModeObserve},
			{ID: "adopted", Namespace: "production", ManagementMode: config.ManagementModeAdopted},
		},
		components: map[string][]*model.ApplicationComponent{
			"native": {
				{ID: 40, AppID: "native", Name: "frontend", Namespace: "production"},
			},
			"observe": {
				{
					ID:                       41,
					AppID:                    "observe",
					Name:                     "socket",
					Namespace:                "production",
					SourceWorkloadAPIVersion: "apps/v1",
					SourceWorkloadKind:       "Deployment",
					SourceWorkloadName:       "legacy-socket",
					SourceWorkloadUID:        &observeUID,
				},
			},
			"adopted": {
				{
					ID:                       42,
					AppID:                    "adopted",
					Name:                     "backend",
					Namespace:                "production",
					SourceWorkloadAPIVersion: "apps/v1",
					SourceWorkloadKind:       "Deployment",
					SourceWorkloadName:       "legacy-backend",
					SourceWorkloadUID:        &adoptedUID,
				},
			},
		},
	}

	bindings, err := NewDataStoreBindingLoader(store)(context.Background())
	require.NoError(t, err)
	require.Len(t, bindings, 3)
	byAppID := make(map[string]SourceBinding, len(bindings))
	for _, binding := range bindings {
		byAppID[binding.AppID] = binding
	}
	require.True(t, byAppID["native"].labelClaimValid())
	require.False(t, byAppID["native"].valid())
	require.True(t, byAppID["observe"].labelClaimValid())
	require.False(t, byAppID["observe"].valid())
	require.True(t, byAppID["observe"].readOnly())
	require.Empty(t, byAppID["observe"].WorkloadUID)
	require.True(t, byAppID["adopted"].valid())
	require.False(t, byAppID["adopted"].readOnly())
	require.Equal(t, "deployment-uid", string(byAppID["adopted"].WorkloadUID))
	require.Equal(t, 1, store.appListCalls)
	require.Equal(t, 1, store.componentListCalls)
}

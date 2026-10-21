package validation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

type profileValidationStore struct{ datastore.DataStore }

func (profileValidationStore) Get(_ context.Context, entity datastore.Entity) error {
	switch item := entity.(type) {
	case *model.NodePool:
		if item.Name == "gpu" {
			return nil
		}
	}
	return datastore.ErrRecordNotExist
}

func TestTryApplicationResolvesProfiles(t *testing.T) {
	nodes, err := repository.NewNodePoolRepositoryWithStore(profileValidationStore{})
	require.NoError(t, err)
	service := NewValidationService(nil, nil, nil, nil, nodes)
	for _, tc := range []struct {
		name   string
		traits spec.Traits
		fields []string
	}{
		{"known", spec.Traits{NodePool: "gpu"}, nil},
		{"missing", spec.Traits{NodePool: "missing"}, []string{"component[0].traits.nodePool"}},
		{"nested init", spec.Traits{Init: []spec.InitTraitSpec{{Name: "init", Image: "busybox:1.37", Traits: spec.Traits{NodePool: "gpu"}}}}, []string{"component[0].traits.init[0].traits.nodePool"}},
		{"nested sidecar", spec.Traits{Sidecar: []spec.SidecarTraitsSpec{{Name: "sidecar", Image: "busybox:1.37", Traits: spec.Traits{NodePool: "gpu"}}}}, []string{"component[0].traits.sidecar[0].traits.nodePool"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := service.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{Name: "demo", Namespace: "default", Components: []apisv1.CreateComponentRequest{{Name: "api", ComponentType: config.ServerJob, Image: "nginx:1.27", Traits: tc.traits}}})
			if len(tc.fields) == 0 {
				require.True(t, response.Valid, "%+v", response.Errors)
			}
			for _, field := range tc.fields {
				requireValidationError(t, response.Errors, field, apisv1.ErrCodeInvalidTraitConfig)
			}
		})
	}
}

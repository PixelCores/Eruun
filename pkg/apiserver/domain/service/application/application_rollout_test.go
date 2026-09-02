package application

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestPrepareComponentsRejectsRolloutForUnsupportedComponent(t *testing.T) {
	_, err := prepareComponents("app-1", "default", []apisv1.CreateComponentRequest{
		{
			Name:          "task",
			ComponentType: config.InstantJob,
			Image:         "busybox:1.36",
			Traits: apisv1.Traits{
				Rollout: &spec.RolloutTraitSpec{Type: "RollingUpdate"},
			},
		},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "rollout is only supported")
}

func TestPrepareComponentsRejectsNestedRollout(t *testing.T) {
	_, err := prepareComponents("app-1", "default", []apisv1.CreateComponentRequest{
		{
			Name:          "api",
			ComponentType: config.ServerJob,
			Image:         "nginx:1.25",
			Traits: apisv1.Traits{
				Sidecar: []spec.SidecarTraitsSpec{
					{
						Name:  "agent",
						Image: "busybox:1.36",
						Traits: spec.Traits{
							Rollout: &spec.RolloutTraitSpec{Type: "RollingUpdate"},
						},
					},
				},
			},
		},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "workload-level")
}

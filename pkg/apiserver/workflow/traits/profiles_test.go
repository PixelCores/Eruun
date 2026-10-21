package traits

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestApplyTraitsRequiresResolvedProfiles(t *testing.T) {
	for _, tc := range []struct {
		name     string
		traits   spec.Traits
		profiles Profiles
	}{
		{"missing nodePool", spec.Traits{NodePool: "gpu"}, Profiles{}},
		{"different nodePool", spec.Traits{NodePool: "gpu"}, Profiles{NodePool: &model.NodePool{Name: "cpu"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &model.ApplicationComponent{Name: "api", Traits: toJSONStruct(tc.traits)}
			workload := &appsv1.Deployment{}
			before := workload.DeepCopy()
			_, err := ApplyTraits(component, workload, tc.profiles)
			require.ErrorContains(t, err, "was not resolved for deployment")
			require.Equal(t, before, workload)
		})
	}
}

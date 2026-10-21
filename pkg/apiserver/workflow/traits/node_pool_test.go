package traits

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestApplyTraitsRequiresResolvedNodePool(t *testing.T) {
	for _, tc := range []struct {
		name     string
		traits   spec.Traits
		nodePool *model.NodePool
	}{
		{"missing nodePool", spec.Traits{NodePool: "gpu"}, nil},
		{"different nodePool", spec.Traits{NodePool: "gpu"}, &model.NodePool{Name: "cpu"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &model.ApplicationComponent{Name: "api", Traits: toJSONStruct(tc.traits)}
			workload := &appsv1.Deployment{}
			before := workload.DeepCopy()
			_, err := ApplyTraits(component, workload, tc.nodePool)
			require.ErrorContains(t, err, "was not resolved for deployment")
			require.Equal(t, before, workload)
		})
	}
}

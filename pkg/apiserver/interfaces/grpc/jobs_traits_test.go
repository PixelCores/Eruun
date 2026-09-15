package grpcapi

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestJobSecurityPolicyTraitsRoundTrip(t *testing.T) {
	input := &eruunv1.JobTraits{
		SecurityPolicy: &eruunv1.AppKubeCoreSecurityContext{
			RunAsUser: ptr.To(int64(1000)), RunAsNonRoot: ptr.To(false),
			Capabilities:   &eruunv1.AppKubeCoreCapabilities{Drop: []string{"ALL"}},
			SeccompProfile: &eruunv1.AppKubeCoreSeccompProfile{Type: "RuntimeDefault"},
		},
		Resources: &eruunv1.JobResourceTrait{Cpu: "100m", Memory: "128Mi"},
	}
	traits, err := jobTraitsInput(input)
	require.NoError(t, err)
	require.NotNil(t, traits.SecurityPolicy)
	require.EqualValues(t, 1000, *traits.SecurityPolicy.RunAsUser)
	require.NotNil(t, traits.SecurityPolicy.RunAsNonRoot)
	require.False(t, *traits.SecurityPolicy.RunAsNonRoot)
	require.Equal(t, []corev1.Capability{"ALL"}, traits.SecurityPolicy.Capabilities.Drop)
	require.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, traits.SecurityPolicy.SeccompProfile.Type)
	require.Equal(t, "100m", traits.Resources.CPU)

	job, err := jobSpecOutput(spec.JobSpec{
		Name: "run", Type: "command", Spec: json.RawMessage(`{"image":"busybox:1.37.0"}`), Traits: traits,
	})
	require.NoError(t, err)
	require.NotNil(t, job.Traits.SecurityPolicy)
	require.NotNil(t, job.Traits.SecurityPolicy.RunAsNonRoot)
	require.False(t, *job.Traits.SecurityPolicy.RunAsNonRoot)
	require.Equal(t, input.SecurityPolicy.GetRunAsUser(), job.Traits.SecurityPolicy.GetRunAsUser())
	require.Equal(t, input.SecurityPolicy.Capabilities.Drop, job.Traits.SecurityPolicy.Capabilities.Drop)
	require.Equal(t, input.SecurityPolicy.SeccompProfile.Type, job.Traits.SecurityPolicy.SeccompProfile.Type)
	require.Equal(t, "100m", job.Traits.Resources.Cpu)
}

func TestJobTraitsWithoutSecurityPolicy(t *testing.T) {
	input, err := jobTraitsInput(&eruunv1.JobTraits{Resources: &eruunv1.JobResourceTrait{Cpu: "1", Memory: "2Gi"}})
	require.NoError(t, err)
	require.Nil(t, input.SecurityPolicy)
	require.Equal(t, "1", input.Resources.CPU)

	output, err := jobTraitsOutput(input)
	require.NoError(t, err)
	require.Nil(t, output.SecurityPolicy)
	require.Equal(t, "1", output.Resources.Cpu)

	missing, err := jobTraitsInput(nil)
	require.NoError(t, err)
	require.Nil(t, missing.SecurityPolicy)
}

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestDefaultWorkflowImagePullPolicy(t *testing.T) {
	require.Equal(t, corev1.PullAlways, DefaultWorkflowImagePullPolicy)
}

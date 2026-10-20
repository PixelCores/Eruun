package jobdeploy

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestWorkloadRestartPatchContainsKubectlAnnotation(t *testing.T) {
	patch, err := buildWorkloadRestartPatch("2026-06-18T00:00:00Z")
	require.NoError(t, err)
	require.True(t, strings.Contains(string(patch), config.AnnotationWorkloadRestartAt))
}

func TestFormatVersionRestartAtPreservesNanoseconds(t *testing.T) {
	now := time.Date(2026, time.August, 1, 12, 34, 56, 123456789, time.FixedZone("UTC+8", 8*60*60))

	require.Equal(t, "2026-08-01T04:34:56.123456789Z", formatVersionRestartAt(now))
}

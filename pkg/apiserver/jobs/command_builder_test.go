package jobs

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/stretchr/testify/require"
)

func TestBuildCommandJobUsesReferencesWithoutApplication(t *testing.T) {
	value := "hello"
	traits := spec.JobTraits{Resources: &spec.ResourceTraitsSpec{CPU: "1", Memory: "2Gi"},
		Envs: []spec.SimplifiedEnvSpec{{Name: "MESSAGE", ValueFrom: spec.ValueSource{Static: &value}},
			{Name: "TOKEN", ValueFrom: spec.ValueSource{Secret: &spec.SecretSelectorSpec{Name: "credentials", Key: "token"}}}},
		EnvFrom: []spec.EnvFromSourceSpec{{Type: "config", SourceName: "settings"}},
		Storage: []spec.StorageTraitSpec{{Name: "input", Type: "persistent", ClaimName: "uploaded-input", MountPath: "/input", ReadOnly: true}}}
	job, err := buildCommandJob("command-task-1", "space", spec.CommandJobSpec{Image: "busybox:1.37.0", Command: []string{"echo"}, Args: []string{"hello"}, TimeoutSeconds: 60}, traits)
	require.NoError(t, err)
	require.Equal(t, "space", job.Namespace)
	require.Empty(t, job.Labels[config.LabelAppID])
	require.EqualValues(t, 0, *job.Spec.BackoffLimit)
	require.False(t, *job.Spec.Template.Spec.AutomountServiceAccountToken)
	require.Nil(t, job.Spec.TTLSecondsAfterFinished, "durable checkpoint establishes retention before creation")
	container := job.Spec.Template.Spec.Containers[0]
	require.Equal(t, "credentials", container.Env[1].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "settings", container.EnvFrom[0].ConfigMapRef.Name)
	require.Equal(t, "uploaded-input", job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName)
	require.Equal(t, "2Gi", container.Resources.Limits.Memory().String())
	var policy workflowconfig.JobRetryPolicy
	require.NoError(t, json.Unmarshal([]byte(job.Annotations[workflowconfig.AnnotationJobRetryPolicy]), &policy))
	require.NoError(t, policy.Validate())
	require.Equal(t, "stop", policy.OnOOM)
	require.Zero(t, policy.MaxRetries)
}

// Application-only traits (sidecar, ingress, rollout, ...) are absent from
// spec.JobTraits, so they cannot reach this function at all. Their rejection is
// covered at the decode boundary in TestJobSpecRejectsApplicationOnlyTraits.
func TestBuildCommandJobRejectsAmbiguousOrUnsupportedTraits(t *testing.T) {
	literal := "value"
	for _, tc := range []struct {
		name   string
		traits spec.JobTraits
	}{
		{"dynamic storage", spec.JobTraits{Storage: []spec.StorageTraitSpec{{Name: "data", Type: "persistent", MountPath: "/data", TmpCreate: true}}}},
		{"host mount", spec.JobTraits{Storage: []spec.StorageTraitSpec{{Name: "host", Type: "host-mounted", MountPath: "/host"}}}},
		{"ambiguous env", spec.JobTraits{Envs: []spec.SimplifiedEnvSpec{{Name: "TOKEN", ValueFrom: spec.ValueSource{Static: &literal, Secret: &spec.SecretSelectorSpec{Name: "s", Key: "token"}}}}}},
		{"invalid secret", spec.JobTraits{EnvFrom: []spec.EnvFromSourceSpec{{Type: "secret", SourceName: "../other"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildCommandJob("command-task", "space", spec.CommandJobSpec{Image: "busybox:1.37.0", Command: []string{"true"}, TimeoutSeconds: 60}, tc.traits)
			require.Error(t, err)
		})
	}
}

package jobs

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestBuildCommandJobUsesReferencesWithoutApplication(t *testing.T) {
	value := "hello"
	traits := spec.Traits{Resources: &spec.ResourceTraitsSpec{CPU: "1", Memory: "2Gi"},
		Envs: []spec.SimplifiedEnvSpec{{Name: "MESSAGE", ValueFrom: spec.ValueSource{Static: &value}},
			{Name: "TOKEN", ValueFrom: spec.ValueSource{Secret: &spec.SecretSelectorSpec{Name: "credentials", Key: "token"}}}},
		EnvFrom: []spec.EnvFromSourceSpec{{Type: "config", SourceName: "settings"}},
		Storage: []spec.StorageTraitSpec{{Name: "input", Type: "persistent", ClaimName: "uploaded-input", MountPath: "/input", ReadOnly: true}}}
	job, err := buildCommandJob("command-task-1", "space", spec.CommandJobSpec{Image: "busybox:1.37.0", Command: []string{"echo"}, Args: []string{"hello"}, TimeoutSeconds: 60}, traits, nil)
	require.NoError(t, err)
	require.Equal(t, "space", job.Namespace)
	require.Empty(t, job.Labels[spec.LabelAppID])
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

func TestBuildCommandJobAppliesNestedComponentTraits(t *testing.T) {
	value := "ready"
	traits := spec.Traits{
		NodePool:       "batch",
		SecurityPolicy: &spec.SecurityPolicySpec{RunAsNonRoot: ptr.To(true)},
		Probes:         []spec.ProbeTraitsSpec{{Type: "startup", Exec: &spec.ExecProbe{Command: []string{"true"}}}},
		Init: []spec.InitTraitSpec{{Name: "prepare", Image: "busybox:1.37.0",
			Properties: spec.Properties{Command: []string{"true"}},
			Traits:     spec.Traits{Envs: []spec.SimplifiedEnvSpec{{Name: "STATE", ValueFrom: spec.ValueSource{Static: &value}}}}}},
		Sidecar: []spec.SidecarTraitsSpec{{Name: "observe", Image: "busybox:1.37.0", Command: []string{"true"},
			Traits: spec.Traits{Resources: &spec.ResourceTraitsSpec{CPU: "100m", Memory: "32Mi"}}}},
	}
	job, err := buildCommandJob("command-task", "space", spec.CommandJobSpec{Image: "busybox:1.37.0", Command: []string{"true"}, TimeoutSeconds: 60}, traits, &model.NodePool{Name: "batch", NodeSelector: map[string]string{"pool": "batch"}})
	require.NoError(t, err)
	pod := job.Spec.Template.Spec
	require.Equal(t, "batch", pod.NodeSelector["pool"])
	require.True(t, *pod.Containers[0].SecurityContext.RunAsNonRoot)
	require.Equal(t, []string{"true"}, pod.Containers[0].StartupProbe.Exec.Command)
	require.Len(t, pod.InitContainers, 1)
	require.Equal(t, "ready", pod.InitContainers[0].Env[0].Value)
	require.Len(t, pod.Containers, 2)
	require.Equal(t, "100m", pod.Containers[1].Resources.Requests.Cpu().String())
	require.Equal(t, "32Mi", pod.Containers[1].Resources.Limits.Memory().String())
}

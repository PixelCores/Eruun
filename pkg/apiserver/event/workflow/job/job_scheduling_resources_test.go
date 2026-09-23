package job

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

func TestAdmissionPodResourceAccountsForInitSidecarsAndOverhead(t *testing.T) {
	container := func(cpu string) corev1.Container {
		return corev1.Container{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}}}
	}
	for _, tc := range []struct {
		name   string
		change func(*corev1.PodSpec)
		want   string
		known  bool
	}{
		{"main sum", func(pod *corev1.PodSpec) {}, "3", true},
		{"sequential init maximum", func(pod *corev1.PodSpec) { pod.InitContainers = []corev1.Container{container("5"), container("4")} }, "5", true},
		{"restartable init overlap", func(pod *corev1.PodSpec) {
			sidecar := container("2")
			sidecar.RestartPolicy = ptr.To(corev1.ContainerRestartPolicyAlways)
			pod.InitContainers = []corev1.Container{sidecar, container("4")}
		}, "6", true},
		{"overhead", func(pod *corev1.PodSpec) {
			pod.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}
		}, "3100m", true},
		{"missing container request", func(pod *corev1.PodSpec) { pod.Containers[0].Resources.Requests = nil }, "0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.PodSpec{Containers: []corev1.Container{container("1"), container("2")}}
			tc.change(pod)
			q, known := admissionPodResource(pod, corev1.ResourceCPU, false)
			require.Equal(t, tc.known, known)
			require.Equal(t, tc.want, q.String())
		})
	}
}

func TestJobSchedulingResourceSnapshotKeepsOnlyQuantities(t *testing.T) {
	workload := &batchv1.Job{Spec: batchv1.JobSpec{Parallelism: ptr.To(int32(2)), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Image: "private.example/image:1", Env: []corev1.EnvVar{{Name: "TOKEN", Value: "do-not-copy"}},
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")}}}},
	}}}}
	before := workload.DeepCopy()
	raw, err := jobSchedulingResourceSnapshot(&model.JobTask{JobType: string(config.JobCommand), JobInfo: workload})
	require.NoError(t, err)
	require.NotContains(t, raw, "TOKEN")
	require.NotContains(t, raw, "private.example")
	require.NotContains(t, raw, "do-not-copy")
	demand := corev1.ResourceList{}
	require.NoError(t, json.Unmarshal([]byte(raw), &demand))
	for key, value := range map[corev1.ResourceName]string{corev1.ResourcePods: "2", corev1.ResourceRequestsCPU: "1", corev1.ResourceRequestsMemory: "2Gi", corev1.ResourceLimitsCPU: "2", corev1.ResourceLimitsMemory: "4Gi"} {
		q := demand[key]
		require.Equal(t, value, q.String())
	}
	require.NotContains(t, demand, corev1.ResourceRequestsEphemeralStorage, "missing Pod declaration remains unknown")
	require.Equal(t, before, workload, "snapshot must not mutate the submitted Pod")
}

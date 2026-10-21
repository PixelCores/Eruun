package jobs

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestCommandJobResolvesNodePoolAtSubmitAndDeployment(t *testing.T) {
	service, raw, ctx := testJobService(t)
	require.NoError(t, raw.Client.AutoMigrate(&model.NodePool{}))
	request := commandRequest()
	request.Stages[0].Steps[0].Traits.NodePool = "batch"
	_, err := service.Submit(ctx, request)
	require.Error(t, err)
	require.Contains(t, bcode.SafeClientMessage(err), "batch")
	pool := &model.NodePool{Name: "batch", NodeSelector: map[string]string{"pool": "batch"}, Tolerations: []corev1.Toleration{{Key: "batch", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}}
	require.NoError(t, raw.Add(ctx, pool))
	accepted, err := service.Submit(ctx, request)
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: accepted.TaskID}
	require.NoError(t, raw.Get(ctx, parent))
	stages, err := BuildTask(ctx, service.Store, parent, "space-ns")
	require.NoError(t, err)
	pod := stages[0][0].JobInfo.(*batchv1.Job).Spec.Template.Spec
	require.Equal(t, pool.NodeSelector, pod.NodeSelector)
	require.Equal(t, pool.Tolerations, pod.Tolerations)
	require.NoError(t, raw.Delete(ctx, pool))
	_, err = BuildTask(ctx, service.Store, parent, "space-ns")
	require.ErrorContains(t, err, "batch")
	// Node pools are rejected below component scope.
	for _, payload := range []string{
		`{"init":[{"name":"init","image":"busybox:1.37","traits":{"nodePool":"batch"}}]}`,
		`{"sidecar":[{"name":"sidecar","image":"busybox:1.37","traits":{"nodePool":"batch"}}]}`,
	} {
		request = commandRequest()
		require.NoError(t, json.Unmarshal([]byte(payload), &request.Stages[0].Steps[0].Traits))
		_, err = service.Submit(ctx, request)
		require.Error(t, err)
	}
}

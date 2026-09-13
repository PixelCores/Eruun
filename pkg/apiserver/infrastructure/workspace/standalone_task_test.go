package workspace

import (
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestStandaloneCommandTaskRequiresWorkspaceIdentityAndUsesPodPolicy(t *testing.T) {
	space := &model.Workspace{ID: "space", Namespace: "own"}
	for _, tc := range []struct {
		name    string
		mutate  func(*model.JobTask)
		allowed bool
	}{
		{"workspace command", func(*model.JobTask) {}, true},
		{"application ownership", func(task *model.JobTask) { task.AppID = "app" }, false},
		{"foreign workspace", func(task *model.JobTask) { task.WorkspaceID = "other" }, false},
		{"missing task", func(task *model.JobTask) { task.TaskID = "" }, false},
		{"foreign namespace", func(task *model.JobTask) { task.Namespace = "other" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "work", Namespace: "own"}, Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "job", Image: "busybox:1.37.0"}}}},
			}}
			task := &model.JobTask{TaskID: "task", WorkspaceID: "space", Namespace: "own", JobType: string(config.JobCommand), JobInfo: payload}
			tc.mutate(task)
			deploy, err := PrepareTask(task, "", space, workspaceConfig(t))
			if !tc.allowed {
				require.ErrorIs(t, err, bcode.ErrForbidden)
				return
			}
			require.NoError(t, err)
			require.True(t, deploy)
			require.False(t, *payload.Spec.Template.Spec.AutomountServiceAccountToken)
			require.True(t, *payload.Spec.Template.Spec.Containers[0].SecurityContext.RunAsNonRoot)
			require.False(t, *payload.Spec.Template.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation)
			require.Equal(t, []corev1.Capability{"ALL"}, payload.Spec.Template.Spec.Containers[0].SecurityContext.Capabilities.Drop)
		})
	}
}

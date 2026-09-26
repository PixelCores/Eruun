package jobs

import (
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPrepareEvaluationTaskIdentityAndRunnerPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		edit    func(*model.JobTask, *batchv1.Job)
		allowed bool
	}{
		{"runner", func(*model.JobTask, *batchv1.Job) {}, true},
		{"foreign workspace", func(t *model.JobTask, _ *batchv1.Job) { t.WorkspaceID = "other" }, false},
		{"foreign task namespace", func(t *model.JobTask, _ *batchv1.Job) { t.Namespace = "other" }, false},
		{"foreign payload namespace", func(_ *model.JobTask, j *batchv1.Job) { j.Namespace = "other" }, false},
		{"missing evaluation", func(t *model.JobTask, _ *batchv1.Job) { t.EvaluationInfo = "" }, false},
		{"wrong image", func(_ *model.JobTask, j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].Image = "other:1" }, false},
		{"wrong command", func(_ *model.JobTask, j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].Command = []string{"sh"} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "runner-job", Namespace: "own"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{ServiceAccountName: workspace.EvaluationRunnerName, Containers: []corev1.Container{{Name: "runner", Image: "runner:0.22.0", Command: []string{"python", "/opt/eruun/runner.py"}}}}}}}
			task := &model.JobTask{Name: payload.Name, Namespace: "own", WorkspaceID: "space", JobType: string(config.JobEval), EvaluationInfo: "{}", JobInfo: payload}
			tc.edit(task, payload)
			err := PrepareEvaluationTask(task, &model.Workspace{ID: "space", Namespace: "own"}, spec.WorkspaceConfig{}, "runner:0.22.0")
			if !tc.allowed {
				require.ErrorIs(t, err, bcode.ErrForbidden)
				return
			}
			require.NoError(t, err)
			require.Same(t, payload, task.JobInfo)
			require.Equal(t, workspace.EvaluationRunnerName, payload.Spec.Template.Spec.ServiceAccountName)
			require.True(t, *payload.Spec.Template.Spec.AutomountServiceAccountToken)
			require.True(t, *payload.Spec.Template.Spec.Containers[0].SecurityContext.RunAsNonRoot)
		})
	}
}

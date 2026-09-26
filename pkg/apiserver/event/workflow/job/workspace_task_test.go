package job

import (
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
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
			deploy, err := PrepareTask(task, "", space, taskWorkspaceConfig(t))
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

func taskWorkspaceConfig(t *testing.T) spec.WorkspaceConfig {
	t.Helper()
	c := &spec.AccountConfig{Origins: []string{"https://console.example.com"}, FrontendURL: "https://console.example.com", Workspace: spec.WorkspaceConfig{ClusterCIDRs: []string{"10.96.0.0/12", "10.244.0.0/16", "192.0.2.0/24"}, StorageClasses: []string{"tenant-storage"}, IngressDomain: "apps.example.com", IngressClass: "nginx", IngressNamespace: "ingress-nginx"}}
	require.NoError(t, c.Validate())
	return c.Workspace
}

func TestApplicationTaskIdentity(t *testing.T) {
	w := &model.Workspace{ID: "a", Namespace: "own"}
	cfg := taskWorkspaceConfig(t)
	task := &model.JobTask{AppID: "app", Namespace: "own", JobType: string(config.JobDeploy), JobInfo: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "own"}}}
	deploy, err := PrepareTask(task, "app", w, cfg)
	require.NoError(t, err)
	require.True(t, deploy)
	task.AppID = "other"
	_, err = PrepareTask(task, "app", w, cfg)
	require.ErrorIs(t, err, bcode.ErrForbidden)
	task.AppID = "app"
	task.JobType = string(config.JobDeployCloud)
	_, err = PrepareTask(task, "app", w, cfg)
	require.ErrorIs(t, err, bcode.ErrForbidden)
}

func TestPrepareScheduledPayloadPreservesPointerAndPolicy(t *testing.T) {
	for _, cron := range []bool{false, true} {
		pod := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "job", Image: "busybox:1.37.0"}}}}
		jobSpec := batchv1.JobSpec{Template: pod}
		payload := interface{}(&batchv1.Job{Spec: jobSpec})
		if cron {
			payload = &batchv1.CronJob{Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: jobSpec}}}
		}
		task := &model.JobTask{AppID: "app", Namespace: "own", JobType: string(config.JobDeployScheduled), JobInfo: payload}
		deploy, err := PrepareTask(task, "app", &model.Workspace{Namespace: "own"}, taskWorkspaceConfig(t))
		require.NoError(t, err)
		require.True(t, deploy)
		require.Same(t, payload, task.JobInfo)
		if cron {
			pod = payload.(*batchv1.CronJob).Spec.JobTemplate.Spec.Template
		} else {
			pod = payload.(*batchv1.Job).Spec.Template
		}
		require.False(t, *pod.Spec.AutomountServiceAccountToken)
		require.True(t, *pod.Spec.Containers[0].SecurityContext.RunAsNonRoot)
	}
}

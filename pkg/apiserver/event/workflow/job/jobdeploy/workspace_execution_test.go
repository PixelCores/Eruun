package jobdeploy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
)

func TestWorkspaceRepeatedDeploymentDoesNotRollout(t *testing.T) {
	space := &model.Workspace{ID: "workspace", Namespace: "space"}
	manager := &workspace.Manager{RESTConfig: &rest.Config{Host: "https://kubernetes.example", Transport: jobtest.WorkspaceRoundTripper(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		return jobtest.WorkspaceResponse(http.StatusCreated, body), err
	})}}
	client, _, err := manager.TenantClient(space)
	require.NoError(t, err)
	for _, localhost := range []bool{false, true} {
		podSpec := func() corev1.PodSpec {
			pod := jobtest.WorkspacePodSpec()
			if localhost {
				for _, containers := range [][]corev1.Container{pod.Containers, pod.InitContainers} {
					for i := range containers {
						containers[i].SecurityContext = &corev1.SecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: ptr.To("profiles/app.json")}}
					}
				}
			}
			return pod
		}
		for _, kind := range []string{"deployment", "statefulset"} {
			t.Run(fmt.Sprintf("%s/localhost=%t", kind, localhost), func(t *testing.T) {
				meta := metav1.ObjectMeta{Name: "app", Namespace: space.Namespace}
				template := corev1.PodTemplateSpec{Spec: podSpec()}
				task := &model.JobTask{AppID: "app", Namespace: space.Namespace}
				if kind == "deployment" {
					desired := &appsv1.Deployment{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{Template: template}}
					task.JobType, task.JobInfo = string(config.JobDeploy), desired
					_, err = jobexec.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					jobtest.RequireWorkspacePodSecurity(t, desired.Spec.Template.Spec)
					current, err := client.AppsV1().Deployments(space.Namespace).Create(context.Background(), desired, metav1.CreateOptions{})
					require.NoError(t, err)
					// Regenerate the same source payload, as the next workflow invocation does.
					desired.Spec.Template.Spec = podSpec()
					_, err = jobexec.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					require.False(t, isDeploymentChanged(current, desired))
					require.False(t, deploymentPodTemplateChanged(current, desired))
					desired.Spec.Template.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To(int64(1001))
					_, err = jobexec.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					require.True(t, isDeploymentChanged(current, desired), "real security changes must still be applied")
					require.True(t, deploymentPodTemplateChanged(current, desired))
				} else {
					desired := &appsv1.StatefulSet{ObjectMeta: meta, Spec: appsv1.StatefulSetSpec{Template: template}}
					task.JobType, task.JobInfo = string(config.JobDeployStore), desired
					_, err = jobexec.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					jobtest.RequireWorkspacePodSecurity(t, desired.Spec.Template.Spec)
					current, err := client.AppsV1().StatefulSets(space.Namespace).Create(context.Background(), desired, metav1.CreateOptions{})
					require.NoError(t, err)
					desired.Spec.Template.Spec = podSpec()
					_, err = jobexec.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					require.False(t, statefulSetPodTemplateChanged(current, desired))
					require.False(t, statefulSetNeedsUpdate(current, desired))
					desired.Spec.Template.Spec.InitContainers[0].SecurityContext.RunAsUser = ptr.To(int64(1001))
					_, err = jobexec.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					require.True(t, statefulSetPodTemplateChanged(current, desired))
					require.True(t, statefulSetNeedsUpdate(current, desired))
				}
			})
		}
	}
}

package jobexec

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
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
	c := &spec.AccountConfig{Origins: []string{"https://console.example.com"}, FrontendURL: "https://console.example.com", Workspace: spec.WorkspaceConfig{ClusterCIDRs: []string{"10.96.0.0/12", "10.244.0.0/16", "192.0.2.0/24"}, StorageClasses: []string{"tenant-storage"}}}
	require.NoError(t, c.Validate())
	c.Workspace.Gateway = spec.GatewayConfig{Provider: spec.GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"}
	require.NoError(t, c.Workspace.Gateway.Validate())
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

func TestRouteTaskUsesPayloadResourcePolicy(t *testing.T) {
	space := &model.Workspace{ID: "space", Namespace: "own"}
	route := func(host string) *unstructured.Unstructured {
		object := &unstructured.Unstructured{Object: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "web", "namespace": "own"},
			"spec": map[string]interface{}{
				"ingressClassName": "nginx",
				"rules":            []interface{}{map[string]interface{}{"host": host}},
			},
		}}
		object.SetGroupVersionKind(networkingv1.SchemeGroupVersion.WithKind("Ingress"))
		return object
	}

	payload := route("web-own.apps.example.com")
	task := &model.JobTask{AppID: "app", Namespace: "own", JobType: string(config.JobDeployRoute), JobInfo: payload}
	deploy, err := PrepareTask(task, "app", space, taskWorkspaceConfig(t))
	require.NoError(t, err)
	require.True(t, deploy)
	prepared, ok := task.JobInfo.(*unstructured.Unstructured)
	require.True(t, ok, "the prepared payload keeps its unstructured type")
	require.Same(t, payload, prepared)

	for _, host := range []string{"victim-other.apps.example.com", "web.own.apps.example.com"} {
		task.JobInfo = route(host)
		_, err = PrepareTask(task, "app", space, taskWorkspaceConfig(t))
		require.ErrorIs(t, err, bcode.ErrForbidden, host)
	}

	unsupported := route("web-own.apps.example.com")
	unsupported.SetAPIVersion("example.io/v1")
	task.JobInfo = unsupported
	_, err = PrepareTask(task, "app", space, taskWorkspaceConfig(t))
	require.ErrorIs(t, err, bcode.ErrForbidden)
}

// The workspace policy round-trips route payloads through JSON; numbers must
// keep their integer form so the next deployment does not see a changed spec.
func TestRouteTaskPreservesIntegerPorts(t *testing.T) {
	space := &model.Workspace{ID: "space", Namespace: "own"}
	pathType := networkingv1.PathTypePrefix
	className := "nginx"
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "own"},
		Spec: networkingv1.IngressSpec{IngressClassName: &className, Rules: []networkingv1.IngressRule{{
			Host: "web-own.apps.example.com",
			IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{Paths: []networkingv1.HTTPIngressPath{{
				Path: "/", PathType: &pathType,
				Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{Name: "web", Port: networkingv1.ServiceBackendPort{Number: 8080}}},
			}}}},
		}}},
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ingress)
	require.NoError(t, err)
	payload := &unstructured.Unstructured{Object: object}
	payload.SetGroupVersionKind(networkingv1.SchemeGroupVersion.WithKind("Ingress"))
	before := payload.DeepCopy()

	task := &model.JobTask{AppID: "app", Namespace: "own", JobType: string(config.JobDeployRoute), JobInfo: payload}
	_, err = PrepareTask(task, "app", space, taskWorkspaceConfig(t))
	require.NoError(t, err)
	rules, _, err := unstructured.NestedSlice(payload.Object, "spec", "rules")
	require.NoError(t, err)
	paths, _, err := unstructured.NestedSlice(rules[0].(map[string]interface{}), "http", "paths")
	require.NoError(t, err)
	port, found, err := unstructured.NestedInt64(paths[0].(map[string]interface{}), "backend", "service", "port", "number")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(8080), port)
	require.Equal(t, before.Object["spec"], payload.Object["spec"])
}

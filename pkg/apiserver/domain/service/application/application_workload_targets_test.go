package application

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func TestNativeRestartTargetsDoNotRequireRendering(t *testing.T) {
	for _, kind := range []config.JobType{config.ServerJob, config.StoreJob} {
		t.Run(string(kind), func(t *testing.T) {
			component := &model.ApplicationComponent{
				Name: "api", AppID: "app", ComponentType: kind,
				Properties: &model.JSONStruct{"ports": "invalid"},
				Traits:     &model.JSONStruct{"sidecar": "invalid"},
			}
			namespace := config.DefaultNamespace
			name := naming.WebServiceName(component.Name, component.ResourceNameKey())
			var object runtime.Object = &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
			if kind == config.StoreJob {
				name = naming.StoreServerName(component.Name, component.ResourceNameKey())
				object = &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
			} else {
				actualNamespace, actualName := resolveDeploymentTarget(component)
				require.Equal(t, namespace, actualNamespace)
				require.Equal(t, name, actualName)
			}
			client := fake.NewSimpleClientset(object)
			service := &applicationsServiceImpl{KubeClient: client}
			reporter := newRestartReporter()
			patch, err := buildRestartPatch("2026-09-26T00:00:00Z")
			require.NoError(t, err)
			restarted := service.restartNativeApplicationComponents(context.Background(), []*model.ApplicationComponent{component}, patch, reporter)
			require.NoError(t, reporter.err())
			require.Equal(t, []string{component.Name}, restarted)
			require.Len(t, client.Actions(), 1)
			require.Equal(t, "patch", client.Actions()[0].GetVerb())
			require.Equal(t, namespace, client.Actions()[0].GetNamespace())
		})
	}
}

package application

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

// Under the gatewayapi provider application cleanup lists the component's
// HTTPRoutes by owner labels through the request-scoped config and deletes
// them; it sends no Ingress request.
func TestCleanupApplicationResourcesDeletesHTTPRouteByLabels(t *testing.T) {
	app := &model.Applications{ID: "app-route", Name: "demo-route", Namespace: "default"}
	component := &model.ApplicationComponent{Name: "Web", AppID: app.ID, Namespace: "default", ComponentType: config.ServerJob, Replicas: 1, Image: "web:1"}
	store := &cleanupStore{app: app, components: []*model.ApplicationComponent{component}, applications: map[string]*model.Applications{app.ID: app}}

	const routes = "/apis/gateway.networking.k8s.io/v1/namespaces/default/httproutes"
	var (
		mu       sync.Mutex
		selector string
		deleted  []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == routes:
			selector = r.URL.Query().Get("labelSelector")
			_, _ = io.WriteString(w, `{"apiVersion":"gateway.networking.k8s.io/v1","kind":"HTTPRouteList","metadata":{},"items":[
				{"apiVersion":"gateway.networking.k8s.io/v1","kind":"HTTPRoute","metadata":{"name":"web-route","namespace":"default"}}]}`)
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, routes+"/"):
			deleted = append(deleted, strings.TrimPrefix(r.URL.Path, routes+"/"))
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Success"}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	clientset := fake.NewSimpleClientset()
	svc := &applicationsServiceImpl{
		Cfg: &config.Config{Gateway: spec.GatewayConfig{Provider: spec.GatewayProviderGatewayAPI, Domain: "apps.example.com", ProxyNamespace: "istio-ingress",
			ParentRef: spec.GatewayParentRef{Namespace: "istio-ingress", Name: "eruun", SectionName: spec.GatewayHTTPSListener}}},
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:     clientset,
		KubeConfig:     &rest.Config{Host: server.URL},
		Store:          store,
		AppRepo:        &mockCleanupAppRepo{store: store},
		ComponentRepo:  &mockCleanupComponentRepo{store: store},
	}

	resp, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.Contains(t, resp.DeletedResources, "HTTPRoute:default/web-route")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"web-route"}, deleted)
	parsed, err := labels.Parse(selector)
	require.NoError(t, err)
	require.True(t, parsed.Matches(labels.Set{spec.LabelAppID: app.ID, spec.LabelComponentName: naming.BoundedLabelValue(component.Name)}))
	require.False(t, parsed.Matches(labels.Set{spec.LabelAppID: "app-other", spec.LabelComponentName: naming.BoundedLabelValue(component.Name)}))
	for _, action := range clientset.Actions() {
		require.NotEqual(t, "ingresses", action.GetResource().Resource)
	}
}

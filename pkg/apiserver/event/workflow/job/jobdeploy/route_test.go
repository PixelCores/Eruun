package jobdeploy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

var ingressGVR = networkingv1.SchemeGroupVersion.WithResource("ingresses")

func TestRouteNeedsUpdate(t *testing.T) {
	baseSpec := ingressSpecForTest("example.com", "/", networkingv1.PathTypePrefix)
	changedSpec := ingressSpecForTest("example.com", "/api", networkingv1.PathTypePrefix)

	cases := []struct {
		name    string
		current *unstructured.Unstructured
		desired *unstructured.Unstructured
		want    bool
	}{
		{
			name:    "same spec and metadata",
			current: newRouteForTest(t, "ing", baseSpec, map[string]string{"app": "a"}, map[string]string{"k": "v"}),
			desired: newRouteForTest(t, "ing", baseSpec, map[string]string{"app": "a"}, map[string]string{"k": "v"}),
		},
		{
			name:    "spec changed",
			current: newRouteForTest(t, "ing", baseSpec, map[string]string{"app": "a"}, nil),
			desired: newRouteForTest(t, "ing", changedSpec, map[string]string{"app": "a"}, nil),
			want:    true,
		},
		{
			name:    "labels changed",
			current: newRouteForTest(t, "ing", baseSpec, map[string]string{"app": "a"}, nil),
			desired: newRouteForTest(t, "ing", baseSpec, map[string]string{"app": "b"}, nil),
			want:    true,
		},
		{
			name:    "extra labels removed triggers update",
			current: newRouteForTest(t, "ing", baseSpec, map[string]string{"app": "a", "extra": "1"}, nil),
			desired: newRouteForTest(t, "ing", baseSpec, map[string]string{"app": "a"}, nil),
			want:    true,
		},
		{
			name:    "system labels preserved",
			current: newRouteForTest(t, "ing", baseSpec, map[string]string{"app": "a", domainspec.LabelManagedBy: config.ManagedByEruun}, nil),
			desired: newRouteForTest(t, "ing", baseSpec, map[string]string{"app": "a"}, nil),
		},
		{
			name:    "annotations changed",
			current: newRouteForTest(t, "ing", baseSpec, nil, map[string]string{"k": "v"}),
			desired: newRouteForTest(t, "ing", baseSpec, nil, map[string]string{"k": "new"}),
			want:    true,
		},
		{
			name:    "annotations removed triggers update",
			current: newRouteForTest(t, "ing", baseSpec, nil, map[string]string{"k": "v"}),
			desired: newRouteForTest(t, "ing", baseSpec, nil, nil),
			want:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, routeNeedsUpdate(tc.current, tc.desired))
		})
	}
}

func TestDeployRouteJobCreatesUpdatesWaitsAndCleansIngress(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{ingressGVR: "IngressList"})
	jobTask := &model.JobTask{
		Name:      "web-ingress",
		Namespace: "team-a",
		JobType:   string(config.JobDeployRoute),
		JobInfo:   newRouteForTest(t, "web-ingress", ingressSpecForTest("web.team-a.apps.example.com", "/", networkingv1.PathTypePrefix), map[string]string{"app": "web"}, nil),
	}
	ctl := NewDeployRouteJobCtl(jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: &jobtest.NoopStore{}, Dynamic: dynamicClient}, locker.NewNoopLocker(jobtest.ShareLockerPrefix))
	require.NotNil(t, ctl)

	ctx := jobexec.WithCleanupTracker(context.Background())
	require.NoError(t, ctl.run(ctx))
	created, err := dynamicClient.Resource(ingressGVR).Namespace("team-a").Get(ctx, "web-ingress", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "web", created.GetLabels()["app"])
	require.NoError(t, ctl.wait(ctx))

	jobTask.JobInfo = newRouteForTest(t, "web-ingress", ingressSpecForTest("web.team-a.apps.example.com", "/api", networkingv1.PathTypePrefix), map[string]string{"app": "web"}, nil)
	require.NoError(t, ctl.run(ctx))
	updated, err := dynamicClient.Resource(ingressGVR).Namespace("team-a").Get(ctx, "web-ingress", metav1.GetOptions{})
	require.NoError(t, err)
	var ingress networkingv1.Ingress
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(updated.Object, &ingress))
	require.Equal(t, "/api", ingress.Spec.Rules[0].HTTP.Paths[0].Path)

	ctl.Clean(ctx)
	_, err = dynamicClient.Resource(ingressGVR).Namespace("team-a").Get(ctx, "web-ingress", metav1.GetOptions{})
	require.Error(t, err, "a route created by this job must be removed by failure cleanup")
}

func TestDeployRouteJobRejectsMissingClientAndUnsupportedPayload(t *testing.T) {
	require.Nil(t, NewDeployRouteJobCtl(nil, &jobexec.Runtime{}, nil))

	ackCount := 0
	jobTask := &model.JobTask{
		Name:      "web-ingress",
		Namespace: "default",
		JobType:   string(config.JobDeployRoute),
		JobInfo:   newRouteForTest(t, "web-ingress", ingressSpecForTest("web.example.com", "/", networkingv1.PathTypePrefix), nil, nil),
	}
	ctl := NewDeployRouteJobCtl(jobTask, &jobexec.Runtime{Store: &jobtest.NoopStore{}, Ack: func() { ackCount++ }}, locker.NewNoopLocker(jobtest.ShareLockerPrefix))
	require.NotNil(t, ctl)
	require.Error(t, ctl.Run(context.Background()))
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusFailed, jobTask.Status)

	ctl.Dynamic = dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	unsupported := &unstructured.Unstructured{}
	unsupported.SetAPIVersion("example.io/v1")
	unsupported.SetKind("Route")
	jobTask.JobInfo = unsupported
	require.ErrorContains(t, ctl.run(context.Background()), "unsupported route kind")

	jobTask.JobInfo = newRouteForTest(t, "web-ingress", ingressSpecForTest("web.example.com", "/", networkingv1.PathTypePrefix), nil, nil)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	statusErr, ok := jobinfo.ExtractStatusError(ctl.wait(cancelled))
	require.True(t, ok)
	require.Equal(t, config.StatusCancelled, statusErr.Status)

	ctl.Job.Timeout = 0
	require.Greater(t, ctl.timeout(), int64(0))
}

func ingressSpecForTest(host, path string, pathType networkingv1.PathType) networkingv1.IngressSpec {
	pt := pathType
	return networkingv1.IngressSpec{
		Rules: []networkingv1.IngressRule{{
			Host: host,
			IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
				Paths: []networkingv1.HTTPIngressPath{{
					Path:     path,
					PathType: &pt,
					Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
						Name: "svc",
						Port: networkingv1.ServiceBackendPort{Number: 80},
					}},
				}},
			}},
		}},
	}
}

func newRouteForTest(t *testing.T, name string, spec networkingv1.IngressSpec, labels, annotations map[string]string) *unstructured.Unstructured {
	t.Helper()
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", Labels: labels, Annotations: annotations},
		Spec:       spec,
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ingress)
	require.NoError(t, err)
	route := &unstructured.Unstructured{Object: object}
	route.SetGroupVersionKind(networkingv1.SchemeGroupVersion.WithKind("Ingress"))
	return route
}

// An existing route is only observed by this job: failure cleanup must keep it,
// and server-populated metadata and status must not cause another Update.
func TestDeployRouteJobKeepsExistingRouteAndSkipsUnchangedUpdate(t *testing.T) {
	desired := newRouteForTest(t, "web-ingress", ingressSpecForTest("web.team-a.apps.example.com", "/", networkingv1.PathTypePrefix), map[string]string{"app": "web"}, nil)
	existing := desired.DeepCopy()
	existing.SetResourceVersion("7")
	existing.SetUID("uid-1")
	existing.SetLabels(map[string]string{"app": "web", domainspec.LabelManagedBy: config.ManagedByEruun})
	require.NoError(t, unstructured.SetNestedSlice(existing.Object, []interface{}{map[string]interface{}{"ip": "203.0.113.10"}}, "status", "loadBalancer", "ingress"))
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{ingressGVR: "IngressList"}, existing)

	jobTask := &model.JobTask{Name: "web-ingress", Namespace: "team-a", JobType: string(config.JobDeployRoute), JobInfo: desired}
	ctl := NewDeployRouteJobCtl(jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: &jobtest.NoopStore{}, Dynamic: dynamicClient}, locker.NewNoopLocker(jobtest.ShareLockerPrefix))
	ctx := jobexec.WithCleanupTracker(context.Background())
	require.NoError(t, ctl.run(ctx))
	for _, action := range dynamicClient.Actions() {
		require.NotEqual(t, "update", action.GetVerb(), "an unchanged route must not be updated")
	}

	ctl.Clean(ctx)
	_, err := dynamicClient.Resource(ingressGVR).Namespace("team-a").Get(context.Background(), "web-ingress", metav1.GetOptions{})
	require.NoError(t, err, "failure cleanup must keep a route this job did not create")
}

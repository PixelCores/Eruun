package jobdeploy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
)

// A resource delete removes only an object this component owns: a missing
// object, another component's object and a shared object all leave the job
// successful. Routes and Services follow the same rules.
func TestDeleteResourceJobDeletesOnlyOwnedUnsharedObject(t *testing.T) {
	owner := map[string]string{domainspec.LabelAppID: "app-1", domainspec.LabelComponentName: "web"}
	withLabels := func(extra map[string]string) map[string]string {
		labels := map[string]string{}
		for k, v := range owner {
			labels[k] = v
		}
		for k, v := range extra {
			labels[k] = v
		}
		return labels
	}
	serviceGVR := corev1.SchemeGroupVersion.WithResource("services")
	kinds := []struct {
		kind string
		gvk  schema.GroupVersionKind
		gvr  schema.GroupVersionResource
		list string
	}{
		{kind: "ingress", gvk: networkingv1.SchemeGroupVersion.WithKind("Ingress"), gvr: ingressGVR, list: "IngressList"},
		{kind: "service", gvk: corev1.SchemeGroupVersion.WithKind("Service"), gvr: serviceGVR, list: "ServiceList"},
	}
	cases := []struct {
		name        string
		existing    map[string]string
		wantDeleted bool
	}{
		{name: "missing"},
		{name: "owned", existing: withLabels(nil), wantDeleted: true},
		{name: "other app", existing: withLabels(map[string]string{domainspec.LabelAppID: "app-2"})},
		{name: "other component", existing: withLabels(map[string]string{domainspec.LabelComponentName: "api"})},
		{name: "shared", existing: withLabels(map[string]string{domainspec.LabelShareName: "edge", domainspec.LabelShareStrategy: string(domainspec.ShareStrategyDefault)})},
		{name: "force shared", existing: withLabels(map[string]string{domainspec.LabelShareName: "edge", domainspec.LabelShareStrategy: string(domainspec.ShareStrategyForce)}), wantDeleted: true},
	}
	for _, kind := range kinds {
		for _, tc := range cases {
			t.Run(kind.kind+"/"+tc.name, func(t *testing.T) {
				object := func(labels map[string]string) *unstructured.Unstructured {
					obj := &unstructured.Unstructured{}
					obj.SetGroupVersionKind(kind.gvk)
					obj.SetName("web-old")
					obj.SetNamespace("team-a")
					obj.SetLabels(labels)
					return obj
				}
				var objects []runtime.Object
				if tc.existing != nil {
					existing := object(tc.existing)
					existing.SetUID("uid-1")
					objects = append(objects, existing)
				}
				dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
					map[schema.GroupVersionResource]string{kind.gvr: kind.list}, objects...)
				jobTask := &model.JobTask{Name: "web-old", Namespace: "team-a", JobType: string(config.JobDeleteResource), JobInfo: object(owner)}
				ctl := NewDeleteResourceJobCtl(jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: &jobtest.NoopStore{}, Dynamic: dynamicClient})
				require.NotNil(t, ctl)

				require.NoError(t, ctl.run(context.Background()))
				_, err := dynamicClient.Resource(kind.gvr).Namespace("team-a").Get(context.Background(), "web-old", metav1.GetOptions{})
				if tc.existing == nil || tc.wantDeleted {
					require.True(t, k8serrors.IsNotFound(err), "object must be absent, got %v", err)
					return
				}
				require.NoError(t, err, "an object the component does not own exclusively must stay")
			})
		}
	}
}

// Only routes and Services can be deleted by this job.
func TestDeleteResourceJobRejectsOtherKinds(t *testing.T) {
	target := &unstructured.Unstructured{}
	target.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	target.SetName("web")
	target.SetNamespace("team-a")
	jobTask := &model.JobTask{Name: "web", Namespace: "team-a", JobType: string(config.JobDeleteResource), JobInfo: target}
	ctl := NewDeleteResourceJobCtl(jobTask, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: &jobtest.NoopStore{}, Dynamic: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())})
	require.ErrorContains(t, ctl.run(context.Background()), "unsupported deletion target kind")
}

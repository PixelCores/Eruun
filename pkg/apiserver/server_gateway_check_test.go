package apiserver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// The Leader reports the gateway provider's API as a readiness reason only when
// the cluster proves it is missing; a forbidden discovery URL is not proof.
func TestGatewayAPIIssue(t *testing.T) {
	served := func(groupVersion string, resources ...string) *metav1.APIResourceList {
		list := &metav1.APIResourceList{GroupVersion: groupVersion}
		for _, name := range resources {
			list.APIResources = append(list.APIResources, metav1.APIResource{Name: name})
		}
		return list
	}
	for _, tc := range []struct {
		name      string
		provider  spec.GatewayProvider
		resources []*metav1.APIResourceList
		reactErr  error
		issue     string
	}{
		{name: "disabled provider is not checked"},
		{name: "ingress served", provider: spec.GatewayProviderIngress, resources: []*metav1.APIResourceList{served("networking.k8s.io/v1", "ingresses")}},
		{name: "gatewayapi served", provider: spec.GatewayProviderGatewayAPI, resources: []*metav1.APIResourceList{served("gateway.networking.k8s.io/v1", "gateways", "httproutes")}},
		{name: "gatewayapi CRDs missing", provider: spec.GatewayProviderGatewayAPI, issue: "requires API gateway.networking.k8s.io/v1"},
		{name: "HTTPRoute CRD missing", provider: spec.GatewayProviderGatewayAPI, resources: []*metav1.APIResourceList{served("gateway.networking.k8s.io/v1", "gateways")}, issue: "requires resource httproutes"},
		{name: "discovery forbidden", provider: spec.GatewayProviderGatewayAPI, reactErr: k8serrors.NewForbidden(schema.GroupResource{}, "", errors.New("denied"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakediscovery.FakeDiscovery{Fake: &ktesting.Fake{Resources: tc.resources}}
			if tc.reactErr != nil {
				client.AddReactor("get", "resource", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, tc.reactErr })
			}
			issue := gatewayAPIIssue(client, tc.provider)
			if tc.issue == "" {
				require.Empty(t, issue)
				return
			}
			require.Contains(t, issue, tc.issue)
		})
	}
}

func TestRuntimeReadyReportsGatewayIssue(t *testing.T) {
	s := &restServer{}
	s.leading.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.leaderCtx = ctx
	ready, reason := s.RuntimeReady()
	require.True(t, ready, reason)

	s.gatewayIssue = "gateway provider gatewayapi requires API gateway.networking.k8s.io/v1, which the cluster does not serve"
	ready, reason = s.RuntimeReady()
	require.False(t, ready)
	require.Equal(t, s.gatewayIssue, reason)
}

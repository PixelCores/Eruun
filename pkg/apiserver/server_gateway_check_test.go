package apiserver

import (
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

// The Leader reports the gateway provider's API as missing only when the
// cluster proves it; a forbidden discovery URL is not proof.
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
			issue, err := gatewayAPIIssue(client, tc.provider)
			if tc.reactErr != nil {
				require.Error(t, err, "an undecided check is not a missing API")
				require.Empty(t, issue)
				return
			}
			require.NoError(t, err)
			if tc.issue == "" {
				require.Empty(t, issue)
				return
			}
			require.Contains(t, issue, tc.issue)
		})
	}
}

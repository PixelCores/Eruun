package apiserver

import (
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/discovery"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// gatewayAPIIssue checks with the platform identity that the cluster serves the
// route resource of the selected gateway provider. It returns a readiness
// reason only when the API or resource is missing; other failures, such as a
// 403 for the discovery URL, cannot prove that and are logged instead.
func gatewayAPIIssue(client discovery.DiscoveryInterface, provider spec.GatewayProvider) string {
	var groupVersion, resource string
	switch provider {
	case spec.GatewayProviderIngress:
		groupVersion, resource = networkingv1.SchemeGroupVersion.String(), "ingresses"
	case spec.GatewayProviderGatewayAPI:
		groupVersion, resource = spec.GatewayAPIGroupVersion.String(), spec.HTTPRouteResource.Resource
	default:
		return ""
	}
	resources, err := client.ServerResourcesForGroupVersion(groupVersion)
	if k8serrors.IsNotFound(err) {
		return fmt.Sprintf("gateway provider %s requires API %s, which the cluster does not serve", provider, groupVersion)
	}
	if err != nil {
		klog.ErrorS(err, "cannot check the gateway provider API", "provider", provider, "groupVersion", groupVersion)
		return ""
	}
	for _, item := range resources.APIResources {
		if item.Name == resource {
			return ""
		}
	}
	return fmt.Sprintf("gateway provider %s requires resource %s in API %s, which the cluster does not serve", provider, resource, groupVersion)
}

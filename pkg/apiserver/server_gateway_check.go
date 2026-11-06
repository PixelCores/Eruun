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
// route resource of the selected gateway provider. It returns a description
// when the API or resource is missing, and an error when the check cannot
// decide, such as a 403 for the discovery URL.
func gatewayAPIIssue(client discovery.DiscoveryInterface, provider spec.GatewayProvider) (string, error) {
	var groupVersion, resource string
	switch provider {
	case spec.GatewayProviderIngress:
		groupVersion, resource = networkingv1.SchemeGroupVersion.String(), "ingresses"
	case spec.GatewayProviderGatewayAPI:
		groupVersion, resource = spec.GatewayAPIGroupVersion.String(), spec.HTTPRouteResource.Resource
	default:
		return "", nil
	}
	resources, err := client.ServerResourcesForGroupVersion(groupVersion)
	if k8serrors.IsNotFound(err) {
		return fmt.Sprintf("gateway provider %s requires API %s, which the cluster does not serve", provider, groupVersion), nil
	}
	if err != nil {
		return "", fmt.Errorf("check gateway provider %s API %s: %w", provider, groupVersion, err)
	}
	for _, item := range resources.APIResources {
		if item.Name == resource {
			return "", nil
		}
	}
	return fmt.Sprintf("gateway provider %s requires resource %s in API %s, which the cluster does not serve", provider, resource, groupVersion), nil
}

// reportGatewayAPI logs, once per Leader term, whether the cluster serves the
// gateway provider's route API. A missing API only affects component routes:
// their deploy jobs fail with their own error, while the rest of the API stays
// ready.
func reportGatewayAPI(client discovery.DiscoveryInterface, provider spec.GatewayProvider) {
	issue, err := gatewayAPIIssue(client, provider)
	if err != nil {
		klog.ErrorS(err, "cannot check the gateway provider API")
		return
	}
	if issue != "" {
		klog.Warningf("component routes will fail to deploy: %s", issue)
	}
}

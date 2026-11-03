package apiserver

import (
	"context"
	"fmt"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/discovery"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// gatewayAPIIssue checks with the platform identity that the cluster serves the
// route resource of the selected gateway provider. It returns a readiness
// reason when the API or resource is missing, and an error when the check
// cannot decide, such as a 403 for the discovery URL.
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

// gatewayAPICheckInterval paces the Leader's recheck of the gateway API.
const gatewayAPICheckInterval = 30 * time.Second

// watchGatewayAPI rechecks the gateway API during a Leader term, so readiness
// recovers once a missing API is served again and a later removal is noticed
// without waiting for another term.
func (s *restServer) watchGatewayAPI(term context.Context) {
	ticker := time.NewTicker(gatewayAPICheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-term.Done():
			return
		case <-ticker.C:
			s.refreshGatewayIssue(term)
		}
	}
}

// refreshGatewayIssue records a new check result only for the term that ran it;
// an undecided check keeps the previous result.
func (s *restServer) refreshGatewayIssue(term context.Context) {
	issue, err := gatewayAPIIssue(s.platformDiscovery, s.cfg.Gateway.Provider)
	if err != nil {
		klog.ErrorS(err, "cannot recheck the gateway provider API")
		return
	}
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()
	if s.leaderCtx == term && term.Err() == nil {
		s.gatewayIssue = issue
	}
}

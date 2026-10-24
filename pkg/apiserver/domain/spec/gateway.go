package spec

import (
	"fmt"

	kvalidation "k8s.io/apimachinery/pkg/util/validation"
)

// GatewayProvider selects the platform entry point that renders component routes.
type GatewayProvider string

const (
	// GatewayProviderNone disables component routes in every workspace.
	GatewayProviderNone    GatewayProvider = ""
	GatewayProviderIngress GatewayProvider = "ingress"
)

// GatewayConfig is the inbound entry point selected when the platform is installed.
// It comes from server configuration, never from the account configuration Secret.
type GatewayConfig struct {
	Provider GatewayProvider
	// Domain is the DNS suffix of component hosts.
	Domain string
	// ProxyNamespace hosts the data-plane Pods admitted by workspace NetworkPolicy.
	ProxyNamespace string
	IngressClass   string
}

// Validate requires exactly the fields used by the selected provider.
func (c GatewayConfig) Validate() error {
	switch c.Provider {
	case GatewayProviderNone:
		if c.Domain != "" || c.ProxyNamespace != "" || c.IngressClass != "" {
			return fmt.Errorf("gateway domain, proxy namespace and ingress class require a gateway provider")
		}
		return nil
	case GatewayProviderIngress:
	default:
		return fmt.Errorf("gateway provider must be empty or %q, got %q", GatewayProviderIngress, c.Provider)
	}
	if len(kvalidation.IsDNS1123Subdomain(c.Domain)) != 0 {
		return fmt.Errorf("gateway domain must be a DNS-1123 subdomain, got %q", c.Domain)
	}
	if len(kvalidation.IsDNS1123Label(c.ProxyNamespace)) != 0 {
		return fmt.Errorf("gateway proxy namespace must be a DNS-1123 label, got %q", c.ProxyNamespace)
	}
	if len(kvalidation.IsDNS1123Subdomain(c.IngressClass)) != 0 {
		return fmt.Errorf("gateway ingress class must be a DNS-1123 subdomain, got %q", c.IngressClass)
	}
	return nil
}

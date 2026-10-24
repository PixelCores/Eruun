package spec

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
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

// GatewayTraitSpec declares traffic that crosses the workspace boundary. Only
// inbound routing is defined in this phase.
type GatewayTraitSpec struct {
	Hosts  []string           `json:"hosts,omitempty"`
	Routes []GatewayRouteSpec `json:"routes"`
}

// GatewayRouteSpec forwards one path to a Service of the same component.
type GatewayRouteSpec struct {
	Path    string              `json:"path,omitempty"`
	Match   string              `json:"match,omitempty"`
	Rewrite string              `json:"rewrite,omitempty"`
	Timeout string              `json:"timeout,omitempty"`
	Backend *GatewayBackendSpec `json:"backend,omitempty"`
}

// GatewayBackendSpec selects a Service and port declared by the component.
// Service names a Service trait; omit it to use the component's only Service.
type GatewayBackendSpec struct {
	Service string `json:"service,omitempty"`
	Port    int32  `json:"port,omitempty"`
}

const (
	GatewayMatchPrefix = "prefix"
	GatewayMatchExact  = "exact"
)

// GatewayDefaultHost is the host of a component whose gateway trait declares
// none: a single DNS label ending with the workspace namespace. Component names
// keep a case-insensitive identity, so the label uses the lowercase name.
func GatewayDefaultHost(componentName, namespace, domain string) string {
	return strings.ToLower(componentName) + "-" + namespace + "." + domain
}

// GatewayHostAllowed reports whether host is one DNS label owned by the
// workspace namespace, directly under the platform domain. Workspace
// namespaces share one fixed-length format, so the suffix names one workspace.
func GatewayHostAllowed(host, namespace, domain string) bool {
	label, rest, ok := strings.Cut(host, ".")
	if !ok || rest != domain || len(kvalidation.IsDNS1123Label(label)) != 0 {
		return false
	}
	prefix, owned := strings.CutSuffix(label, "-"+namespace)
	return owned && prefix != ""
}

// GatewayHosts returns the effective hosts of a gateway trait and rejects any
// host the workspace does not own.
func GatewayHosts(gateway GatewayTraitSpec, componentName, namespace string, cfg GatewayConfig) ([]string, error) {
	hosts := gateway.Hosts
	if len(hosts) == 0 {
		hosts = []string{GatewayDefaultHost(componentName, namespace, cfg.Domain)}
	}
	for _, host := range hosts {
		if !GatewayHostAllowed(host, namespace, cfg.Domain) {
			return nil, fmt.Errorf("host %q must be one DNS label of at most 63 characters ending with %q under %q", host, "-"+namespace, cfg.Domain)
		}
	}
	return hosts, nil
}

// ResolveGatewayBackend resolves a route backend against the component's
// Service traits, or its default Service when it declares none. The returned
// service is the Service trait name; empty means the component's generated
// Service name.
func ResolveGatewayBackend(backend *GatewayBackendSpec, services []ServiceTraitSpec, ports []Ports) (string, int32, error) {
	var want GatewayBackendSpec
	if backend != nil {
		want = *backend
	}
	var candidates []ServiceTraitSpec
	for _, service := range services {
		if accessType, _ := NormalizeServiceAccessType(service.Type); accessType != ServiceAccessExternal {
			candidates = append(candidates, service)
		}
	}
	var declared []int32
	var service string
	switch {
	case want.Service != "":
		found := false
		for _, candidate := range candidates {
			if candidate.Name == want.Service {
				service, declared, found = candidate.Name, tcpServicePorts(candidate.Ports), true
				break
			}
		}
		if !found {
			return "", 0, fmt.Errorf("backend service %q is not a named, non-external Service trait of the component", want.Service)
		}
	case len(candidates) > 1:
		return "", 0, fmt.Errorf("backend service is required when the component has several non-external Service traits; name them and select one")
	case len(candidates) == 1:
		service, declared = candidates[0].Name, tcpServicePorts(candidates[0].Ports)
	case len(services) == 0:
		for _, port := range ports {
			if port.Port > 0 {
				declared = append(declared, port.Port)
			}
		}
	}
	if len(declared) == 0 {
		return "", 0, fmt.Errorf("the component has no non-external Service with a TCP port to route to")
	}
	if want.Port == 0 {
		return service, declared[0], nil
	}
	for _, port := range declared {
		if port == want.Port {
			return service, port, nil
		}
	}
	return "", 0, fmt.Errorf("backend port %d is not a TCP port of the selected Service", want.Port)
}

// tcpServicePorts lists the TCP ports of a Service trait, reading the protocol
// the way the Service renderer does.
func tcpServicePorts(ports []ServicePortTraitSpec) []int32 {
	var tcp []int32
	for _, port := range ports {
		switch strings.ToUpper(strings.TrimSpace(port.Protocol)) {
		case "", string(corev1.ProtocolTCP):
			if port.Port > 0 {
				tcp = append(tcp, port.Port)
			}
		}
	}
	return tcp
}

// EffectivePath is the route path, "/" when omitted.
func (r GatewayRouteSpec) EffectivePath() string {
	if r.Path == "" {
		return "/"
	}
	return r.Path
}

// EffectiveMatch is the route match type, prefix when omitted.
func (r GatewayRouteSpec) EffectiveMatch() string {
	if r.Match == "" {
		return GatewayMatchPrefix
	}
	return r.Match
}

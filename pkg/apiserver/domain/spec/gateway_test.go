package spec

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGatewayConfigValidate(t *testing.T) {
	ingress := GatewayConfig{Provider: GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"}
	for _, tc := range []struct {
		name   string
		mutate func(*GatewayConfig)
		base   GatewayConfig
		errMsg string
	}{
		{name: "disabled", base: GatewayConfig{}},
		{name: "disabled with domain", base: GatewayConfig{Domain: "apps.example.com"}, errMsg: "require a gateway provider"},
		{name: "disabled with ingress class", base: GatewayConfig{IngressClass: "nginx"}, errMsg: "require a gateway provider"},
		{name: "ingress", base: ingress},
		{name: "unknown provider", base: ingress, mutate: func(c *GatewayConfig) { c.Provider = "gatewayapi" }, errMsg: "gateway provider must be empty or"},
		{name: "ingress without domain", base: ingress, mutate: func(c *GatewayConfig) { c.Domain = "" }, errMsg: "gateway domain"},
		{name: "ingress with wildcard domain", base: ingress, mutate: func(c *GatewayConfig) { c.Domain = "*.example.com" }, errMsg: "gateway domain"},
		{name: "ingress without proxy namespace", base: ingress, mutate: func(c *GatewayConfig) { c.ProxyNamespace = "" }, errMsg: "gateway proxy namespace"},
		{name: "proxy namespace is not a label", base: ingress, mutate: func(c *GatewayConfig) { c.ProxyNamespace = "ingress.nginx" }, errMsg: "gateway proxy namespace"},
		{name: "ingress without class", base: ingress, mutate: func(c *GatewayConfig) { c.IngressClass = "" }, errMsg: "gateway ingress class"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.base
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			err := cfg.Validate()
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.errMsg)
		})
	}
}

func TestGatewayHostsOwnership(t *testing.T) {
	cfg := GatewayConfig{Provider: GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"}
	namespace := "eruun-ws-0123456789abcdef0123456789abcdef"

	hosts, err := GatewayHosts(GatewayTraitSpec{}, "qwen", namespace, cfg)
	require.NoError(t, err)
	require.Equal(t, []string{"qwen-" + namespace + ".apps.example.com"}, hosts)
	hosts, err = GatewayHosts(GatewayTraitSpec{}, "Qwen", namespace, cfg)
	require.NoError(t, err)
	require.Equal(t, []string{"qwen-" + namespace + ".apps.example.com"}, hosts)

	// 21 characters is the longest prefix that fits one DNS label.
	_, err = GatewayHosts(GatewayTraitSpec{}, strings.Repeat("a", 21), namespace, cfg)
	require.NoError(t, err)
	_, err = GatewayHosts(GatewayTraitSpec{}, strings.Repeat("a", 22), namespace, cfg)
	require.ErrorContains(t, err, "at most 63 characters")

	for _, host := range []string{
		"chat-" + namespace + ".apps.example.com",
	} {
		require.True(t, GatewayHostAllowed(host, namespace, cfg.Domain), host)
	}
	for _, host := range []string{
		"chat." + namespace + ".apps.example.com",
		"chat-eruun-ws-ffffffffffffffffffffffffffffffff.apps.example.com",
		"-" + namespace + ".apps.example.com",
		"chat-" + namespace + ".other.example.com",
		"Chat-" + namespace + ".apps.example.com",
		"*.apps.example.com",
		"",
	} {
		require.False(t, GatewayHostAllowed(host, namespace, cfg.Domain), host)
	}
}

func TestResolveGatewayBackend(t *testing.T) {
	api := ServiceTraitSpec{Name: "api", Ports: []ServicePortTraitSpec{{Port: 8080}, {Port: 9090, Protocol: "UDP"}}}
	web := ServiceTraitSpec{Name: "web", Ports: []ServicePortTraitSpec{{Port: 3000}}}
	unnamed := ServiceTraitSpec{Ports: []ServicePortTraitSpec{{Port: 7000}}}
	external := ServiceTraitSpec{Name: "upstream", Type: string(ServiceAccessExternal), ExternalName: "example.com", Ports: []ServicePortTraitSpec{{Port: 443}}}
	for _, tc := range []struct {
		name     string
		backend  *GatewayBackendSpec
		services []ServiceTraitSpec
		ports    []Ports
		service  string
		port     int32
		errMsg   string
	}{
		{name: "default service from properties", ports: []Ports{{Port: 8000}}, port: 8000},
		{name: "default service port", backend: &GatewayBackendSpec{Port: 8001}, ports: []Ports{{Port: 8000}, {Port: 8001}}, port: 8001},
		{name: "only service trait, unnamed", services: []ServiceTraitSpec{unnamed}, port: 7000},
		{name: "external traits are skipped", services: []ServiceTraitSpec{external, web}, service: "web", port: 3000},
		{name: "named service and port", backend: &GatewayBackendSpec{Service: "api", Port: 8080}, services: []ServiceTraitSpec{api, web}, service: "api", port: 8080},
		{name: "ambiguous services", services: []ServiceTraitSpec{api, web}, errMsg: "backend service is required"},
		{name: "unnamed trait cannot be selected", backend: &GatewayBackendSpec{Service: "default"}, services: []ServiceTraitSpec{unnamed, web}, errMsg: "not a named, non-external Service trait"},
		{name: "external service cannot be selected", backend: &GatewayBackendSpec{Service: "upstream"}, services: []ServiceTraitSpec{external}, errMsg: "not a named, non-external Service trait"},
		{name: "udp port", backend: &GatewayBackendSpec{Service: "api", Port: 9090}, services: []ServiceTraitSpec{api}, errMsg: "not a TCP port"},
		{name: "protocol is read like the Service renderer", services: []ServiceTraitSpec{{Name: "lower", Ports: []ServicePortTraitSpec{{Port: 5000, Protocol: " tcp "}}}}, service: "lower", port: 5000},
		{name: "no service", errMsg: "no non-external Service"},
		{name: "only external services", services: []ServiceTraitSpec{external}, ports: []Ports{{Port: 8000}}, errMsg: "no non-external Service"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, port, err := ResolveGatewayBackend(tc.backend, tc.services, tc.ports)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.service, service)
			require.Equal(t, tc.port, port)
		})
	}
}

package spec

import (
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

package apiserver

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestLoadAccountConfigAttachesServerGateway(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"origins":["https://console.example.com"],"frontendURL":"https://console.example.com/account","workspace":{"clusterCIDRs":["10.0.0.0/8"]}}`), 0o600))
	gateway := spec.GatewayConfig{Provider: spec.GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"}

	accounts, err := loadAccountConfig(&config.Config{AuthConfigFile: path, Gateway: gateway})
	require.NoError(t, err)
	require.Equal(t, gateway, accounts.Workspace.Gateway)
}

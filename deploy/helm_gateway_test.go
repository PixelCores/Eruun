package deploy_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
)

// The platform Gateway admits routes from namespaces carrying the workspace
// owner label; the Chart and the server must name the same label, or every
// workspace route would be rejected by the https listener.
func TestHelmGatewaySelectsWorkspaceOwnerLabel(t *testing.T) {
	template, err := os.ReadFile("helm/eruun/templates/gateway.yaml")
	require.NoError(t, err)
	require.Contains(t, string(template), "- key: "+workspace.OwnerLabel+"\n")
}

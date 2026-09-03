package spec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccountConfigurationFailsClosed(t *testing.T) {
	valid := `{"origins":["https://console.example.com"],"frontendURL":"https://console.example.com/account","workspace":{"clusterCIDRs":["10.0.0.0/8"]}}`
	for _, tc := range []struct {
		name, raw string
		valid     bool
	}{
		{"valid", valid, true},
		{"unknown field", strings.Replace(valid, `"origins"`, `"disableAuthentication":true,"origins"`, 1), false},
		{"missing network", strings.Replace(valid, `"10.0.0.0/8"`, ``, 1), false},
		{"plaintext origin", strings.ReplaceAll(valid, "https://", "http://"), false},
		{"wildcard origin", strings.ReplaceAll(valid, "console.example.com", "*.example.com"), false},
		{"trailing document", valid + `{}`, false},
		{"partial SMTP", strings.Replace(valid, `"workspace"`, `"smtp":{"password":"test-password"},"workspace"`, 1), false},
		{"partial SMS", strings.Replace(valid, `"workspace"`, `"sms":{"templateCode":"SMS_TEST"},"workspace"`, 1), false},
		{"untrusted proxy format", strings.Replace(valid, `"workspace"`, `"trustedProxyCIDRs":["*"],"workspace"`, 1), false},
		{"placeholder OAuth", strings.Replace(valid, `"workspace"`, `"github":{"enabled":true,"clientId":"__REPLACE__","clientSecret":"__REPLACE__","redirectURI":"https://console.example.com/oauth"},"workspace"`, 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "accounts.json")
			require.NoError(t, os.WriteFile(path, []byte(tc.raw), 0600))
			cfg, err := LoadAccountConfig(path)
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "kube-system", cfg.Workspace.DNSNamespace)
			pods := cfg.Workspace.Quota["pods"]
			require.Equal(t, "20", pods.String())
			require.False(t, cfg.AllowedOrigin("https://console.example.com.attacker.test"))
			require.False(t, cfg.AllowedURL("https://console.example.com@attacker.test"))
		})
	}
	_, err := LoadAccountConfig("")
	require.Error(t, err)
}

package workspace

import (
	"net/http"

	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// APIClient keeps controllers' trusted contexts intact while ensuring that
// HTTP resource operations (including logs and exec) use the selected tenant's
// restricted identity. The original client remains with namespace management.
func APIClient(config *rest.Config, policy spec.WorkspaceConfig) (kubernetes.Interface, *rest.Config, error) {
	cfg := rest.CopyConfig(config)
	cfg.ContentType = "application/json"
	cfg.AcceptContentTypes = "application/json"
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return &requestTransport{next: next, config: policy}
	})
	client, err := kubernetes.NewForConfig(cfg)
	return client, cfg, err
}

type requestTransport struct {
	next   http.RoundTripper
	config spec.WorkspaceConfig
}

func (t *requestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	scope, scoped := access.FromContext(req.Context())
	if !scoped || scope.ClusterOperation {
		return t.next.RoundTrip(req)
	}
	copy := req.Clone(req.Context())
	copy.Header = req.Header.Clone()
	copy.Header.Set("Impersonate-User", "system:serviceaccount:"+scope.Namespace+":"+runnerName)
	copy.Header.Del("Impersonate-Group")
	copy.Header.Del("Impersonate-Uid")
	return (&tenantTransport{next: t.next, namespace: scope.Namespace, config: t.config}).RoundTrip(copy)
}

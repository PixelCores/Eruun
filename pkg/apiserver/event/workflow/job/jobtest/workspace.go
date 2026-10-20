package jobtest

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

type WorkspaceRoundTripper func(*http.Request) (*http.Response, error)

func (f WorkspaceRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func WorkspaceResponse(code int, body []byte) *http.Response {
	return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}
}

func WorkspacePodSpec() corev1.PodSpec {
	return corev1.PodSpec{
		Containers:     []corev1.Container{{Name: "app", Image: "example.com/app:1"}, {Name: "sidecar", Image: "example.com/sidecar:1"}},
		InitContainers: []corev1.Container{{Name: "init", Image: "example.com/init:1"}},
	}
}

func RequireWorkspacePodSecurity(t *testing.T, pod corev1.PodSpec) {
	t.Helper()
	require.Equal(t, ptr.To(false), pod.AutomountServiceAccountToken)
	for _, c := range append(pod.Containers, pod.InitContainers...) {
		require.NotNil(t, c.SecurityContext, c.Name)
		require.Equal(t, ptr.To(false), c.SecurityContext.AllowPrivilegeEscalation)
		require.Equal(t, ptr.To(true), c.SecurityContext.RunAsNonRoot)
		require.Equal(t, []corev1.Capability{"ALL"}, c.SecurityContext.Capabilities.Drop)
		require.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, c.SecurityContext.SeccompProfile.Type)
		require.Nil(t, c.SecurityContext.SeccompProfile.LocalhostProfile)
	}
}

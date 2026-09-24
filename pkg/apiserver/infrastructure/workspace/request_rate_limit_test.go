package workspace

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

func TestDerivedClientsShareBudgetAndPreserveTenantIsolation(t *testing.T) {
	var identities []string
	base := &rest.Config{
		Host:        "https://kubernetes.example",
		RateLimiter: flowcontrol.NewTokenBucketRateLimiter(0.000001, 4),
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			identities = append(identities, req.Header.Get("Impersonate-User"))
			require.Equal(t, "preserved", req.Header.Get("X-Test-Wrapper"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"runner"}}`)),
				Request:    req,
			}, nil
		}),
		WrapTransport: func(next http.RoundTripper) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				req = req.Clone(req.Context())
				req.Header.Set("X-Test-Wrapper", "preserved")
				return next.RoundTrip(req)
			})
		},
	}
	manager := &Manager{RESTConfig: base, Config: workspaceConfig(t)}
	tenantA, configA, err := manager.TenantClient(&model.Workspace{ID: "a", Namespace: "space-a"})
	require.NoError(t, err)
	tenantB, configB, err := manager.TenantClient(&model.Workspace{ID: "b", Namespace: "space-b"})
	require.NoError(t, err)
	apiClient, apiConfig, err := APIClient(base, manager.Config)
	require.NoError(t, err)
	for _, derived := range []*rest.Config{configA, configB, apiConfig} {
		require.Same(t, base.RateLimiter, derived.RateLimiter)
		require.Equal(t, "application/json", derived.ContentType)
	}
	require.Empty(t, base.ContentType)
	require.Empty(t, base.Impersonate.UserName)
	require.Empty(t, apiConfig.Impersonate.UserName)

	_, err = tenantA.CoreV1().Pods("space-a").Get(context.Background(), "runner", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = tenantB.CoreV1().Pods("space-b").Get(context.Background(), "runner", metav1.GetOptions{})
	require.NoError(t, err)
	ctx := access.WithScope(context.Background(), access.Scope{WorkspaceID: "a", Namespace: "space-a", Role: "member"})
	_, err = apiClient.CoreV1().Pods("space-a").Get(ctx, "runner", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = tenantA.CoreV1().Pods("space-b").Get(context.Background(), "runner", metav1.GetOptions{})
	require.ErrorIs(t, err, bcode.ErrForbidden)
	require.Equal(t, []string{
		"system:serviceaccount:space-a:eruun-runner",
		"system:serviceaccount:space-b:eruun-runner",
		"system:serviceaccount:space-a:eruun-runner",
	}, identities)

	// Creating another tenant client must not replenish the exhausted bucket.
	newTenant, _, err := manager.TenantClient(&model.Workspace{ID: "a", Namespace: "space-a"})
	require.NoError(t, err)
	limitedCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = newTenant.CoreV1().Pods("space-a").Get(limitedCtx, "runner", metav1.GetOptions{})
	require.ErrorContains(t, err, "client rate limiter Wait returned an error")
	require.Len(t, identities, 3)
}

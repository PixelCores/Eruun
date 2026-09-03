package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/access"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func workspaceConfig(t *testing.T) spec.WorkspaceConfig {
	t.Helper()
	c := &spec.AccountConfig{Origins: []string{"https://console.example.com"}, FrontendURL: "https://console.example.com", Workspace: spec.WorkspaceConfig{ClusterCIDRs: []string{"10.96.0.0/12", "10.244.0.0/16", "192.0.2.0/24"}, StorageClasses: []string{"tenant-storage"}, IngressDomain: "apps.example.com", IngressClass: "nginx", IngressNamespace: "ingress-nginx"}}
	require.NoError(t, c.Validate())
	return c.Workspace
}

func TestEnsureNamespaceBaselineAndConcurrentInitialization(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := &model.Workspace{ID: "workspace-a", Namespace: "eruun-ws-a"}
	m := &Manager{Client: client, Config: workspaceConfig(t)}
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() { errs <- m.Ensure(ctx, w) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	ns, err := client.CoreV1().Namespaces().Get(ctx, w.Namespace, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, w.ID, ns.Labels[OwnerLabel])
	require.Equal(t, "restricted", ns.Labels["pod-security.kubernetes.io/enforce"])
	for _, name := range []string{runnerName, "default"} {
		sa, err := client.CoreV1().ServiceAccounts(w.Namespace).Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		require.NotNil(t, sa.AutomountServiceAccountToken)
		require.False(t, *sa.AutomountServiceAccountToken)
	}
	role, err := client.RbacV1().Roles(w.Namespace).Get(ctx, runnerName, metav1.GetOptions{})
	require.NoError(t, err)
	for _, rule := range role.Rules {
		require.NotContains(t, rule.Resources, "*")
		require.NotContains(t, rule.Resources, "roles")
		require.NotContains(t, rule.Resources, "serviceaccounts/token")
	}
	quota, err := client.CoreV1().ResourceQuotas(w.Namespace).Get(ctx, baselineName, metav1.GetOptions{})
	require.NoError(t, err)
	pods := quota.Spec.Hard["pods"]
	require.Equal(t, "20", pods.String())
	gpu := quota.Spec.Hard["requests.nvidia.com/gpu"]
	require.True(t, gpu.IsZero())
	_, err = client.CoreV1().LimitRanges(w.Namespace).Get(ctx, baselineName, metav1.GetOptions{})
	require.NoError(t, err)
	policy, err := client.NetworkingV1().NetworkPolicies(w.Namespace).Get(ctx, baselineName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, policy.Spec.PolicyTypes, 2)
	require.NotNil(t, policy.Spec.Egress[0].To[0].PodSelector)
	require.Nil(t, policy.Spec.Egress[0].To[0].NamespaceSelector)
	require.Equal(t, "kube-system", policy.Spec.Egress[1].To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"])
	public := policy.Spec.Egress[2]
	require.Len(t, public.Ports, 2)
	for _, cidr := range m.Config.ClusterCIDRs {
		require.Contains(t, public.To[0].IPBlock.Except, cidr)
	}
}

func TestEnsureWrongOwnerAndPartialFailure(t *testing.T) {
	ctx := context.Background()
	w := &model.Workspace{ID: "a", Namespace: "eruun-ws-a"}
	client := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: w.Namespace, Labels: map[string]string{OwnerLabel: "b"}}})
	m := &Manager{Client: client, Config: workspaceConfig(t)}
	require.ErrorIs(t, m.Ensure(ctx, w), bcode.ErrAccountConflict)
	require.Len(t, client.Actions(), 1)
	client = fake.NewSimpleClientset()
	m.Client = client
	fail := true
	client.PrependReactor("create", "networkpolicies", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, errors.New("CNI baseline write failed")
		}
		return false, nil, nil
	})
	require.Error(t, m.Ensure(ctx, w))
	for _, action := range client.Actions() {
		require.NotEqual(t, "delete", action.GetVerb())
		require.NotEqual(t, "deployments", action.GetResource().Resource)
	}
	fail = false
	require.NoError(t, m.Ensure(ctx, w))
	_, err := client.NetworkingV1().NetworkPolicies(w.Namespace).Get(ctx, baselineName, metav1.GetOptions{})
	require.NoError(t, err)
}

type recordingTransport struct {
	calls  int
	object map[string]interface{}
	header http.Header
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls++
	r.header = req.Header.Clone()
	if req.Body != nil {
		_ = json.NewDecoder(req.Body).Decode(&r.object)
	}
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: req}, nil
}

func TestTransportRejectsEscapesWithoutUpstreamCalls(t *testing.T) {
	cfg := workspaceConfig(t)
	for _, tc := range []struct{ name, path, body string }{
		{"foreign namespace", "/apis/apps/v1/namespaces/other/deployments", `{"spec":{}}`},
		{"cluster RBAC", "/apis/rbac.authorization.k8s.io/v1/clusterroles", `{}`},
		{"change baseline", "/apis/networking.k8s.io/v1/namespaces/own/networkpolicies", `{}`},
		{"metadata namespace", "/apis/apps/v1/namespaces/own/deployments", `{"metadata":{"namespace":"other"}}`},
		{"host network", "/api/v1/namespaces/own/pods", `{"spec":{"hostNetwork":true}}`},
		{"init privileged", "/api/v1/namespaces/own/pods", `{"spec":{"initContainers":[{"name":"init","image":"busybox:1.37","securityContext":{"privileged":true}}]}}`},
		{"sidecar capabilities", "/api/v1/namespaces/own/pods", `{"spec":{"containers":[{"name":"sidecar","securityContext":{"capabilities":{"add":["SYS_ADMIN"]}}}]}}`},
		{"hostpath", "/api/v1/namespaces/own/pods", `{"spec":{"volumes":[{"name":"root","hostPath":{"path":"/"}}]}}`},
		{"service token", "/api/v1/namespaces/own/pods", `{"spec":{"automountServiceAccountToken":true}}`},
		{"runner identity", "/api/v1/namespaces/own/pods", `{"spec":{"serviceAccountName":"eruun-runner"}}`},
		{"nodeport", "/api/v1/namespaces/own/services", `{"spec":{"type":"NodePort"}}`},
		{"foreign PV", "/api/v1/namespaces/own/persistentvolumeclaims", `{"spec":{"storageClassName":"tenant-storage","volumeName":"other-volume"}}`},
		{"storage class", "/api/v1/namespaces/own/persistentvolumeclaims", `{"spec":{"storageClassName":"administrator"}}`},
		{"foreign host", "/apis/networking.k8s.io/v1/namespaces/own/ingresses", `{"spec":{"rules":[{"host":"victim.apps.example.com"}]}}`},
		{"catchall ingress", "/apis/networking.k8s.io/v1/namespaces/own/ingresses", `{"spec":{"ingressClassName":"nginx","rules":[{}]}}`},
		{"default backend", "/apis/networking.k8s.io/v1/namespaces/own/ingresses", `{"spec":{"ingressClassName":"nginx","defaultBackend":{},"rules":[{"host":"web.own.apps.example.com"}]}}`},
		{"ephemeral container", "/api/v1/namespaces/own/pods", `{"spec":{"ephemeralContainers":[{"name":"debug"}]}}`},
		{"ingress annotations", "/apis/networking.k8s.io/v1/namespaces/own/ingresses", `{"metadata":{"annotations":{"nginx.ingress.kubernetes.io/server-snippet":"x"}},"spec":{"rules":[]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := &recordingTransport{}
			tr := &tenantTransport{next: next, namespace: "own", config: cfg}
			req, _ := http.NewRequest(http.MethodPost, "https://kubernetes.example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			_, err := tr.RoundTrip(req)
			require.ErrorIs(t, err, bcode.ErrForbidden)
			require.Zero(t, next.calls)
		})
	}
}

func TestAPIResourceRequestsUseWorkspaceIdentity(t *testing.T) {
	var calls int
	transport := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		require.Equal(t, "system:serviceaccount:own:eruun-runner", r.Header.Get("Impersonate-User"))
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("logs"))}, nil
	})
	_, cfg, err := APIClient(&rest.Config{Host: "https://kubernetes.example", Transport: transport}, workspaceConfig(t))
	require.NoError(t, err)
	tr, err := rest.TransportFor(cfg)
	require.NoError(t, err)
	ctx := access.WithScope(context.Background(), access.Scope{WorkspaceID: "a", Namespace: "own", Role: "member"})
	for _, ns := range []string{"other", "own"} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://kubernetes.example/api/v1/namespaces/"+ns+"/pods/web/log", nil)
		response, err := tr.RoundTrip(req)
		if ns == "other" {
			require.ErrorIs(t, err, bcode.ErrForbidden)
			require.Zero(t, calls)
		} else {
			require.NoError(t, err)
			response.Body.Close()
			require.Equal(t, 1, calls)
		}
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDeleteWorkspaceChecksCustomResourcesBeforeDeletion(t *testing.T) {
	for _, populated := range []bool{true, false} {
		t.Run(fmt.Sprint(populated), func(t *testing.T) {
			var deletes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api":
					io.WriteString(w, `{"kind":"APIVersions","apiVersion":"v1","versions":["v1"]}`)
				case "/apis":
					io.WriteString(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"widgets.example.com","versions":[{"groupVersion":"widgets.example.com/v1","version":"v1"}],"preferredVersion":{"groupVersion":"widgets.example.com/v1","version":"v1"}}]}`)
				case "/api/v1":
					io.WriteString(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[{"name":"pods","namespaced":true,"kind":"Pod","verbs":["list"]}]}`)
				case "/apis/widgets.example.com/v1":
					io.WriteString(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"widgets.example.com/v1","resources":[{"name":"widgets","namespaced":true,"kind":"Widget","verbs":["list"]}]}`)
				case "/api/v1/namespaces/own":
					if r.Method == http.MethodDelete {
						deletes.Add(1)
					}
					io.WriteString(w, `{"kind":"Namespace","apiVersion":"v1","metadata":{"name":"own","uid":"namespace-uid","labels":{"eruun.io/workspace-id":"a"}}}`)
				case "/apis/widgets.example.com/v1/namespaces/own/widgets":
					if populated {
						io.WriteString(w, `{"items":[{"metadata":{"name":"business-resource"}}]}`)
					} else {
						io.WriteString(w, `{"items":[]}`)
					}
				case "/api/v1/namespaces/own/pods":
					io.WriteString(w, `{"items":[]}`)
				default:
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			require.NoError(t, err)
			m := &Manager{Client: client}
			err = m.DeleteEmpty(context.Background(), &model.Workspace{ID: "a", Namespace: "own"})
			if populated {
				require.ErrorIs(t, err, bcode.ErrWorkspaceNotEmpty)
				require.Zero(t, deletes.Load())
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 1, deletes.Load())
			}
		})
	}
}

func TestTransportSecuresDeploymentInitAndSidecar(t *testing.T) {
	next := &recordingTransport{}
	tr := &tenantTransport{next: next, namespace: "own", config: workspaceConfig(t)}
	body := `{"spec":{"template":{"spec":{"containers":[{"name":"main"},{"name":"sidecar"}],"initContainers":[{"name":"init"}]}}}}`
	req, _ := http.NewRequest(http.MethodPost, "https://kubernetes.example/apis/apps/v1/namespaces/own/deployments", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := tr.RoundTrip(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	pod := mapAt(mapAt(mapAt(next.object, "spec"), "template"), "spec")
	require.Equal(t, false, pod["automountServiceAccountToken"])
	for _, key := range []string{"containers", "initContainers"} {
		for _, item := range pod[key].([]interface{}) {
			sc := mapAt(item.(map[string]interface{}), "securityContext")
			require.Equal(t, false, sc["allowPrivilegeEscalation"])
			require.Equal(t, true, sc["runAsNonRoot"])
			require.Equal(t, "RuntimeDefault", mapAt(sc, "seccompProfile")["type"])
			require.Equal(t, []interface{}{"ALL"}, mapAt(sc, "capabilities")["drop"])
		}
	}
}

func TestTaskValidationAndImpersonation(t *testing.T) {
	w := &model.Workspace{ID: "a", Namespace: "own"}
	cfg := workspaceConfig(t)
	task := &model.JobTask{AppID: "app", Namespace: "own", JobType: string(config.JobDeploy), JobInfo: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "own"}}}
	deploy, err := ValidateTask(task, "app", w, cfg)
	require.NoError(t, err)
	require.True(t, deploy)
	task.AppID = "other"
	_, err = ValidateTask(task, "app", w, cfg)
	require.ErrorIs(t, err, bcode.ErrForbidden)
	task.AppID = "app"
	task.JobType = string(config.JobDeployCloud)
	_, err = ValidateTask(task, "app", w, cfg)
	require.ErrorIs(t, err, bcode.ErrForbidden)
	manager := &Manager{RESTConfig: &rest.Config{Host: "https://kubernetes.example"}, Config: cfg}
	_, restConfig, err := manager.TenantClient(w)
	require.NoError(t, err)
	require.Equal(t, "system:serviceaccount:own:eruun-runner", restConfig.Impersonate.UserName)
	traits := &spec.Traits{Init: []spec.InitTraitSpec{{Traits: spec.Traits{SecurityPolicy: &corev1.SecurityContext{Privileged: ptr.To(true)}}}}}
	require.ErrorIs(t, ValidateTraits("own", "app", traits, nil, cfg), bcode.ErrForbidden)
}

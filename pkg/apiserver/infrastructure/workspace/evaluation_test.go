package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type evaluationTransport func(*http.Request) (*http.Response, error)

func (f evaluationTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func evaluationObject() map[string]interface{} {
	var obj map[string]interface{}
	_ = json.Unmarshal([]byte(`{"metadata":{"name":"eruun-job-task","namespace":"space"},"spec":{"template":{"spec":{"serviceAccountName":"eruun-evaluation-runner","automountServiceAccountToken":true,"containers":[{"name":"runner","image":"runner:0.22.0","command":["python","/opt/eruun/runner.py"]}]}}}}`), &obj)
	return obj
}

func TestEvaluationTransportLimitsServiceAccountException(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trusted bool
		edit    func(map[string]interface{})
		valid   bool
	}{
		{"untrusted capability", false, nil, false},
		{"fixed runner", true, nil, true},
		{"foreign namespace", true, func(o map[string]interface{}) { mapAt(o, "metadata")["namespace"] = "foreign" }, false},
		{"different task", true, func(o map[string]interface{}) { mapAt(o, "metadata")["name"] = "eruun-job-other" }, false},
		{"different image", true, func(o map[string]interface{}) {
			c := mapAt(mapAt(mapAt(o, "spec"), "template"), "spec")["containers"].([]interface{})[0].(map[string]interface{})
			c["image"] = "user-image:1"
		}, false},
		{"shell entrypoint", true, func(o map[string]interface{}) {
			c := mapAt(mapAt(mapAt(o, "spec"), "template"), "spec")["containers"].([]interface{})[0].(map[string]interface{})
			c["command"] = []interface{}{"sh", "-c", "evil"}
		}, false},
		{"privileged", true, func(o map[string]interface{}) {
			c := mapAt(mapAt(mapAt(o, "spec"), "template"), "spec")["containers"].([]interface{})[0].(map[string]interface{})
			c["securityContext"] = map[string]interface{}{"privileged": true}
		}, false},
		{"hostPath", true, func(o map[string]interface{}) {
			p := mapAt(mapAt(mapAt(o, "spec"), "template"), "spec")
			p["volumes"] = []interface{}{map[string]interface{}{"name": "host", "hostPath": map[string]interface{}{"path": "/"}}}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := evaluationObject()
			if tc.edit != nil {
				tc.edit(obj)
			}
			body, err := json.Marshal(obj)
			require.NoError(t, err)
			ctx := context.Background()
			if tc.trusted {
				ctx = WithEvaluationRunner(ctx, "eruun-job-task", "runner:0.22.0")
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://cluster/apis/batch/v1/namespaces/space/jobs", bytes.NewReader(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			called := false
			transport := &tenantTransport{namespace: "space", next: evaluationTransport(func(r *http.Request) (*http.Response, error) {
				called = true
				raw, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				var prepared map[string]interface{}
				require.NoError(t, json.Unmarshal(raw, &prepared))
				pod := mapAt(mapAt(mapAt(prepared, "spec"), "template"), "spec")
				require.Equal(t, EvaluationRunnerName, pod["serviceAccountName"])
				require.Equal(t, true, pod["automountServiceAccountToken"])
				security := mapAt(pod["containers"].([]interface{})[0].(map[string]interface{}), "securityContext")
				require.Equal(t, true, security["runAsNonRoot"])
				require.Equal(t, false, security["allowPrivilegeEscalation"])
				return &http.Response{StatusCode: 201, Body: http.NoBody}, nil
			})}
			_, err = transport.RoundTrip(req)
			if tc.valid {
				require.NoError(t, err)
				require.True(t, called)
			} else {
				require.ErrorIs(t, err, bcode.ErrForbidden)
				require.False(t, called)
			}
		})
	}
}

func TestEnsureEvaluationRunnerPreservesRestrictedNamespace(t *testing.T) {
	ctx := context.Background()
	space := &model.Workspace{ID: "space-id", Namespace: "space"}
	client := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: space.Namespace, Labels: map[string]string{OwnerLabel: space.ID, "pod-security.kubernetes.io/enforce": "restricted"}}})
	manager := &Manager{Client: client}
	for i := 0; i < 2; i++ {
		require.NoError(t, manager.EnsureEvaluationRunner(ctx, space, spec.JobRunnerEgress{CIDR: "192.0.2.1/32", Port: 443}))
	}
	sa, err := client.CoreV1().ServiceAccounts(space.Namespace).Get(ctx, EvaluationRunnerName, metav1.GetOptions{})
	require.NoError(t, err)
	require.False(t, *sa.AutomountServiceAccountToken)
	role, err := client.RbacV1().Roles(space.Namespace).Get(ctx, EvaluationRunnerName, metav1.GetOptions{})
	require.NoError(t, err)
	for _, rule := range role.Rules {
		for _, resource := range rule.Resources {
			require.Contains(t, []string{"pods", "pods/exec"}, resource)
		}
		require.NotContains(t, rule.Verbs, "*")
	}
	policy, err := client.NetworkingV1().NetworkPolicies(space.Namespace).Get(ctx, EvaluationRunnerName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{EvaluationRunnerLabel: "true"}, policy.Spec.PodSelector.MatchLabels)
	require.Equal(t, "192.0.2.1/32", policy.Spec.Egress[0].To[0].IPBlock.CIDR)
	ns, err := client.CoreV1().Namespaces().Get(ctx, space.Namespace, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "restricted", ns.Labels["pod-security.kubernetes.io/enforce"])
}

func TestEvaluationTransportScopesMultipleWorkflowJobs(t *testing.T) {
	first := WithEvaluationRunner(context.Background(), "evaluation-first", "runner:0.22.0")
	both := WithEvaluationRunner(first, "evaluation-second", "runner:0.22.0")
	for _, tc := range []struct {
		name        string
		ctx         context.Context
		jobName     string
		wantAllowed bool
	}{
		{"first", both, "evaluation-first", true},
		{"second", both, "evaluation-second", true},
		{"foreign", both, "evaluation-other", false},
		{"parent context stays immutable", first, "evaluation-second", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := evaluationObject()
			mapAt(obj, "metadata")["name"] = tc.jobName
			raw, err := json.Marshal(obj)
			require.NoError(t, err)
			req, err := http.NewRequestWithContext(tc.ctx, http.MethodPost, "https://cluster/apis/batch/v1/namespaces/space/jobs", bytes.NewReader(raw))
			require.NoError(t, err)
			called := false
			transport := &tenantTransport{namespace: "space", next: evaluationTransport(func(*http.Request) (*http.Response, error) {
				called = true
				return &http.Response{StatusCode: 201, Body: http.NoBody}, nil
			})}
			_, err = transport.RoundTrip(req)
			require.Equal(t, tc.wantAllowed, called)
			if tc.wantAllowed {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, bcode.ErrForbidden)
			}
		})
	}
}

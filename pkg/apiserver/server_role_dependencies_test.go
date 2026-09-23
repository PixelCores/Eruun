package apiserver

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event"
	workflowevent "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type sandboxRoleTestTransport func(*http.Request) (*http.Response, error)

func (f sandboxRoleTestTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestSandboxPlatformClientKeepsTenantScopeAndSharedRequestBudget(t *testing.T) {
	var requests []*http.Request
	base := &rest.Config{Host: "https://kubernetes.example", RateLimiter: flowcontrol.NewTokenBucketRateLimiter(0.000001, 3),
		Transport: sandboxRoleTestTransport(func(req *http.Request) (*http.Response, error) {
			requests = append(requests, req.Clone(req.Context()))
			body := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"runner","namespace":"space-a"}}`
			if strings.HasPrefix(req.URL.Path, "/apis/agents.kruise.io/") {
				body = `{"apiVersion":"agents.kruise.io/v1alpha1","kind":"Sandbox","metadata":{"name":"trial","namespace":"space-a","uid":"sandbox-uid"}}`
			}
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
		})}
	// Match server assembly: keep the platform config before wrapping the typed
	// user API client; both copies retain the original limiter instance.
	platformConfig := rest.CopyConfig(base)
	tenant, tenantConfig, err := workspace.APIClient(base, spec.WorkspaceConfig{})
	require.NoError(t, err)
	require.Same(t, platformConfig.RateLimiter, tenantConfig.RateLimiter)
	server := &restServer{cfg: config.Config{Role: config.RuntimeRoleAPI, Jobs: &spec.JobsRuntimeConfig{}}, jobs: &jobs.Service{Kube: tenant}}
	require.NoError(t, server.initSandboxObserver(tenant, platformConfig))
	ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: "workspace-a", Namespace: "space-a", Role: "member"})
	_, err = server.jobs.SandboxClient.Resource(jobs.SandboxGVR).Namespace("space-a").Create(ctx,
		&unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "agents.kruise.io/v1alpha1", "kind": "Sandbox", "metadata": map[string]interface{}{"name": "trial", "namespace": "space-a"}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = server.jobs.SandboxClient.Resource(jobs.SandboxGVR).Namespace("space-a").Get(ctx, "trial", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = server.jobs.Kube.CoreV1().Pods("space-a").Get(ctx, "runner", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, requests, 3)
	for _, req := range requests[:2] {
		require.Empty(t, req.Header.Get("Impersonate-User"))
		require.Contains(t, req.URL.Path, "/namespaces/space-a/sandboxes")
	}
	require.Equal(t, "system:serviceaccount:space-a:eruun-runner", requests[2].Header.Get("Impersonate-User"))
	require.Equal(t, "/api/v1/namespaces/space-a/pods/runner", requests[2].URL.Path)
	limited, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err = server.jobs.SandboxClient.Resource(jobs.SandboxGVR).Namespace("space-a").Get(limited, "trial", metav1.GetOptions{})
	require.ErrorContains(t, err, "client rate limiter Wait returned an error")
	require.Len(t, requests, 3, "typed API and platform Sandbox requests exhaust one shared bucket")

	// Namespace provisioning still gives the tenant runner no Sandbox CRD access.
	client := fake.NewSimpleClientset()
	manager := &workspace.Manager{Client: client, Config: spec.WorkspaceConfig{ClusterCIDRs: []string{"10.96.0.0/12"}}}
	require.NoError(t, manager.Ensure(context.Background(), &model.Workspace{ID: "workspace-a", Namespace: "space-a"}))
	role, err := client.RbacV1().Roles("space-a").Get(context.Background(), "eruun-runner", metav1.GetOptions{})
	require.NoError(t, err)
	for _, rule := range role.Rules {
		require.NotContains(t, rule.APIGroups, "agents.kruise.io")
		require.NotContains(t, rule.Resources, "sandboxes")
	}
}

func TestBuildRuntimeQueuesBuildsOnlyRoleQueues(t *testing.T) {
	for _, tc := range []struct {
		role                    config.RuntimeRole
		dispatch, delay, result bool
	}{
		{role: config.RuntimeRoleAPI},
		{role: config.RuntimeRoleController, delay: true, result: true},
		{role: config.RuntimeRoleScheduler, dispatch: true},
		{role: config.RuntimeRoleWorker, dispatch: true, delay: true},
	} {
		t.Run(string(tc.role), func(t *testing.T) {
			cfg := config.NewConfig()
			cfg.Role = tc.role
			cfg.Messaging.Type = config.KAFKA
			cfg.Messaging.KafkaBrokers = []string{"127.0.0.1:9092"}
			server := &restServer{cfg: *cfg}

			queues, err := server.buildRuntimeQueues(nil)

			require.NoError(t, err)
			require.Equal(t, tc.dispatch, queues.Dispatch != nil)
			require.Equal(t, tc.delay, queues.Delay != nil)
			require.Equal(t, tc.result, queues.Result != nil)
			for _, queue := range []msg.Queue{queues.Dispatch, queues.Delay, queues.Result} {
				if queue != nil {
					require.NoError(t, queue.Close(context.Background()))
				}
			}
		})
	}
}

func TestSandboxObserversAreOnlyBuiltForConfiguredConsumers(t *testing.T) {
	for _, role := range []config.RuntimeRole{config.RuntimeRoleAPI, config.RuntimeRoleController, config.RuntimeRoleScheduler, config.RuntimeRoleWorker} {
		for _, configured := range []bool{false, true} {
			t.Run(string(role)+"/"+map[bool]string{false: "commands", true: "evaluations"}[configured], func(t *testing.T) {
				server := &restServer{cfg: config.Config{Role: role}, jobs: &jobs.Service{}}
				if configured {
					server.cfg.Jobs = &spec.JobsRuntimeConfig{}
				}
				limiter := flowcontrol.NewTokenBucketRateLimiter(100, 300)
				cfg := &rest.Config{Host: "https://kubernetes.example", RateLimiter: limiter}
				require.NoError(t, server.initSandboxObserver(fake.NewSimpleClientset(), cfg))
				want := configured && (role == config.RuntimeRoleAPI || role == config.RuntimeRoleController)
				require.Equal(t, want, server.sandboxObserver != nil)
				require.Equal(t, want, server.jobs.SandboxClient != nil)
				if want {
					require.Same(t, server.sandboxObserver, server.jobs.SandboxObserver)
					_, err := server.jobs.SandboxObserver.Sandbox("space", "pending")
					require.ErrorIs(t, err, informer.ErrSandboxObservationUnavailable)
					ready, _ := server.RuntimeReady()
					require.True(t, ready, "Sandbox initial sync must not block API or standby Controller readiness")
				}
				require.Same(t, limiter, cfg.RateLimiter)
			})
		}
	}
}

func TestInitRoleObserversBuildsOnlyOwnedObserver(t *testing.T) {
	for _, tc := range []struct {
		role                      config.RuntimeRole
		wantManager, wantObserver bool
	}{
		{role: config.RuntimeRoleAPI},
		{role: config.RuntimeRoleController, wantManager: true},
		{role: config.RuntimeRoleScheduler},
		{role: config.RuntimeRoleWorker, wantObserver: true},
	} {
		t.Run(string(tc.role), func(t *testing.T) {
			server := &restServer{cfg: config.Config{Role: tc.role}}

			server.initRoleObservers(fake.NewSimpleClientset())

			require.Equal(t, tc.wantManager, server.InformerManager != nil)
			require.Equal(t, tc.wantObserver, server.resourceObserver != nil)
		})
	}
}

func TestConfigureWorkflowEventWorkersAssignsRoleDependencies(t *testing.T) {
	dispatch := &testServerQueue{}
	delay := &testServerQueue{}
	result := &testServerQueue{}
	observer := informer.NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	worker := &workflowevent.Workflow{}
	workers := []event.Worker{worker}

	configureWorkflowEventWorkers(workers, &msg.RuntimeQueues{
		Dispatch: dispatch,
		Delay:    delay,
		Result:   result,
	}, observer)

	require.Same(t, dispatch, worker.Queue)
	require.Same(t, delay, worker.DelayQueue)
	require.Same(t, result, worker.ResultQueue)
	require.Same(t, observer, worker.ResourceWaiter)
}

package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

type workspaceRoundTripper func(*http.Request) (*http.Response, error)

func (f workspaceRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func workspaceResponse(code int, body []byte) *http.Response {
	return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}
}
func workspacePodSpec() corev1.PodSpec {
	return corev1.PodSpec{
		Containers:     []corev1.Container{{Name: "app", Image: "example.com/app:1"}, {Name: "sidecar", Image: "example.com/sidecar:1"}},
		InitContainers: []corev1.Container{{Name: "init", Image: "example.com/init:1"}},
	}
}
func requireWorkspacePodSecurity(t *testing.T, pod corev1.PodSpec) {
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

func TestWorkspaceRepeatedDeploymentDoesNotRollout(t *testing.T) {
	space := &model.Workspace{ID: "workspace", Namespace: "space"}
	manager := &workspace.Manager{RESTConfig: &rest.Config{Host: "https://kubernetes.example", Transport: workspaceRoundTripper(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		return workspaceResponse(http.StatusCreated, body), err
	})}}
	client, _, err := manager.TenantClient(space)
	require.NoError(t, err)
	for _, localhost := range []bool{false, true} {
		podSpec := func() corev1.PodSpec {
			pod := workspacePodSpec()
			if localhost {
				for _, containers := range [][]corev1.Container{pod.Containers, pod.InitContainers} {
					for i := range containers {
						containers[i].SecurityContext = &corev1.SecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: ptr.To("profiles/app.json")}}
					}
				}
			}
			return pod
		}
		for _, kind := range []string{"deployment", "statefulset"} {
			t.Run(fmt.Sprintf("%s/localhost=%t", kind, localhost), func(t *testing.T) {
				meta := metav1.ObjectMeta{Name: "app", Namespace: space.Namespace}
				template := corev1.PodTemplateSpec{Spec: podSpec()}
				task := &model.JobTask{AppID: "app", Namespace: space.Namespace}
				if kind == "deployment" {
					desired := &appsv1.Deployment{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{Template: template}}
					task.JobType, task.JobInfo = string(config.JobDeploy), desired
					_, err = workspace.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					requireWorkspacePodSecurity(t, desired.Spec.Template.Spec)
					current, err := client.AppsV1().Deployments(space.Namespace).Create(context.Background(), desired, metav1.CreateOptions{})
					require.NoError(t, err)
					// Regenerate the same source payload, as the next workflow invocation does.
					desired.Spec.Template.Spec = podSpec()
					_, err = workspace.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					require.False(t, isDeploymentChanged(current, desired))
					require.False(t, deploymentPodTemplateChanged(current, desired))
					desired.Spec.Template.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To(int64(1001))
					_, err = workspace.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					require.True(t, isDeploymentChanged(current, desired), "real security changes must still be applied")
					require.True(t, deploymentPodTemplateChanged(current, desired))
				} else {
					desired := &appsv1.StatefulSet{ObjectMeta: meta, Spec: appsv1.StatefulSetSpec{Template: template}}
					task.JobType, task.JobInfo = string(config.JobDeployStore), desired
					_, err = workspace.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					requireWorkspacePodSecurity(t, desired.Spec.Template.Spec)
					current, err := client.AppsV1().StatefulSets(space.Namespace).Create(context.Background(), desired, metav1.CreateOptions{})
					require.NoError(t, err)
					desired.Spec.Template.Spec = podSpec()
					_, err = workspace.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					require.False(t, statefulSetPodTemplateChanged(current, desired))
					require.False(t, statefulSetNeedsUpdate(current, desired))
					desired.Spec.Template.Spec.InitContainers[0].SecurityContext.RunAsUser = ptr.To(int64(1001))
					_, err = workspace.PrepareTask(task, "app", space, manager.Config)
					require.NoError(t, err)
					require.True(t, statefulSetPodTemplateChanged(current, desired))
					require.True(t, statefulSetNeedsUpdate(current, desired))
				}
			})
		}
	}
}

type delayedWorkspaceStore struct {
	*resultOutboxTestStore
	apps   map[string]*model.Applications
	spaces map[string]*model.Workspace
}

func (s *delayedWorkspaceStore) Get(ctx context.Context, e datastore.Entity) error {
	switch v := e.(type) {
	case *model.Applications:
		a, ok := s.apps[v.ID]
		if !ok {
			return datastore.ErrRecordNotExist
		}
		*v = *a
		return nil
	case *model.Workspace:
		w, ok := s.spaces[v.ID]
		if !ok {
			return datastore.ErrRecordNotExist
		}
		*v = *w
		return nil
	default:
		return s.resultOutboxTestStore.Get(ctx, e)
	}
}
func (s *delayedWorkspaceStore) List(ctx context.Context, e datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	if q, ok := e.(*model.Applications); ok {
		var apps []datastore.Entity
		for _, a := range s.apps {
			if a.WorkspaceID == q.WorkspaceID {
				apps = append(apps, a)
			}
		}
		return apps, nil
	}
	return s.resultOutboxTestStore.List(ctx, e, opts)
}
func delayedWorkspaceFixture(t *testing.T) (*delayedWorkspaceStore, *workspace.Manager, []*DelayJobPayload) {
	t.Helper()
	store := &delayedWorkspaceStore{resultOutboxTestStore: newResultOutboxTestStore(), apps: map[string]*model.Applications{}, spaces: map[string]*model.Workspace{}}
	root := fake.NewSimpleClientset()
	var payloads []*DelayJobPayload
	for id := 1; id <= 2; id++ {
		space := &model.Workspace{ID: fmt.Sprintf("space-%d", id), Namespace: fmt.Sprintf("namespace-%d", id)}
		app := &model.Applications{ID: fmt.Sprintf("app-%d", id), WorkspaceID: space.ID, Namespace: space.Namespace}
		store.apps[app.ID], store.spaces[space.ID] = app, space
		_, err := root.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: space.Namespace, Labels: map[string]string{workspace.OwnerLabel: space.ID}}}, metav1.CreateOptions{})
		require.NoError(t, err)
		payload := seedDelayRecoveryCheckpoint(t, store.resultOutboxTestStore, id, time.Now().Unix()-10)
		payload.Namespace, payload.Job.Namespace = space.Namespace, space.Namespace
		payload.Job.Spec.Template.Spec = workspacePodSpec()
		payload.Job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		raw, err := json.Marshal(payload)
		require.NoError(t, err)
		store.jobInfos[id].AppID, store.jobInfos[id].DelayPayload = app.ID, string(raw)
		payloads = append(payloads, payload)
	}
	root.ClearActions()
	return store, &workspace.Manager{Client: root, RESTConfig: &rest.Config{Host: "https://kubernetes.example"}}, payloads
}

func TestDelayedJobsUsePersistedWorkspaceAndRestrictedClient(t *testing.T) {
	store, manager, payloads := delayedWorkspaceFixture(t)
	created := map[string]*batchv1.Job{}
	manager.RESTConfig.Transport = workspaceRoundTripper(func(r *http.Request) (*http.Response, error) {
		scope, ok := access.FromContext(r.Context())
		require.True(t, ok)
		require.Equal(t, "system:serviceaccount:"+scope.Namespace+":eruun-runner", r.Header.Get("Impersonate-User"))
		require.True(t, strings.HasPrefix(r.URL.Path, "/apis/batch/v1/namespaces/"+scope.Namespace+"/jobs"))
		if r.Method == http.MethodGet {
			if job := created[scope.Namespace]; job != nil {
				raw, err := json.Marshal(job)
				return workspaceResponse(200, raw), err
			}
			return workspaceResponse(404, []byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)), nil
		}
		require.Equal(t, http.MethodPost, r.Method)
		var job batchv1.Job
		require.NoError(t, json.NewDecoder(r.Body).Decode(&job))
		require.Equal(t, scope.Namespace, job.Namespace)
		requireWorkspacePodSecurity(t, job.Spec.Template.Spec)
		created[scope.Namespace] = &job
		raw, err := json.Marshal(job)
		return workspaceResponse(201, raw), err
	})
	dispatcher := NewDelayDispatcher(nil, manager, access.NewStore(store), "", "")
	for _, payload := range payloads {
		// No live WorkflowQueue or user token: committed JobInfo remains authoritative.
		require.NoError(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}))
		require.NoError(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}), "duplicate dispatch must recover the same outbox")
	}
	require.Len(t, created, 2)
	require.Len(t, store.outboxes, 2)
	for _, a := range manager.Client.(*fake.Clientset).Actions() {
		require.Equal(t, "get", a.GetVerb())
		require.Equal(t, "namespaces", a.GetResource().Resource)
	}
}

func TestDelayedJobsRejectInvalidWorkspaceBeforeWrites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*delayedWorkspaceStore, *workspace.Manager, *DelayJobPayload)
	}{
		{"queue namespace", func(_ *delayedWorkspaceStore, _ *workspace.Manager, p *DelayJobPayload) { p.Namespace = "namespace-2" }},
		{"job namespace", func(_ *delayedWorkspaceStore, _ *workspace.Manager, p *DelayJobPayload) {
			p.Job.Namespace = "namespace-2"
		}},
		{"privileged init", func(_ *delayedWorkspaceStore, _ *workspace.Manager, p *DelayJobPayload) {
			p.Job.Spec.Template.Spec.InitContainers[0].SecurityContext = &corev1.SecurityContext{Privileged: ptr.To(true)}
		}},
		{"privileged sidecar", func(_ *delayedWorkspaceStore, _ *workspace.Manager, p *DelayJobPayload) {
			p.Job.Spec.Template.Spec.Containers[1].SecurityContext = &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(true)}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, manager, payloads := delayedWorkspaceFixture(t)
			tt.mutate(store, manager, payloads[0])
			before, err := json.Marshal(store.jobInfos)
			require.NoError(t, err)
			manager.RESTConfig.Transport = workspaceRoundTripper(func(*http.Request) (*http.Response, error) {
				t.Error("rejected workload reached Kubernetes")
				return nil, fmt.Errorf("unexpected Kubernetes request")
			})
			dispatcher := NewDelayDispatcher(nil, manager, access.NewStore(store), "", "")
			require.Error(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payloads[0]}))
			require.Empty(t, store.outboxes)
			after, err := json.Marshal(store.jobInfos)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after))
			for _, a := range manager.Client.(*fake.Clientset).Actions() {
				require.Equal(t, "get", a.GetVerb())
			}
		})
	}
}

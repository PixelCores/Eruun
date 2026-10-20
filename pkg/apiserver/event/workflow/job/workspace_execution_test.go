package job

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
)

type delayedWorkspaceStore struct {
	*resultOutboxTestStore
	apps   map[string]*model.Applications
	spaces map[string]*model.Workspace
}

func (s *delayedWorkspaceStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
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
func delayedWorkspaceFixture(t *testing.T) (*delayedWorkspaceStore, *workspace.Manager, []*jobinfo.DelayJobPayload) {
	t.Helper()
	store := &delayedWorkspaceStore{resultOutboxTestStore: newResultOutboxTestStore(), apps: map[string]*model.Applications{}, spaces: map[string]*model.Workspace{}}
	root := fake.NewSimpleClientset()
	var payloads []*jobinfo.DelayJobPayload
	for id := 1; id <= 2; id++ {
		space := &model.Workspace{ID: fmt.Sprintf("space-%d", id), Namespace: fmt.Sprintf("namespace-%d", id)}
		app := &model.Applications{ID: fmt.Sprintf("app-%d", id), WorkspaceID: space.ID, Namespace: space.Namespace}
		store.apps[app.ID], store.spaces[space.ID] = app, space
		_, err := root.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: space.Namespace, Labels: map[string]string{workspace.OwnerLabel: space.ID}}}, metav1.CreateOptions{})
		require.NoError(t, err)
		payload := seedDelayRecoveryCheckpoint(t, store.resultOutboxTestStore, id, time.Now().Unix()-10)
		payload.Namespace, payload.Job.Namespace = space.Namespace, space.Namespace
		payload.Job.Spec.Template.Spec = jobtest.WorkspacePodSpec()
		payload.Job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		raw, err := json.Marshal(payload)
		require.NoError(t, err)
		store.jobInfos[id].AppID, store.jobInfos[id].DelayPayload = app.ID, string(raw)
		store.jobInfos[id].WorkspaceID = app.WorkspaceID
		payloads = append(payloads, payload)
	}
	root.ClearActions()
	return store, &workspace.Manager{Client: root, RESTConfig: &rest.Config{Host: "https://kubernetes.example"}}, payloads
}

func TestDelayedJobsUsePersistedWorkspaceAndRestrictedClient(t *testing.T) {
	store, manager, payloads := delayedWorkspaceFixture(t)
	created := map[string]*batchv1.Job{}
	manager.RESTConfig.Transport = jobtest.WorkspaceRoundTripper(func(r *http.Request) (*http.Response, error) {
		scope, ok := access.FromContext(r.Context())
		require.True(t, ok)
		require.Equal(t, "system:serviceaccount:"+scope.Namespace+":eruun-runner", r.Header.Get("Impersonate-User"))
		require.True(t, strings.HasPrefix(r.URL.Path, "/apis/batch/v1/namespaces/"+scope.Namespace+"/jobs"))
		if r.Method == http.MethodGet {
			if job := created[scope.Namespace]; job != nil {
				raw, err := json.Marshal(job)
				return jobtest.WorkspaceResponse(200, raw), err
			}
			return jobtest.WorkspaceResponse(404, []byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)), nil
		}
		require.Equal(t, http.MethodPost, r.Method)
		var job batchv1.Job
		require.NoError(t, json.NewDecoder(r.Body).Decode(&job))
		require.Equal(t, scope.Namespace, job.Namespace)
		jobtest.RequireWorkspacePodSecurity(t, job.Spec.Template.Spec)
		created[scope.Namespace] = &job
		raw, err := json.Marshal(job)
		return jobtest.WorkspaceResponse(201, raw), err
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

func TestDelayedJobsRejectInvalidPersistedWorkloadBeforeKubernetesWrites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*delayedWorkspaceStore, *workspace.Manager, *jobinfo.DelayJobPayload)
	}{
		{"checkpoint namespace", func(_ *delayedWorkspaceStore, _ *workspace.Manager, p *jobinfo.DelayJobPayload) {
			p.Namespace = "namespace-2"
		}},
		{"job namespace", func(_ *delayedWorkspaceStore, _ *workspace.Manager, p *jobinfo.DelayJobPayload) {
			p.Job.Namespace = "namespace-2"
		}},
		{"privileged init", func(_ *delayedWorkspaceStore, _ *workspace.Manager, p *jobinfo.DelayJobPayload) {
			p.Job.Spec.Template.Spec.InitContainers[0].SecurityContext = &corev1.SecurityContext{Privileged: ptr.To(true)}
		}},
		{"privileged sidecar", func(_ *delayedWorkspaceStore, _ *workspace.Manager, p *jobinfo.DelayJobPayload) {
			p.Job.Spec.Template.Spec.Containers[1].SecurityContext = &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(true)}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, manager, payloads := delayedWorkspaceFixture(t)
			tt.mutate(store, manager, payloads[0])
			// The current queue protocol carries identity only. Security checks
			// validate the workload restored from the authoritative checkpoint.
			raw, err := json.Marshal(payloads[0])
			require.NoError(t, err)
			store.jobInfos[1].DelayPayload = string(raw)
			unrelated := *store.jobInfos[2]
			manager.RESTConfig.Transport = jobtest.WorkspaceRoundTripper(func(*http.Request) (*http.Response, error) {
				t.Error("rejected workload reached Kubernetes")
				return nil, fmt.Errorf("unexpected Kubernetes request")
			})
			dispatcher := NewDelayDispatcher(nil, manager, access.NewStore(store), "", "")
			require.Error(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payloads[0]}))
			require.Empty(t, store.outboxes)
			require.Equal(t, string(config.StatusFailed), store.jobInfos[1].Status)
			require.NotEmpty(t, store.jobInfos[1].Error)
			require.Equal(t, unrelated, *store.jobInfos[2])
			for _, a := range manager.Client.(*fake.Clientset).Actions() {
				require.Equal(t, "get", a.GetVerb())
			}
		})
	}
}

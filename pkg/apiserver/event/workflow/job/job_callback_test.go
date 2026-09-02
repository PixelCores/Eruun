package job

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func newCallbackTestServer(t *testing.T, handler http.HandlerFunc, connState ...func(net.Conn, http.ConnState)) *httptest.Server {
	t.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("skipping callback network test in restricted environment: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	if len(connState) > 0 {
		server.Config.ConnState = connState[0]
	}
	server.Listener = listener
	server.Start()
	return server
}

func TestCallbackJobCtlRunPostSuccess(t *testing.T) {
	var payload CallbackPayload
	var idempotencyKey string
	closed := make(chan struct{}, 1)
	server := newCallbackTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		idempotencyKey = r.Header.Get(callbackIdempotencyHeader)
		defer r.Body.Close()
		err := json.NewDecoder(r.Body).Decode(&payload)
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}), func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	})
	defer server.Close()

	jobTask := &model.JobTask{
		Name:         "callback",
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		TaskID:       "task-1",
		JobType:      string(config.JobDeployCallback),
		ExecutionKey: "execution-1",
		JobInfo: &CallbackJobInfo{
			Event:  "success",
			URL:    server.URL,
			Method: http.MethodPost,
			Payload: CallbackPayload{
				Event:        "success",
				Status:       string(config.StatusCompleted),
				AppID:        "app-1",
				WorkflowID:   "wf-1",
				WorkflowName: "deploy",
				TaskID:       "task-1",
				WorkflowType: config.WorkflowTaskTypeWorkflow,
			},
		},
	}

	ctl := NewCallbackJobCtl(jobTask, nil, &spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})
	require.NotNil(t, ctl)
	err := ctl.Run(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, jobTask.Info)

	var record CallbackJobRecord
	err = json.Unmarshal([]byte(jobTask.Info), &record)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, record.StatusCode)
	require.Equal(t, "success", record.Event)
	require.Equal(t, server.URL, record.URL)
	require.Equal(t, string(config.StatusCompleted), payload.Status)
	require.Equal(t, "execution-1", payload.ExecutionKey)
	require.Equal(t, "execution-1", idempotencyKey)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("expected callback client to close its idle connection")
	}
}

func TestCallbackJobCtlRunGetSuccess(t *testing.T) {
	server := newCallbackTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		query := r.URL.Query()
		require.Equal(t, "success", query.Get("event"))
		require.Equal(t, "app-1", query.Get("appId"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	jobTask := &model.JobTask{
		Name:       "callback",
		WorkflowID: "wf-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDeployCallback),
		JobInfo: &CallbackJobInfo{
			Event:  "success",
			URL:    server.URL,
			Method: http.MethodGet,
			Payload: CallbackPayload{
				Event:        "success",
				Status:       string(config.StatusCompleted),
				AppID:        "app-1",
				WorkflowID:   "wf-1",
				WorkflowName: "deploy",
				TaskID:       "task-1",
				WorkflowType: config.WorkflowTaskTypeWorkflow,
			},
		},
	}

	ctl := NewCallbackJobCtl(jobTask, nil, &spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})
	require.NotNil(t, ctl)
	err := ctl.Run(context.Background())
	require.NoError(t, err)
}

func TestCallbackJobCtlRunRejectsPrivateTargetWhenDisabled(t *testing.T) {
	server := newCallbackTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	jobTask := &model.JobTask{
		Name:       "callback",
		WorkflowID: "wf-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDeployCallback),
		JobInfo: &CallbackJobInfo{
			Event:  "success",
			URL:    server.URL,
			Method: http.MethodGet,
			Payload: CallbackPayload{
				Event:        "success",
				Status:       string(config.StatusCompleted),
				AppID:        "app-1",
				WorkflowID:   "wf-1",
				WorkflowName: "deploy",
				TaskID:       "task-1",
				WorkflowType: config.WorkflowTaskTypeWorkflow,
			},
		},
	}

	ctl := NewCallbackJobCtl(jobTask, nil, &spec.URLSecurityPolicySpec{AllowPrivateByDefault: false})
	require.NotNil(t, ctl)
	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "private")
}

func TestCallbackJobCtlRunRequiresURLSecurityPolicy(t *testing.T) {
	jobTask := &model.JobTask{
		Name:       "callback",
		WorkflowID: "wf-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDeployCallback),
		JobInfo: &CallbackJobInfo{
			Event:  "success",
			URL:    "https://example.com/callback",
			Method: http.MethodGet,
		},
	}

	ctl := NewCallbackJobCtl(jobTask, nil, nil)
	require.NotNil(t, ctl)
	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "url security policy is required")
}

func TestCallbackJobCtlRunRejectsRedirectToPrivateTarget(t *testing.T) {
	privateTarget := newCallbackTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer privateTarget.Close()

	redirectServer := newCallbackTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, privateTarget.URL, http.StatusFound)
	}))
	defer redirectServer.Close()

	entryURL := strings.Replace(redirectServer.URL, "127.0.0.1", "localhost", 1)
	jobTask := &model.JobTask{
		Name:       "callback",
		WorkflowID: "wf-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDeployCallback),
		JobInfo: &CallbackJobInfo{
			Event:  "success",
			URL:    entryURL,
			Method: http.MethodGet,
			Payload: CallbackPayload{
				Event:        "success",
				Status:       string(config.StatusCompleted),
				AppID:        "app-1",
				WorkflowID:   "wf-1",
				WorkflowName: "deploy",
				TaskID:       "task-1",
				WorkflowType: config.WorkflowTaskTypeWorkflow,
			},
		},
	}

	policy := &spec.URLSecurityPolicySpec{
		AllowedHostPatterns: []string{"localhost"},
	}
	ctl := NewCallbackJobCtl(jobTask, nil, policy)
	require.NotNil(t, ctl)
	err := ctl.Run(context.Background())
	require.Error(t, err)
}

func TestCallbackJobCtlRunRejectsCrossOriginRedirectBeforeForwardingHeaders(t *testing.T) {
	targetCalled := make(chan struct{}, 1)
	target := newCallbackTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalled <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	entry := newCallbackTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer entry.Close()

	jobTask := &model.JobTask{
		Name:       "callback",
		WorkflowID: "wf-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDeployCallback),
		JobInfo: &CallbackJobInfo{
			Event:  "success",
			URL:    entry.URL,
			Method: http.MethodGet,
			Headers: map[string]string{
				"Authorization": "Bearer callback-secret",
				"Cookie":        "session=callback-secret",
				"X-Api-Key":     "callback-secret",
				"X-Custom-Auth": "callback-secret",
			},
		},
	}

	ctl := NewCallbackJobCtl(jobTask, nil, &spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})
	require.NotNil(t, ctl)
	err := ctl.Run(context.Background())
	require.ErrorContains(t, err, "different origin")
	select {
	case <-targetCalled:
		t.Fatal("cross-origin redirect target must not receive a request")
	default:
	}
}

func TestCallbackJobCtlRunAllowsSameOriginRedirect(t *testing.T) {
	server := newCallbackTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/finish", http.StatusFound)
			return
		}
		require.Equal(t, "/finish", r.URL.Path)
		require.Equal(t, "trace-1", r.Header.Get("X-Trace-ID"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	jobTask := &model.JobTask{
		Name:       "callback",
		WorkflowID: "wf-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDeployCallback),
		JobInfo: &CallbackJobInfo{
			Event:   "success",
			URL:     server.URL + "/start",
			Method:  http.MethodGet,
			Headers: map[string]string{"X-Trace-ID": "trace-1"},
		},
	}

	ctl := NewCallbackJobCtl(jobTask, nil, &spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})
	require.NotNil(t, ctl)
	require.NoError(t, ctl.Run(context.Background()))
}

func TestCallbackURLsShareOriginUsesEffectivePort(t *testing.T) {
	require.True(t, callbackURLsShareOrigin(
		&url.URL{Scheme: "https", Host: "EXAMPLE.com"},
		&url.URL{Scheme: "https", Host: "example.COM:443"},
	))
	require.True(t, callbackURLsShareOrigin(
		&url.URL{Scheme: "http", Host: "[2001:db8::1]"},
		&url.URL{Scheme: "http", Host: "[2001:DB8::1]:80"},
	))
	require.False(t, callbackURLsShareOrigin(
		&url.URL{Scheme: "https", Host: "example.com"},
		&url.URL{Scheme: "http", Host: "example.com:443"},
	))
	require.False(t, callbackURLsShareOrigin(
		&url.URL{Scheme: "https", Host: "example.com"},
		&url.URL{Scheme: "https", Host: "example.com:8443"},
	))
}

func TestCallbackURLsShareOriginCanonicalizesIDNAHosts(t *testing.T) {
	require.True(t, callbackURLsShareOrigin(
		&url.URL{Scheme: "https", Host: "bücher.example"},
		&url.URL{Scheme: "https", Host: "xn--bcher-kva.example:443"},
	))
	require.False(t, callbackURLsShareOrigin(
		&url.URL{Scheme: "https", Host: "σ.example"},
		&url.URL{Scheme: "https", Host: "ς.example"},
	))
}

func TestCallbackTimeoutUsesDefaultWhenZero(t *testing.T) {
	timeout := callbackTimeout(0, 0, 0)
	require.Equal(t, workflowconfig.DefaultWorkflowCallbackTimeout, timeout)
}

func TestCallbackTimeoutCapsToMax(t *testing.T) {
	timeout := callbackTimeout(int64((96*time.Hour)/time.Second), int64((72*time.Hour)/time.Second), 0)
	require.Equal(t, 72*time.Hour, timeout)
}

func TestCallbackTimeoutCapsToSubSecondMax(t *testing.T) {
	timeout := callbackTimeout(3600, 1, int64(500*time.Millisecond))
	require.Equal(t, 500*time.Millisecond, timeout)
}

func TestSanitizeCallbackHeadersRedactsSensitiveValues(t *testing.T) {
	headers := sanitizeCallbackHeaders(map[string]string{
		"Authorization": "Bearer secret-token",
		"X-Api-Key":     "api-key-value",
		"X-Trace-ID":    "trace-1",
		" secret ":      "hidden",
		" ":             "ignored",
	})

	require.Equal(t, callbackLogRedacted, headers["Authorization"])
	require.Equal(t, callbackLogRedacted, headers["X-Api-Key"])
	require.Equal(t, callbackLogRedacted, headers["secret"])
	require.Equal(t, "trace-1", headers["X-Trace-ID"])
	require.NotContains(t, headers, " ")
}

func TestSanitizeCallbackURLRedactsSensitiveQueryValues(t *testing.T) {
	sanitized := sanitizeCallbackURL("https://example.com/callback?token=abc&signature=sig&key=raw&workflowId=wf-1&event=success")

	parsed, err := url.Parse(sanitized)
	require.NoError(t, err)
	values := parsed.Query()
	require.Equal(t, callbackLogRedacted, values.Get("token"))
	require.Equal(t, callbackLogRedacted, values.Get("signature"))
	require.Equal(t, callbackLogRedacted, values.Get("key"))
	require.Equal(t, "wf-1", values.Get("workflowId"))
	require.Equal(t, "success", values.Get("event"))
}

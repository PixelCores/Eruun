// Package kubetest provides Kubernetes API test servers for packages that
// exercise real client-go call paths. It is imported only by tests.
package kubetest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/client-go/rest"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/spdy"
)

// ExecRequest is one pods/exec call as the API server receives it.
type ExecRequest struct {
	// Context ends when the client closes the exec connection.
	Context   context.Context
	Namespace string
	Pod       string
	Container string
	Command   []string
	Stdin     io.Reader
	Stdout    io.Writer
	Stderr    io.Writer
}

// ExecHandler runs one exec request. Returning ExitCode reports a non-zero
// command exit; any other error is reported as an exec failure status.
type ExecHandler func(ExecRequest) error

// ExitCode is a command exit status returned by an ExecHandler.
type ExitCode int

func (c ExitCode) Error() string { return "command exited with code " + strconv.Itoa(int(c)) }

// NewExecServer serves the pods/exec subresource over SPDY with the v4 channel
// protocol and returns a REST config that points at it.
func NewExecServer(t testing.TB, handler ExecHandler) *rest.Config {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		serveExec(t, w, req, handler)
	}))
	t.Cleanup(server.Close)
	return &rest.Config{Host: server.URL}
}

type execStream struct {
	stream    httpstream.Stream
	replySent <-chan struct{}
}

func serveExec(t testing.TB, w http.ResponseWriter, req *http.Request, handler ExecHandler) {
	namespace, pod, ok := execTarget(req.URL.Path)
	if !ok {
		http.NotFound(w, req)
		return
	}
	if _, err := httpstream.Handshake(req, w, []string{remotecommand.StreamProtocolV4Name}); err != nil {
		return
	}
	streams := make(chan execStream, 8)
	conn := spdy.NewResponseUpgrader().UpgradeResponse(w, req, func(stream httpstream.Stream, replySent <-chan struct{}) error {
		streams <- execStream{stream: stream, replySent: replySent}
		return nil
	})
	if conn == nil {
		return
	}
	defer conn.Close()

	query := req.URL.Query()
	expected := 1
	for _, name := range []string{"stdin", "stdout", "stderr"} {
		if query.Get(name) == "true" {
			expected++
		}
	}
	byType := make(map[string]httpstream.Stream, expected)
	timeout := time.After(5 * time.Second)
	for len(byType) < expected {
		select {
		case received := <-streams:
			<-received.replySent
			byType[received.stream.Headers().Get(corev1.StreamType)] = received.stream
		case <-timeout:
			t.Errorf("exec %s/%s: received %d of %d streams", namespace, pod, len(byType), expected)
			return
		}
	}

	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	go func() {
		select {
		case <-conn.CloseChan():
			cancel()
		case <-ctx.Done():
		}
	}()
	request := ExecRequest{
		Context: ctx, Namespace: namespace, Pod: pod, Container: query.Get("container"), Command: query["command"],
		Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard,
	}
	if stream := byType[corev1.StreamTypeStdin]; stream != nil {
		request.Stdin = stream
	}
	if stream := byType[corev1.StreamTypeStdout]; stream != nil {
		request.Stdout = stream
	}
	if stream := byType[corev1.StreamTypeStderr]; stream != nil {
		request.Stderr = stream
	}
	err := handler(request)
	for _, name := range []string{corev1.StreamTypeStdout, corev1.StreamTypeStderr} {
		if stream := byType[name]; stream != nil {
			_ = stream.Close()
		}
	}
	if ctx.Err() != nil {
		return // The client closed the connection; nobody reads the status.
	}
	errorStream := byType[corev1.StreamTypeError]
	if err := json.NewEncoder(errorStream).Encode(execStatus(err)); err != nil {
		t.Errorf("write exec status: %v", err)
	}
	_ = errorStream.Close()
}

func execTarget(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	// api/v1/namespaces/<namespace>/pods/<pod>/exec
	if len(parts) != 7 || parts[2] != "namespaces" || parts[4] != "pods" || parts[6] != "exec" {
		return "", "", false
	}
	return parts[3], parts[5], true
}

func execStatus(err error) metav1.Status {
	if err == nil {
		return metav1.Status{Status: metav1.StatusSuccess}
	}
	var exit ExitCode
	if errors.As(err, &exit) {
		return metav1.Status{
			Status:  metav1.StatusFailure,
			Reason:  remotecommand.NonZeroExitCodeReason,
			Message: exit.Error(),
			Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{{
				Type:    remotecommand.ExitCodeCauseType,
				Message: strconv.Itoa(int(exit)),
			}}},
		}
	}
	return metav1.Status{Status: metav1.StatusFailure, Message: err.Error()}
}

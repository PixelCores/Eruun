package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestWorkflowCallbackTerminalTarget(t *testing.T) {
	full := &WorkflowCallback{
		Success: " https://example.com/success ", Failure: " https://example.com/failure ",
		Cancelled: " https://example.com/cancelled ", Timeout: " https://example.com/timeout ", Reject: " https://example.com/reject ",
		Methods: map[string]string{"success": " get ", "failure": " post ", "cancelled": " patch ", "timeout": " put ", "reject": " delete "},
	}
	fallback := &WorkflowCallback{Failure: " https://example.com/failure ", Methods: map[string]string{"failure": " post ", "cancelled": "PUT"}}
	cases := []struct {
		name     string
		callback *WorkflowCallback
		status   config.Status
		event    string
		url      string
		method   string
	}{
		{name: "nil", status: config.StatusFailed},
		{name: "completed", callback: full, status: config.StatusCompleted, event: "success", url: "https://example.com/success", method: "GET"},
		{name: "passed", callback: full, status: config.StatusPassed, event: "success", url: "https://example.com/success", method: "GET"},
		{name: "failed", callback: full, status: config.StatusFailed, event: "failure", url: "https://example.com/failure", method: "POST"},
		{name: "cancelled", callback: full, status: config.StatusCancelled, event: "cancelled", url: "https://example.com/cancelled", method: "PATCH"},
		{name: "timeout", callback: full, status: config.StatusTimeout, event: "timeout", url: "https://example.com/timeout", method: "PUT"},
		{name: "rejected", callback: full, status: config.StatusReject, event: "reject", url: "https://example.com/reject", method: "DELETE"},
		{name: "cancelled fallback", callback: fallback, status: config.StatusCancelled, event: "failure", url: "https://example.com/failure", method: "POST"},
		{name: "timeout fallback", callback: fallback, status: config.StatusTimeout, event: "failure", url: "https://example.com/failure", method: "POST"},
		{name: "rejected fallback", callback: fallback, status: config.StatusReject, event: "failure", url: "https://example.com/failure", method: "POST"},
		{name: "success does not fall back", callback: fallback, status: config.StatusCompleted, event: "success"},
		{name: "whitespace target does not fall back", callback: &WorkflowCallback{Cancelled: " \t", Failure: fallback.Failure, Methods: map[string]string{"cancelled": " patch "}}, status: config.StatusCancelled, event: "cancelled", method: "PATCH"},
		{name: "missing target", callback: &WorkflowCallback{}, status: config.StatusFailed, event: "failure"},
		{name: "method key is case sensitive", callback: &WorkflowCallback{Success: full.Success, Methods: map[string]string{"SUCCESS": "POST"}}, status: config.StatusCompleted, event: "success", url: "https://example.com/success"},
		{name: "nonterminal", callback: full, status: config.StatusRunning},
		{name: "unknown status", callback: full, status: config.Status("unknown")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event, url, method := tc.callback.TerminalTarget(tc.status)
			require.Equal(t, tc.event, event)
			require.Equal(t, tc.url, url)
			require.Equal(t, tc.method, method)
		})
	}
}

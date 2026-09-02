package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/net/idna"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

const (
	callbackResponseMaxBytes  = 4096
	defaultCallbackTimeout    = 10 * time.Second
	callbackLogRedacted       = "<redacted>"
	callbackIdempotencyHeader = "Idempotency-Key"
)

var callbackHTTPClient = &http.Client{}

type CallbackPayload struct {
	Event        string                  `json:"event"`
	Status       string                  `json:"status"`
	AppID        string                  `json:"appId"`
	WorkflowID   string                  `json:"workflowId"`
	WorkflowName string                  `json:"workflowName"`
	TaskID       string                  `json:"taskId"`
	ExecutionKey string                  `json:"executionKey,omitempty"`
	WorkflowType config.WorkflowTaskType `json:"workflowType"`
	StepName     string                  `json:"stepName,omitempty"`
	Message      string                  `json:"message,omitempty"`
	ApprovalPath string                  `json:"approvalPath,omitempty"`
	StartTime    int64                   `json:"startTime,omitempty"`
	EndTime      int64                   `json:"endTime,omitempty"`
	Reason       string                  `json:"reason,omitempty"`
}

type CallbackJobInfo struct {
	Event          string            `json:"event"`
	URL            string            `json:"url"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers,omitempty"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitempty"`
	TimeoutMaxSec  int64             `json:"timeoutMaxSec,omitempty"`
	TimeoutMaxNS   int64             `json:"timeoutMaxNs,omitempty"`
	Payload        CallbackPayload   `json:"payload"`
}

type CallbackJobRecord struct {
	Event      string `json:"event"`
	URL        string `json:"url"`
	Method     string `json:"method"`
	StatusCode int    `json:"statusCode,omitempty"`
	Response   string `json:"response,omitempty"`
	Error      string `json:"error,omitempty"`
}

type CallbackJobCtl struct {
	job               *model.JobTask
	store             datastore.DataStore
	urlSecurityPolicy *spec.URLSecurityPolicySpec
}

func NewCallbackJobCtl(job *model.JobTask, store datastore.DataStore, urlSecurityPolicy *spec.URLSecurityPolicySpec) *CallbackJobCtl {
	if job == nil {
		klog.Errorf("CallbackJobCtl: job is nil")
		return nil
	}
	return &CallbackJobCtl{job: job, store: store, urlSecurityPolicy: urlSecurityPolicy}
}

func (c *CallbackJobCtl) Clean(ctx context.Context) {}

func (c *CallbackJobCtl) SaveInfo(ctx context.Context) error {
	return saveJobInfo(ctx, c.store, c.job)
}

func (c *CallbackJobCtl) Run(ctx context.Context) error {
	if c.urlSecurityPolicy == nil {
		return fmt.Errorf("url security policy is required")
	}
	info, err := callbackInfoFromJobInfo(c.job)
	if err != nil {
		return fmt.Errorf("callback job info is invalid: %w", err)
	}
	if info.Payload.ExecutionKey == "" {
		info.Payload.ExecutionKey = c.job.ExecutionKey
	}
	method := normalizeCallbackMethod(info.Method)
	if method == "" {
		method = http.MethodPost
	}
	if !isCallbackMethodSupported(method) {
		return fmt.Errorf("unsupported callback method %q", method)
	}
	info.Method = method

	payloadBytes, err := json.Marshal(info.Payload)
	if err != nil {
		klog.ErrorS(err, "workflow callback request payload marshal failed", callbackLogValues(info, method, strings.TrimSpace(info.URL), "", "", 0)...)
		return fmt.Errorf("marshal callback payload: %w", err)
	}

	requestURL := strings.TrimSpace(info.URL)
	body := io.Reader(nil)
	requestBody := ""
	if method == http.MethodGet || method == http.MethodDelete {
		requestURL, err = appendCallbackQuery(requestURL, info.Payload)
		if err != nil {
			klog.ErrorS(err, "workflow callback request build failed", callbackLogValues(info, method, requestURL, string(payloadBytes), requestBody, 0)...)
			return err
		}
	} else {
		requestBody = string(payloadBytes)
		body = bytes.NewReader(payloadBytes)
	}
	if _, err := utils.ValidateURLTarget(ctx, requestURL, c.urlSecurityPolicy); err != nil {
		klog.ErrorS(err, "workflow callback request blocked by url policy", callbackLogValues(info, method, requestURL, string(payloadBytes), requestBody, 0)...)
		return fmt.Errorf("validate callback url: %w", err)
	}

	requestCtx, cancel := context.WithTimeout(ctx, callbackTimeout(info.TimeoutSeconds, info.TimeoutMaxSec, info.TimeoutMaxNS))
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, method, requestURL, body)
	if err != nil {
		klog.ErrorS(err, "workflow callback request build failed", callbackLogValues(info, method, requestURL, string(payloadBytes), requestBody, 0)...)
		return fmt.Errorf("build callback request: %w", err)
	}
	for key, value := range info.Headers {
		name := strings.TrimSpace(key)
		if name == "" {
			continue
		}
		req.Header.Set(name, value)
	}
	if info.Payload.ExecutionKey != "" && req.Header.Get(callbackIdempotencyHeader) == "" {
		req.Header.Set(callbackIdempotencyHeader, info.Payload.ExecutionKey)
	}
	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	klog.InfoS("workflow callback request sending", callbackLogValues(info, method, requestURL, string(payloadBytes), requestBody, 0)...)

	client, err := newCallbackURLPolicyHTTPClient(callbackHTTPClient, c.urlSecurityPolicy)
	if err != nil {
		c.job.Info = recordCallbackJobInfo(info, 0, "", err)
		return fmt.Errorf("create callback http client: %w", err)
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		c.job.Info = recordCallbackJobInfo(info, 0, "", err)
		klog.ErrorS(err, "workflow callback request failed", callbackLogValues(info, method, requestURL, string(payloadBytes), requestBody, 0)...)
		return err
	}
	defer resp.Body.Close()

	respBody, _ := readCallbackBody(resp.Body)
	c.job.Info = recordCallbackJobInfo(info, resp.StatusCode, respBody, nil)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		err := fmt.Errorf("callback request failed with status: %d", resp.StatusCode)
		klog.ErrorS(err, "workflow callback response received", callbackLogValues(info, method, requestURL, string(payloadBytes), requestBody, resp.StatusCode, respBody)...)
		return err
	}
	klog.InfoS("workflow callback response received", callbackLogValues(info, method, requestURL, string(payloadBytes), requestBody, resp.StatusCode, respBody)...)
	return nil
}

func normalizeCallbackMethod(method string) string {
	return strings.ToUpper(strings.TrimSpace(method))
}

func isCallbackMethodSupported(method string) bool {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

func callbackURLsShareOrigin(first, second *url.URL) bool {
	if first == nil || second == nil ||
		!strings.EqualFold(first.Scheme, second.Scheme) {
		return false
	}
	canonicalHostname := func(target *url.URL) (string, error) {
		hostname := target.Hostname()
		for i := 0; i < len(hostname); i++ {
			if hostname[i] >= utf8.RuneSelf {
				return idna.Lookup.ToASCII(hostname)
			}
		}
		return hostname, nil
	}
	firstHostname, err := canonicalHostname(first)
	if err != nil || firstHostname == "" {
		return false
	}
	secondHostname, err := canonicalHostname(second)
	if err != nil || secondHostname == "" || !strings.EqualFold(firstHostname, secondHostname) {
		return false
	}
	port := func(target *url.URL) string {
		if target.Port() != "" {
			return target.Port()
		}
		switch strings.ToLower(target.Scheme) {
		case "http":
			return "80"
		case "https":
			return "443"
		default:
			return ""
		}
	}
	return port(first) == port(second)
}

func newCallbackURLPolicyHTTPClient(base *http.Client, policy *spec.URLSecurityPolicySpec) (*http.Client, error) {
	if base == nil {
		base = &http.Client{}
	}
	baseRedirect := base.CheckRedirect
	callbackBase := &http.Client{
		Transport: base.Transport,
		Jar:       base.Jar,
		Timeout:   base.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > 0 && !callbackURLsShareOrigin(via[0].URL, req.URL) {
				return fmt.Errorf("callback redirect to a different origin is not allowed")
			}
			if baseRedirect != nil {
				return baseRedirect(req, via)
			}
			return nil
		},
	}
	return utils.NewURLPolicyHTTPClient(callbackBase, policy)
}

func callbackTimeout(seconds, maxSeconds, maxNS int64) time.Duration {
	maxTimeout := config.DefaultWorkflowCallbackTimeoutMax
	if maxNS > 0 {
		maxTimeout = time.Duration(maxNS)
	} else if maxSeconds > 0 {
		maxTimeout = time.Duration(maxSeconds) * time.Second
	}
	return config.ResolveWorkflowCallbackTimeout(seconds, maxTimeout)
}

func appendCallbackQuery(rawURL string, payload CallbackPayload) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse callback url: %w", err)
	}
	values := parsed.Query()
	values.Set("event", payload.Event)
	values.Set("status", payload.Status)
	values.Set("appId", payload.AppID)
	values.Set("workflowId", payload.WorkflowID)
	values.Set("workflowName", payload.WorkflowName)
	values.Set("taskId", payload.TaskID)
	values.Set("workflowType", string(payload.WorkflowType))
	if payload.StepName != "" {
		values.Set("stepName", payload.StepName)
	}
	if payload.Message != "" {
		values.Set("message", payload.Message)
	}
	if payload.ApprovalPath != "" {
		values.Set("approvalPath", payload.ApprovalPath)
	}
	if payload.StartTime > 0 {
		values.Set("startTime", fmt.Sprintf("%d", payload.StartTime))
	}
	if payload.EndTime > 0 {
		values.Set("endTime", fmt.Sprintf("%d", payload.EndTime))
	}
	if payload.Reason != "" {
		values.Set("reason", payload.Reason)
	}
	parsed.RawQuery = values.Encode()
	return parsed.String(), nil
}

func recordCallbackJobInfo(info *CallbackJobInfo, statusCode int, response string, err error) string {
	record := CallbackJobRecord{
		Event:      info.Event,
		URL:        info.URL,
		Method:     normalizeCallbackMethod(info.Method),
		StatusCode: statusCode,
		Response:   response,
	}
	if err != nil {
		record.Error = err.Error()
	}
	data, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		return ""
	}
	return string(data)
}

func callbackLogValues(info *CallbackJobInfo, method, requestURL, payload, requestBody string, statusCode int, response ...string) []interface{} {
	event := ""
	headers := map[string]string(nil)
	appID := ""
	workflowID := ""
	workflowName := ""
	taskID := ""
	workflowType := ""
	if info != nil {
		event = info.Event
		headers = sanitizeCallbackHeaders(info.Headers)
		appID = info.Payload.AppID
		workflowID = info.Payload.WorkflowID
		workflowName = info.Payload.WorkflowName
		taskID = info.Payload.TaskID
		workflowType = string(info.Payload.WorkflowType)
	}

	values := []interface{}{
		"event", event,
		"method", method,
		"url", sanitizeCallbackURL(requestURL),
		"requestPayload", payload,
		"requestBody", requestBody,
		"requestHeaders", headers,
		"appID", appID,
		"workflowID", workflowID,
		"workflowName", workflowName,
		"taskID", taskID,
		"workflowType", workflowType,
	}
	if statusCode > 0 {
		values = append(values, "statusCode", statusCode)
	}
	if len(response) > 0 {
		values = append(values, "response", response[0])
	}
	return values
}

func sanitizeCallbackHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	cleaned := make(map[string]string, len(headers))
	for key, value := range headers {
		name := strings.TrimSpace(key)
		if name == "" {
			continue
		}
		if isSensitiveCallbackHeader(name) {
			cleaned[name] = callbackLogRedacted
			continue
		}
		cleaned[name] = strings.TrimSpace(value)
	}
	if len(cleaned) == 0 {
		return nil
	}
	return cleaned
}

func sanitizeCallbackURL(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	values := parsed.Query()
	for key := range values {
		if isSensitiveCallbackQueryParam(key) {
			values[key] = []string{callbackLogRedacted}
		}
	}
	parsed.RawQuery = values.Encode()
	return parsed.String()
}

func isSensitiveCallbackHeader(name string) bool {
	compact := compactCallbackLogKey(name)
	if compact == "authorization" || compact == "proxyauthorization" {
		return true
	}
	return strings.Contains(compact, "token") ||
		strings.Contains(compact, "secret") ||
		strings.Contains(compact, "password") ||
		strings.Contains(compact, "passwd") ||
		strings.Contains(compact, "credential") ||
		strings.Contains(compact, "apikey")
}

func isSensitiveCallbackQueryParam(name string) bool {
	compact := compactCallbackLogKey(name)
	return compact == "key" ||
		strings.HasSuffix(compact, "key") ||
		strings.Contains(compact, "token") ||
		strings.Contains(compact, "secret") ||
		strings.Contains(compact, "password") ||
		strings.Contains(compact, "passwd") ||
		strings.Contains(compact, "credential") ||
		strings.Contains(compact, "signature")
}

func compactCallbackLogKey(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	replacer := strings.NewReplacer("-", "", "_", "", " ", "")
	return replacer.Replace(name)
}

func readCallbackBody(reader io.Reader) (string, error) {
	if reader == nil {
		return "", nil
	}
	data, err := io.ReadAll(io.LimitReader(reader, callbackResponseMaxBytes))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

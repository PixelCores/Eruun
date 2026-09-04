package application

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

type applicationCallbackSelection struct {
	callback     *model.JSONStruct
	setCallback  bool
	overwriteAll bool
}

func (c *applicationsServiceImpl) resolveCreateApplicationCallback(ctx context.Context, req apisv1.CreateApplicationsRequest) (applicationCallbackSelection, error) {
	selection := applicationCallbackSelection{
		overwriteAll: strings.TrimSpace(req.ID) != "" && req.Callback != nil,
	}

	callback := req.Callback
	if !selection.overwriteAll && len(req.WorkflowSteps) > 0 && !callbackIsEmpty(req.WorkflowCallback) {
		callback = req.WorkflowCallback
	}
	if callback == nil {
		return selection, nil
	}
	selection.setCallback = true

	callbackMax := workflowconfig.ResolveWorkflowCallbackTimeoutMax(c.Cfg.WorkflowRuntime())
	var urlPolicy *spec.URLSecurityPolicySpec
	var err error
	if workflowCallbackRequiresURLPolicy(callback) {
		urlPolicy, err = loadURLSecurityPolicy(ctx, c.URLSecurityPolicyProvider)
		if err != nil {
			return selection, err
		}
	}
	normalized, err := normalizeWorkflowCallback(ctx, callback, callbackMax, urlPolicy)
	if err != nil {
		return selection, bcode.ErrWorkflowConfig
	}
	if normalized == nil {
		return selection, nil
	}
	selection.callback, err = model.NewJSONStructByStruct(normalized)
	if err != nil {
		return selection, bcode.ErrWorkflowConfig
	}
	return selection, nil
}

func (c *applicationsServiceImpl) resolveVersionUpdateTaskCallback(ctx context.Context, callback *apisv1.WorkflowCallback) (*model.JSONStruct, error) {
	return c.resolveOperationTaskCallback(ctx, callback)
}

func (c *applicationsServiceImpl) resolveOperationTaskCallback(ctx context.Context, callback *apisv1.WorkflowCallback) (*model.JSONStruct, error) {
	if callbackIsEmpty(callback) {
		return nil, nil
	}
	callbackMax := workflowconfig.ResolveWorkflowCallbackTimeoutMax(c.Cfg.WorkflowRuntime())
	var urlPolicy *spec.URLSecurityPolicySpec
	var err error
	if workflowCallbackRequiresURLPolicy(callback) {
		urlPolicy, err = loadURLSecurityPolicy(ctx, c.URLSecurityPolicyProvider)
		if err != nil {
			return nil, err
		}
	}
	normalized, err := normalizeWorkflowCallback(ctx, callback, callbackMax, urlPolicy)
	if err != nil {
		return nil, bcode.ErrWorkflowConfig
	}
	if normalized == nil {
		return nil, nil
	}
	stored, err := model.NewJSONStructByStruct(normalized)
	if err != nil {
		return nil, bcode.ErrWorkflowConfig
	}
	return stored, nil
}

func (c *applicationsServiceImpl) normalizeWorkflowCallbackForWrite(ctx context.Context, callback *apisv1.WorkflowCallback) (*apisv1.WorkflowCallback, error) {
	callbackMax := workflowconfig.ResolveWorkflowCallbackTimeoutMax(c.Cfg.WorkflowRuntime())
	var urlPolicy *spec.URLSecurityPolicySpec
	var err error
	if workflowCallbackRequiresURLPolicy(callback) {
		urlPolicy, err = loadURLSecurityPolicy(ctx, c.URLSecurityPolicyProvider)
		if err != nil {
			return nil, err
		}
	}
	normalized, err := normalizeWorkflowCallback(ctx, callback, callbackMax, urlPolicy)
	if err != nil {
		return nil, bcode.ErrWorkflowConfig
	}
	return normalized, nil
}

func normalizeWorkflowCallback(ctx context.Context, callback *apisv1.WorkflowCallback, timeoutMax time.Duration, urlPolicy *spec.URLSecurityPolicySpec) (*apisv1.WorkflowCallback, error) {
	if callback == nil {
		return nil, nil
	}
	normalized := *callback
	normalized.Success = strings.TrimSpace(normalized.Success)
	normalized.Failure = strings.TrimSpace(normalized.Failure)
	normalized.Timeout = strings.TrimSpace(normalized.Timeout)
	normalized.Reject = strings.TrimSpace(normalized.Reject)
	normalized.Cancelled = strings.TrimSpace(normalized.Cancelled)
	normalized.Headers = normalizeCallbackHeaders(normalized.Headers)

	methods, err := normalizeCallbackMethods(normalized.Methods)
	if err != nil {
		return nil, err
	}
	normalized.Methods = methods

	if normalized.TimeoutSeconds < 0 {
		return nil, fmt.Errorf("callback timeoutSeconds must be >= 0")
	}
	normalized.TimeoutSeconds = workflowconfig.ClampWorkflowCallbackTimeoutSeconds(normalized.TimeoutSeconds, timeoutMax)
	if callbackIsEmpty(&normalized) {
		return nil, nil
	}
	if !hasCallbackURLs(&normalized) {
		return nil, fmt.Errorf("callback url is required")
	}
	if err := validateCallbackURLs(ctx, &normalized, urlPolicy); err != nil {
		return nil, err
	}
	return &normalized, nil
}

func normalizeCallbackHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	cleaned := make(map[string]string, len(headers))
	for key, value := range headers {
		name := strings.TrimSpace(key)
		if name == "" {
			continue
		}
		cleaned[name] = strings.TrimSpace(value)
	}
	if len(cleaned) == 0 {
		return nil
	}
	return cleaned
}

func normalizeCallbackMethods(methods map[string]string) (map[string]string, error) {
	if len(methods) == 0 {
		return nil, nil
	}
	normalized := make(map[string]string, len(methods))
	for key, value := range methods {
		name := strings.ToLower(strings.TrimSpace(key))
		if name == "" {
			continue
		}
		method := strings.ToUpper(strings.TrimSpace(value))
		if method == "" {
			continue
		}
		if !isSupportedCallbackMethod(method) {
			return nil, fmt.Errorf("unsupported callback method %q", method)
		}
		normalized[name] = method
	}
	if len(normalized) == 0 {
		return nil, nil
	}
	return normalized, nil
}

func isSupportedCallbackMethod(method string) bool {
	switch strings.ToUpper(method) {
	case "GET", "POST", "PUT", "DELETE":
		return true
	default:
		return false
	}
}

func hasCallbackURLs(callback *apisv1.WorkflowCallback) bool {
	return callback != nil && (callback.Success != "" || callback.Failure != "" || callback.Timeout != "" || callback.Reject != "" || callback.Cancelled != "")
}

func callbackIsEmpty(callback *apisv1.WorkflowCallback) bool {
	if callback == nil {
		return true
	}
	return callback.Success == "" &&
		callback.Failure == "" &&
		callback.Timeout == "" &&
		callback.Reject == "" &&
		callback.Cancelled == "" &&
		len(callback.Methods) == 0 &&
		len(callback.Headers) == 0 &&
		callback.TimeoutSeconds == 0
}

func validateCallbackURLs(ctx context.Context, callback *apisv1.WorkflowCallback, urlPolicy *spec.URLSecurityPolicySpec) error {
	if callback == nil {
		return nil
	}
	if err := validateCallbackURL(ctx, callback.Success, urlPolicy); err != nil {
		return err
	}
	if err := validateCallbackURL(ctx, callback.Failure, urlPolicy); err != nil {
		return err
	}
	if err := validateCallbackURL(ctx, callback.Timeout, urlPolicy); err != nil {
		return err
	}
	if err := validateCallbackURL(ctx, callback.Reject, urlPolicy); err != nil {
		return err
	}
	if err := validateCallbackURL(ctx, callback.Cancelled, urlPolicy); err != nil {
		return err
	}
	return nil
}

func validateCallbackURL(ctx context.Context, value string, urlPolicy *spec.URLSecurityPolicySpec) error {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parsed, err := url.ParseRequestURI(value)
	if err != nil {
		return fmt.Errorf("invalid callback url %q", value)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		if _, err := utils.ValidateURLTarget(ctx, value, urlPolicy); err != nil {
			return fmt.Errorf("callback url %q is not allowed: %w", value, err)
		}
		return nil
	default:
		return fmt.Errorf("callback url %q must use http or https", value)
	}
}

func workflowCallbackRequiresURLPolicy(callback *apisv1.WorkflowCallback) bool {
	if callback == nil {
		return false
	}
	return strings.TrimSpace(callback.Success) != "" ||
		strings.TrimSpace(callback.Failure) != "" ||
		strings.TrimSpace(callback.Timeout) != "" ||
		strings.TrimSpace(callback.Reject) != "" ||
		strings.TrimSpace(callback.Cancelled) != ""
}

func loadURLSecurityPolicy(ctx context.Context, provider *urlpolicy.Provider) (*spec.URLSecurityPolicySpec, error) {
	policy, err := urlpolicy.ResolvePolicy(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", bcode.ErrURLSecurityPolicyUnavailable, err)
	}
	return policy, nil
}

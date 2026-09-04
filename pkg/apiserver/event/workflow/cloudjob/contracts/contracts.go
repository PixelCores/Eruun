package contracts

import (
	"context"
	"time"
)

// CloudJobInfo is the serialized job payload built from component properties.cloud.
type CloudJobInfo struct {
	Provider     string                 `json:"provider,omitempty"`
	Action       string                 `json:"action,omitempty"`
	Params       map[string]interface{} `json:"params,omitempty"`
	ExecutionKey string                 `json:"executionKey,omitempty"`
}

// CloudJobRequest is the normalized request passed to cloud providers.
type CloudJobRequest struct {
	Provider                 string                 `json:"provider"`
	Action                   string                 `json:"action"`
	Params                   map[string]interface{} `json:"params,omitempty"`
	Name                     string                 `json:"name,omitempty"`
	Namespace                string                 `json:"namespace,omitempty"`
	WorkflowID               string                 `json:"workflowId,omitempty"`
	ProjectID                string                 `json:"projectId,omitempty"`
	AppID                    string                 `json:"appId,omitempty"`
	TaskID                   string                 `json:"taskId,omitempty"`
	ExecutionKey             string                 `json:"executionKey,omitempty"`
	RuntimeProviderSnapshot  interface{}            `json:"-"`
	ResumeFromPersistedState bool                   `json:"-"`
}

// CloudJobResult is the provider execution response persisted in cloud checkpoints.
type CloudJobResult struct {
	RequestID string                 `json:"requestId,omitempty"`
	Message   string                 `json:"message,omitempty"`
	Output    map[string]interface{} `json:"output,omitempty"`
}

// CloudRuntime defines the initialized provider runtime used by cloud actions.
// It may hold zero, one, or multiple provider dependencies, and is not limited to a single vendor client.
type CloudRuntime interface {
	Call(ctx context.Context, action string, params map[string]interface{}) (*CloudJobResult, error)
}

// CloudActionProgress is the incremental action result returned by CloudAction.
type CloudActionProgress struct {
	Done         bool                   `json:"done,omitempty"`
	State        map[string]interface{} `json:"state,omitempty"`
	Result       *CloudJobResult        `json:"result,omitempty"`
	RequeueAfter time.Duration          `json:"requeueAfter,omitempty"`
}

// CloudAction defines a resumable cloud operation that may need multiple rounds.
type CloudAction interface {
	Validate(req *CloudJobRequest) error
	Run(ctx context.Context, runtime CloudRuntime, req *CloudJobRequest, state map[string]interface{}) (*CloudActionProgress, error)
}

// CloudActionFactory builds a CloudAction implementation on demand.
type CloudActionFactory func() CloudAction

func CloneCloudParams(src map[string]interface{}) map[string]interface{} {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

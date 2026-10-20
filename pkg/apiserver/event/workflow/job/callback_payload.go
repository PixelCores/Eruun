package job

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

type CallbackJobInfo struct {
	Event          string            `json:"event"`
	URL            string            `json:"url"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers,omitempty"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitempty"`
	TimeoutMaxNS   int64             `json:"timeoutMaxNs,omitempty"`
	Payload        CallbackPayload   `json:"payload"`
}

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

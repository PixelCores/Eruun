package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"

	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

var (
	ErrDelayQueueUnavailable = errors.New("delay queue unavailable")
)

type DelayJobPayload struct {
	ExecuteAt      int64        `json:"executeAt"`
	Namespace      string       `json:"namespace,omitempty"`
	JobType        string       `json:"jobType,omitempty"`
	TaskID         string       `json:"taskId"`
	ExecutionKey   string       `json:"executionKey"`
	RunGeneration  uint64       `json:"runGeneration"`
	RunToken       string       `json:"runToken,omitempty"`
	ServiceName    string       `json:"serviceName,omitempty"`
	TimeoutSeconds int64        `json:"timeoutSeconds,omitempty"`
	Job            *batchv1.Job `json:"job"`
}

func EnqueueDelayJob(ctx context.Context, queue msg.Queue, payload *DelayJobPayload) (string, error) {
	if err := validateDelayJobPayload(payload); err != nil {
		return "", err
	}
	if queue == nil {
		return "", ErrDelayQueueUnavailable
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal delay payload: %w", err)
	}
	return queue.Enqueue(ctx, raw)
}

func validateDelayJobPayload(payload *DelayJobPayload) error {
	if payload == nil || payload.Job == nil {
		return fmt.Errorf("delay payload and job are required")
	}
	if strings.TrimSpace(payload.Job.Name) == "" {
		return fmt.Errorf("delay payload job name is required")
	}
	if strings.TrimSpace(payload.TaskID) == "" {
		return fmt.Errorf("delay payload task ID is required")
	}
	if strings.TrimSpace(payload.ExecutionKey) == "" {
		return fmt.Errorf("delay payload execution key is required")
	}
	if payload.RunGeneration == 0 {
		return fmt.Errorf("delay payload run generation is required")
	}
	return nil
}

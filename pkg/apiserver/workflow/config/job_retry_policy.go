package config

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

const (
	AnnotationJobRetryPolicy = "eruun.io/job-retry-policy"
	AnnotationJobAttempt     = "eruun.io/job-attempt"
)

// JobRetryPolicy opts an immediate Kubernetes Job into bounded OOM recovery.
// All other workload failures stop execution. WorkflowFailurePolicy separately
// controls cleanup after the final failure.
type JobRetryPolicy struct {
	OnOOM              string              `json:"onOOM"`
	MaxRetries         uint                `json:"maxRetries,omitempty"`
	BackoffSeconds     int64               `json:"backoffSeconds,omitempty"`
	MemoryGrowthFactor int64               `json:"memoryGrowthFactor,omitempty"`
	CPUGrowthFactor    int64               `json:"cpuGrowthFactor,omitempty"`
	MaxResources       corev1.ResourceList `json:"maxResources,omitempty"`
}

func (p *JobRetryPolicy) Validate() error {
	if p == nil {
		return nil
	}
	switch p.OnOOM {
	case "stop":
		if p.MaxRetries != 0 || p.BackoffSeconds != 0 || p.MemoryGrowthFactor != 0 || p.CPUGrowthFactor != 0 || len(p.MaxResources) != 0 {
			return fmt.Errorf("onOOM stop does not accept retry or resource growth settings")
		}
		return nil
	case "retry", "resize":
		if p.MaxRetries < 1 || p.MaxRetries > 10 {
			return fmt.Errorf("maxRetries must be between 1 and 10")
		}
		if p.BackoffSeconds < 1 || p.BackoffSeconds > 3600 {
			return fmt.Errorf("backoffSeconds must be between 1 and 3600")
		}
	default:
		return fmt.Errorf("onOOM must be stop, retry, or resize")
	}
	if p.OnOOM == "retry" {
		if p.MemoryGrowthFactor != 0 || p.CPUGrowthFactor != 0 || len(p.MaxResources) != 0 {
			return fmt.Errorf("onOOM retry does not accept resource growth settings")
		}
		return nil
	}
	if p.MemoryGrowthFactor < 2 || p.MemoryGrowthFactor > 4 {
		return fmt.Errorf("memoryGrowthFactor must be between 2 and 4")
	}
	if p.CPUGrowthFactor < 0 || p.CPUGrowthFactor > 4 {
		return fmt.Errorf("cpuGrowthFactor must be between 0 and 4; 0 or 1 leaves CPU unchanged")
	}
	for name, quantity := range p.MaxResources {
		if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
			return fmt.Errorf("maxResources only supports cpu and memory")
		}
		if quantity.Sign() <= 0 {
			return fmt.Errorf("maxResources.%s must be positive", name)
		}
	}
	if memory := p.MaxResources[corev1.ResourceMemory]; memory.Sign() <= 0 {
		return fmt.Errorf("maxResources.memory is required for resize")
	}
	if cpu := p.MaxResources[corev1.ResourceCPU]; p.CPUGrowthFactor > 1 && cpu.Sign() <= 0 {
		return fmt.Errorf("maxResources.cpu is required when CPU grows")
	}
	return nil
}

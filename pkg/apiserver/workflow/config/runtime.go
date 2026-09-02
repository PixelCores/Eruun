// Package config owns workflow runtime settings and shared execution policies.
// It does not depend on server configuration, domain models, or workflow controllers.
package config

import (
	"fmt"
	"time"
)

// RuntimeConfig controls how workflow steps are executed.
type RuntimeConfig struct {
	// SequentialMaxConcurrency caps how many jobs within a sequential
	// workflow step may run at once. Values <= 0 fall back to 1.
	SequentialMaxConcurrency int
	// DispatchPollInterval determines dispatcher scan cadence.
	DispatchPollInterval time.Duration
	// WorkerStaleInterval determines how frequently workers reclaim messages.
	WorkerStaleInterval time.Duration
	// WorkerAutoClaimMinIdle minimum idle before a message is considered stale.
	WorkerAutoClaimMinIdle time.Duration
	// WorkerAutoClaimCount batch size for AutoClaim operations.
	WorkerAutoClaimCount int
	// WorkerReadCount number of messages fetched per worker read.
	WorkerReadCount int
	// WorkerReadBlock blocking duration for worker reads.
	WorkerReadBlock time.Duration
	// DefaultJobTimeout per-job timeout.
	DefaultJobTimeout time.Duration
	// CallbackTimeoutMax is the upper bound for workflow callback timeout.
	CallbackTimeoutMax time.Duration
	// MaxConcurrentWorkflows limits how many workflow controllers each worker process runs in parallel.
	MaxConcurrentWorkflows int
	// WorkerMaxReadFailures is the max consecutive read failures before worker exits.
	// Set to 0 for infinite retries (recommended for resilience).
	WorkerMaxReadFailures int
	// WorkerMaxClaimFailures is the max consecutive claim failures before worker exits.
	// Set to 0 for infinite retries (recommended for resilience).
	WorkerMaxClaimFailures int
	// WorkerBackoffMin is the minimum backoff duration for worker retries.
	WorkerBackoffMin time.Duration
	// WorkerBackoffMax is the maximum backoff duration for worker retries.
	WorkerBackoffMax time.Duration
	// HeartbeatInterval controls how frequently a running worker renews its database lease.
	HeartbeatInterval time.Duration
	// LeaseDuration is the database ownership lease for queued and running tasks.
	LeaseDuration time.Duration
	// LeaseReaperInterval controls stale task recovery cadence.
	LeaseReaperInterval time.Duration
	// WorkerDrainTimeout bounds graceful shutdown before in-flight tasks are fenced.
	WorkerDrainTimeout time.Duration
}

const (
	DefaultDispatchPollInterval        = 3 * time.Second
	DefaultWorkerStaleInterval         = 15 * time.Second
	DefaultWorkerAutoClaimIdle         = 60 * time.Second
	DefaultWorkerAutoClaimCount        = 50
	DefaultWorkerReadCount             = 10
	DefaultWorkerReadBlock             = 2 * time.Second
	DefaultMaxConcurrentWorkflows      = 100
	DefaultWorkflowHeartbeatInterval   = 10 * time.Second
	DefaultWorkflowLeaseDuration       = 30 * time.Second
	DefaultWorkflowLeaseReaperInterval = 10 * time.Second
	DefaultWorkerDrainTimeout          = 60 * time.Second
	DefaultWorkerBackoffMin            = 200 * time.Millisecond // 最小退避时间
	DefaultWorkerBackoffMax            = 5 * time.Minute        // 最大退避时间
	DefaultWorkerMaxReadFailures       = 10                     // 连续 10 次失败后退出
	DefaultWorkerMaxClaimFailures      = 10                     // 连续 10 次失败后退出
)

// DefaultRuntimeConfig returns the server defaults for workflow execution.
func DefaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		SequentialMaxConcurrency: 1,
		DispatchPollInterval:     DefaultDispatchPollInterval,
		WorkerStaleInterval:      DefaultWorkerStaleInterval,
		WorkerAutoClaimMinIdle:   DefaultWorkerAutoClaimIdle,
		WorkerAutoClaimCount:     DefaultWorkerAutoClaimCount,
		WorkerReadCount:          DefaultWorkerReadCount,
		WorkerReadBlock:          DefaultWorkerReadBlock,
		DefaultJobTimeout:        60 * time.Second,
		CallbackTimeoutMax:       DefaultWorkflowCallbackTimeoutMax,
		MaxConcurrentWorkflows:   DefaultMaxConcurrentWorkflows,
		WorkerMaxReadFailures:    0, // 0 = infinite retries (resilient)
		WorkerMaxClaimFailures:   0, // 0 = infinite retries (resilient)
		WorkerBackoffMin:         DefaultWorkerBackoffMin,
		WorkerBackoffMax:         DefaultWorkerBackoffMax,
		HeartbeatInterval:        DefaultWorkflowHeartbeatInterval,
		LeaseDuration:            DefaultWorkflowLeaseDuration,
		LeaseReaperInterval:      DefaultWorkflowLeaseReaperInterval,
		WorkerDrainTimeout:       DefaultWorkerDrainTimeout,
	}
}

// Validate checks workflow settings before the runtime starts.
func (c RuntimeConfig) Validate() []error {
	var errs []error
	if c.SequentialMaxConcurrency <= 0 {
		errs = append(errs, fmt.Errorf("workflow sequential max concurrency must be >= 1"))
	}
	if c.DispatchPollInterval <= 0 {
		errs = append(errs, fmt.Errorf("workflow dispatch poll interval must be > 0"))
	}
	if c.WorkerStaleInterval <= 0 {
		errs = append(errs, fmt.Errorf("workflow worker stale interval must be > 0"))
	}
	if c.WorkerAutoClaimMinIdle <= 0 {
		errs = append(errs, fmt.Errorf("workflow worker auto-claim min idle must be > 0"))
	}
	if c.WorkerAutoClaimCount <= 0 {
		errs = append(errs, fmt.Errorf("workflow worker auto-claim count must be > 0"))
	}
	if c.WorkerReadCount <= 0 {
		errs = append(errs, fmt.Errorf("workflow worker read count must be > 0"))
	}
	if c.WorkerReadBlock <= 0 {
		errs = append(errs, fmt.Errorf("workflow worker read block must be > 0"))
	}
	if c.DefaultJobTimeout <= 0 {
		errs = append(errs, fmt.Errorf("workflow default job timeout must be > 0"))
	}
	if c.CallbackTimeoutMax <= 0 {
		errs = append(errs, fmt.Errorf("workflow callback timeout max must be > 0"))
	}
	if c.MaxConcurrentWorkflows <= 0 {
		errs = append(errs, fmt.Errorf("workflow max concurrent executions must be > 0"))
	}
	if c.HeartbeatInterval <= 0 {
		errs = append(errs, fmt.Errorf("workflow heartbeat interval must be > 0"))
	}
	if c.LeaseDuration <= c.HeartbeatInterval {
		errs = append(errs, fmt.Errorf("workflow lease duration must be greater than heartbeat interval"))
	}
	if c.LeaseReaperInterval <= 0 {
		errs = append(errs, fmt.Errorf("workflow lease reaper interval must be > 0"))
	}
	if c.WorkerDrainTimeout <= 0 {
		errs = append(errs, fmt.Errorf("workflow worker drain timeout must be > 0"))
	}
	return errs
}

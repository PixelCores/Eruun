package workflow

import (
	"context"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func (w *Workflow) waitingTasks(ctx context.Context) ([]*model.WorkflowQueue, error) {
	queryCtx, cancel := context.WithTimeout(ctx, config.WaitingTasksQueryTimeout)
	defer cancel()
	return w.WorkflowService.WaitingTasks(queryCtx)
}

func (w *Workflow) enqueueDispatch(ctx context.Context, payload []byte) (string, error) {
	enqueueCtx, cancel := context.WithTimeout(ctx, config.QueueDispatchTimeout)
	defer cancel()
	return w.Queue.Enqueue(enqueueCtx, payload)
}

func (w *Workflow) dispatchPollInterval() time.Duration {
	if w.Cfg != nil && w.Cfg.Workflow.DispatchPollInterval > 0 {
		return w.Cfg.Workflow.DispatchPollInterval
	}
	return config.DefaultDispatchPollInterval
}

func (w *Workflow) workerStaleInterval() time.Duration {
	if w.Cfg != nil && w.Cfg.Workflow.WorkerStaleInterval > 0 {
		return w.Cfg.Workflow.WorkerStaleInterval
	}
	return config.DefaultWorkerStaleInterval
}

func (w *Workflow) workerAutoClaimMinIdle() time.Duration {
	if w.Cfg != nil && w.Cfg.Workflow.WorkerAutoClaimMinIdle > 0 {
		return w.Cfg.Workflow.WorkerAutoClaimMinIdle
	}
	return config.DefaultWorkerAutoClaimIdle
}

func (w *Workflow) workerAutoClaimCount() int {
	if w.Cfg != nil && w.Cfg.Workflow.WorkerAutoClaimCount > 0 {
		return w.Cfg.Workflow.WorkerAutoClaimCount
	}
	return config.DefaultWorkerAutoClaimCount
}

func (w *Workflow) workerReadCount() int {
	if w.Cfg != nil && w.Cfg.Workflow.WorkerReadCount > 0 {
		return w.Cfg.Workflow.WorkerReadCount
	}
	return config.DefaultWorkerReadCount
}

func (w *Workflow) workerReadBlock() time.Duration {
	if w.Cfg != nil && w.Cfg.Workflow.WorkerReadBlock > 0 {
		return w.Cfg.Workflow.WorkerReadBlock
	}
	return config.DefaultWorkerReadBlock
}

func (w *Workflow) workerMaxReadFailures() int {
	if w.Cfg != nil {
		return w.Cfg.Workflow.WorkerMaxReadFailures
	}
	return config.DefaultWorkerMaxReadFailures
}

func (w *Workflow) workerMaxClaimFailures() int {
	if w.Cfg != nil {
		return w.Cfg.Workflow.WorkerMaxClaimFailures
	}
	return config.DefaultWorkerMaxClaimFailures
}

func (w *Workflow) workerBackoffMin() time.Duration {
	if w.Cfg != nil && w.Cfg.Workflow.WorkerBackoffMin > 0 {
		return w.Cfg.Workflow.WorkerBackoffMin
	}
	return config.DefaultWorkerBackoffMin
}

func (w *Workflow) workerBackoffMax() time.Duration {
	if w.Cfg != nil && w.Cfg.Workflow.WorkerBackoffMax > 0 {
		return w.Cfg.Workflow.WorkerBackoffMax
	}
	return config.DefaultWorkerBackoffMax
}

func (w *Workflow) workflowHeartbeatInterval() time.Duration {
	if w.Cfg != nil && w.Cfg.Workflow.HeartbeatInterval > 0 {
		return w.Cfg.Workflow.HeartbeatInterval
	}
	return config.DefaultWorkflowHeartbeatInterval
}

func (w *Workflow) workflowLeaseDuration() time.Duration {
	if w.Cfg != nil && w.Cfg.Workflow.LeaseDuration > 0 {
		return w.Cfg.Workflow.LeaseDuration
	}
	return config.DefaultWorkflowLeaseDuration
}

func (w *Workflow) workflowLeaseReaperInterval() time.Duration {
	if w.Cfg != nil && w.Cfg.Workflow.LeaseReaperInterval > 0 {
		return w.Cfg.Workflow.LeaseReaperInterval
	}
	return config.DefaultWorkflowLeaseReaperInterval
}

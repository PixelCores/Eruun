package event

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow"
)

// ControllerWorker runs leader-only reconciliation and result coordination.
type ControllerWorker interface {
	StartController(ctx context.Context, errChan chan error)
}

// SchedulerWorker runs leader-only queue admission and recovery loops.
type SchedulerWorker interface {
	StartScheduler(ctx context.Context, errChan chan error, ready func())
}

// WorkerSubscriber runs the message-bus consumer for the worker role.
// consumerCtx stops message intake; executionCtx owns already-started work.
type WorkerSubscriber interface {
	StartWorker(consumerCtx, executionCtx context.Context, errChan chan error, ready, stopped func())
}

type Worker interface {
	WorkerSubscriber
}

// InitEvent init all event worker
func InitEvent() []Worker {
	workflowCol := &workflow.Workflow{}
	return []Worker{workflowCol}
}

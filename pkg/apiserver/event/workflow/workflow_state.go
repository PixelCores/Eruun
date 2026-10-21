package workflow

import (
	"context"

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

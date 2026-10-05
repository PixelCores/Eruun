package job

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

// runAdmittedTestJob exercises component execution and persistence after admission.
// Scheduler/admission tests retain runJob and their real durable scheduling state.
func runAdmittedTestJob(ctx context.Context, task *model.JobTask, configRuntime *Runtime) error {
	runtime := newJobRuntime(configRuntime)
	defer runtime.close()
	runtime.Store = withJobTestOwner(runtime.Store, task)
	if runtime.Ack == nil {
		runtime.Ack = func() {}
	}
	task.Status = config.StatusPrepare
	task.Error = ""
	task.StartTime = time.Now().Unix()
	runtime.Ack()
	ctx = WithCleanupTracker(ctx)
	return runAdmittedJob(ctx, initJobCtl(task, runtime), task, runtime.Client, runtime.Store, runtime.Ack, runtime, trace.SpanFromContext(ctx))
}

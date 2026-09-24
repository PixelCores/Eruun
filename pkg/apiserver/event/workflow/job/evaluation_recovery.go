package job

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"k8s.io/client-go/kubernetes"
)

const EvaluationDeadlineAnnotation = "eruun.io/evaluation-deadline"

type evaluationRecoveryKey struct{}

// EvaluationRecovery is supplied by the evaluation service. It runs outside
// runJob's admission lifetime, so a successor never inherits the old permit.
type EvaluationRecovery func(context.Context, *model.JobTask) (bool, error)

func WithEvaluationRecovery(ctx context.Context, recover EvaluationRecovery) context.Context {
	return context.WithValue(ctx, evaluationRecoveryKey{}, recover)
}
func runJobWithEvaluationRecovery(ctx context.Context, task *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), runtime *jobRuntime) error {
	recover, _ := ctx.Value(evaluationRecoveryKey{}).(EvaluationRecovery)
	if recover == nil || task.JobType != string(config.JobEval) {
		return runJob(ctx, task, client, store, ack, runtime)
	}
	// The preflight also resumes a recovery reservation after worker takeover.
	if _, err := recover(ctx, task); err != nil {
		return err
	}
	for {
		if err := runJob(ctx, task, client, store, ack, runtime); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return infrastructureStopCause(ctx)
		}
		again, err := recover(ctx, task)
		if err != nil {
			return err
		}
		if !again {
			return nil
		}
	}
}

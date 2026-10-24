package jobexec

import (
	"context"

	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type GenerateServiceResult struct {
	Service           client.Object
	AdditionalObjects []client.Object
}

type taskIDKey struct{}

// Runtime contains the dependencies and execution policy for a RunJobs call.
// RunJobs creates fresh lock and persistence state for each invocation.
type Runtime struct {
	Concurrency       int
	Client            kubernetes.Interface
	Store             datastore.DataStore
	Ack               func()
	StopOnFailure     bool
	RedisClient       *redis.Client
	Cache             cache.ICache
	URLSecurityPolicy *spec.URLSecurityPolicySpec
	DelayQueue        msg.Queue
	ResourceWaiter    informer.ComponentReadyObserver
	KubeConfig        *rest.Config
	// Dynamic writes component routes with the same tenant identity as Client.
	Dynamic         dynamic.Interface
	ShareLocker     locker.Locker
	ArchiveUploader ArchiveUploader
}

func (r *Runtime) Close() {
	if r == nil || r.ShareLocker == nil {
		return
	}
	if err := r.ShareLocker.Close(); err != nil {
		klog.ErrorS(err, "close share locker failed")
	}
}

// WithTaskMetadata injects workflow identifiers into context so job controllers
// can derive cancellation signals when needed.
func WithTaskMetadata(ctx context.Context, taskID string) context.Context {
	return context.WithValue(ctx, taskIDKey{}, taskID)
}

// TaskIDFromContext extracts the workflow task identifier from context.
func TaskIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(taskIDKey{}).(string); ok {
		return v
	}
	return ""
}

func InfrastructureStopCause(ctx context.Context) error {
	if !signal.IsInfrastructureStop(ctx) {
		return nil
	}
	return context.Cause(ctx)
}

func isSharedDefaultSkippedConfigJob(job *model.JobTask) bool {
	if job == nil {
		return false
	}
	if job.Status != config.StatusSkipped {
		return false
	}
	if !isConfigComponentJobType(job.JobType) {
		return false
	}
	strategy, shared := jobinfo.ShareStrategyFromJobInfo(job.JobInfo)
	if !shared {
		return false
	}
	return strategy == spec.ShareStrategyDefault
}

const EvaluationDeadlineAnnotation = "eruun.io/evaluation-deadline"

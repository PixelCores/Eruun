package job

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// resourceListFn counts resources by a label selector for share conflict detection.
type resourceListFn func(context.Context, metav1.ListOptions) (int, error)

// handleSharedResource resolves share strategy and optionally marks the job skipped.
func handleSharedResource(
	ctx context.Context,
	job *model.JobTask,
	ack func(),
	labels map[string]string,
	kind domainspec.ResourceKind,
	listFn resourceListFn,
	lockProvider locker.Locker,
	logSkip func(strategy domainspec.ShareStrategy),
) (func(), bool, error) {
	shareName, shareStrategy := shareInfoFromLabels(labels)
	unlock, skipped, err := resolveSharedResource(ctx, shareName, shareStrategy, kind, listFn, lockProvider)
	if err != nil {
		return nil, false, err
	}
	if skipped {
		if logSkip != nil {
			logSkip(shareStrategy)
		}
		if job != nil {
			job.Status = config.StatusSkipped
			job.Error = ""
		}
		if ack != nil {
			ack()
		}
	}
	return unlock, skipped, nil
}

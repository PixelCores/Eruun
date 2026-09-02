package job

import (
	"context"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

type runJobFunc func(context.Context) error
type waitJobFunc func(context.Context) error

func startJobTransition(job *model.JobTask, ack func()) {
	job.Status = config.StatusRunning
	job.Error = ""
	if ack != nil {
		ack()
	}
}

func finishJobTransitionWithError(ctx context.Context, job *model.JobTask, err error, logMsg string) error {
	if err == nil {
		return nil
	}
	if logMsg != "" {
		klog.FromContext(ctx).Error(err, logMsg)
	}
	applyJobError(job, err, "")
	return err
}

func applyJobError(job *model.JobTask, err error, message string) {
	if job == nil || err == nil {
		return
	}
	job.Error = jobErrorMessage(err, message)
	job.Status = statusFromError(err)
}

func jobErrorMessage(err error, message string) string {
	message = strings.TrimSpace(message)
	if message != "" {
		return message
	}
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

func statusFromError(err error) config.Status {
	if statusErr, ok := ExtractStatusError(err); ok {
		return statusErr.Status
	}
	return config.StatusFailed
}

func completeJobTransition(job *model.JobTask) error {
	if job.Status == config.StatusSkipped {
		job.Error = ""
		return nil
	}
	job.Status = config.StatusCompleted
	job.Error = ""
	return nil
}

// runJobWithStatus provides a consistent status transition flow around job execution.
func runJobWithStatus(ctx context.Context, job *model.JobTask, ack func(), runFn runJobFunc, logMsg string) error {
	startJobTransition(job, ack)
	if err := runFn(ctx); err != nil {
		return finishJobTransitionWithError(ctx, job, err, logMsg)
	}
	return completeJobTransition(job)
}

// runJobWithWait provides a consistent status transition flow around job execution and waiting.
func runJobWithWait(ctx context.Context, job *model.JobTask, ack func(), runFn runJobFunc, waitFn waitJobFunc, runLogMsg, waitLogMsg string) error {
	startJobTransition(job, ack)
	if err := runFn(ctx); err != nil {
		return finishJobTransitionWithError(ctx, job, err, runLogMsg)
	}
	if job.Status == config.StatusSkipped {
		job.Error = ""
		return nil
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return finishJobTransitionWithError(ctx, job, err, waitLogMsg)
		}
	}
	return completeJobTransition(job)
}

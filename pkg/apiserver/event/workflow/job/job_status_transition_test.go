package job

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestRunJobWithWaitCompletes(t *testing.T) {
	job := &model.JobTask{}
	ackCount := 0

	err := runJobWithWait(context.Background(), job, func() { ackCount++ }, func(context.Context) error {
		return nil
	}, func(context.Context) error {
		return nil
	}, "run failed", "wait failed")

	require.NoError(t, err)
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusCompleted, job.Status)
	require.Empty(t, job.Error)
}

func TestRunJobWithWaitPreservesSkippedStatus(t *testing.T) {
	job := &model.JobTask{}

	err := runJobWithWait(context.Background(), job, nil, func(context.Context) error {
		job.Status = config.StatusSkipped
		return nil
	}, func(context.Context) error {
		t.Fatal("wait should not be called for skipped job")
		return nil
	}, "run failed", "wait failed")

	require.NoError(t, err)
	require.Equal(t, config.StatusSkipped, job.Status)
	require.Empty(t, job.Error)
}

func TestRunJobWithWaitUsesStatusErrorFromWait(t *testing.T) {
	job := &model.JobTask{}
	expected := NewStatusError(config.StatusTimeout, errors.New("wait timeout"))

	err := runJobWithWait(context.Background(), job, nil, func(context.Context) error {
		return nil
	}, func(context.Context) error {
		return expected
	}, "run failed", "wait failed")

	require.Error(t, err)
	require.Equal(t, config.StatusTimeout, job.Status)
	require.Equal(t, "wait timeout", job.Error)
}

func TestApplyJobErrorUsesExplicitMessageAndStatusError(t *testing.T) {
	job := &model.JobTask{}
	err := NewStatusError(config.StatusCancelled, errors.New("context canceled"))

	applyJobError(job, err, "cancelled by user")

	require.Equal(t, config.StatusCancelled, job.Status)
	require.Equal(t, "cancelled by user", job.Error)
}

func TestApplyJobErrorDefaultsPlainErrorsToFailed(t *testing.T) {
	job := &model.JobTask{}

	applyJobError(job, errors.New("boom"), "")

	require.Equal(t, config.StatusFailed, job.Status)
	require.Equal(t, "boom", job.Error)
}

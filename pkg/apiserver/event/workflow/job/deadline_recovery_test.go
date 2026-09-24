package job

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRecoveredEvaluationRetryPreservesAbsoluteDeadline(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline time.Duration
		valid    bool
	}{
		{"remaining budget", 5 * time.Second, true},
		{"expired", -time.Second, false},
		{"cannot extend budget", time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
			task.JobType = string(config.JobEval)
			workload := task.JobInfo.(*batchv1.Job)
			deadline := time.Now().Add(tc.deadline).UnixNano()
			workload.Annotations[EvaluationDeadlineAnnotation] = strconv.FormatInt(deadline, 10)
			ctl := NewInstantJobCtl(task, fake.NewSimpleClientset(), &retryCheckpointStore{}, func() {})
			checkpoint, err := ctl.newRetryCheckpoint(context.Background(), workload)
			if !tc.valid {
				require.ErrorContains(t, err, "invalid evaluation recovery deadline")
				return
			}
			require.NoError(t, err)
			require.Equal(t, deadline, checkpoint.Deadline)
			restored, err := decodeInstantJobRetryCheckpoint(task)
			require.NoError(t, err)
			require.Equal(t, deadline, restored.Deadline)
		})
	}
}

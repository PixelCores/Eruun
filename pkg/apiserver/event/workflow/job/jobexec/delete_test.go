package jobexec

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestRecreateWaitsForForegroundGarbageCollection(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "collected", true: "pods still terminating"}[pending], func(t *testing.T) {
			live := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default", UID: "old-job", ResourceVersion: "9"}}
			desired := live.DeepCopy()
			desired.Annotations = map[string]string{config.AnnotationJobRunPolicy: string(workflowconfig.JobRunPolicyRecreate)}
			client := fake.NewSimpleClientset(live)
			client.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
				options := action.(k8stesting.DeleteAction).GetDeleteOptions()
				require.Equal(t, metav1.DeletePropagationForeground, *options.PropagationPolicy)
				require.Equal(t, live.UID, *options.Preconditions.UID)
				require.Nil(t, options.Preconditions.ResourceVersion)
				// Model the Job finalizer remaining until the API server's GC has
				// removed dependent Pods; fake clients do not run garbage collection.
				return pending, nil, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			_, err := ApplyJobRunPolicy(ctx, client, nil, desired, config.JobDeployInstant)
			if pending {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				_, getErr := client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
				require.NoError(t, getErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestForegroundJobDeleteTimeoutIncludesPodGracePeriod(t *testing.T) {
	grace := func(seconds int64) *batchv1.Job {
		job := &batchv1.Job{}
		job.Spec.Template.Spec.TerminationGracePeriodSeconds = &seconds
		return job
	}
	require.Equal(t, JobDeleteTimeout+time.Duration(corev1.DefaultTerminationGracePeriodSeconds)*time.Second, foregroundJobDeleteTimeout(&batchv1.Job{}))
	require.Equal(t, JobDeleteTimeout+5*time.Minute, foregroundJobDeleteTimeout(grace(300)))
	require.Equal(t, JobDeleteTimeout, foregroundJobDeleteTimeout(grace(0)))
}

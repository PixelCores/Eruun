package job

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestFailedBatchJobCleanupCascadesOnlyCreatedJobs(t *testing.T) {
	for _, scheduled := range []bool{false, true} {
		for _, created := range []bool{false, true} {
			for _, conflict := range []bool{false, true} {
				t.Run(fmt.Sprintf("scheduled=%t/created=%t/conflict=%t", scheduled, created, conflict), func(t *testing.T) {
					live := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default", UID: "job-uid", ResourceVersion: "7"}}
					client := fake.NewSimpleClientset(live)
					deletes := 0
					client.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
						deletes++
						options := action.(k8stesting.DeleteAction).GetDeleteOptions()
						require.Equal(t, metav1.DeletePropagationForeground, *options.PropagationPolicy)
						require.Equal(t, live.UID, *options.Preconditions.UID)
						// Active Jobs update status between read and delete; UID alone pins identity.
						require.Nil(t, options.Preconditions.ResourceVersion)
						if conflict {
							return true, nil, k8serrors.NewConflict(schema.GroupResource{Group: "batch", Resource: "jobs"}, live.Name, errors.New("identity changed"))
						}
						return false, nil, nil
					})
					ctx := WithCleanupTracker(context.Background())
					if created {
						MarkResourceCreated(ctx, spec.ResourceJob, live.Namespace, live.Name)
					} else {
						markResourceObserved(ctx, spec.ResourceJob, live.Namespace, live.Name)
					}
					task := &model.JobTask{Name: live.Name, Namespace: live.Namespace, JobType: string(config.JobDeployInstant)}
					if scheduled {
						NewScheduledJobCtl(task, &Runtime{Client: client}).Clean(ctx)
					} else {
						NewInstantJobCtl(task, &Runtime{Client: client}).Clean(ctx)
					}
					_, err := client.BatchV1().Jobs(live.Namespace).Get(ctx, live.Name, metav1.GetOptions{})
					if created && !conflict {
						require.True(t, k8serrors.IsNotFound(err))
					} else {
						require.NoError(t, err)
					}
					if created {
						require.Equal(t, 1, deletes)
					} else {
						require.Zero(t, deletes)
					}
				})
			}
		}
	}
}

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
			_, err := applyJobRunPolicy(ctx, client, nil, desired, config.JobDeployInstant)
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
	require.Equal(t, jobDeleteTimeout+time.Duration(corev1.DefaultTerminationGracePeriodSeconds)*time.Second, foregroundJobDeleteTimeout(&batchv1.Job{}))
	require.Equal(t, jobDeleteTimeout+5*time.Minute, foregroundJobDeleteTimeout(grace(300)))
	require.Equal(t, jobDeleteTimeout, foregroundJobDeleteTimeout(grace(0)))
}

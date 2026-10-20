package job

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
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
					ctx := jobexec.WithCleanupTracker(context.Background())
					if created {
						jobexec.MarkResourceCreated(ctx, spec.ResourceJob, live.Namespace, live.Name)
					} else {
						jobexec.MarkResourceObserved(ctx, spec.ResourceJob, live.Namespace, live.Name)
					}
					task := &model.JobTask{Name: live.Name, Namespace: live.Namespace, JobType: string(config.JobDeployInstant)}
					if scheduled {
						NewScheduledJobCtl(task, &jobexec.Runtime{Client: client}).Clean(ctx)
					} else {
						NewInstantJobCtl(task, &jobexec.Runtime{Client: client}).Clean(ctx)
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

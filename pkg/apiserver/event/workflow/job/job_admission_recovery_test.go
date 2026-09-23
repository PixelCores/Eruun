package job

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

func TestJobAdmissionCancellationStopsRecoveredInstantExecution(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		for _, reason := range []string{"signal first", "database first", "database before signal", "new generation", "new token", "new worker"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, reason), func(t *testing.T) {
				db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
				require.NoError(t, err)
				connection, err := db.DB()
				require.NoError(t, err)
				connection.SetMaxOpenConns(1)
				t.Cleanup(func() { require.NoError(t, connection.Close()) })
				require.NoError(t, db.AutoMigrate(&model.SystemSetting{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.ApplicationComponent{}, &model.Applications{}))
				store := &sqlstore.Driver{Client: *db}
				require.NoError(t, repository.EnsureJobSchedulerPolicy(context.Background(), store))

				task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
				task.JobType, task.WorkspaceID = string(config.JobCommand), "workspace"
				task.OwnerRunGeneration, task.RunToken, task.WorkerID = 2, "recovery-token", "recovery-worker"
				task.Status = config.StatusRunning
				desired := task.JobInfo.(*batchv1.Job)
				desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
				desired.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
				live := desired.DeepCopy()
				live.UID = "original-live-job"
				checkpoint := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired, Attempt: 1,
					CurrentUID: live.UID, Deadline: time.Now().Add(time.Hour).UnixNano()}
				encoded, err := json.Marshal(checkpoint)
				require.NoError(t, err)
				task.InternalInfo = string(encoded)
				lease := time.Now().Add(time.Minute)
				owner := &model.WorkflowQueue{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Status: config.StatusRunning,
					RunGeneration: 2, RunToken: task.RunToken, WorkerID: task.WorkerID, LeaseExpiresAt: &lease}
				require.NoError(t, store.Add(context.Background(), owner))
				record := buildJobInfoRecord(task)
				require.NoError(t, store.Add(context.Background(), &record))
				pod := retryTestPod(live, "")
				pod.Spec = live.Spec.Template.Spec
				pod.Status = corev1.PodStatus{Phase: corev1.PodRunning}
				client := fake.NewSimpleClientset(live, pod)

				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				result := make(chan error, 1)
				go func() {
					result <- RunJobs(ctx, []*model.JobTask{task}, concurrency, client, nil, store, func() {}, true, nil, nil, nil, nil, nil)
				}()
				require.Eventually(t, func() bool {
					var count int64
					return db.Model(&model.JobInfo{}).Where("execution_key = ? AND scheduling_state = ?", task.ExecutionKey, "queued").Count(&count).Error == nil && count == 1
				}, time.Second, 10*time.Millisecond)
				require.Equal(t, 1, countClientActions(client, "get", "jobs"), "verify the existing UID before ordinary admission")
				for _, verb := range []string{"create", "update", "patch", "delete"} {
					require.Zero(t, countClientActions(client, verb, "jobs"), "recovery must not mutate the live workload while waiting")
				}

				staleOwner := reason == "new generation" || reason == "new token" || reason == "new worker"
				ownerStillRunning := reason == "signal first"
				switch reason {
				case "database first", "database before signal":
					require.NoError(t, db.Model(owner).Update("status", config.StatusCancelled).Error)
				case "new generation":
					require.NoError(t, db.Model(owner).Update("run_generation", 3).Error)
				case "new token":
					require.NoError(t, db.Model(owner).Update("run_token", "new-token").Error)
				case "new worker":
					require.NoError(t, db.Model(owner).Update("worker_id", "new-worker").Error)
				}
				if reason != "database first" {
					cancel(context.Canceled)
				}
				select {
				case err := <-result:
					if staleOwner || ownerStillRunning {
						require.ErrorIs(t, err, signal.ErrInfrastructureStop)
					} else {
						require.NoError(t, err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("recovered execution did not stop waiting")
				}

				var saved model.JobInfo
				require.NoError(t, db.Where("execution_key = ?", task.ExecutionKey).First(&saved).Error)
				if staleOwner || ownerStillRunning {
					require.Equal(t, string(config.StatusRunning), saved.Status)
					for _, verb := range []string{"create", "update", "patch", "delete"} {
						require.Zero(t, countClientActions(client, verb, "jobs"), "a stale owner cannot mutate the current execution")
					}
				} else {
					require.Equal(t, string(config.StatusCancelled), saved.Status)
					require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
					for _, action := range client.Actions() {
						if action.Matches("delete", "jobs") {
							deletion := action.(k8stesting.DeleteAction)
							require.Equal(t, live.Name, deletion.GetName())
							require.NotNil(t, deletion.GetDeleteOptions().Preconditions)
							require.Equal(t, &live.UID, deletion.GetDeleteOptions().Preconditions.UID)
						}
					}
				}
				require.Zero(t, countClientActions(client, "create", "jobs"), "cancellation must not restart the checkpointed attempt")
				require.Zero(t, countClientActions(client, "create", "pods"))
				currentPod, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, corev1.RestartPolicyNever, currentPod.Spec.RestartPolicy)
			})
		}
	}
}

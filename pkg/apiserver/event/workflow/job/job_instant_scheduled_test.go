package job

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
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
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobdelay"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinstant"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

func TestNewInstantAndScheduledCtlNilJob(t *testing.T) {
	require.Nil(t, jobinstant.NewInstantJobCtl(nil, &jobexec.Runtime{Client: nil, Store: nil, Ack: nil}))
	require.Nil(t, jobdelay.NewScheduledJobCtl(nil, &jobexec.Runtime{Client: nil, Store: nil, Ack: nil}))
}

func TestDelayedJobControllersCommitWithoutQueue(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobDeployInstant, config.JobDeployScheduled} {
		t.Run(string(jobType), func(t *testing.T) {
			store := &jobInfoSaveStore{}
			jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name:      "durable-delay",
				Namespace: "default",
				Annotations: map[string]string{
					config.AnnotationJobStartTime: "4102444800",
				},
			}}
			task := &model.JobTask{
				Name:          "durable-delay",
				TaskID:        "task-durable-delay",
				Namespace:     "default",
				JobType:       string(jobType),
				ExecutionKey:  "execution-durable-delay",
				RunGeneration: 2,
				JobInfo:       jobObj,
			}
			task.AppID = "app"
			jobObj.Spec.Template.Spec = jobtest.WorkspacePodSpec()
			jobObj.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
			_, err := jobexec.PrepareTask(task, "app", &model.Workspace{ID: "workspace", Namespace: "default"}, spec.WorkspaceConfig{})
			require.NoError(t, err)
			var ctl JobCtl
			if jobType == config.JobDeployInstant {
				ctl = jobinstant.NewInstantJobCtl(task, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: jobtest.WithJobTestOwner(store, task), Ack: func() {}})
			} else {
				ctl = jobdelay.NewScheduledJobCtl(task, &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: jobtest.WithJobTestOwner(store, task), Ack: func() {}})
			}

			require.NoError(t, ctl.Run(context.Background()))
			require.Equal(t, config.StatusDistributed, task.Status)
			require.NotNil(t, store.added)
			require.Equal(t, config.JobDelayStatePending, store.added.DelayState)
			require.NotEmpty(t, store.added.DelayPayload)
			var persisted jobinfo.DelayJobPayload
			require.NoError(t, json.Unmarshal([]byte(store.added.DelayPayload), &persisted))
			jobtest.RequireWorkspacePodSecurity(t, persisted.Job.Spec.Template.Spec)
		})
	}
}

func TestStampJobExecutionIdentityAlwaysIncludesTaskID(t *testing.T) {
	jobObj := &batchv1.Job{}

	jobresult.StampJobExecutionIdentity(&model.JobTask{TaskID: "task-1"}, jobObj)

	require.Equal(t, "task-1", jobObj.Annotations[config.AnnotationJobTaskID])
}

func TestRunJobsPreservesNonTerminalStateAfterRecreateOwnershipReadFailure(t *testing.T) {
	currentOwner := model.WorkflowQueue{
		TaskID: "task-1", WorkspaceID: "test-workspace", Status: config.StatusRunning, RunGeneration: 1, RunToken: "token-1", WorkerID: "worker-old",
	}
	for _, concurrencyCase := range []struct {
		name        string
		concurrency int
	}{
		{name: "serial", concurrency: 1},
		{name: "parallel", concurrency: 2},
	} {
		for _, jobType := range []config.JobType{config.JobDeployInstant, config.JobDeployScheduled} {
			t.Run(concurrencyCase.name+"/"+string(jobType), func(t *testing.T) {
				temporaryErr := errors.New("temporary database outage")
				desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name: "demo", Namespace: "default",
					Annotations: map[string]string{config.AnnotationJobRunPolicy: string(workflowconfig.JobRunPolicyRecreate)},
				}}
				jobTask := &model.JobTask{
					Name: "demo", Namespace: "default", TaskID: "task-1", WorkspaceID: currentOwner.WorkspaceID, JobType: string(jobType), JobInfo: desired,
					ExecutionKey: "execution-1", RunGeneration: 1, RunToken: "token-1",
					OwnerRunGeneration: 1, WorkerID: "worker-old",
				}
				db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "ownership.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
				require.NoError(t, err)
				connection, err := db.DB()
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, connection.Close()) })
				require.NoError(t, db.AutoMigrate(&model.JobInfo{}, &model.WorkflowQueue{}, &model.Applications{}, &model.ApplicationComponent{}))
				store := &sqlstore.Driver{Client: *db}
				now := time.Now().UTC()
				lease := now.Add(time.Minute)
				owner := currentOwner
				owner.LeaseExpiresAt = &lease
				require.NoError(t, store.Add(context.Background(), &owner))
				record := jobinfo.BuildJobInfoRecord(jobTask)
				record.Status = string(config.StatusPrepare)
				record.SchedulingState = workflowconfig.JobSchedulingAdmitted
				record.SchedulingOwnerStatus = config.StatusRunning
				record.SchedulingGeneration = owner.RunGeneration
				record.SchedulingQueuedAt = &now
				require.NoError(t, store.Add(context.Background(), &record))
				var deletionStarted atomic.Bool
				require.NoError(t, db.Callback().Query().Before("gorm:query").Register("fail-ownership-read-after-recreate", func(tx *gorm.DB) {
					if _, ok := tx.Statement.Dest.(*model.WorkflowQueue); ok && deletionStarted.Load() {
						tx.AddError(temporaryErr)
					}
				}))
				oldJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name: "demo", Namespace: "default", UID: "old-job-uid",
					Annotations: map[string]string{
						config.AnnotationJobExecutionKey:  "execution-1",
						config.AnnotationJobRunGeneration: "1",
					},
				}}
				replacement := oldJob.DeepCopy()
				replacement.UID = "new-job-uid"
				client := fake.NewSimpleClientset()
				getJobs := 0
				client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
					getJobs++
					if getJobs == 1 {
						return true, oldJob.DeepCopy(), nil
					}
					return true, replacement.DeepCopy(), nil
				})
				client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
					deletionStarted.Store(true)
					return true, nil, nil
				})
				ackCount := 0

				runErr := RunJobs(context.Background(), []*model.JobTask{jobTask}, &jobexec.Runtime{
					Concurrency: concurrencyCase.concurrency,
					Client:      client,
					Store:       store,
					Ack: func() {
						ackCount++
					},
					StopOnFailure: true,
				})

				require.ErrorIs(t, runErr, signal.ErrInfrastructureStop)
				require.ErrorContains(t, runErr, temporaryErr.Error())
				require.True(t, deletionStarted.Load(), "ownership failure must be injected after Job deletion")
				require.Equal(t, config.StatusPrepare, jobTask.Status)
				require.Empty(t, jobTask.Error)
				require.Zero(t, jobTask.EndTime)
				require.Equal(t, 2, ackCount)
				require.Equal(t, 2, getJobs)
				for _, action := range client.Actions() {
					require.False(t, action.Matches("create", "jobs"), "infrastructure recovery must not create or adopt the replacement Job")
				}
			})
		}
	}
}

func TestImmediateJobCreateRejectsAlreadyExistsReplacement(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobDeployInstant, config.JobDeployScheduled} {
		t.Run(string(jobType), func(t *testing.T) {
			desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "demo", Namespace: "default",
				Annotations: map[string]string{
					config.AnnotationJobExecutionKey:  "execution-1",
					config.AnnotationJobRunGeneration: "1",
				},
			}}
			replacement := desired.DeepCopy()
			replacement.UID = "new-job-uid"
			replacement.Annotations[config.AnnotationJobExecutionKey] = "execution-2"
			replacement.Annotations[config.AnnotationJobRunGeneration] = "2"
			jobTask := &model.JobTask{
				Name: desired.Name, Namespace: desired.Namespace, JobType: string(jobType), JobInfo: desired,
			}
			client := fake.NewSimpleClientset()
			getJobs := 0
			client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
				getJobs++
				if getJobs == 1 {
					return true, nil, k8serrors.NewNotFound(
						schema.GroupResource{Group: batchv1.GroupName, Resource: "jobs"},
						desired.Name,
					)
				}
				return true, replacement.DeepCopy(), nil
			})
			client.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, k8serrors.NewAlreadyExists(
					schema.GroupResource{Group: batchv1.GroupName, Resource: "jobs"},
					desired.Name,
				)
			})

			var created bool
			var err error
			if jobType == config.JobDeployInstant {
				created, err = jobinstant.NewInstantJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: &jobtest.NoopStore{}, Ack: func() {}}).CreateBatchJob(context.Background(), desired)
			} else {
				created, err = jobdelay.NewScheduledJobCtl(jobTask, &jobexec.Runtime{Client: client, Store: &jobtest.NoopStore{}, Ack: func() {}}).CreateBatchJob(context.Background(), desired)
			}

			require.False(t, created)
			require.ErrorIs(t, err, jobinfo.ErrJobExecutionIdentityChanged)
			require.Equal(t, 2, getJobs)
		})
	}
}

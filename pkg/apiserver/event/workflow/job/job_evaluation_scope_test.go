package job

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func evaluationScopeStore(t *testing.T) (*gorm.DB, *access.Store, context.Context) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "evaluation.db")), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	connection, err := db.DB()
	require.NoError(t, err)
	connection.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.JobArtifact{}, &model.Applications{}, &model.ApplicationComponent{}, &model.ResourceCreationBudget{}))
	store := access.NewStore(&sqlstore.Driver{Client: *db})
	require.NoError(t, repository.EnsureJobSchedulerPolicy(context.Background(), store))
	return db, store, access.WithScope(context.Background(), access.Scope{WorkspaceID: "space", Namespace: "space-ns"})
}

func evaluationScopeTask(t *testing.T, store *access.Store, appID string) *model.JobTask {
	t.Helper()
	lease := time.Now().Add(time.Hour)
	parent := &model.WorkflowQueue{TaskID: "parent", AppID: appID, WorkspaceID: "space", Status: config.StatusRunning,
		RunGeneration: 1, RunToken: "run-token", WorkerID: "worker", LeaseExpiresAt: &lease}
	if appID == "" {
		parent.Type = config.WorkflowTaskTypeJob
		parent.JobSpec = `{"name":"evaluation","stages":[{"name":"run","steps":[{"name":"evaluate","type":"job","traits":{"eval":{"env":"ack","agent":"oracle","taskPackageId":"11111111-1111-1111-1111-111111111111"}}}]}]}`
	} else {
		require.NoError(t, store.Add(context.Background(), &model.Applications{ID: appID, WorkspaceID: "space", Namespace: "space-ns"}))
	}
	require.NoError(t, store.Add(context.Background(), parent))
	backoff, automount := int32(0), false
	workload := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "evaluation-runner", Namespace: "space-ns", Annotations: map[string]string{
			config.AnnotationJobRunPolicy:           string(workflowconfig.JobRunPolicyRecreate),
			workflowconfig.AnnotationJobRetryPolicy: `{"onOOM":"stop"}`,
		}},
		Spec: batchv1.JobSpec{BackoffLimit: &backoff, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &automount,
			Containers: []corev1.Container{{Name: "job", Image: "example.com/runner:0.22.0", ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{"python", "/opt/eruun/runner.py"}}},
		}}},
	}
	if appID != "" {
		workload.Annotations[config.AnnotationComponentName] = "benchmark"
	}
	task := &model.JobTask{Name: workload.Name, Namespace: workload.Namespace, AppID: appID, WorkspaceID: "space",
		TaskID: parent.TaskID, JobType: string(config.JobEval), JobInfo: workload, Timeout: 60,
		ExecutionKey: "evaluation-execution", RunGeneration: 1, OwnerRunGeneration: 1, RunToken: parent.RunToken, WorkerID: parent.WorkerID,
		EvaluationInfo: `{"runnerToken":"private-evaluation-capability"}`}
	jobinfo.ApplyExecutionIdentity(task)
	return task
}

func TestEvaluationScopeRunsThroughAdmissionAndPersistsPrivateSnapshot(t *testing.T) {
	for _, appID := range []string{"app", ""} {
		name := "application"
		if appID == "" {
			name = "standalone"
		}
		t.Run(name, func(t *testing.T) {
			db, store, scopedCtx := evaluationScopeStore(t)
			task := evaluationScopeTask(t, store, appID)
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			installRetryJobReactor(t, client, 1, "OOMKilled", &created)
			observer := startJobTestObserver(t, client)
			client.ClearActions()
			ctx, cancel := context.WithTimeout(scopedCtx, 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				result <- RunJobs(ctx, []*model.JobTask{task}, &jobexec.Runtime{
					Concurrency:    1,
					Client:         client,
					Store:          store,
					Ack:            func() {},
					StopOnFailure:  true,
					ResourceWaiter: observer,
				})
			}()
			require.Eventually(t, func() bool {
				var count int64
				return db.Model(&model.JobInfo{}).Where("execution_key = ? AND scheduling_state = ?", task.ExecutionKey, "queued").Count(&count).Error == nil && count == 1
			}, time.Second, 10*time.Millisecond, "evaluation must pass RunJobs scope checks and enter scheduling")
			require.Empty(t, client.Actions(), "queued evaluations cannot launch before admission")
			count, err := repository.AdmitQueuedJobs(context.Background(), store)
			require.NoError(t, err)
			require.Equal(t, 1, count)
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("evaluation did not finish after admission")
			}
			require.Len(t, created, 1)
			require.Equal(t, config.StatusFailed, task.Status, "the synthetic Runner failure must reach terminal SaveInfo")
			saved, err := jobinfo.FindExistingJobInfo(scopedCtx, store, task)
			require.NoError(t, err)
			require.NotNil(t, saved)
			require.Equal(t, appID, saved.AppID)
			require.Equal(t, "space", saved.WorkspaceID)
			require.Equal(t, task.EvaluationInfo, saved.EvaluationInfo)
			require.Equal(t, string(config.StatusFailed), saved.Status)
			require.Equal(t, "released", saved.SchedulingState)
			require.True(t, jobinfo.HasInstantJobRetryCheckpoint(saved))
			if appID != "" {
				require.Equal(t, "benchmark", saved.ServiceName)
			}
			payload, err := json.Marshal(saved)
			require.NoError(t, err)
			require.NotContains(t, string(payload), "private-evaluation-capability")
		})
	}
}

func TestEvaluationScopeRejectsForeignAndForgedOwnership(t *testing.T) {
	for _, name := range []string{"foreign application", "foreign workspace", "detached application evaluation", "application command", "forged standalone type"} {
		t.Run(name, func(t *testing.T) {
			db, store, ctx := evaluationScopeStore(t)
			appID := "app"
			if name == "forged standalone type" {
				appID = ""
			}
			task := evaluationScopeTask(t, store, appID)
			switch name {
			case "foreign application":
				require.NoError(t, db.Model(&model.Applications{}).Where("id = ?", appID).Update("workspaceid", "other").Error)
			case "foreign workspace":
				task.WorkspaceID = "other"
			case "detached application evaluation":
				task.AppID = ""
			case "application command", "forged standalone type":
				task.JobType = string(config.JobCommand)
			}
			client := fake.NewSimpleClientset()
			require.Error(t, RunJobs(ctx, []*model.JobTask{task}, &jobexec.Runtime{
				Concurrency:   1,
				Client:        client,
				Store:         store,
				Ack:           func() {},
				StopOnFailure: true,
			}))
			require.Empty(t, client.Actions())
			if name == "foreign application" {
				_, err := jobinfo.FindExistingJobInfo(ctx, store, task)
				require.Error(t, err, "parent scope must also protect lifecycle lookup")
			}
		})
	}
}

func TestStandaloneJobVolumeRunsThroughPVCController(t *testing.T) {
	for _, name := range []string{spec.JobVolumeName("parent"), "foreign-pvc"} {
		t.Run(name, func(t *testing.T) {
			db, store, scopedCtx := evaluationScopeStore(t)
			lease := time.Now().Add(time.Hour)
			parent := &model.WorkflowQueue{TaskID: "parent", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob,
				Status: config.StatusRunning, RunGeneration: 1, RunToken: "run-token", WorkerID: "worker", LeaseExpiresAt: &lease,
				JobSpec: `{"name":"pipeline","volume":{"size":"1Gi"},"stages":[{"name":"run","steps":[{"name":"command","type":"command","spec":{"image":"busybox:1.37.0","command":["true"]}}]}]}`}
			require.NoError(t, store.Add(context.Background(), parent))
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "space-ns", Labels: map[string]string{spec.JobTaskLabel: parent.TaskID}},
				Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}}
			task := &model.JobTask{Name: name, Namespace: "space-ns", WorkspaceID: "space", TaskID: parent.TaskID,
				JobType: string(config.JobDeployPVC), JobInfo: pvc, Timeout: 60, Status: config.StatusQueued,
				ExecutionKey: "volume-execution", RunGeneration: 1, OwnerRunGeneration: 1, RunToken: parent.RunToken, WorkerID: parent.WorkerID}
			jobinfo.ApplyExecutionIdentity(task)
			client := fake.NewSimpleClientset()
			client.PrependReactor("create", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
				action.(k8stesting.CreateAction).GetObject().(*corev1.PersistentVolumeClaim).Status.Phase = corev1.ClaimPending
				return false, nil, nil
			})
			ctx, cancel := context.WithTimeout(scopedCtx, 5*time.Second)
			defer cancel()
			run := &jobexec.Runtime{Concurrency: 1, Client: client, Store: store, Ack: func() {}, StopOnFailure: true}
			if name == "foreign-pvc" {
				err := RunJobs(ctx, []*model.JobTask{task}, run)
				require.ErrorContains(t, err, "authorize workspace job")
				require.Empty(t, client.Actions(), "foreign PVC must be rejected before Kubernetes access")
				_, err = jobinfo.FindExistingJobInfo(ctx, store, task)
				require.Error(t, err, "lifecycle lookup must retain the PVC name authorization")
				var count int64
				require.NoError(t, db.Model(&model.JobInfo{}).Count(&count).Error)
				require.Zero(t, count)
				return
			}
			result := make(chan error, 1)
			go func() { result <- RunJobs(ctx, []*model.JobTask{task}, run) }()
			require.Eventually(t, func() bool {
				var count int64
				return db.Model(&model.JobInfo{}).Where("execution_key = ? AND scheduling_state = ?", task.ExecutionKey, "queued").Count(&count).Error == nil && count == 1
			}, time.Second, 10*time.Millisecond)
			count, err := repository.AdmitQueuedJobs(context.Background(), store)
			require.NoError(t, err)
			require.Equal(t, 1, count)
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("PVC did not finish after admission")
			}
			require.Equal(t, config.StatusCompleted, task.Status)
			created, err := client.CoreV1().PersistentVolumeClaims("space-ns").Get(ctx, name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, "parent", created.Labels[spec.JobTaskLabel])
			saved, err := jobinfo.FindExistingJobInfo(ctx, store, task)
			require.NoError(t, err)
			require.NotNil(t, saved)
			require.Equal(t, name, saved.ServiceName)
			require.Equal(t, string(config.StatusCompleted), saved.Status)
		})
	}
}

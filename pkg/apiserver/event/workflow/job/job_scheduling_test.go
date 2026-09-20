package job

import (
	"context"
	"errors"
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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

func TestJobAdmissionUsesTaskWorkspaceAndApplication(t *testing.T) {
	for _, tc := range []struct {
		name     string
		workflow config.WorkflowTaskType
		job      config.JobType
		appID    string
	}{
		{name: "import scan", workflow: config.WorkflowTaskTypeResourceImportScan, job: config.JobResourceImportScan},
		{name: "import manage", workflow: config.WorkflowTaskTypeResourceImportManage, job: config.JobResourceImportManage},
		{name: "application", job: config.JobDeployService, appID: "app"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
			require.NoError(t, err)
			connection, err := db.DB()
			require.NoError(t, err)
			connection.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })
			require.NoError(t, db.AutoMigrate(&model.SystemSetting{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.Applications{}))
			store := access.NewStore(&sqlstore.Driver{Client: *db})
			ctx := context.Background()
			require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, store))
			if tc.appID != "" {
				require.NoError(t, store.Add(ctx, &model.Applications{ID: tc.appID, WorkspaceID: "workspace", Namespace: "namespace"}))
			}
			lease := time.Now().Add(time.Hour)
			owner := &model.WorkflowQueue{TaskID: "task", AppID: tc.appID, WorkspaceID: "workspace", Type: tc.workflow,
				Status: config.StatusRunning, RunGeneration: 1, RunToken: "token", WorkerID: "worker", LeaseExpiresAt: &lease}
			require.NoError(t, store.Add(ctx, owner))
			task := &model.JobTask{TaskID: owner.TaskID, AppID: owner.AppID, WorkspaceID: owner.WorkspaceID,
				JobType: string(tc.job), ExecutionKey: "execution", Status: config.StatusPrepare,
				RunGeneration: 1, RunToken: owner.RunToken, WorkerID: owner.WorkerID}
			record := buildJobInfoRecord(task)
			require.NoError(t, repository.EnqueueJobForScheduling(ctx, store, owner, &record, nil))
			n, err := repository.AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			scopedCtx := access.WithScope(ctx, access.Scope{WorkspaceID: "workspace", Namespace: "namespace"})
			release, err := waitForJobAdmission(scopedCtx, store, task, nil)
			require.NoError(t, err)
			require.NoError(t, release())
			require.NoError(t, store.Get(scopedCtx, &record))
			require.Equal(t, "released", record.SchedulingState)
		})
	}
}

func TestJobRunnerWaitsForGlobalPriorityAdmission(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	connection, err := db.DB()
	require.NoError(t, err)
	connection.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.ApplicationComponent{}, &model.Applications{}))
	store := &sqlstore.Driver{Client: *db}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, store))
	require.NoError(t, db.Model(&model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler}).Update("value", `{"strategy":"priority","maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1,"agingSeconds":60}`).Error)
	client := fake.NewSimpleClientset()
	results := map[string]chan error{}
	for _, name := range []string{"background", "high"} {
		expires := time.Now().Add(time.Minute)
		owner := &model.WorkflowQueue{TaskID: name, WorkspaceID: "space-" + name, Status: config.StatusRunning, RunGeneration: 1, RunToken: "token-" + name, WorkerID: "worker-" + name, LeaseExpiresAt: &expires}
		require.NoError(t, store.Add(ctx, owner))
		task := &model.JobTask{Name: name, TaskID: name, Namespace: "default", WorkspaceID: owner.WorkspaceID, ExecutionKey: name, RunGeneration: 1, OwnerRunGeneration: 1, RunToken: owner.RunToken, WorkerID: owner.WorkerID, JobType: string(config.JobDeployConfigMap), SchedulingClass: name, JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}}
		result := make(chan error, 1)
		results[name] = result
		go func() { result <- runJob(ctx, task, client, store, func() {}, nil) }()
		require.Eventually(t, func() bool {
			var count int64
			return db.Model(&model.JobInfo{}).Where("execution_key = ? AND scheduling_state = ?", name, "queued").Count(&count).Error == nil && count == 1
		}, time.Second, 10*time.Millisecond)
	}
	resources, err := client.CoreV1().ConfigMaps("default").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, resources.Items)
	admitted, err := repository.AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, admitted)
	select {
	case err := <-results["high"]:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("high priority job did not finish")
	}
	resources, err = client.CoreV1().ConfigMaps("default").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, resources.Items, 1)
	require.Equal(t, "high", resources.Items[0].Name)
	admitted, err = repository.AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, admitted)
	select {
	case err := <-results["background"]:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("background job did not resume")
	}
	var active int64
	require.NoError(t, db.Model(&model.JobInfo{}).Where("scheduling_state = ?", "admitted").Count(&active).Error)
	require.Zero(t, active)
}

func TestCancelledKubernetesJobKeepsAdmissionWhenOnlineCleanupFails(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	connection, err := db.DB()
	require.NoError(t, err)
	connection.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.Workspace{}, &model.ApplicationComponent{}, &model.Applications{}))
	store := &sqlstore.Driver{Client: *db}
	ctx := context.Background()
	require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, store))
	require.NoError(t, db.Model(&model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler}).Update("value", `{"strategy":"priority","maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1,"agingSeconds":60}`).Error)
	require.NoError(t, store.Add(ctx, &model.Workspace{ID: "workspace", Namespace: "default"}))

	lease := time.Now().Add(time.Minute)
	owner := &model.WorkflowQueue{
		TaskID: "cancelled-task", WorkspaceID: "workspace", Status: config.StatusRunning,
		RunGeneration: 1, RunToken: "cancelled-token", WorkerID: "cancelled-worker", LeaseExpiresAt: &lease,
	}
	require.NoError(t, store.Add(ctx, owner))
	desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "cancelled-job", Namespace: "default", UID: "cancelled-job-uid"}}
	task := &model.JobTask{
		Name: desired.Name, Namespace: desired.Namespace, TaskID: owner.TaskID, WorkspaceID: owner.WorkspaceID,
		ExecutionKey: "cancelled-execution", RunGeneration: 1, OwnerRunGeneration: 1,
		OwnerStatus: config.StatusRunning, RunToken: owner.RunToken, WorkerID: owner.WorkerID,
		JobType: string(config.JobDeployInstant), JobInfo: desired,
	}
	cleanupErr := errors.New("kubernetes API unavailable")
	deleteAttempts := 0
	client := fake.NewSimpleClientset()
	client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		deleteAttempts++
		if deleteAttempts == 1 {
			return true, nil, cleanupErr
		}
		return false, nil, nil
	})
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	result := make(chan error, 1)
	go func() { result <- runJob(runCtx, task, client, store, func() {}, nil) }()
	require.Eventually(t, func() bool {
		var count int64
		return db.Model(&model.JobInfo{}).Where("execution_key = ? AND scheduling_state = ?", task.ExecutionKey, workflowconfig.JobSchedulingQueued).Count(&count).Error == nil && count == 1
	}, time.Second, 10*time.Millisecond)
	admitted, err := repository.AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, admitted)
	require.Eventually(t, func() bool {
		_, err := client.BatchV1().Jobs(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		return err == nil
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, db.Model(owner).Update("status", config.StatusCancelled).Error)
	cancelRun()
	select {
	case err := <-result:
		require.ErrorIs(t, err, signal.ErrInfrastructureStop)
		require.ErrorIs(t, err, cleanupErr)
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled job did not return after cleanup failure")
	}
	require.Equal(t, config.StatusCancelled, task.Status)
	var saved model.JobInfo
	require.NoError(t, db.Where("execution_key = ?", task.ExecutionKey).First(&saved).Error)
	require.Equal(t, string(config.StatusCancelled), saved.Status)
	require.Equal(t, workflowconfig.JobSchedulingAdmitted, saved.SchedulingState)
	require.Equal(t, cancelledJobCleanupPending, saved.SchedulingReason)
	_, err = client.BatchV1().Jobs(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	require.NoError(t, err)

	waitingOwner := &model.WorkflowQueue{
		TaskID: "waiting-task", WorkspaceID: "other-workspace", Status: config.StatusRunning,
		RunGeneration: 1, RunToken: "waiting-token", WorkerID: "waiting-worker", LeaseExpiresAt: &lease,
	}
	require.NoError(t, store.Add(ctx, waitingOwner))
	waitingKey := "waiting-execution"
	waiting := &model.JobInfo{
		TaskID: waitingOwner.TaskID, WorkspaceID: waitingOwner.WorkspaceID, Type: string(config.JobDeployInstant),
		Status: string(config.StatusPrepare), ExecutionKey: &waitingKey, RunGeneration: 1,
	}
	require.NoError(t, repository.EnqueueJobForScheduling(ctx, store, waitingOwner, waiting, nil))
	admitted, err = repository.AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Zero(t, admitted, "a live cancelled workload must keep consuming its admission")
	require.NoError(t, db.Where("execution_key = ?", waitingKey).First(waiting).Error)
	require.Equal(t, workflowconfig.JobSchedulingQueued, waiting.SchedulingState)

	cleaned, err := CleanupRecoveredCancelledJobs(ctx, client, store)
	require.NoError(t, err)
	require.Equal(t, 1, cleaned)
	require.NoError(t, db.Where("execution_key = ?", task.ExecutionKey).First(&saved).Error)
	require.Equal(t, workflowconfig.JobSchedulingReleased, saved.SchedulingState)
	require.Equal(t, cancelledJobCleanupComplete, saved.SchedulingReason)
}

func TestJobAdmissionWaitExitReleasesQueueWithoutKubernetesEffects(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		for _, reason := range []string{"cancel", "cancel after database commit", "database cancellation before signal", "infrastructure stop", "ownership transfer", "cancel after ownership transfer"} {
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
				lease := time.Now().Add(time.Minute)
				owner := &model.WorkflowQueue{TaskID: "task", WorkspaceID: "workspace", Status: config.StatusRunning, RunGeneration: 1,
					RunToken: "token", WorkerID: "worker", LeaseExpiresAt: &lease}
				require.NoError(t, store.Add(context.Background(), owner))
				task := &model.JobTask{Name: "work", TaskID: owner.TaskID, Namespace: "default", WorkspaceID: owner.WorkspaceID,
					ExecutionKey: "execution", RunGeneration: 1, OwnerRunGeneration: 1, RunToken: owner.RunToken, WorkerID: owner.WorkerID,
					JobType: string(config.JobDeployConfigMap), JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "work", Namespace: "default"}}}
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				client := fake.NewSimpleClientset()
				result := make(chan error, 1)
				go func() {
					result <- RunJobs(ctx, []*model.JobTask{task}, concurrency, client, nil, store, func() {}, true, nil, nil, nil, nil, nil)
				}()
				require.Eventually(t, func() bool {
					var count int64
					return db.Model(&model.JobInfo{}).Where("execution_key = ? AND scheduling_state = ?", task.ExecutionKey, "queued").Count(&count).Error == nil && count == 1
				}, time.Second, 10*time.Millisecond)
				require.Empty(t, client.Actions())
				switch reason {
				case "cancel":
					cancel(context.Canceled)
				case "cancel after database commit", "database cancellation before signal":
					require.NoError(t, db.Model(owner).Update("status", config.StatusCancelled).Error)
					if reason == "cancel after database commit" {
						cancel(context.Canceled)
					}
				case "infrastructure stop":
					cancel(signal.ErrInfrastructureStop)
				case "ownership transfer", "cancel after ownership transfer":
					require.NoError(t, db.Model(owner).Updates(map[string]interface{}{"run_generation": 2, "run_token": "new-token", "worker_id": "new-worker"}).Error)
					if reason == "cancel after ownership transfer" {
						cancel(context.Canceled)
					}
				}
				cancelled := reason == "cancel after database commit" || reason == "database cancellation before signal"
				select {
				case err := <-result:
					if cancelled {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, signal.ErrInfrastructureStop)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("job did not stop waiting")
				}
				require.Empty(t, client.Actions(), "a waiting job must never run Kubernetes operations")
				var stored model.JobInfo
				require.NoError(t, db.Where("execution_key = ?", task.ExecutionKey).First(&stored).Error)
				if cancelled {
					require.Equal(t, string(config.StatusCancelled), stored.Status)
				} else {
					require.Equal(t, string(config.StatusPrepare), stored.Status)
				}
				if reason != "ownership transfer" && reason != "cancel after ownership transfer" {
					require.Equal(t, "released", stored.SchedulingState, "exiting a queue wait must release the owned scheduling entry")
				}
				admitted, err := repository.AdmitQueuedJobs(context.Background(), store)
				require.NoError(t, err)
				require.Zero(t, admitted, "the departed worker must not leave an orphan Job to admit")
				require.NoError(t, db.Where("execution_key = ?", task.ExecutionKey).First(&stored).Error)
				require.Equal(t, "released", stored.SchedulingState)
			})
		}
	}
}

func TestJobRunnerRechecksApplicationAfterAdmission(t *testing.T) {
	for _, change := range []string{"observe", "deleted", "unchanged"} {
		t.Run(change, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
			require.NoError(t, err)
			connection, err := db.DB()
			require.NoError(t, err)
			connection.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })
			require.NoError(t, db.AutoMigrate(&model.SystemSetting{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.ApplicationComponent{}, &model.Applications{}))
			store := &sqlstore.Driver{Client: *db}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, store))
			app := &model.Applications{ID: "app", WorkspaceID: "workspace", ManagementMode: config.ManagementModeNative}
			require.NoError(t, store.Add(ctx, app))
			lease := time.Now().Add(time.Minute)
			owner := &model.WorkflowQueue{TaskID: "task", WorkspaceID: app.WorkspaceID, Status: config.StatusRunning, RunGeneration: 1,
				RunToken: "token", WorkerID: "worker", LeaseExpiresAt: &lease}
			require.NoError(t, store.Add(ctx, owner))
			task := &model.JobTask{Name: "work", AppID: app.ID, TaskID: owner.TaskID, Namespace: "default", WorkspaceID: app.WorkspaceID,
				ExecutionKey: "execution", RunGeneration: 1, OwnerRunGeneration: 1, RunToken: owner.RunToken, WorkerID: owner.WorkerID,
				JobType: string(config.JobDeployConfigMap), JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "work", Namespace: "default"}}}
			client := fake.NewSimpleClientset()
			result := make(chan error, 1)
			go func() { result <- runJob(ctx, task, client, store, func() {}, nil) }()
			require.Eventually(t, func() bool {
				var count int64
				return db.Model(&model.JobInfo{}).Where("execution_key = ? AND scheduling_state = ?", task.ExecutionKey, "queued").Count(&count).Error == nil && count == 1
			}, time.Second, 10*time.Millisecond)
			require.Empty(t, client.Actions())
			switch change {
			case "observe":
				require.NoError(t, db.Model(app).Update("management_mode", config.ManagementModeObserve).Error)
			case "deleted":
				require.NoError(t, store.Delete(ctx, app))
			}
			admitted, err := repository.AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, admitted)
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("admitted job did not return")
			}
			if change == "unchanged" {
				require.Equal(t, config.StatusCompleted, task.Status)
				_, err := client.CoreV1().ConfigMaps("default").Get(ctx, task.Name, metav1.GetOptions{})
				require.NoError(t, err)
			} else {
				require.Equal(t, config.StatusFailed, task.Status)
				require.Empty(t, client.Actions(), "an application losing write permission while queued must not access Kubernetes")
			}
			var stored model.JobInfo
			require.NoError(t, db.Where("execution_key = ?", task.ExecutionKey).First(&stored).Error)
			require.Equal(t, string(task.Status), stored.Status)
			require.Equal(t, "released", stored.SchedulingState)
		})
	}
}

func TestJobAdmissionUnfencedCallsCannotImpersonateApprovalOwner(t *testing.T) {
	for _, status := range []config.Status{"", config.StatusRunning, config.StatusWaitingApprove} {
		t.Run(string(status), func(t *testing.T) {
			task := &model.JobTask{TaskID: "task", ExecutionKey: "callback", WorkspaceID: "workspace",
				JobType: string(config.JobDeployCallback), OwnerStatus: status}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			release, err := waitForJobAdmission(ctx, &noopStore{}, task, nil)
			if status == config.StatusWaitingApprove {
				require.ErrorIs(t, err, repository.ErrWorkflowOwnershipRequired)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, release())
		})
	}
}

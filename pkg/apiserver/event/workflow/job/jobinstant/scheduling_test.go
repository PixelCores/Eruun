package jobinstant

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

func TestJobAdmissionUsesTaskWorkspaceAndApplication(t *testing.T) {
	for _, tc := range []struct {
		name     string
		workflow config.WorkflowTaskType
		job      config.JobType
		appID    string
	}{
		{name: "application", job: config.JobDeployService, appID: "app"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "scheduling.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
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
			record := jobinfo.BuildJobInfoRecord(task)
			require.NoError(t, repository.EnqueueJobForScheduling(ctx, store, owner, &record, nil))
			n, err := repository.AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			scopedCtx := access.WithScope(ctx, access.Scope{WorkspaceID: "workspace", Namespace: "namespace"})
			release, err := WaitForJobAdmission(scopedCtx, store, task, nil)
			require.NoError(t, err)
			require.NoError(t, release())
			require.NoError(t, store.Get(scopedCtx, &record))
			require.Equal(t, "released", record.SchedulingState)
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
			release, err := WaitForJobAdmission(ctx, &jobtest.NoopStore{}, task, nil)
			if status == config.StatusWaitingApprove {
				require.ErrorIs(t, err, repository.ErrWorkflowOwnershipRequired)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, release())
		})
	}
}

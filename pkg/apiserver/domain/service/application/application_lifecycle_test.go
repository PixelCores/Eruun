package application

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func newApplicationCallbackStore(t *testing.T, entities ...datastore.Entity) *sqlstore.Driver {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "callbacks.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.Applications{}, &model.ApplicationComponent{}, &model.Workflow{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.SystemSetting{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	store := &sqlstore.Driver{Client: *db}
	for _, entity := range entities {
		require.NoError(t, store.Add(context.Background(), entity))
	}
	require.NoError(t, repository.EnsureJobSchedulerPolicy(context.Background(), store))
	return store
}

func admitApplicationCallback(t *testing.T, store *sqlstore.Driver) {
	t.Helper()
	require.Eventually(t, func() bool {
		n, err := repository.AdmitQueuedJobs(context.Background(), store)
		return err == nil && n == 1
	}, 2*time.Second, 10*time.Millisecond)
	// The HTTP handler may return before Job completion/release is persisted.
	// Join that asynchronous work before the test closes its database.
	t.Cleanup(func() {
		require.Eventually(t, func() bool {
			rows, err := store.List(context.Background(), &model.JobInfo{}, &datastore.ListOptions{FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{{Key: "type", Values: []string{string(config.JobDeployCallback)}}}}})
			if err != nil || len(rows) != 1 {
				return false
			}
			job := rows[0].(*model.JobInfo)
			return job.Status == string(config.StatusCompleted) && job.SchedulingState == workflowconfig.JobSchedulingReleased
		}, 2*time.Second, 10*time.Millisecond)
	})
}

func newLifecycleCallbackServer(t *testing.T) (*httptest.Server, <-chan string) {
	t.Helper()
	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		select {
		case received <- string(body):
		default:
		}
	}))
	t.Cleanup(server.Close)
	return server, received
}

func requireLifecycleCallback(t *testing.T, received <-chan string, event, status, taskID string, workflowType config.WorkflowTaskType) {
	t.Helper()
	select {
	case body := <-received:
		require.Contains(t, body, `"event":"`+event+`"`)
		require.Contains(t, body, `"status":"`+status+`"`)
		require.Contains(t, body, `"taskId":"`+taskID+`"`)
		require.Contains(t, body, `"workflowId":""`)
		require.Contains(t, body, `"workflowType":"`+string(workflowType)+`"`)
	case <-time.After(2 * time.Second):
		t.Fatalf("lifecycle callback %s not received", event)
	}
}

func requireNoCallbackReceived(t *testing.T, received <-chan string) {
	t.Helper()
	select {
	case body := <-received:
		t.Fatalf("unexpected callback received: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
}

func captureLifecycleKlogOutput(t *testing.T, fn func()) string {
	t.Helper()
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)

	os.Stderr = w
	klog.SetOutput(w)
	defer func() {
		os.Stderr = oldStderr
		klog.SetOutput(oldStderr)
		_ = r.Close()
		_ = w.Close()
	}()

	fn()
	klog.Flush()
	require.NoError(t, w.Close())
	os.Stderr = oldStderr
	klog.SetOutput(oldStderr)

	output, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return string(output)
}

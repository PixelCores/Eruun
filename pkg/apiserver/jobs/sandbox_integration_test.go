//go:build integration

package jobs

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	mysqldsn "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func mysqlSandboxFixture(t *testing.T) (*runnerFixture, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN is required for isolated MySQL Sandbox integration tests")
	}
	parsed, err := mysqldsn.ParseDSN(dsn)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(parsed.DBName, "eruun_scheduler_test"), "integration database must start with eruun_scheduler_test")
	parsed.ClientFoundRows = true
	db, err := gorm.Open(mysqlgorm.Open(parsed.FormatDSN()), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(16)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	models := []interface{}{&model.Workspace{}, &model.Applications{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.JobArtifact{}, &model.ArtifactChunk{}, &model.JobDelivery{}, &model.SystemSetting{}, &model.ResourceCreationBudget{}, &model.JobSandbox{}}
	for _, entity := range models {
		require.False(t, db.Migrator().HasTable(entity), "integration schema must be empty")
	}
	require.NoError(t, db.AutoMigrate(models...))
	t.Cleanup(func() { require.NoError(t, db.Migrator().DropTable(models...)) })
	raw := &sqlstore.Driver{Client: *db}
	ctx := context.Background()
	require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, raw))
	require.NoError(t, raw.Add(ctx, &model.Workspace{ID: "space", Namespace: "space-ns"}))
	cfg := &config.Config{Accounts: &spec.AccountConfig{}, Jobs: &spec.JobsRuntimeConfig{RunnerImage: "example.com/eruun-harbor:0.22.0", APIURL: "https://eruun.example.com"}}
	service, err := New(account.NewStore(raw), fake.NewSimpleClientset(), cfg)
	require.NoError(t, err)
	ctx = account.WithScope(ctx, account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "member", UserID: "member"})
	return attachSandboxFixture(t, newRunnerFixtureForService(t, service, raw, ctx), true)
}

func TestMySQLSandboxLifecycle(t *testing.T) {
	t.Run("concurrent-identical-intent-and-zero-value-release", func(t *testing.T) {
		f, client := mysqlSandboxFixture(t)
		ctx := context.Background()
		var group sync.WaitGroup
		errs := make(chan error, 8)
		for range 8 {
			group.Go(func() { _, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("same")); errs <- err })
		}
		group.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		row := sandboxRow(t, f, "same")
		require.Equal(t, 1, row.CreateAttempts)
		require.NotNil(t, row.AdmittedAt)
		require.Empty(t, row.LeaseToken)
		require.Nil(t, row.LeaseUntil)
		makeSandboxReady(t, f, client, "same")
		_, err := f.service.RunnerSandboxGet(ctx, f.identity, "same")
		require.NoError(t, err)
		require.False(t, sandboxRow(t, f, "same").StartReserved)
		_, err = f.service.RunnerSandboxRelease(ctx, f.identity, "same", SandboxReleaseRequest{CollectionComplete: ptr.To(true)})
		require.NoError(t, err)
		row = sandboxRow(t, f, "same")
		require.Equal(t, sandboxReleased, row.State)
		require.False(t, row.SlotReserved)
		require.Empty(t, row.Reason)
		require.Empty(t, row.LeaseToken)
		require.Nil(t, row.LeaseUntil)
	})
	t.Run("concurrent-trials-share-execution-capacity", func(t *testing.T) {
		f, client := mysqlSandboxFixture(t)
		ctx := context.Background()
		var group sync.WaitGroup
		errs := make(chan error, 3)
		for _, trial := range []string{"one", "two", "three"} {
			group.Go(func() { _, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest(trial)); errs <- err })
		}
		group.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		count, err := f.raw.Count(ctx, &model.JobSandbox{SlotReserved: true}, nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
		creates := 0
		for _, action := range client.Actions() {
			if action.GetVerb() == "create" {
				creates++
			}
		}
		require.Equal(t, 1, creates)
	})
	t.Run("orphaned-owners-retain-then-release-capacity", func(t *testing.T) {
		f, client := mysqlSandboxFixture(t)
		ctx := context.Background()
		created, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
		require.NoError(t, err)
		require.NoError(t, f.raw.Delete(ctx, f.parent))
		require.NoError(t, f.raw.Delete(ctx, f.record))
		row := sandboxRow(t, f, "trial")
		row.ReconcileAt = time.Now().UTC().Add(-time.Minute)
		require.NoError(t, f.raw.Put(ctx, row))

		require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
		row = sandboxRow(t, f, "trial")
		require.Equal(t, sandboxRetained, row.State)
		require.True(t, row.SlotReserved)
		past := time.Now().UTC().Add(-time.Minute)
		row.RetainUntil, row.ReconcileAt = &past, past
		require.NoError(t, f.raw.Put(ctx, row))
		require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
		row = sandboxRow(t, f, "trial")
		require.Equal(t, sandboxReleased, row.State)
		require.False(t, row.SlotReserved)
		_, err = client.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
		require.True(t, k8serrors.IsNotFound(err))
		require.NotEmpty(t, created.SandboxUID)
	})
	t.Run("uncertain-response-cancel-late-create-and-old-lease", func(t *testing.T) {
		f, client := mysqlSandboxFixture(t)
		ctx := context.Background()
		var late *unstructured.Unstructured
		client.PrependReactor("create", "sandboxes", func(action k8stesting.Action) (bool, runtime.Object, error) {
			late = action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
			late.SetUID("late-create")
			return true, nil, errors.New("uncertain response")
		})
		_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
		require.Error(t, err)
		stale := *sandboxRow(t, f, "trial")
		auth, err := f.service.authorizeRunner(ctx, f.identity)
		require.NoError(t, err)
		f.parent.Status = config.StatusCancelled
		require.NoError(t, f.raw.Put(ctx, f.parent))
		updated, err := f.raw.CompareAndSwap(ctx, &stale, "id", stale.ID, map[string]interface{}{"lease_until": time.Now().UTC().Add(-time.Minute), "reconcile_at": time.Now().UTC().Add(-time.Minute)})
		require.NoError(t, err)
		require.True(t, updated)
		require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
		row := sandboxRow(t, f, "trial")
		require.Equal(t, "creation_outcome_unknown", row.Reason)
		require.True(t, row.SlotReserved)
		called := false
		err = f.service.mutateSandbox(ctx, auth, &stale, func(datastore.DataStore, *model.JobSandbox, time.Time) error { called = true; return nil })
		require.ErrorIs(t, err, ErrRunnerConflict)
		require.False(t, called)
		require.NoError(t, client.Tracker().Create(SandboxGVR, late, row.Namespace))
		row.ReconcileAt = time.Now().UTC().Add(-time.Minute)
		require.NoError(t, f.raw.Put(ctx, row))
		require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
		row = sandboxRow(t, f, "trial")
		require.Equal(t, "late-create", row.SandboxUID)
		require.Equal(t, sandboxRetained, row.State)
		require.True(t, row.SlotReserved)
		require.False(t, row.StartReserved)
		require.NotNil(t, row.RetainUntil)
	})
}

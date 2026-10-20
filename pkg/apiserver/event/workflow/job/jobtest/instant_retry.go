package jobtest

import (
	"context"
	"fmt"
	"path/filepath"
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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

type RetryCreationBudgetStore struct {
	datastore.DataStore
	CreationStore *sqlstore.Driver
}

// Keep checkpoint failure injection local while exercising the production
// creation-budget transaction, database clock and persisted policy/debt.
func NewRetryCreationBudgetStore(t *testing.T, store datastore.DataStore) *RetryCreationBudgetStore {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "creation-budget.db")), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	connection, err := db.DB()
	require.NoError(t, err)
	connection.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	require.NoError(t, db.AutoMigrate(&model.SystemSetting{}, &model.ResourceCreationBudget{}))
	s := &RetryCreationBudgetStore{DataStore: store, CreationStore: &sqlstore.Driver{Client: *db}}
	require.NoError(t, repository.EnsureJobSchedulerPolicy(context.Background(), s.CreationStore))
	return s
}

func (s *RetryCreationBudgetStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.CreationStore.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		return fn(&RetryCreationBudgetStore{DataStore: s.DataStore, CreationStore: tx.(*sqlstore.Driver)})
	})
}

func (s *RetryCreationBudgetStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.WithReadCommittedTransaction(ctx, fn)
}

func (s *RetryCreationBudgetStore) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	return s.CreationStore.CurrentDatabaseTime(ctx)
}

// Route only budget records to SQL. Checkpoint writes and parent fencing retain
// the wrapped fixture's ownership checks and failure injection inside callbacks.
func (s *RetryCreationBudgetStore) recordStore(entity datastore.Entity) datastore.DataStore {
	switch entity.(type) {
	case *model.SystemSetting, *model.ResourceCreationBudget:
		return s.CreationStore
	default:
		return s.DataStore
	}
}

func (s *RetryCreationBudgetStore) Add(ctx context.Context, entity datastore.Entity) error {
	return s.recordStore(entity).Add(ctx, entity)
}

func (s *RetryCreationBudgetStore) Put(ctx context.Context, entity datastore.Entity) error {
	return s.recordStore(entity).Put(ctx, entity)
}

func (s *RetryCreationBudgetStore) Get(ctx context.Context, entity datastore.Entity) error {
	return s.recordStore(entity).Get(ctx, entity)
}

func (s *RetryCreationBudgetStore) CompareAndSwap(ctx context.Context, entity datastore.Entity, field string, value interface{}, updates map[string]interface{}) (bool, error) {
	return s.recordStore(entity).CompareAndSwap(ctx, entity, field, value, updates)
}

func (s *RetryCreationBudgetStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	return s.DataStore.(datastore.ConditionalCompareAndSwap).CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

func RetryTestPod(job *batchv1.Job, reason string) *corev1.Pod {
	controller := true
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-" + string(job.UID), Namespace: job.Namespace,
		Labels:          map[string]string{batchv1.JobNameLabel: job.Name},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &controller}}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: job.Spec.Template.Spec.Containers[0].Name,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: reason, ExitCode: 137}}}}}}
}

func InstallRetryJobReactor(t *testing.T, client *fake.Clientset, failAttempts int, reason string, created *[]*batchv1.Job) {
	t.Helper()
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		job.UID = types.UID(fmt.Sprintf("job-%d", len(*created)+1))
		job.ResourceVersion = "1"
		*created = append(*created, job.DeepCopy())
		if len(*created) <= failAttempts {
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
			require.NoError(t, client.Tracker().Add(RetryTestPod(job, reason)))
		} else {
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		}
		require.NoError(t, client.Tracker().Add(job))
		return true, job, nil
	})
}

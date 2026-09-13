//go:build integration

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqldriver "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestMySQLRunnerClaimRowLockHasOneWinner(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("MYSQL_TEST_DSN"))
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN not set; requires an isolated MySQL test database")
	}
	db, err := gorm.Open(mysqlgorm.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.Workspace{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.JobArtifact{}, &model.ArtifactChunk{}, &model.JobDelivery{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	driver := &sqldriver.Driver{Client: *db}
	workspaceID, taskID := uuid.NewString(), uuid.NewString()
	namespace := "test-" + strings.ReplaceAll(workspaceID, "-", "")[:20]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, driver.Add(ctx, &model.Workspace{ID: workspaceID, Namespace: namespace}))
	t.Cleanup(func() {
		for _, entity := range []datastore.Entity{
			&model.JobDelivery{WorkspaceID: workspaceID}, &model.ArtifactChunk{WorkspaceID: workspaceID},
			&model.JobArtifact{WorkspaceID: workspaceID}, &model.JobInfo{WorkspaceID: workspaceID},
			&model.WorkflowQueue{WorkspaceID: workspaceID}, &model.Workspace{ID: workspaceID},
		} {
			require.NoError(t, driver.DeleteByFilter(context.Background(), entity, nil))
		}
	})
	datasetID := uuid.NewString()
	require.NoError(t, driver.Add(ctx, &model.JobArtifact{ID: datasetID, WorkspaceID: workspaceID, Kind: artifacts.KindDataset, Digest: strings.Repeat("a", 64)}))
	declaration := spec.JobSpec{
		Name: "evaluation", Type: string(config.JobAgentEvaluation),
		Spec: json.RawMessage(`{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"` + datasetID + `","agent":{"name":"oracle"}}`),
	}
	policy := spec.DefaultJobResultPolicy()
	declaration.ResultPolicy = &policy
	require.NoError(t, declaration.Normalize())
	declarationJSON, err := json.Marshal(declaration)
	require.NoError(t, err)
	parent := &model.WorkflowQueue{
		TaskID: taskID, WorkspaceID: workspaceID, Type: config.WorkflowTaskTypeJob, Status: config.StatusRunning,
		JobSpec: string(declarationJSON), JobToken: strings.Repeat("f", 64), RunGeneration: 1,
	}
	require.NoError(t, driver.Add(ctx, parent))
	client := fake.NewSimpleClientset()
	cfg := &config.Config{Jobs: &spec.JobsRuntimeConfig{RunnerImage: "example.com/runner:0.22.0", APIURL: "https://eruun.example.com"}}
	service, err := New(account.NewStore(driver), client, cfg)
	require.NoError(t, err)
	scoped := account.WithScope(ctx, account.Scope{WorkspaceID: workspaceID, Namespace: namespace, Role: "member"})
	task, err := BuildTask(scoped, service.Store, cfg, parent, namespace)
	require.NoError(t, err)
	task.ExecutionKey, task.RunGeneration, task.Attempt = "execution", 1, 1
	workflowjob.ApplyTaskIDAnnotation(task)
	workflowjob.ApplyExecutionIdentity(task)
	workload := task.JobInfo.(*batchv1.Job)
	workload.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	workload.UID = "job-uid"
	checkpoint, err := json.Marshal(map[string]any{
		"kind": "instant_job_retry", "version": 1, "attempt": 1, "job": workload,
		"currentUID": workload.UID, "deadline": time.Now().Add(time.Hour).UnixNano(),
	})
	require.NoError(t, err)
	record := &model.JobInfo{
		Type: task.JobType, TaskID: taskID, WorkspaceID: workspaceID, ServiceName: workload.Name,
		Status: string(config.StatusRunning), ExecutionKey: ptr.To(task.ExecutionKey), RunGeneration: 1, Attempt: 1, InternalInfo: string(checkpoint),
	}
	require.NoError(t, driver.Add(ctx, record))
	require.NoError(t, client.Tracker().Add(workload))
	identities := make([]RunnerIdentity, 2)
	for index := range identities {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: workload.Name + "-pod-" + string(rune('a'+index)), Namespace: namespace, UID: types.UID("pod-" + string(rune('a'+index))),
			Annotations:     workload.Spec.Template.Annotations,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: workload.Name, UID: workload.UID, Controller: ptr.To(true)}},
		}, Spec: workload.Spec.Template.Spec}
		require.NoError(t, client.Tracker().Add(pod))
		identities[index] = RunnerIdentity{TaskID: taskID, Token: parent.JobToken, PodName: pod.Name, PodUID: string(pod.UID)}
	}

	var wait sync.WaitGroup
	errorsSeen := make([]error, len(identities))
	for index := range identities {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, errorsSeen[index] = service.RunnerEvent(ctx, identities[index], RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "claim"})
		}(index)
	}
	wait.Wait()
	winners, conflicts := 0, 0
	for _, err := range errorsSeen {
		if err == nil {
			winners++
		} else if errors.Is(err, ErrRunnerConflict) {
			conflicts++
		}
	}
	require.Equal(t, 1, winners)
	require.Equal(t, 1, conflicts)
}

package jobs

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
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
	"k8s.io/utils/ptr"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func testJobService(t *testing.T) (*Service, *sqlstore.Driver, context.Context) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "jobs.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(&model.Workspace{}, &model.Applications{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.JobArtifact{}, &model.ArtifactChunk{}, &model.JobDelivery{}))
	raw := &sqlstore.Driver{Client: *db}
	require.NoError(t, raw.Add(context.Background(), &model.Workspace{ID: "space", Namespace: "space-ns"}))
	cfg := &config.Config{Accounts: &spec.AccountConfig{}, Jobs: &spec.JobsRuntimeConfig{RunnerImage: "example.com/eruun-harbor:0.22.0", APIURL: "https://eruun.example.com"}}
	service, err := New(account.NewStore(raw), fake.NewSimpleClientset(), cfg)
	require.NoError(t, err)
	return service, raw, account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "member", UserID: "member"})
}

func commandRequest() SubmitRequest {
	return SubmitRequest{WorkspaceID: "space", JobSpec: spec.JobSpec{Name: "one command", Type: "command", Spec: json.RawMessage(`{"image":"busybox:1.37.0","command":["echo"],"args":["hello"]}`)}}
}

func TestSubmitCommandUsesWorkspaceQueueWithoutApplication(t *testing.T) {
	service, raw, ctx := testJobService(t)
	accepted, err := service.Submit(ctx, commandRequest())
	require.NoError(t, err)
	require.Len(t, accepted.TaskID, 24)
	require.Equal(t, config.StatusWaiting, accepted.Status)
	parent := &model.WorkflowQueue{TaskID: accepted.TaskID}
	require.NoError(t, raw.Get(ctx, parent))
	require.Empty(t, parent.AppID)
	require.Empty(t, parent.WorkflowID)
	require.Equal(t, config.WorkflowTaskTypeJob, parent.Type)
	require.Equal(t, "space", parent.WorkspaceID)
	require.Len(t, parent.JobToken, 64)
	encoded, err := json.Marshal(parent)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), parent.JobToken)
	count, err := raw.Count(ctx, &model.Applications{}, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	second, err := service.Submit(ctx, commandRequest())
	require.NoError(t, err)
	require.NotEqual(t, accepted.TaskID, second.TaskID)
	_, err = service.Get(account.WithScope(context.Background(), account.Scope{WorkspaceID: "other", Namespace: "other-ns", Role: "member"}), accepted.TaskID)
	require.Error(t, err)
	_, err = service.Submit(account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "viewer"}), commandRequest())
	require.ErrorIs(t, err, bcode.ErrForbidden)
	_, err = service.Submit(context.Background(), commandRequest())
	require.ErrorIs(t, err, bcode.ErrForbidden)
}

type runnerFixture struct {
	service  *Service
	raw      *sqlstore.Driver
	parent   *model.WorkflowQueue
	record   *model.JobInfo
	workload *batchv1.Job
	pod      *corev1.Pod
	identity RunnerIdentity
}

func newRunnerFixture(t *testing.T) *runnerFixture {
	t.Helper()
	service, raw, ctx := testJobService(t)
	dataset := &model.JobArtifact{ID: "11111111-1111-1111-1111-111111111111", WorkspaceID: "space", Kind: artifacts.KindDataset, Digest: strings.Repeat("a", 64)}
	require.NoError(t, raw.Add(ctx, dataset))
	accepted, err := service.Submit(ctx, SubmitRequest{WorkspaceID: "space", JobSpec: spec.JobSpec{Name: "evaluate", Type: "agent_evaluation", Spec: json.RawMessage(`{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"11111111-1111-1111-1111-111111111111","agent":{"name":"oracle"}}`)}})
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: accepted.TaskID}
	require.NoError(t, raw.Get(ctx, parent))
	parent.Status, parent.RunGeneration, parent.RunToken, parent.WorkerID = config.StatusRunning, 4, "replacement-lease", "replacement-worker"
	require.NoError(t, raw.Put(ctx, parent))
	task, err := BuildTask(ctx, service.Store, service.Config, parent, "space-ns")
	require.NoError(t, err)
	task.RunGeneration, task.OwnerRunGeneration, task.ExecutionKey = 2, 4, "original-execution"
	workflowjob.ApplyTaskIDAnnotation(task)
	workflowjob.ApplyExecutionIdentity(task)
	workload := task.JobInfo.(*batchv1.Job)
	workload.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	workload.UID = "original-job"
	checkpoint, err := json.Marshal(map[string]any{"kind": "instant_job_retry", "version": 1, "attempt": 1, "job": workload, "currentUID": workload.UID, "deadline": time.Now().Add(time.Hour).UnixNano()})
	require.NoError(t, err)
	record := &model.JobInfo{Type: string(config.JobAgentEvaluation), TaskID: parent.TaskID, WorkspaceID: "space", ServiceName: workload.Name, Status: string(config.StatusRunning), ExecutionKey: ptr.To(task.ExecutionKey), RunGeneration: 2, Attempt: 1, InternalInfo: string(checkpoint)}
	require.NoError(t, raw.Add(ctx, record))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: workload.Name + "-pod", Namespace: workload.Namespace, UID: "original-pod", Annotations: workload.Spec.Template.Annotations,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: workload.Name, UID: workload.UID, Controller: ptr.To(true)}}}, Spec: workload.Spec.Template.Spec}
	client := service.Kube.(*fake.Clientset)
	require.NoError(t, client.Tracker().Add(workload))
	require.NoError(t, client.Tracker().Add(pod))
	return &runnerFixture{service: service, raw: raw, parent: parent, record: record, workload: workload, pod: pod,
		identity: RunnerIdentity{TaskID: parent.TaskID, Token: parent.JobToken, PodName: pod.Name, PodUID: string(pod.UID)}}
}

func claimRunner(t *testing.T, f *runnerFixture) {
	t.Helper()
	ack, err := f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "claim"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), ack.AcceptedSequence)
	require.Equal(t, "continue", ack.Action)
	require.Empty(t, ack.StopOutcome)
	require.NoError(t, f.raw.Get(context.Background(), f.record))
}

func TestRunnerCapabilityAcceptsRecoveredPodAndRejectsSpoofedIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *runnerFixture)
	}{
		{"token", func(t *testing.T, f *runnerFixture) { f.identity.Token = strings.Repeat("0", 64) }},
		{"pod UID", func(t *testing.T, f *runnerFixture) { f.identity.PodUID = "different-pod" }},
		{"namespace", func(t *testing.T, f *runnerFixture) {
			f.pod.Namespace = "other-ns"
			require.NoError(t, f.service.Kube.(*fake.Clientset).Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "space-ns", f.pod.Name))
			require.NoError(t, f.service.Kube.(*fake.Clientset).Tracker().Add(f.pod))
		}},
		{"pod task", func(t *testing.T, f *runnerFixture) {
			f.pod.Annotations[config.AnnotationJobTaskID] = "different-task"
			_, err := f.service.Kube.CoreV1().Pods(f.pod.Namespace).Update(context.Background(), f.pod, metav1.UpdateOptions{})
			require.NoError(t, err)
		}},
		{"owner UID", func(t *testing.T, f *runnerFixture) {
			f.pod.OwnerReferences[0].UID = "different-job"
			_, err := f.service.Kube.CoreV1().Pods(f.pod.Namespace).Update(context.Background(), f.pod, metav1.UpdateOptions{})
			require.NoError(t, err)
		}},
		{"checkpoint UID", func(t *testing.T, f *runnerFixture) {
			var cp map[string]any
			require.NoError(t, json.Unmarshal([]byte(f.record.InternalInfo), &cp))
			cp["currentUID"] = "replaced-job"
			raw, err := json.Marshal(cp)
			require.NoError(t, err)
			f.record.InternalInfo = string(raw)
			require.NoError(t, f.raw.Put(context.Background(), f.record))
		}},
		{"malformed checkpoint", func(t *testing.T, f *runnerFixture) {
			f.record.InternalInfo = "{}"
			require.NoError(t, f.raw.Put(context.Background(), f.record))
		}},
		{"live Job generation", func(t *testing.T, f *runnerFixture) {
			f.workload.Annotations[config.AnnotationJobRunGeneration] = "3"
			_, err := f.service.Kube.BatchV1().Jobs(f.workload.Namespace).Update(context.Background(), f.workload, metav1.UpdateOptions{})
			require.NoError(t, err)
		}},
		{"attempt", func(t *testing.T, f *runnerFixture) {
			f.record.Attempt = 2
			require.NoError(t, f.raw.Put(context.Background(), f.record))
		}},
		{"parent recovered to waiting", func(t *testing.T, f *runnerFixture) {
			f.parent.Status = config.StatusWaiting
			f.parent.RunToken, f.parent.WorkerID, f.parent.LeaseExpiresAt = "", "", nil
			require.NoError(t, f.raw.Put(context.Background(), f.parent))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRunnerFixture(t)
			_, err := f.service.authorizeRunner(context.Background(), f.identity)
			require.NoError(t, err, "same Pod survives workflow lease generation replacement")
			tc.mutate(t, f)
			_, err = f.service.authorizeRunner(context.Background(), f.identity)
			require.ErrorIs(t, err, bcode.ErrUnauthorized)
		})
	}
}

func resultArchive(t *testing.T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gz)
	for name, data := range map[string]string{"result.json": `{"collectionComplete":true,"executionStatus":"succeeded"}`, "outputs/trial/trajectory.json": `[{"reward":1}]`} {
		require.NoError(t, tarWriter.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(data)), Typeflag: tar.TypeReg}))
		_, err := io.WriteString(tarWriter, data)
		require.NoError(t, err)
	}
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gz.Close())
	return buffer.Bytes()
}

type readHook struct {
	io.Reader
	hook func()
}

func (r *readHook) Read(p []byte) (int, error) {
	if r.hook != nil {
		hook := r.hook
		r.hook = nil
		hook()
	}
	return r.Reader.Read(p)
}

func TestRunnerResultPublicationFencesCheckpointAndAllowsCancellationUpload(t *testing.T) {
	t.Run("cancelled same Pod", func(t *testing.T) {
		f := newRunnerFixture(t)
		claimRunner(t, f)
		f.parent.Status = config.StatusCancelled
		lease := time.Now().Add(time.Minute)
		f.parent.LeaseExpiresAt = &lease
		require.NoError(t, f.raw.Put(context.Background(), f.parent))
		data := resultArchive(t)
		result, err := f.service.RunnerResult(context.Background(), f.identity, bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, artifacts.KindSource, result.Kind)
		require.Equal(t, f.parent.TaskID, result.TaskID)
		again, err := f.service.RunnerResult(context.Background(), f.identity, bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, result.ID, again.ID)

		complete := true
		terminal := RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 2, Kind: "terminal", Terminal: &RunnerTerminal{
			Outcome: "succeeded", ArtifactID: result.ID, ArtifactDigest: result.Digest, CollectionComplete: &complete, Reason: "evaluation_succeeded",
		}}
		_, err = f.service.RunnerEvent(context.Background(), f.identity, terminal)
		var stopConflict *RunnerStopConflictError
		require.ErrorAs(t, err, &stopConflict)
		require.Equal(t, "cancelled", stopConflict.Outcome)

		terminal.Terminal.Outcome = "cancelled"
		terminal.Terminal.Reason = "evaluation_cancelled"
		ack, err := f.service.RunnerEvent(context.Background(), f.identity, terminal)
		require.NoError(t, err)
		require.Equal(t, uint64(2), ack.AcceptedSequence)
		require.Equal(t, "stop", ack.Action)
		require.Equal(t, "cancelled", ack.StopOutcome)
	})
	t.Run("cancelled cleanup pending survives released workflow lease", func(t *testing.T) {
		f := newRunnerFixture(t)
		claimRunner(t, f)
		f.parent.Status = config.StatusCancelled
		f.parent.RunToken, f.parent.WorkerID, f.parent.LeaseExpiresAt = "", "", nil
		f.record.Status = string(config.StatusCancelled)
		f.record.SchedulingReason = "parent workflow cancelled"
		require.True(t, workflowjob.IsCancelledJobCleanupPending(f.record))
		require.NoError(t, f.raw.Put(context.Background(), f.parent))
		require.NoError(t, f.raw.Put(context.Background(), f.record))

		ack, err := f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 2, Kind: "heartbeat"})
		require.NoError(t, err)
		require.Equal(t, "stop", ack.Action)
		require.Equal(t, "cancelled", ack.StopOutcome)

		result, err := f.service.RunnerResult(context.Background(), f.identity, bytes.NewReader(resultArchive(t)))
		require.NoError(t, err)
		complete := true
		ack, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 3, Kind: "terminal", Terminal: &RunnerTerminal{
			Outcome: "cancelled", ArtifactID: result.ID, ArtifactDigest: result.Digest, CollectionComplete: &complete, Reason: "evaluation_cancelled",
		}})
		require.NoError(t, err)
		require.Equal(t, uint64(3), ack.AcceptedSequence)
		require.Equal(t, "stop", ack.Action)
		require.Equal(t, "cancelled", ack.StopOutcome)
	})
	t.Run("recovered terminal checkpoint", func(t *testing.T) {
		f := newRunnerFixture(t)
		claimRunner(t, f)
		f.parent.Status = config.StatusCancelled
		f.record.Status = string(config.StatusCancelled)
		require.NoError(t, f.raw.Put(context.Background(), f.parent))
		require.NoError(t, f.raw.Put(context.Background(), f.record))

		_, err := f.service.RunnerResult(context.Background(), f.identity, bytes.NewReader(resultArchive(t)))

		require.ErrorIs(t, err, bcode.ErrUnauthorized)
	})
	t.Run("checkpoint changes during transfer", func(t *testing.T) {
		f := newRunnerFixture(t)
		claimRunner(t, f)
		reader := &readHook{Reader: bytes.NewReader(resultArchive(t)), hook: func() {
			var checkpoint map[string]any
			require.NoError(t, json.Unmarshal([]byte(f.record.InternalInfo), &checkpoint))
			checkpoint["currentUID"] = "replacement-job"
			raw, err := json.Marshal(checkpoint)
			require.NoError(t, err)
			f.record.InternalInfo = string(raw)
			require.NoError(t, f.raw.Put(context.Background(), f.record))
		}}
		_, err := f.service.RunnerResult(context.Background(), f.identity, reader)
		require.ErrorIs(t, err, bcode.ErrUnauthorized)
		count, err := f.raw.Count(context.Background(), &model.JobArtifact{TaskID: f.parent.TaskID, Kind: artifacts.KindSource}, nil)
		require.NoError(t, err)
		require.Zero(t, count)
	})
	t.Run("checkpoint becomes terminal during transfer", func(t *testing.T) {
		f := newRunnerFixture(t)
		claimRunner(t, f)
		reader := &readHook{Reader: bytes.NewReader(resultArchive(t)), hook: func() {
			f.record.Status = string(config.StatusCancelled)
			require.NoError(t, f.raw.Put(context.Background(), f.record))
		}}
		_, err := f.service.RunnerResult(context.Background(), f.identity, reader)
		require.ErrorIs(t, err, bcode.ErrUnauthorized)
		count, err := f.raw.Count(context.Background(), &model.JobArtifact{TaskID: f.parent.TaskID, Kind: artifacts.KindSource}, nil)
		require.NoError(t, err)
		require.Zero(t, count)
	})
}

func TestRunnerEventsClaimSequenceProgressTerminalAndPublicStatus(t *testing.T) {
	f := newRunnerFixture(t)
	err := f.service.RunnerDataset(context.Background(), f.identity, io.Discard)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	claimRunner(t, f)

	phase := RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 2, Kind: "phase", Phase: "running"}
	ack, err := f.service.RunnerEvent(context.Background(), f.identity, phase)
	require.NoError(t, err)
	require.Equal(t, uint64(2), ack.AcceptedSequence)
	ack, err = f.service.RunnerEvent(context.Background(), f.identity, phase)
	require.NoError(t, err)
	require.Equal(t, uint64(2), ack.AcceptedSequence)
	ack, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "claim"})
	require.NoError(t, err)
	require.Equal(t, uint64(2), ack.AcceptedSequence)

	_, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 2, Kind: "phase", Phase: "finalizing"})
	require.ErrorIs(t, err, ErrRunnerConflict)
	_, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 3, Kind: "progress", Progress: &RunnerProgress{CompletedTrials: 2, TotalTrials: 4}})
	require.NoError(t, err)
	_, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 4, Kind: "progress", Progress: &RunnerProgress{CompletedTrials: 1, TotalTrials: 4}})
	require.ErrorIs(t, err, ErrRunnerConflict)
	_, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 5, Kind: "phase", Phase: "preparing"})
	require.ErrorIs(t, err, ErrRunnerConflict)
	_, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 6, Kind: "phase", Phase: "finalizing"})
	require.NoError(t, err)

	artifact, err := f.service.RunnerResult(context.Background(), f.identity, bytes.NewReader(resultArchive(t)))
	require.NoError(t, err)
	complete := true
	terminal := RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 7, Kind: "terminal", Terminal: &RunnerTerminal{
		Outcome: "succeeded", ArtifactID: artifact.ID, ArtifactDigest: artifact.Digest, CollectionComplete: &complete, Reason: "evaluation_succeeded",
	}}
	ack, err = f.service.RunnerEvent(context.Background(), f.identity, terminal)
	require.NoError(t, err)
	require.Equal(t, uint64(7), ack.AcceptedSequence)
	ack, err = f.service.RunnerEvent(context.Background(), f.identity, terminal)
	require.NoError(t, err)
	require.Equal(t, uint64(7), ack.AcceptedSequence)
	conflictingTerminal := terminal
	conflictingTerminal.Sequence = 8
	conflictingTerminal.Terminal = &RunnerTerminal{
		Outcome: "failed", ArtifactID: artifact.ID, ArtifactDigest: artifact.Digest, CollectionComplete: &complete, Reason: "evaluation_failed",
	}
	_, err = f.service.RunnerEvent(context.Background(), f.identity, conflictingTerminal)
	require.ErrorIs(t, err, ErrRunnerConflict)
	_, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 9, Kind: "heartbeat"})
	require.ErrorIs(t, err, ErrRunnerConflict)

	ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "viewer"})
	detail, err := f.service.Get(ctx, f.parent.TaskID)
	require.NoError(t, err)
	require.NotNil(t, detail.RunnerStatus)
	require.Equal(t, "finalizing", detail.RunnerStatus.Phase)
	require.Equal(t, uint64(7), detail.RunnerStatus.Sequence)
	require.False(t, detail.RunnerStatus.Stale)
	require.Equal(t, "succeeded", detail.RunnerStatus.Terminal.Outcome)
	encoded, err := json.Marshal(detail.RunnerStatus)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), f.identity.Token)
	require.NotContains(t, string(encoded), f.identity.PodUID)
	require.NotContains(t, string(encoded), string(f.workload.UID))
}

func TestRunnerSucceededTerminalRequiresSucceededArtifact(t *testing.T) {
	f := newRunnerFixture(t)
	claimRunner(t, f)
	artifact, err := f.service.RunnerResult(context.Background(), f.identity, bytes.NewReader(resultArchive(t)))
	require.NoError(t, err)
	artifact.Summary = []byte(`{"collectionComplete":true,"executionStatus":"failed"}`)
	require.NoError(t, f.raw.Put(context.Background(), artifact))

	complete := true
	_, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 2, Kind: "terminal", Terminal: &RunnerTerminal{
		Outcome: "succeeded", ArtifactID: artifact.ID, ArtifactDigest: artifact.Digest, CollectionComplete: &complete, Reason: "evaluation_succeeded",
	}})
	require.ErrorIs(t, err, ErrRunnerConflict)
}

func TestRunnerEventsConcurrentClaimHasOneOwner(t *testing.T) {
	f := newRunnerFixture(t)
	replacement := f.pod.DeepCopy()
	replacement.Name = f.pod.Name + "-replacement"
	replacement.UID = "replacement-pod"
	require.NoError(t, f.service.Kube.(*fake.Clientset).Tracker().Add(replacement))
	replacementIdentity := f.identity
	replacementIdentity.PodName, replacementIdentity.PodUID = replacement.Name, string(replacement.UID)

	identities := []RunnerIdentity{f.identity, replacementIdentity}
	errorsSeen := make([]error, len(identities))
	var wait sync.WaitGroup
	for index := range identities {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, errorsSeen[index] = f.service.RunnerEvent(context.Background(), identities[index], RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "claim"})
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

func TestRunnerEventsStopAndStaleUseDatabaseReceiveTime(t *testing.T) {
	f := newRunnerFixture(t)
	claimRunner(t, f)
	state, _, err := decodeRunnerState(f.record)
	require.NoError(t, err)
	state.ClaimedAt = time.Now().Add(-RunnerStaleAfter - time.Second)
	state.LastReceivedAt = state.ClaimedAt
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, workflowjob.SetEvaluationRunnerCheckpoint(f.record, raw))
	require.NoError(t, f.raw.Put(context.Background(), f.record))
	ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "viewer"})
	detail, err := f.service.Get(ctx, f.parent.TaskID)
	require.NoError(t, err)
	require.True(t, detail.RunnerStatus.Stale)
	_, err = f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 2, Kind: "phase", Phase: "running"})
	require.NoError(t, err)
	detail, err = f.service.Get(ctx, f.parent.TaskID)
	require.NoError(t, err)
	require.False(t, detail.RunnerStatus.Stale)
	require.Nil(t, detail.RunnerStatus.LastHeartbeatAt)

	f.parent.Status = config.StatusCancelled
	lease := time.Now().Add(time.Minute)
	f.parent.LeaseExpiresAt = &lease
	require.NoError(t, f.raw.Put(context.Background(), f.parent))
	ack, err := f.service.RunnerEvent(context.Background(), f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 3, Kind: "heartbeat"})
	require.NoError(t, err)
	require.Equal(t, "stop", ack.Action)
	require.Equal(t, "cancelled", ack.StopOutcome)

	deadlineFixture := newRunnerFixture(t)
	var checkpoint map[string]any
	require.NoError(t, json.Unmarshal([]byte(deadlineFixture.record.InternalInfo), &checkpoint))
	checkpoint["deadline"] = time.Now().Add(-time.Second).UnixNano()
	checkpointJSON, err := json.Marshal(checkpoint)
	require.NoError(t, err)
	deadlineFixture.record.InternalInfo = string(checkpointJSON)
	require.NoError(t, deadlineFixture.raw.Put(context.Background(), deadlineFixture.record))
	ack, err = deadlineFixture.service.RunnerEvent(context.Background(), deadlineFixture.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "claim"})
	require.NoError(t, err)
	require.Equal(t, "stop", ack.Action)
	require.Equal(t, "timed_out", ack.StopOutcome)
	artifact, err := deadlineFixture.service.RunnerResult(context.Background(), deadlineFixture.identity, bytes.NewReader(resultArchive(t)))
	require.NoError(t, err)
	complete := true
	terminal := RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 2, Kind: "terminal", Terminal: &RunnerTerminal{
		Outcome: "succeeded", ArtifactID: artifact.ID, ArtifactDigest: artifact.Digest, CollectionComplete: &complete, Reason: "evaluation_succeeded",
	}}
	_, err = deadlineFixture.service.RunnerEvent(context.Background(), deadlineFixture.identity, terminal)
	var stopConflict *RunnerStopConflictError
	require.ErrorAs(t, err, &stopConflict)
	require.Equal(t, "timed_out", stopConflict.Outcome)
	terminal.Terminal.Outcome = "cancelled"
	terminal.Terminal.Reason = "evaluation_cancelled"
	_, err = deadlineFixture.service.RunnerEvent(context.Background(), deadlineFixture.identity, terminal)
	require.ErrorAs(t, err, &stopConflict)
	require.Equal(t, "timed_out", stopConflict.Outcome)
	terminal.Terminal.Outcome = "timed_out"
	terminal.Terminal.Reason = "evaluation_timed_out"
	ack, err = deadlineFixture.service.RunnerEvent(context.Background(), deadlineFixture.identity, terminal)
	require.NoError(t, err)
	require.Equal(t, uint64(2), ack.AcceptedSequence)
	require.Equal(t, "timed_out", ack.StopOutcome)
}

func TestGetQueuedEvaluationOmitsRunnerStatusUntilRetryCheckpointExists(t *testing.T) {
	f := newRunnerFixture(t)
	f.record.Status = string(config.StatusWaiting)
	f.record.SchedulingState = workflowconfig.JobSchedulingQueued
	f.record.InternalInfo = ""
	require.NoError(t, f.raw.Put(context.Background(), f.record))
	ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "viewer"})

	detail, err := f.service.Get(ctx, f.parent.TaskID)
	require.NoError(t, err)
	require.Nil(t, detail.RunnerStatus)

	f.record.InternalInfo = "{"
	require.NoError(t, f.raw.Put(context.Background(), f.record))
	_, err = f.service.Get(ctx, f.parent.TaskID)
	require.ErrorContains(t, err, "decode instant Job retry checkpoint")
}

func TestRunnerEventValidation(t *testing.T) {
	complete := true
	validTerminal := &RunnerTerminal{Outcome: "succeeded", ArtifactID: strings.Repeat("a", 64), ArtifactDigest: strings.Repeat("b", 64), CollectionComplete: &complete}
	for _, event := range []RunnerEvent{
		{},
		{ProtocolVersion: "v2", Sequence: 1, Kind: "claim"},
		{ProtocolVersion: RunnerProtocolVersion, Sequence: 0, Kind: "claim"},
		{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "unknown"},
		{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "phase", Phase: "done"},
		{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "progress", Progress: &RunnerProgress{CompletedTrials: 2, TotalTrials: 1}},
		{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "terminal", Terminal: &RunnerTerminal{Outcome: "succeeded"}},
		{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "terminal", Terminal: func() *RunnerTerminal {
			value := *validTerminal
			value.Message = strings.Repeat("x", 513)
			return &value
		}()},
	} {
		require.Error(t, event.validate())
	}
	require.NoError(t, (RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 1, Kind: "terminal", Terminal: validTerminal}).validate())
}

func TestCommandRuntimePersistsCheckpointAndTerminalResultThroughScopedStore(t *testing.T) {
	service, raw, ctx := testJobService(t)
	accepted, err := service.Submit(ctx, commandRequest())
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: accepted.TaskID}
	require.NoError(t, raw.Get(ctx, parent))
	parent.Status, parent.RunGeneration, parent.RunToken, parent.WorkerID = config.StatusRunning, 1, "lease", "worker"
	require.NoError(t, raw.Put(ctx, parent))
	task, err := BuildTask(ctx, service.Store, service.Config, parent, "space-ns")
	require.NoError(t, err)
	task.RunGeneration, task.OwnerRunGeneration, task.OwnerStatus = 1, 1, config.StatusRunning
	task.RunToken, task.WorkerID, task.ExecutionKey = parent.RunToken, parent.WorkerID, "command-execution"
	workflowjob.ApplyTaskIDAnnotation(task)
	workflowjob.ApplyExecutionIdentity(task)
	client := service.Kube.(*fake.Clientset)
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		live := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		live.UID = "executed-job"
		live.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		require.NoError(t, client.Tracker().Add(live))
		return true, live, nil
	})
	ctl := workflowjob.NewInstantJobCtl(task, service.Kube, service.Store, func() {})
	require.NoError(t, ctl.Run(ctx))
	require.Equal(t, config.StatusCompleted, task.Status)
	require.NoError(t, ctl.SaveInfo(ctx))
	// Generic TaskID-only readers power existing cancellation and task queries.
	records, err := service.Store.List(ctx, &model.JobInfo{TaskID: parent.TaskID}, nil)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0].(*model.JobInfo)
	require.Equal(t, string(config.JobCommand), record.Type)
	require.Equal(t, string(config.StatusCompleted), record.Status)
	require.True(t, workflowjob.HasInstantJobRetryCheckpoint(record))
	_, err = client.BatchV1().Jobs(task.Namespace).Get(ctx, task.Name, metav1.GetOptions{})
	require.Error(t, err, "terminal state must be committed before cleanup")
}

package jobs

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func checkpointFixture(t *testing.T, trials ...string) (*runnerFixture, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	f, client := sandboxFixture(t, false)
	require.NoError(t, f.raw.Client.AutoMigrate(&model.JobCheckpoint{}))
	info, err := decodeEvaluationInfo(f.record.EvaluationInfo)
	require.NoError(t, err)
	info.Traits.Evaluation.Agent, info.Traits.Evaluation.Model = "codex", "gpt-5"
	info.Traits.Evaluation.Concurrency = 4
	info.Traits.Evaluation.Recovery = &spec.EvaluationRecoverySpec{AgentVersion: spec.CodexRecoveryVersion, ReplaySafe: true}
	encoded, err := json.Marshal(info)
	require.NoError(t, err)
	f.record.EvaluationInfo = string(encoded)
	require.NoError(t, f.raw.Put(context.Background(), f.record))
	claimRunner(t, f)
	client.PrependReactor("create", "checkpoints", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		object.SetUID(types.UID(uuid.NewString()))
		object.SetGeneration(1)
		err := client.Tracker().Create(CheckpointGVR, object, object.GetNamespace())
		return true, object, err
	})
	for _, trial := range trials {
		_, err := f.service.RunnerSandboxCreate(context.Background(), f.identity, sandboxRequest(trial))
		require.NoError(t, err)
		makeSandboxReady(t, f, client, trial)
		response, err := f.service.RunnerSandboxGet(context.Background(), f.identity, trial)
		require.NoError(t, err)
		require.Equal(t, sandboxReady, response.State)
	}
	return f, client
}

func checkpointArchive(t *testing.T, trials ...string) []byte {
	t.Helper()
	manifest := CheckpointManifest{Version: 1, HarborVersion: spec.HarborVersion, Agent: "codex", AgentVersion: spec.CodexRecoveryVersion, DatasetDigest: strings.Repeat("a", 64), RemainingSeconds: 600}
	for _, trial := range trials {
		manifest.Members = append(manifest.Members, CheckpointTrial{TrialID: trial, SessionID: "session-" + trial, Stage: "agent"})
	}
	data, err := json.Marshal(manifest)
	require.NoError(t, err)
	var output bytes.Buffer
	gz := gzip.NewWriter(&output)
	tw := tar.NewWriter(gz)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "checkpoint.json", Mode: 0600, Size: int64(len(data))}))
	_, err = tw.Write(data)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return output.Bytes()
}

func checkpointRow(t *testing.T, f *runnerFixture, id string) *model.JobCheckpoint {
	t.Helper()
	row := &model.JobCheckpoint{ID: id}
	require.NoError(t, f.raw.Get(context.Background(), row))
	return row
}

func setSnapshotPhase(t *testing.T, f *runnerFixture, client *dynamicfake.FakeDynamicClient, id, trial, phase string) {
	t.Helper()
	row := checkpointRow(t, f, id)
	members, err := checkpointMembers(row)
	require.NoError(t, err)
	for _, member := range members {
		if member.TrialID != trial {
			continue
		}
		object, err := client.Resource(CheckpointGVR).Namespace(row.Namespace).Get(context.Background(), member.SnapshotName, metav1.GetOptions{})
		require.NoError(t, err)
		object.Object["status"] = map[string]interface{}{"phase": phase, "checkpointId": "snapshot-" + trial}
		require.NoError(t, client.Tracker().Update(CheckpointGVR, object, row.Namespace))
		return
	}
	t.Fatal("checkpoint member missing")
}

func TestCheckpointRequiresWholeSnapshotSetAndImmutableMaterial(t *testing.T) {
	f, client := checkpointFixture(t, "a", "b")
	ctx := context.Background()
	data := checkpointArchive(t, "a", "b")
	response, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, sandboxPending, response.State, response.Reason)
	_, err = f.service.RunnerCheckpointPut(ctx, f.identity, "parallel", bytes.NewReader(data))
	require.ErrorIs(t, err, ErrRunnerConflict)
	_, err = f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
	require.ErrorIs(t, err, ErrRunnerConflict)
	setSnapshotPhase(t, f, client, "point", "a", "Succeeded")
	response, err = f.service.RunnerCheckpointGet(ctx, f.identity, "point")
	require.NoError(t, err)
	require.Equal(t, sandboxPending, response.State)
	setSnapshotPhase(t, f, client, "point", "b", "Succeeded")
	response, err = f.service.RunnerCheckpointGet(ctx, f.identity, "point")
	require.NoError(t, err)
	require.Equal(t, sandboxReady, response.State)
	_, err = f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(data))
	require.NoError(t, err)
	creates := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource() == CheckpointGVR {
			creates++
		}
	}
	require.Equal(t, 2, creates)
	row := checkpointRow(t, f, "point")
	artifact, err := f.service.Artifacts.Get(ctx, row.WorkspaceID, row.MaterialID)
	require.NoError(t, err)
	require.Equal(t, artifacts.KindCheckpoint, artifact.Kind)
	require.NotEmpty(t, artifact.Digest)
	results, err := f.service.Artifacts.List(ctx, row.WorkspaceID, artifacts.KindSource, row.TaskID, row.ExecutionKey)
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestCheckpointMaterialRejectsCorruptionAndStaleOwner(t *testing.T) {
	t.Run("corrupt gzip", func(t *testing.T) {
		f, _ := checkpointFixture(t, "a")
		data := checkpointArchive(t, "a")
		data[len(data)-5] ^= 1
		_, err := f.service.RunnerCheckpointPut(context.Background(), f.identity, "point", bytes.NewReader(data))
		require.ErrorIs(t, err, artifacts.ErrInvalidArchive)
		count, err := f.raw.Count(context.Background(), &model.JobCheckpoint{}, nil)
		require.NoError(t, err)
		require.Zero(t, count)
	})
	t.Run("stale owner after upload", func(t *testing.T) {
		f, _ := checkpointFixture(t, "a")
		reader := &readHook{Reader: bytes.NewReader(checkpointArchive(t, "a")), hook: func() {
			require.NoError(t, f.raw.Get(context.Background(), f.record))
			f.record.Status = string(config.StatusFailed)
			require.NoError(t, f.raw.Put(context.Background(), f.record))
		}}
		_, err := f.service.RunnerCheckpointPut(context.Background(), f.identity, "point", reader)
		require.Error(t, err)
		count, err := f.raw.Count(context.Background(), &model.JobCheckpoint{}, nil)
		require.NoError(t, err)
		require.Zero(t, count)
	})
}

func TestCheckpointCannotCommitChangedSourceOrFailedSnapshot(t *testing.T) {
	for _, scenario := range []string{"changed pod", "failed snapshot", "changed snapshot UID"} {
		t.Run(scenario, func(t *testing.T) {
			f, client := checkpointFixture(t, "a")
			ctx := context.Background()
			_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
			require.NoError(t, err)
			setSnapshotPhase(t, f, client, "point", "a", "Succeeded")
			switch scenario {
			case "changed pod":
				row := sandboxRow(t, f, "a")
				pod, err := f.service.Kube.CoreV1().Pods(row.Namespace).Get(ctx, row.PodName, metav1.GetOptions{})
				require.NoError(t, err)
				pod.UID = "replacement"
				_, err = f.service.Kube.CoreV1().Pods(row.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "failed snapshot":
				setSnapshotPhase(t, f, client, "point", "a", "Failed")
			case "changed snapshot UID":
				point := checkpointRow(t, f, "point")
				members, err := checkpointMembers(point)
				require.NoError(t, err)
				object, err := client.Resource(CheckpointGVR).Namespace(point.Namespace).Get(ctx, members[0].SnapshotName, metav1.GetOptions{})
				require.NoError(t, err)
				object.SetUID("replacement")
				require.NoError(t, client.Tracker().Update(CheckpointGVR, object, point.Namespace))
			}
			response, err := f.service.RunnerCheckpointGet(ctx, f.identity, "point")
			require.NoError(t, err)
			require.Equal(t, sandboxFailed, response.State)
		})
	}
}

func TestCheckpointTimeoutDoesNotDeleteRunningSnapshotAndProtectsSource(t *testing.T) {
	f, client := checkpointFixture(t, "a")
	ctx := context.Background()
	_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
	require.NoError(t, err)
	setSnapshotPhase(t, f, client, "point", "a", "Running")
	point := checkpointRow(t, f, "point")
	_, err = f.raw.CompareAndSwap(ctx, point, "id", point.ID, map[string]interface{}{"create_time": time.Now().Add(-11 * time.Minute)})
	require.NoError(t, err)
	response, err := f.service.RunnerCheckpointGet(ctx, f.identity, "point")
	require.NoError(t, err)
	require.Equal(t, sandboxFailed, response.State)
	require.NoError(t, f.service.maintainCheckpoint(ctx, checkpointRow(t, f, "point")))
	for _, action := range client.Actions() {
		require.False(t, action.GetVerb() == "delete" && action.GetResource() == CheckpointGVR)
	}
	active, err := f.service.sandboxCheckpointActive(ctx, sandboxRow(t, f, "a"))
	require.NoError(t, err)
	require.True(t, active)
	_, err = f.service.RunnerCheckpointPut(ctx, f.identity, "next", bytes.NewReader(checkpointArchive(t, "a")))
	require.ErrorIs(t, err, ErrRunnerConflict)
	setSnapshotPhase(t, f, client, "point", "a", "Failed")
	require.NoError(t, f.service.maintainCheckpoint(ctx, checkpointRow(t, f, "point")))
	require.True(t, checkpointRow(t, f, "point").Cleaned)
	require.ErrorIs(t, f.raw.Get(ctx, &model.JobArtifact{ID: point.MaterialID}), datastore.ErrRecordNotExist)
}

func TestCheckpointCreateResponseLossUsesSameOwnedSnapshot(t *testing.T) {
	f, client := checkpointFixture(t, "a")
	client.PrependReactor("create", "checkpoints", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		object.SetUID("cloud-created")
		require.NoError(t, client.Tracker().Create(CheckpointGVR, object, object.GetNamespace()))
		return true, nil, errors.New("response lost")
	})
	response, err := f.service.RunnerCheckpointPut(context.Background(), f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
	require.NoError(t, err)
	require.Equal(t, sandboxPending, response.State)
	members, err := checkpointMembers(checkpointRow(t, f, "point"))
	require.NoError(t, err)
	require.Equal(t, "cloud-created", members[0].SnapshotUID)
}

func TestCheckpointRejectsOmittedWriterAndBlocksAdmissionDuringBarrier(t *testing.T) {
	f, _ := checkpointFixture(t, "a", "b")
	ctx := context.Background()
	_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "partial", bytes.NewReader(checkpointArchive(t, "a")))
	require.ErrorIs(t, err, ErrRunnerConflict)
	_, err = f.service.RunnerCheckpointPut(ctx, f.identity, "whole", bytes.NewReader(checkpointArchive(t, "a", "b")))
	require.NoError(t, err)
	_, err = f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("new-trial"))
	require.ErrorIs(t, err, ErrRunnerConflict)
	complete := true
	_, err = f.service.RunnerSandboxRelease(ctx, f.identity, "a", SandboxReleaseRequest{CollectionComplete: &complete})
	require.ErrorIs(t, err, ErrRunnerConflict)
}

func TestCheckpointArtifactFailureRollsBackPointAndMaterial(t *testing.T) {
	f, _ := checkpointFixture(t, "a")
	require.NoError(t, f.raw.Client.Exec("CREATE TRIGGER reject_checkpoint_chunk BEFORE INSERT ON eruun_artifact_chunk BEGIN SELECT RAISE(ABORT, 'injected chunk failure'); END").Error)
	_, err := f.service.RunnerCheckpointPut(context.Background(), f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
	require.ErrorContains(t, err, "injected chunk failure")
	for _, entity := range []datastore.Entity{&model.JobCheckpoint{}, &model.JobArtifact{Kind: artifacts.KindCheckpoint}, &model.ArtifactChunk{}} {
		count, err := f.raw.Count(context.Background(), entity, nil)
		require.NoError(t, err)
		require.Zero(t, count)
	}
}

func TestCheckpointClonePreservesTemplateAndRejectsUnisolatedExecution(t *testing.T) {
	f, client := checkpointFixture(t, "a")
	ctx := context.Background()
	_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
	require.NoError(t, err)
	setSnapshotPhase(t, f, client, "point", "a", "Succeeded")
	response, err := f.service.RunnerCheckpointGet(ctx, f.identity, "point")
	require.NoError(t, err)
	require.Equal(t, sandboxReady, response.State)
	point := checkpointRow(t, f, "point")
	_, err = f.raw.CompareAndSwap(ctx, point, "id", point.ID, map[string]interface{}{"referenced_by_execution_key": "new-execution"})
	require.NoError(t, err)
	auth, err := f.service.authorizeRunner(ctx, f.identity)
	require.NoError(t, err)
	auth.evaluation.ResumeCheckpointID = point.ID
	original := sandboxRow(t, f, "a")
	clone := *original
	clone.ID, clone.ExecutionKey, clone.SandboxName, clone.RunnerUID = "1234567890123456789012345678901234567890123456789012345678901234", "new-execution", "new-sandbox", "new-runner"
	desired, err := buildSandbox(&clone, auth, time.Now())
	require.NoError(t, err)
	_, err = f.service.checkpointRestore(ctx, auth, &clone, desired)
	require.Error(t, err)
	auth.evaluation.RecoveryIsolated = true
	restored, err := f.service.checkpointRestore(ctx, auth, &clone, desired)
	require.NoError(t, err)
	require.True(t, restored)
	source, err := client.Resource(SandboxGVR).Namespace(original.Namespace).Get(ctx, original.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	originalSpec, _, err := unstructured.NestedMap(source.Object, "spec", "template", "spec")
	require.NoError(t, err)
	cloneSpec, _, err := unstructured.NestedMap(desired.Object, "spec", "template", "spec")
	require.NoError(t, err)
	require.Equal(t, originalSpec, cloneSpec)
	annotations, _, err := unstructured.NestedStringMap(desired.Object, "spec", "template", "metadata", "annotations")
	require.NoError(t, err)
	require.Equal(t, "snapshot-a", annotations[checkpointRestoreAnnotation])
	require.Equal(t, "new-sandbox", desired.GetName())
	clone.StorageMiB++
	_, err = f.service.checkpointRestore(ctx, auth, &clone, desired)
	require.ErrorIs(t, err, ErrRunnerConflict)
}

func TestCheckpointRefPreventsExpiryAndTerminalExecutionReleasesIt(t *testing.T) {
	f, client := checkpointFixture(t, "a")
	ctx := context.Background()
	_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
	require.NoError(t, err)
	setSnapshotPhase(t, f, client, "point", "a", "Succeeded")
	_, err = f.service.RunnerCheckpointGet(ctx, f.identity, "point")
	require.NoError(t, err)
	point := checkpointRow(t, f, "point")
	key := "new-execution"
	reference := &model.JobInfo{WorkspaceID: point.WorkspaceID, TaskID: point.TaskID, ExecutionKey: &key, Status: string(config.StatusDistributed)}
	require.NoError(t, f.raw.Add(ctx, reference))
	_, err = f.raw.CompareAndSwap(ctx, point, "id", point.ID, map[string]interface{}{"referenced_by_execution_key": key, "expires_at": time.Now().Add(-time.Minute)})
	require.NoError(t, err)
	f.record.Status = string(config.StatusFailed)
	require.NoError(t, f.raw.Put(ctx, f.record))
	require.NoError(t, f.service.maintainCheckpoint(ctx, checkpointRow(t, f, "point")))
	require.False(t, checkpointRow(t, f, "point").Cleaned)
	reference.Status = string(config.StatusCompleted)
	require.NoError(t, f.raw.Put(ctx, reference))
	require.NoError(t, f.service.maintainCheckpoint(ctx, checkpointRow(t, f, "point")))
	require.True(t, checkpointRow(t, f, "point").Cleaned)
	require.Empty(t, checkpointRow(t, f, "point").ReferencedByExecutionKey)
}

func TestCheckpointParentCancellationReleasesUndispatchedReference(t *testing.T) {
	f, client := checkpointFixture(t, "a")
	ctx := context.Background()
	_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
	require.NoError(t, err)
	setSnapshotPhase(t, f, client, "point", "a", "Succeeded")
	_, err = f.service.RunnerCheckpointGet(ctx, f.identity, "point")
	require.NoError(t, err)
	point := checkpointRow(t, f, "point")
	key := "undispatched-execution"
	reference := &model.JobInfo{WorkspaceID: point.WorkspaceID, TaskID: point.TaskID, ExecutionKey: &key, Status: string(config.StatusDistributed)}
	require.NoError(t, f.raw.Add(ctx, reference))
	_, err = f.raw.CompareAndSwap(ctx, point, "id", point.ID, map[string]interface{}{"referenced_by_execution_key": key})
	require.NoError(t, err)
	f.parent.Status = config.StatusCancelled
	require.NoError(t, f.raw.Put(ctx, f.parent))
	require.NoError(t, f.service.maintainCheckpoint(ctx, checkpointRow(t, f, "point")))
	require.True(t, checkpointRow(t, f, "point").Cleaned)
	require.Empty(t, checkpointRow(t, f, "point").ReferencedByExecutionKey)
}

func TestCheckpointMaterialIntegrityIsCheckedBeforeReady(t *testing.T) {
	f, client := checkpointFixture(t, "a")
	ctx := context.Background()
	_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
	require.NoError(t, err)
	point := checkpointRow(t, f, "point")
	chunk := &model.ArtifactChunk{ID: point.MaterialID + "-000000"}
	_, err = f.raw.CompareAndSwap(ctx, chunk, "id", chunk.ID, map[string]interface{}{"data": []byte("corrupt")})
	require.NoError(t, err)
	setSnapshotPhase(t, f, client, "point", "a", "Succeeded")
	_, err = f.service.RunnerCheckpointGet(ctx, f.identity, "point")
	require.ErrorContains(t, err, "archive integrity mismatch")
	require.Equal(t, sandboxPending, checkpointRow(t, f, "point").State)
}

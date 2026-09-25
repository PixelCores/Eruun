package jobs

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

func checkpointFixture(t *testing.T, trials ...string) (*runnerFixture, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	f, client := sandboxFixture(t, false)
	require.NoError(t, f.raw.Client.AutoMigrate(&model.JobCheckpoint{}))
	info, err := decodeEvaluationInfo(f.record.EvaluationInfo)
	require.NoError(t, err)
	info.Traits.Evaluation.Agent, info.Traits.Evaluation.Model = "codex", "gpt-5"
	info.Traits.Evaluation.Concurrency = max(4, len(trials))
	info.Traits.Evaluation.Recovery = &spec.EvaluationRecoverySpec{AgentVersion: spec.CodexRecoveryVersion, ReplaySafe: true}
	encoded, err := json.Marshal(info)
	require.NoError(t, err)
	f.record.EvaluationInfo = string(encoded)
	require.NoError(t, f.raw.Put(context.Background(), f.record))
	if len(trials) > 4 {
		setting := &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler}
		require.NoError(t, f.raw.Get(context.Background(), setting))
		var policy map[string]interface{}
		require.NoError(t, json.Unmarshal(setting.Value, &policy))
		policy["resourceCreationBurst"] = len(trials) * 2
		setting.Value, err = json.Marshal(policy)
		require.NoError(t, err)
		require.NoError(t, f.raw.Put(context.Background(), setting))
	}
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

func TestMissingCheckpointCRKeepsSourceUntilRetentionBound(t *testing.T) {
	for _, knownUID := range []bool{false, true} {
		name := "create outcome unknown"
		if knownUID {
			name = "running CR disappeared"
		}
		t.Run(name, func(t *testing.T) {
			f, client := checkpointFixture(t, "a")
			ctx := context.Background()
			_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a")))
			require.NoError(t, err)
			point := checkpointRow(t, f, "point")
			members, err := checkpointMembers(point)
			require.NoError(t, err)
			require.Len(t, members, 1)
			require.True(t, members[0].CreateRequested)
			setSnapshotPhase(t, f, client, "point", "a", "Running")
			require.NoError(t, client.Resource(CheckpointGVR).Namespace(point.Namespace).Delete(ctx, members[0].SnapshotName, metav1.DeleteOptions{}))
			if !knownUID {
				members[0].SnapshotUID = ""
				raw, err := json.Marshal(members)
				require.NoError(t, err)
				_, err = f.raw.CompareAndSwap(ctx, point, "id", point.ID, map[string]interface{}{"members": raw})
				require.NoError(t, err)
			}
			_, err = f.raw.CompareAndSwap(ctx, point, "id", point.ID, map[string]interface{}{
				"create_time": time.Now().Add(-11 * time.Minute), "lease_until": time.Now().Add(-time.Second),
			})
			require.NoError(t, err)
			require.NoError(t, f.service.maintainCheckpoint(ctx, checkpointRow(t, f, "point")))
			require.False(t, checkpointRow(t, f, "point").Cleaned)

			source := sandboxRow(t, f, "a")
			source.ReleaseRequested = true
			require.NoError(t, f.raw.Put(ctx, source))
			require.NoError(t, f.service.maintainSandbox(ctx, source))
			_, err = client.Resource(SandboxGVR).Namespace(source.Namespace).Get(ctx, source.SandboxName, metav1.GetOptions{})
			require.NoError(t, err, "source Sandbox must survive an unresolved snapshot")
			require.Equal(t, "checkpoint_running", sandboxRow(t, f, "a").Reason)

			// The existing absolute retention bound still permits eventual cleanup.
			expired := time.Now().Add(-sandboxRetention - time.Minute)
			_, err = f.raw.CompareAndSwap(ctx, point, "id", point.ID, map[string]interface{}{"source_deadline": expired})
			require.NoError(t, err)
			require.NoError(t, f.service.maintainCheckpoint(ctx, checkpointRow(t, f, "point")))
			require.True(t, checkpointRow(t, f, "point").Cleaned)
			require.NoError(t, f.service.maintainSandbox(ctx, sandboxRow(t, f, "a")))
			_, err = client.Resource(SandboxGVR).Namespace(source.Namespace).Get(ctx, source.SandboxName, metav1.GetOptions{})
			require.True(t, k8serrors.IsNotFound(err), "source Sandbox can be released after the retention bound")
		})
	}
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

func TestCheckpointSlowReadsCompleteWholeSet(t *testing.T) {
	trials := strings.Fields("a b c d e f g h i j k l m n o p")
	for _, cancelled := range []bool{false, true} {
		name := "complete"
		if cancelled {
			name = "caller deadline"
		}
		t.Run(name, func(t *testing.T) {
			f, client := checkpointFixture(t, trials...)
			ctx := context.Background()
			_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, trials...)))
			require.NoError(t, err)
			for _, trial := range trials {
				setSnapshotPhase(t, f, client, "point", trial, "Succeeded")
			}
			var running, peak, reads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				active := running.Add(1)
				defer running.Add(-1)
				for previous := peak.Load(); active > previous && !peak.CompareAndSwap(previous, active); previous = peak.Load() {
				}
				// A serial pass needs 16 * 2 * 340ms = 10.88s, longer than
				// the operation window. Individual API calls remain healthy.
				select {
				case <-time.After(340 * time.Millisecond):
				case <-r.Context().Done():
					return
				}
				parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
				if r.Method != http.MethodGet || len(parts) != 7 {
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				resource := SandboxGVR
				resource.Resource = parts[5]
				object, err := client.Resource(resource).Namespace(parts[4]).Get(r.Context(), parts[6], metav1.GetOptions{})
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				reads.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(object); err != nil && r.Context().Err() == nil {
					t.Errorf("write checkpoint observation: %v", err)
				}
			}))
			defer server.Close()
			f.service.SandboxClient, err = dynamic.NewForConfig(&rest.Config{Host: server.URL, QPS: 100, Burst: 100})
			require.NoError(t, err)
			if cancelled {
				limited, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				_, err = f.service.RunnerCheckpointGet(limited, f.identity, "point")
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, sandboxPending, checkpointRow(t, f, "point").State)
				require.Eventually(t, func() bool { return running.Load() == 0 }, time.Second, time.Millisecond)
				// A later poll after lease expiry still rechecks the complete set.
				_, err = f.raw.CompareAndSwap(ctx, &model.JobCheckpoint{ID: "point"}, "id", "point", map[string]interface{}{"lease_until": time.Now().Add(-time.Second)})
				require.NoError(t, err)
			}
			response, err := f.service.RunnerCheckpointGet(ctx, f.identity, "point")
			require.NoError(t, err)
			require.Equal(t, sandboxReady, response.State)
			require.Equal(t, int32(32), reads.Load(), "every source and snapshot must be reread")
			require.Greater(t, peak.Load(), int32(1))
			require.LessOrEqual(t, peak.Load(), int32(8))
		})
	}
}

func TestCheckpointRevalidatesPreviouslySuccessfulMember(t *testing.T) {
	for _, scenario := range []string{"pod UID changed", "source spec changed", "snapshot failed"} {
		t.Run(scenario, func(t *testing.T) {
			f, client := checkpointFixture(t, "a", "b")
			ctx := context.Background()
			_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a", "b")))
			require.NoError(t, err)
			setSnapshotPhase(t, f, client, "point", "a", "Succeeded")
			response, err := f.service.RunnerCheckpointGet(ctx, f.identity, "point")
			require.NoError(t, err)
			require.Equal(t, sandboxPending, response.State)
			members, err := checkpointMembers(checkpointRow(t, f, "point"))
			require.NoError(t, err)
			require.NotEmpty(t, members[0].SnapshotID)
			setSnapshotPhase(t, f, client, "point", "b", "Succeeded")
			reason := ""
			switch scenario {
			case "pod UID changed":
				row := sandboxRow(t, f, "a")
				pod, err := f.service.Kube.CoreV1().Pods(row.Namespace).Get(ctx, row.PodName, metav1.GetOptions{})
				require.NoError(t, err)
				pod.UID = "replacement"
				_, err = f.service.Kube.CoreV1().Pods(row.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
				require.NoError(t, err)
				reason = "source_identity_changed"
			case "source spec changed":
				row := sandboxRow(t, f, "a")
				object, err := client.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
				require.NoError(t, err)
				require.NoError(t, unstructured.SetNestedField(object.Object, time.Now().UTC().Format(time.RFC3339), "spec", "shutdownTime"))
				require.NoError(t, client.Tracker().Update(SandboxGVR, object, row.Namespace))
				reason = "source_spec_changed"
			case "snapshot failed":
				setSnapshotPhase(t, f, client, "point", "a", "Failed")
				reason = "snapshot_failed"
			}
			response, err = f.service.RunnerCheckpointGet(ctx, f.identity, "point")
			require.NoError(t, err)
			require.Equal(t, sandboxFailed, response.State)
			require.Equal(t, reason, response.Reason)
		})
	}
}

func TestCheckpointParallelObservationRetainsCommitFencing(t *testing.T) {
	for _, scenario := range []string{"stale claim", "expired lease", "execution deadline"} {
		t.Run(scenario, func(t *testing.T) {
			f, client := checkpointFixture(t, "a", "b")
			ctx := context.Background()
			_, err := f.service.RunnerCheckpointPut(ctx, f.identity, "point", bytes.NewReader(checkpointArchive(t, "a", "b")))
			require.NoError(t, err)
			for _, trial := range []string{"a", "b"} {
				setSnapshotPhase(t, f, client, "point", trial, "Succeeded")
			}
			var once sync.Once
			client.PrependReactor("get", "checkpoints", func(k8stesting.Action) (bool, runtime.Object, error) {
				once.Do(func() {
					if scenario == "expired lease" {
						_, err := f.raw.CompareAndSwap(ctx, &model.JobCheckpoint{ID: "point"}, "id", "point", map[string]interface{}{"lease_until": time.Now().Add(-time.Second)})
						require.NoError(t, err)
						return
					}
					require.NoError(t, f.raw.Get(ctx, f.record))
					var state map[string]interface{}
					require.NoError(t, json.Unmarshal([]byte(f.record.InternalInfo), &state))
					if scenario == "stale claim" {
						state["runner"].(map[string]interface{})["ownerPodUID"] = "replacement"
					} else {
						state["deadline"] = time.Now().Add(-time.Second).UnixNano()
					}
					encoded, err := json.Marshal(state)
					require.NoError(t, err)
					f.record.InternalInfo = string(encoded)
					require.NoError(t, f.raw.Put(ctx, f.record))
				})
				return false, nil, nil
			})
			response, err := f.service.RunnerCheckpointGet(ctx, f.identity, "point")
			switch scenario {
			case "stale claim":
				require.ErrorIs(t, err, bcode.ErrUnauthorized)
			case "expired lease":
				require.ErrorIs(t, err, ErrRunnerConflict)
			case "execution deadline":
				require.NoError(t, err)
				require.Equal(t, sandboxFailed, response.State)
				require.Equal(t, "execution_deadline", response.Reason)
			}
			require.NotEqual(t, sandboxReady, checkpointRow(t, f, "point").State)
		})
	}
}

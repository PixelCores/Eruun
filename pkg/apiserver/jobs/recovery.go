package jobs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

const maxEvaluationRecoveries = 3

// EvaluationExecutionRank keeps a recovered execution attached to the original
// workflow slot during takeover. Each recovery still owns a distinct JobInfo.
func EvaluationExecutionRank(record *model.JobInfo, expectedRoot string) (int, bool) {
	if record == nil || record.ExecutionKey == nil {
		return 0, false
	}
	if *record.ExecutionKey == expectedRoot {
		return 0, true
	}
	if record.Type != string(config.JobEval) {
		return 0, false
	}
	info, err := decodeEvaluationInfo(record.EvaluationInfo)
	if err != nil || info.RootExecutionKey != expectedRoot || info.RecoveryIndex < 1 || info.RecoveryIndex > maxEvaluationRecoveries || info.ResumeCheckpointID == "" {
		return 0, false
	}
	return info.RecoveryIndex, *record.ExecutionKey == recoveryExecutionKey(info.RootExecutionKey, info.RecoveryIndex)
}
func recoveryExecutionKey(root string, index int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/recovery/%d", root, index)))
	return hex.EncodeToString(sum[:])
}
func evaluationRecord(ctx context.Context, store datastore.DataStore, task *model.JobTask, key string) (*model.JobInfo, error) {
	rows, err := store.List(ctx, &model.JobInfo{WorkspaceID: task.WorkspaceID, TaskID: task.TaskID}, &datastore.ListOptions{FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{{Key: "execution_key", Values: []string{key}}}}, Page: 1, PageSize: 2})
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("evaluation execution identity is unavailable")
	}
	row, ok := rows[0].(*model.JobInfo)
	if !ok || row.Type != string(config.JobEval) || row.ExecutionKey == nil || *row.ExecutionKey != key {
		return nil, datastore.ErrEntityInvalid
	}
	return row, nil
}
func recoveryOwnership(ctx context.Context, store datastore.DataStore, task *model.JobTask, fn func(artifacts.Backend) error) error {
	generation := task.OwnerRunGeneration
	if generation == 0 {
		generation = task.RunGeneration
	}
	if task.RunToken == "" || task.WorkerID == "" {
		return repository.ErrWorkflowOwnershipRequired
	}
	return repository.WithWorkflowTaskOwnership(ctx, store, &model.WorkflowQueue{TaskID: task.TaskID, RunGeneration: generation, RunToken: task.RunToken, WorkerID: task.WorkerID, Status: config.StatusRunning}, func(tx datastore.DataStore) error {
		backend, err := artifacts.RequireBackend(tx)
		if err != nil {
			return fmt.Errorf("evaluation recovery transaction: %w", err)
		}
		return fn(backend)
	})
}

// RecoverEvaluation resumes a durable reservation or selects the latest complete
// point after an infrastructure failure. Reported agent/verifier failures, user
// cancellation, and exhausted deadlines never cause an automatic replay.
func (s *Service) RecoverEvaluation(ctx context.Context, task *model.JobTask) (bool, error) {
	if task == nil || task.JobType != string(config.JobEval) {
		return false, nil
	}
	info, err := decodeEvaluationInfo(task.EvaluationInfo)
	if err != nil {
		return false, err
	}
	if info.Traits.Evaluation.Recovery == nil {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if info.ResumeCheckpointID != "" && !info.RecoveryIsolated {
		if err := s.isolateRecovery(ctx, task, info); err != nil {
			return false, err
		}
		return false, nil
	}
	if task.Status != config.StatusFailed {
		if task.Status == config.StatusCompleted || task.Status == config.StatusCancelled || task.Status == config.StatusTimeout {
			return false, s.releaseRecoveryReference(ctx, task)
		}
		return false, nil
	}
	source, err := evaluationRecord(ctx, s.Store, task, task.ExecutionKey)
	if err != nil {
		return false, err
	}
	if source.InternalInfo == "" {
		// Startup can fail before the first execution checkpoint is committed.
		// There is no Runner to recover; malformed or lost checkpoints must
		// still fail closed instead of being treated as an unstarted execution.
		if task.InternalInfo != "" || source.Status != string(config.StatusFailed) {
			return false, ErrRunnerConflict
		}
		return false, s.releaseRecoveryReference(ctx, task)
	}
	state, deadline, err := decodeRunnerState(source)
	if err != nil {
		return false, err
	}
	if state == nil || state.Terminal != nil || info.RecoveryIndex >= maxEvaluationRecoveries || deadline <= time.Now().Add(spec.EvaluationCollectionGraceSeconds*time.Second).UnixNano() {
		return false, s.releaseRecoveryReference(ctx, task)
	}
	var next *model.JobInfo
	err = recoveryOwnership(ctx, s.Store, task, func(tx artifacts.Backend) error {
		old := &model.JobInfo{ID: source.ID}
		if err := tx.GetForUpdate(ctx, old); err != nil {
			return err
		}
		if old.Status != string(config.StatusFailed) || old.EvaluationInfo != source.EvaluationInfo {
			return ErrRunnerConflict
		}

		now, err := tx.CurrentDatabaseTime(ctx)
		if err != nil {
			return err
		}
		rows, err := tx.List(ctx, &model.JobCheckpoint{WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, ExecutionKey: task.ExecutionKey, State: "ready"}, &datastore.ListOptions{SortBy: []datastore.SortOption{{Key: "create_time", Order: datastore.SortOrderDescending}}, Page: 1, PageSize: 3})
		if err != nil {
			return err
		}
		// A failed restore may retry its already protected source point when it has
		// not yet produced a newer complete point of its own.
		if info.ResumeCheckpointID != "" {
			prior := &model.JobCheckpoint{ID: info.ResumeCheckpointID}
			if err := tx.Get(ctx, prior); err != nil {
				return err
			}
			rows = append(rows, prior)
		}
		var point *model.JobCheckpoint
		for _, entity := range rows {
			candidate, ok := entity.(*model.JobCheckpoint)
			if !ok {
				return datastore.ErrEntityInvalid
			}
			locked := &model.JobCheckpoint{ID: candidate.ID}
			if err := tx.GetForUpdate(ctx, locked); err != nil {
				return err
			}
			if locked.WorkspaceID != task.WorkspaceID || locked.TaskID != task.TaskID || locked.State != "ready" || locked.Cleaned || !now.Before(locked.ExpiresAt) || !now.Add(spec.EvaluationCollectionGraceSeconds*time.Second).Before(locked.SourceDeadline) || (locked.ReferencedByExecutionKey != "" && locked.ReferencedByExecutionKey != task.ExecutionKey) {
				continue
			}
			point = locked
			break
		}
		if point == nil {
			return nil
		}
		nextInfo := *info
		if nextInfo.RootExecutionKey == "" {
			nextInfo.RootExecutionKey = task.ExecutionKey
		}
		nextInfo.RecoveryIndex++
		nextKey := recoveryExecutionKey(nextInfo.RootExecutionKey, nextInfo.RecoveryIndex)
		nextInfo.RecoveryOfExecutionKey = task.ExecutionKey
		nextInfo.ResumeCheckpointID, nextInfo.ExecutionDeadline = point.ID, deadline
		nextInfo.RecoveryIsolated = false
		nextInfo.RecoveryRunnerStopped = false
		nextInfo.RecoveryName = "eruun-recovery-" + nextKey[:32]
		token := make([]byte, 32)
		if _, err := rand.Read(token); err != nil {
			return err
		}
		nextInfo.RunnerToken = hex.EncodeToString(token)
		encoded, err := json.Marshal(nextInfo)
		if err != nil {
			return err
		}
		next = &model.JobInfo{Type: old.Type, WorkflowID: old.WorkflowID, ProductID: old.ProductID, WorkspaceID: old.WorkspaceID, AppID: old.AppID, TaskID: old.TaskID, ServiceName: old.ServiceName, Status: string(config.StatusQueued), ExecutionKey: ptr.To(nextKey), RunGeneration: old.RunGeneration, Attempt: 1, EvaluationInfo: string(encoded)}
		// Standalone lifecycle lookup uses the workload name. Application
		// evaluations instead retain their stable component identity.
		if old.AppID == "" {
			next.ServiceName = nextInfo.RecoveryName
		}
		if err := tx.Add(ctx, next); err != nil {
			return err
		}
		revoked := *info
		if _, err := rand.Read(token); err != nil {
			return err
		}
		revoked.RunnerToken = hex.EncodeToString(token)
		encoded, err = json.Marshal(revoked)
		if err != nil {
			return err
		}
		old.EvaluationInfo = string(encoded)
		if err := tx.Put(ctx, old); err != nil {
			return err
		}
		point.ReferencedByExecutionKey = nextKey
		return tx.Put(ctx, point)
	})
	if err != nil || next == nil {
		return false, err
	}
	task.ExecutionKey, task.EvaluationInfo = *next.ExecutionKey, next.EvaluationInfo
	task.InternalInfo, task.Info, task.Error = "", "", ""
	task.StartTime, task.EndTime, task.RetryCount, task.Attempt = 0, 0, 0, 1
	task.Status = config.StatusQueued
	nextInfo, err := decodeEvaluationInfo(next.EvaluationInfo)
	if err != nil {
		return false, err
	}
	if err := s.isolateRecovery(ctx, task, nextInfo); err != nil {
		return false, err
	}
	if err := BuildEvaluationTask(ctx, s.Store, s.Config, task, spec.JobTraits{}); err != nil {
		return false, err
	}
	workflowjob.ApplyExecutionIdentity(task)
	return true, nil
}

func (s *Service) isolateRecovery(ctx context.Context, task *model.JobTask, info *evaluationInfo) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if s.Kube == nil || s.SandboxClient == nil {
		return fmt.Errorf("evaluation recovery isolation clients unavailable")
	}
	source, err := evaluationRecord(ctx, s.Store, task, info.RecoveryOfExecutionKey)
	if err != nil {
		return err
	}
	state, _, err := decodeRunnerState(source)
	if err != nil {
		return fmt.Errorf("evaluation recovery source claim unavailable: %w", err)
	}
	if state == nil {
		return fmt.Errorf("evaluation recovery source claim unavailable")
	}
	// A heartbeat timeout or a missing API object does not prove that a node's
	// process stopped. Require terminal status for the exact source Pod UID.
	if !info.RecoveryRunnerStopped {
		if err := s.confirmStoppedPod(ctx, task.Namespace, state.OwnerPodName, state.OwnerPodUID, nil); err != nil {
			return fmt.Errorf("isolate source Runner: %w", err)
		}
		if err := recoveryOwnership(ctx, s.Store, task, func(tx artifacts.Backend) error {
			record, err := evaluationRecord(ctx, tx, task, task.ExecutionKey)
			if err != nil {
				return err
			}
			current, err := decodeEvaluationInfo(record.EvaluationInfo)
			if err != nil {
				return err
			}
			current.RecoveryRunnerStopped = true
			raw, err := json.Marshal(current)
			if err != nil {
				return err
			}
			record.EvaluationInfo = string(raw)
			if err := tx.Put(ctx, record); err != nil {
				return err
			}
			task.EvaluationInfo = record.EvaluationInfo
			return nil
		}); err != nil {
			return err
		}
	}

	rows, err := s.Store.List(ctx, &model.JobSandbox{WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, ExecutionKey: info.RecoveryOfExecutionKey}, nil)
	if err != nil {
		return err
	}
	for _, entity := range rows {
		row, ok := entity.(*model.JobSandbox)
		if !ok {
			return datastore.ErrEntityInvalid
		}
		if row.Reason == "recovery_isolated" {
			continue
		}
		if row.PodUID == "" {
			if row.SandboxUID != "" || row.CreateAttempts > 0 || row.State == sandboxPending {
				return fmt.Errorf("source Sandbox creation is not settled")
			}
			continue
		}
		released := false
		if err := recoveryOwnership(ctx, s.Store, task, func(tx artifacts.Backend) error {
			locked := &model.JobSandbox{ID: row.ID}
			if err := tx.GetForUpdate(ctx, locked); err != nil {
				return err
			}
			if locked.PodUID != row.PodUID || locked.SandboxUID != row.SandboxUID {
				return ErrRunnerConflict
			}
			now, err := tx.CurrentDatabaseTime(ctx)
			if err != nil {
				return err
			}
			released = locked.State == sandboxReleased
			if !released {
				locked.State = sandboxRetained
			}
			locked.Reason, locked.ReleaseRequested = "recovery_isolation", true
			locked.RetainUntil = &now
			// Any older maintenance lease can no longer commit or extend retention.
			locked.LeaseToken = ""
			locked.LeaseUntil = nil
			return tx.Put(ctx, locked)
		}); err != nil {
			return err
		}
		var waitForShutdown func() error
		if !released {
			obj, err := s.SandboxClient.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if string(obj.GetUID()) != row.SandboxUID || !ownedSandbox(row, obj) {
				return ErrRunnerConflict
			}
			patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"uid": row.SandboxUID, "resourceVersion": obj.GetResourceVersion()}, "spec": map[string]any{"shutdownTime": time.Now().UTC().Format(time.RFC3339)}})
			if err != nil {
				return err
			}
			if _, err = s.SandboxClient.Resource(SandboxGVR).Namespace(row.Namespace).Patch(ctx, row.SandboxName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
				return err
			}
			waitForShutdown = func() error { return nil }
		}
		// Release only confirms the Sandbox CR is gone. Its exact Pod must
		// still provide termination evidence before a clone can run.
		if err := s.confirmStoppedPod(ctx, row.Namespace, row.PodName, row.PodUID, waitForShutdown); err != nil {
			return fmt.Errorf("isolate source Sandbox %s: %w", row.TrialID, err)
		}
		if err := recoveryOwnership(ctx, s.Store, task, func(tx artifacts.Backend) error {
			locked := &model.JobSandbox{ID: row.ID}
			if err := tx.GetForUpdate(ctx, locked); err != nil {
				return err
			}
			if locked.PodUID != row.PodUID || locked.SandboxUID != row.SandboxUID || locked.Reason != "recovery_isolation" {
				return ErrRunnerConflict
			}
			locked.Reason = "recovery_isolated"
			return tx.Put(ctx, locked)
		}); err != nil {
			return err
		}

	}
	return recoveryOwnership(ctx, s.Store, task, func(tx artifacts.Backend) error {
		record, err := evaluationRecord(ctx, tx, task, task.ExecutionKey)
		if err != nil {
			return err
		}
		current, err := decodeEvaluationInfo(record.EvaluationInfo)
		if err != nil {
			return err
		}
		if current.ResumeCheckpointID != info.ResumeCheckpointID || current.RecoveryOfExecutionKey != info.RecoveryOfExecutionKey {
			return ErrRunnerConflict
		}
		current.RecoveryIsolated = true
		raw, err := json.Marshal(current)
		if err != nil {
			return err
		}
		record.EvaluationInfo = string(raw)
		if err := tx.Put(ctx, record); err != nil {
			return err
		}
		task.EvaluationInfo = record.EvaluationInfo
		return nil
	})
}

func (s *Service) confirmStoppedPod(ctx context.Context, namespace, name, uid string, stop func() error) error {
	if name == "" || uid == "" {
		return fmt.Errorf("source Pod identity missing")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	requested := false
	for {
		pod, err := s.Kube.CoreV1().Pods(namespace).Get(waitCtx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("source Pod termination unconfirmed: %w", err)
		}
		if string(pod.UID) != uid {
			return fmt.Errorf("source Pod UID changed; termination unconfirmed")
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			if !confirmedContainerTermination(pod) {
				return fmt.Errorf("source Pod lacks confirmed container termination")
			}
			return nil
		}
		if !requested {
			if stop == nil {
				return fmt.Errorf("source Runner termination unconfirmed")
			}
			if err := stop(); err != nil {
				return err
			}
			requested = true
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("source Pod termination unconfirmed: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}
func (s *Service) releaseRecoveryReference(ctx context.Context, task *model.JobTask) error {
	return recoveryOwnership(ctx, s.Store, task, func(tx artifacts.Backend) error {
		rows, err := tx.List(ctx, &model.JobCheckpoint{WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, ReferencedByExecutionKey: task.ExecutionKey}, nil)
		if err != nil {
			return err
		}
		for _, entity := range rows {
			row, ok := entity.(*model.JobCheckpoint)
			if !ok {
				return datastore.ErrEntityInvalid
			}

			locked := &model.JobCheckpoint{ID: row.ID}
			if err := tx.GetForUpdate(ctx, locked); err != nil {
				return err
			}
			if locked.ReferencedByExecutionKey != task.ExecutionKey {
				continue
			}
			locked.ReferencedByExecutionKey = ""
			if err := tx.Put(ctx, locked); err != nil {
				return err
			}
		}
		return nil
	})
}

// Node-loss controllers may synthesize a Failed phase without observing the
// process. Only kubelet/provider-reported completed container states qualify.
func confirmedContainerTermination(pod *corev1.Pod) bool {
	if pod.Status.Reason == "NodeLost" || len(pod.Spec.Containers) == 0 {
		return false
	}
	ended := func(name string, statuses []corev1.ContainerStatus) bool {
		for _, status := range statuses {
			if status.Name != name {
				continue
			}
			term := status.State.Terminated
			return term != nil && !term.FinishedAt.IsZero() && term.Reason != "ContainerStatusUnknown" && term.Reason != "NodeLost"
		}
		return false
	}
	for _, container := range pod.Spec.Containers {
		if !ended(container.Name, pod.Status.ContainerStatuses) {
			return false
		}
	}
	for _, container := range pod.Spec.InitContainers {
		if !ended(container.Name, pod.Status.InitContainerStatuses) {
			return false
		}
	}
	for _, container := range pod.Spec.EphemeralContainers {
		if !ended(container.Name, pod.Status.EphemeralContainerStatuses) {
			return false
		}
	}
	return true
}

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/google/uuid"
)

const checkpointTimeout = 10 * time.Minute

var checkpointIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

type CheckpointManifest struct {
	Version           int               `json:"version"`
	HarborVersion     string            `json:"harborVersion"`
	Agent             string            `json:"agent"`
	AgentVersion      string            `json:"agentVersion"`
	DatasetDigest     string            `json:"datasetDigest"`
	CompletedTrialIDs []string          `json:"completedTrialIds"`
	Members           []CheckpointTrial `json:"members"`
	Trials            json.RawMessage   `json:"trials"`
	RemainingSeconds  float64           `json:"remainingSeconds"`
	Files             json.RawMessage   `json:"files"`
}

type CheckpointTrial struct {
	TrialID   string `json:"trialId"`
	SessionID string `json:"sessionId"`
	Stage     string `json:"stage"`
}

// CheckpointMember contains only server-observed identities and specification.
// Runner material never supplies Kubernetes coordinates or snapshot handles.
type CheckpointMember struct {
	TrialID         string          `json:"trialId"`
	SandboxID       string          `json:"sandboxId"`
	SandboxUID      string          `json:"sandboxUID"`
	PodName         string          `json:"podName"`
	PodUID          string          `json:"podUID"`
	SandboxSpec     json.RawMessage `json:"sandboxSpec,omitempty"`
	SnapshotName    string          `json:"snapshotName"`
	SnapshotUID     string          `json:"snapshotUID,omitempty"`
	SnapshotID      string          `json:"snapshotId,omitempty"`
	CreateRequested bool            `json:"createRequested,omitempty"`
}

type CheckpointResponse struct {
	ID     string `json:"id"`
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
}

func checkpointResponse(row *model.JobCheckpoint) *CheckpointResponse {
	return &CheckpointResponse{ID: row.ID, State: row.State, Reason: row.Reason}
}

func validateCheckpointManifest(raw json.RawMessage, evaluation *evaluationInfo, digest string) (*CheckpointManifest, error) {
	var manifest CheckpointManifest
	if err := spec.DecodeJobJSON(raw, &manifest); err != nil {
		return nil, bcode.ErrJobInput
	}
	eval := evaluation.Traits.Evaluation
	if eval.Recovery == nil || !eval.Recovery.ReplaySafe || manifest.Version != 1 || manifest.HarborVersion != spec.HarborVersion || manifest.RemainingSeconds <= 0 || manifest.Agent != eval.Agent || manifest.AgentVersion != eval.Recovery.AgentVersion || manifest.DatasetDigest != digest || len(manifest.Members) < 1 || len(manifest.Members) > 1024 || len(manifest.CompletedTrialIDs) > 10000 {
		return nil, bcode.ErrJobInput
	}
	seen := map[string]bool{}
	for _, id := range manifest.CompletedTrialIDs {
		if !sandboxTrialID.MatchString(id) || seen[id] {
			return nil, bcode.ErrJobInput
		}
		seen[id] = true
	}
	for _, member := range manifest.Members {
		if !sandboxTrialID.MatchString(member.TrialID) || seen[member.TrialID] || len(member.SessionID) < 1 || len(member.SessionID) > 255 || member.Stage != "agent" {
			return nil, bcode.ErrJobInput
		}
		seen[member.TrialID] = true
	}
	return &manifest, nil
}

func (s *Service) checkpointRunner(ctx context.Context, identity RunnerIdentity) (context.Context, *runnerAuthorization, error) {
	if s.SandboxClient == nil || s.Artifacts == nil {
		return ctx, nil, bcode.ErrServiceUnavailable
	}
	auth, err := s.authorizeRunner(ctx, identity)
	if err != nil {
		return ctx, nil, err
	}
	if auth.evaluation.Traits.Evaluation.Recovery == nil {
		return ctx, nil, bcode.ErrJobInput
	}
	ctx = account.WithScope(ctx, account.Scope{WorkspaceID: auth.task.WorkspaceID, Namespace: auth.namespace, Role: "member"})
	return ctx, auth, nil
}

func (s *Service) RunnerCheckpointPut(ctx context.Context, identity RunnerIdentity, id string, input io.Reader) (*CheckpointResponse, error) {
	if !checkpointIDPattern.MatchString(id) {
		return nil, bcode.ErrJobInput
	}
	ctx, auth, err := s.checkpointRunner(ctx, identity)
	if err != nil {
		return nil, err
	}
	dataset, err := s.Artifacts.Get(ctx, auth.task.WorkspaceID, auth.evaluation.Traits.Evaluation.TaskPackageID)
	if err != nil {
		return nil, err
	}
	err = s.Artifacts.PutCheckpoint(ctx, auth.task.WorkspaceID, auth.task.TaskID, *auth.job.ExecutionKey, id, input, func(tx artifacts.Backend, artifact *model.JobArtifact, raw json.RawMessage) error {
		now, deadline, stopped, err := lockSandboxRunner(ctx, tx, auth)
		if err != nil {
			return err
		}
		if stopped != "" {
			return ErrRunnerConflict
		}
		manifest, err := validateCheckpointManifest(raw, auth.evaluation, dataset.Digest)
		if err != nil {
			return err
		}
		existing := &model.JobCheckpoint{ID: id}
		if err := tx.Get(ctx, existing); err == nil {
			if existing.WorkspaceID != auth.task.WorkspaceID || existing.ExecutionKey != *auth.job.ExecutionKey || existing.RunnerUID != identity.PodUID || existing.MaterialID != artifact.ID || string(existing.Manifest) != string(raw) {
				return ErrRunnerConflict
			}
			return nil
		} else if !errors.Is(err, datastore.ErrRecordNotExist) {
			return err
		}
		// A failed point with unfinished snapshots still owns the Pod snapshot
		// operation. Deleting a running ACS Checkpoint cannot stop that work.
		active, err := tx.List(ctx, &model.JobCheckpoint{WorkspaceID: auth.task.WorkspaceID, ExecutionKey: *auth.job.ExecutionKey}, &datastore.ListOptions{Page: 1, PageSize: 5, FilterOptions: datastore.FilterOptions{NotEqual: []datastore.ComparisonQueryOption{{Key: "cleaned", Value: true}}}})
		if err != nil {
			return err
		}
		if len(active) >= 4 {
			return ErrRunnerConflict
		}
		for _, record := range active {
			point := record.(*model.JobCheckpoint)
			if point.State != sandboxReady {
				return ErrRunnerConflict
			}
		}
		members := make([]CheckpointMember, 0, len(manifest.Members))
		memberIDs := make(map[string]bool, len(manifest.Members))
		for _, member := range manifest.Members {
			memberIDs[member.TrialID] = true
			row := &model.JobSandbox{ID: sandboxID(auth.task.WorkspaceID, *auth.job.ExecutionKey, member.TrialID)}
			if err := tx.GetForUpdate(ctx, row); err != nil {
				return err
			}
			if row.State != sandboxReady || row.ReleaseRequested || row.PodUID == "" || row.SandboxUID == "" || row.RunnerUID != identity.PodUID || row.Namespace != auth.namespace {
				return ErrRunnerConflict
			}
			members = append(members, CheckpointMember{TrialID: member.TrialID, SandboxID: row.ID, SandboxUID: row.SandboxUID, PodName: row.PodName, PodUID: row.PodUID, SnapshotName: "eruun-cp-" + sandboxID(auth.task.WorkspaceID, id, member.TrialID)[:40]})
		}
		// The material must cover every still-active trial, including trials
		// whose environment is being admitted. A Runner cannot obtain a
		// supposedly complete point by omitting another live writer.
		sandboxes, err := tx.List(ctx, &model.JobSandbox{WorkspaceID: auth.task.WorkspaceID, ExecutionKey: *auth.job.ExecutionKey, SlotReserved: true}, &datastore.ListOptions{Page: 1, PageSize: 1025})
		if err != nil {
			return err
		}
		if len(sandboxes) > 1024 {
			return ErrRunnerConflict
		}
		for _, entity := range sandboxes {
			row := entity.(*model.JobSandbox)
			if !row.ReleaseRequested && !memberIDs[row.TrialID] {
				return ErrRunnerConflict
			}
		}
		sort.Slice(members, func(i, j int) bool { return members[i].TrialID < members[j].TrialID })
		encoded, err := json.Marshal(members)
		if err != nil {
			return err
		}
		point := &model.JobCheckpoint{ID: id, WorkspaceID: auth.task.WorkspaceID, TaskID: auth.task.TaskID, JobID: auth.job.ID, ExecutionKey: *auth.job.ExecutionKey, RunnerUID: identity.PodUID, RunnerPodName: identity.PodName, Namespace: auth.namespace,
			State: sandboxPending, MaterialID: artifact.ID, Manifest: raw, Members: encoded, SourceDeadline: deadline, ExpiresAt: deadline.Add(sandboxRetention), ReconcileAt: now}
		if err := tx.Add(ctx, point); err != nil {
			return err
		}
		// The generic datastore stamps wall-clock creation time. This timestamp
		// also starts the snapshot timeout, so replace it with the database clock
		// in the same transaction to fence skew between API replicas.
		updated, err := tx.CompareAndSwap(ctx, point, "id", point.ID, map[string]interface{}{"create_time": now})
		if err != nil {
			return err
		}
		if !updated {
			return ErrRunnerConflict
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.RunnerCheckpointGet(ctx, identity, id)
}

func (s *Service) RunnerCheckpointGet(ctx context.Context, identity RunnerIdentity, id string) (*CheckpointResponse, error) {
	if !checkpointIDPattern.MatchString(id) {
		return nil, bcode.ErrJobInput
	}
	ctx, auth, err := s.checkpointRunner(ctx, identity)
	if err != nil {
		return nil, err
	}
	row := &model.JobCheckpoint{ID: id}
	advance := false
	err = artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		now, _, stopped, err := lockSandboxRunner(ctx, tx, auth)
		if err != nil {
			return err
		}
		if err := tx.GetForUpdate(ctx, row); err != nil {
			return err
		}
		if row.WorkspaceID != auth.task.WorkspaceID || row.ExecutionKey != *auth.job.ExecutionKey || row.RunnerUID != identity.PodUID {
			return bcode.ErrUnauthorized
		}
		if row.State != sandboxPending {
			return nil
		}
		if stopped != "" || now.Sub(row.CreateTime) >= checkpointTimeout {
			row.State, row.Reason, row.ExpiresAt = sandboxFailed, "checkpoint_timeout", now
			if stopped != "" {
				row.Reason = stopped
			}
		} else if row.LeaseUntil == nil || !now.Before(*row.LeaseUntil) {
			until := now.Add(sandboxLeaseDuration)
			row.LeaseToken, row.LeaseUntil, advance = uuid.NewString(), &until, true
		}
		return putCheckpoint(ctx, tx, row)
	})
	if err != nil {
		return nil, err
	}
	if advance {
		operation, cancel := context.WithTimeout(ctx, sandboxOperationTimeout)
		defer cancel()
		if err := s.advanceCheckpoint(operation, auth, row); err != nil {
			return nil, err
		}
	}
	return checkpointResponse(row), nil
}

func putCheckpoint(ctx context.Context, tx datastore.DataStore, row *model.JobCheckpoint) error {
	updated, err := tx.CompareAndSwap(ctx, row, "id", row.ID, map[string]interface{}{"state": row.State, "reason": row.Reason, "members": row.Members, "expires_at": row.ExpiresAt,
		"lease_token": row.LeaseToken, "lease_until": row.LeaseUntil, "reconcile_at": row.ReconcileAt, "cleaned": row.Cleaned, "referenced_by_execution_key": row.ReferencedByExecutionKey})
	if err != nil {
		return err
	}
	if !updated {
		return ErrRunnerConflict
	}
	return nil
}

func (s *Service) mutateCheckpoint(ctx context.Context, auth *runnerAuthorization, row *model.JobCheckpoint, change func(artifacts.Backend, *model.JobCheckpoint, time.Time) error) error {
	return artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		now, _, stopped, err := lockSandboxRunner(ctx, tx, auth)
		if err != nil {
			return err
		}
		current := &model.JobCheckpoint{ID: row.ID}
		if err := tx.GetForUpdate(ctx, current); err != nil {
			return err
		}
		if current.State != sandboxPending || current.RunnerUID != auth.identity.PodUID || current.LeaseToken != row.LeaseToken || current.LeaseUntil == nil || !now.Before(*current.LeaseUntil) {
			return ErrRunnerConflict
		}
		if stopped != "" {
			current.State, current.Reason, current.ExpiresAt = sandboxFailed, stopped, now
		} else if err := change(tx, current, now); err != nil {
			return err
		}
		if err := putCheckpoint(ctx, tx, current); err != nil {
			return err
		}
		*row = *current
		return nil
	})
}

func (s *Service) RunnerCheckpointMaterial(ctx context.Context, identity RunnerIdentity, id string, output io.Writer) error {
	ctx, auth, err := s.checkpointRunner(ctx, identity)
	if err != nil {
		return err
	}
	row := &model.JobCheckpoint{ID: id}
	err = artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		now, _, stopped, err := lockSandboxRunner(ctx, tx, auth)
		if err != nil {
			return err
		}
		if stopped != "" {
			return ErrRunnerConflict
		}
		if err := tx.GetForUpdate(ctx, row); err != nil {
			return err
		}
		if auth.evaluation.ResumeCheckpointID != id || !auth.evaluation.RecoveryIsolated || row.WorkspaceID != auth.task.WorkspaceID || row.TaskID != auth.task.TaskID || row.ReferencedByExecutionKey != *auth.job.ExecutionKey || row.State != sandboxReady || row.Cleaned || !now.Before(row.ExpiresAt) {
			return bcode.ErrUnauthorized
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := s.Artifacts.Download(ctx, row.WorkspaceID, row.MaterialID, output); err != nil {
		return fmt.Errorf("download checkpoint material: %w", err)
	}
	return nil
}

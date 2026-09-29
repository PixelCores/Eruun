package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/google/uuid"
)

const (
	sandboxRetention        = 24 * time.Hour
	sandboxOperationTimeout = 10 * time.Second
	sandboxLeaseDuration    = 30 * time.Second
	sandboxPending          = "pending"
	sandboxReady            = "ready"
	sandboxReleased         = "released"
	sandboxRetained         = "retained"
	sandboxFailed           = "failed"
)

var sandboxTrialID = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)

type SandboxRequest struct {
	TrialID    string `json:"trialId"`
	Image      string `json:"image"`
	StorageMiB int64  `json:"storageMiB"`
}

type SandboxReleaseRequest struct {
	SandboxUID         string `json:"sandboxUID"`
	PodUID             string `json:"podUID"`
	CollectionComplete *bool  `json:"collectionComplete"`
}

type SandboxResponse struct {
	TrialID             string     `json:"trialId"`
	State               string     `json:"state"`
	Admitted            bool       `json:"admitted"`
	AdmissionAgeSeconds *float64   `json:"admissionAgeSeconds,omitempty"`
	Namespace           string     `json:"namespace"`
	SandboxName         string     `json:"sandboxName"`
	SandboxUID          string     `json:"sandboxUID"`
	PodName             string     `json:"podName"`
	PodUID              string     `json:"podUID"`
	ContainerName       string     `json:"containerName"`
	RetainUntil         *time.Time `json:"retainUntil,omitempty"`
	Reason              string     `json:"reason,omitempty"`
	Restored            bool       `json:"restored,omitempty"`
}

func sandboxResponse(row *model.JobSandbox) *SandboxResponse {
	return &SandboxResponse{TrialID: row.TrialID, State: row.State, Admitted: row.SandboxUID != "", Namespace: row.Namespace,
		SandboxName: row.SandboxName, SandboxUID: row.SandboxUID, PodName: row.PodName, PodUID: row.PodUID,
		ContainerName: "main", RetainUntil: row.RetainUntil, Reason: row.Reason}
}

func sandboxID(workspaceID, executionKey, trialID string) string {
	digest := sha256.Sum256([]byte(workspaceID + "\x00" + executionKey + "\x00" + trialID))
	return hex.EncodeToString(digest[:])
}

func (r SandboxRequest) validate() error {
	if !sandboxTrialID.MatchString(r.TrialID) || len(r.Image) > 1024 || !spec.ExplicitJobImage(r.Image) || r.StorageMiB < 1 || r.StorageMiB > 1048576 {
		return bcode.ErrJobInput
	}
	return nil
}

func (s *Service) RunnerSandboxCreate(ctx context.Context, identity RunnerIdentity, request SandboxRequest) (*SandboxResponse, error) {
	if err := request.validate(); err != nil {
		return nil, err
	}
	return s.runnerSandbox(ctx, identity, request.TrialID, &request, nil)
}

func (s *Service) RunnerSandboxGet(ctx context.Context, identity RunnerIdentity, trialID string) (*SandboxResponse, error) {
	return s.runnerSandbox(ctx, identity, trialID, nil, nil)
}

func (s *Service) RunnerSandboxRelease(ctx context.Context, identity RunnerIdentity, trialID string, request SandboxReleaseRequest) (*SandboxResponse, error) {
	if request.CollectionComplete == nil || len(request.SandboxUID) > 64 || len(request.PodUID) > 64 {
		return nil, bcode.ErrJobInput
	}
	return s.runnerSandbox(ctx, identity, trialID, nil, &request)
}

// lockSandboxRunner repeats the exact claim and execution checks after both
// durable owner rows are locked. Kubernetes authorization happens before this.
func lockSandboxRunner(ctx context.Context, tx artifacts.Backend, auth *runnerAuthorization) (time.Time, time.Time, string, error) {

	task := &model.WorkflowQueue{TaskID: auth.task.TaskID}
	if err := tx.GetForUpdate(ctx, task); err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	if err := validateLockedRunnerTask(task, auth); err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	job := &model.JobInfo{ID: auth.job.ID}
	if err := tx.GetForUpdate(ctx, job); err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	if err := validateLockedRunnerJob(ctx, tx, job, auth, task.Status); err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	state, deadline, err := decodeRunnerState(job)
	if err != nil || !runnerOwnerMatches(state, auth) || deadline <= 0 {
		return time.Time{}, time.Time{}, "", bcode.ErrUnauthorized
	}

	now, err := tx.CurrentDatabaseTime(ctx)
	if err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	end := time.Unix(0, deadline)
	stop := ""
	if task.Status == config.StatusCancelled {
		stop = "cancelled"
	} else if !now.Before(end) {
		stop = "execution_deadline"
	} else if state.Terminal != nil {
		stop = "execution_finished"
	}
	return now, end, stop, nil
}

// lockSandboxMaintenanceOwners preserves the task -> job -> sandbox lock order
// used by runner mutations. Missing owners are allowed because application
// deletion may remove them before the bounded Sandbox retention period ends.
func lockSandboxMaintenanceOwners(ctx context.Context, tx artifacts.Backend, candidate *model.JobSandbox) (*model.WorkflowQueue, *model.JobInfo, bool, error) {

	orphaned := false
	task := &model.WorkflowQueue{TaskID: candidate.TaskID}
	if err := tx.GetForUpdate(ctx, task); err != nil {
		if !errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, nil, false, err
		}
		orphaned = true
	} else if task.TaskID != candidate.TaskID || task.WorkspaceID != candidate.WorkspaceID {
		return nil, nil, false, ErrRunnerConflict
	}
	job := &model.JobInfo{ID: candidate.JobID}
	if err := tx.GetForUpdate(ctx, job); err != nil {
		if !errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, nil, false, err
		}
		orphaned = true
	} else if job.ID != candidate.JobID || job.TaskID != candidate.TaskID || job.WorkspaceID != candidate.WorkspaceID || job.ExecutionKey == nil || *job.ExecutionKey != candidate.ExecutionKey {
		return nil, nil, false, ErrRunnerConflict
	}
	return task, job, orphaned, nil
}

func stopSandbox(row *model.JobSandbox, now time.Time, reason string) {
	if row.ReleaseRequested || row.State == sandboxReleased {
		return
	}
	row.ReleaseRequested, row.Reason, row.StartReserved = true, reason, false
	until := now.Add(sandboxRetention)
	row.RetainUntil, row.State = &until, sandboxRetained
	if row.SandboxUID == "" && row.CreateAttempts == 0 {
		row.State, row.SlotReserved = sandboxReleased, false
	}
}

func (s *Service) runnerSandbox(ctx context.Context, identity RunnerIdentity, trialID string, request *SandboxRequest, release *SandboxReleaseRequest) (*SandboxResponse, error) {
	if !sandboxTrialID.MatchString(trialID) {
		return nil, bcode.ErrJobInput
	}
	if s.SandboxClient == nil || s.SandboxObserver == nil {
		return nil, bcode.ErrServiceUnavailable
	}
	auth, err := s.authorizeRunner(ctx, identity)
	if err != nil {
		return nil, err
	}
	ctx = account.WithScope(ctx, account.Scope{WorkspaceID: auth.task.WorkspaceID, Namespace: auth.namespace, Role: "member"})
	id := sandboxID(auth.task.WorkspaceID, *auth.job.ExecutionKey, trialID)
	var row *model.JobSandbox
	advance := false
	var databaseNow, databaseSampledAt time.Time
	err = artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		now, deadline, stopped, err := lockSandboxRunner(ctx, tx, auth)
		if err != nil {
			return err
		}
		databaseNow, databaseSampledAt = now, time.Now()
		row = &model.JobSandbox{ID: id}
		err = tx.Get(ctx, row) // JobInfo lock serializes first-intent insertion.
		create := errors.Is(err, datastore.ErrRecordNotExist)
		if err != nil && !create {
			return err
		}
		if auth.evaluation.Traits.Evaluation.Recovery != nil && (create || release != nil) {
			pending, err := tx.Count(ctx, &model.JobCheckpoint{WorkspaceID: auth.task.WorkspaceID, ExecutionKey: *auth.job.ExecutionKey, State: sandboxPending}, nil)
			if err != nil {
				return err
			}
			if pending > 0 {
				return ErrRunnerConflict
			}
		}
		if create {
			if request == nil {
				return bcode.ErrNotFound
			}
			raw, _ := json.Marshal(request)
			digest := sha256.Sum256(raw)
			row = &model.JobSandbox{ID: id, WorkspaceID: auth.task.WorkspaceID, TaskID: auth.task.TaskID, JobID: auth.job.ID,
				ExecutionKey: *auth.job.ExecutionKey, TrialID: trialID, Namespace: auth.namespace, RequestDigest: hex.EncodeToString(digest[:]),
				Image: request.Image, StorageMiB: request.StorageMiB, SandboxName: "eruun-sbx-" + id[:40], RunnerUID: identity.PodUID,
				RunnerPodName: identity.PodName, Deadline: deadline, State: sandboxPending}
		} else {
			if row.WorkspaceID != auth.task.WorkspaceID || row.TaskID != auth.task.TaskID || row.JobID != auth.job.ID || row.ExecutionKey != *auth.job.ExecutionKey || row.RunnerUID != identity.PodUID || row.Namespace != auth.namespace {
				return bcode.ErrUnauthorized
			}
			if request != nil {
				raw, _ := json.Marshal(request)
				digest := sha256.Sum256(raw)
				if row.RequestDigest != hex.EncodeToString(digest[:]) {
					return ErrRunnerConflict
				}
			}
		}
		if release != nil {
			if (release.SandboxUID != "" && release.SandboxUID != row.SandboxUID) || (release.PodUID != "" && release.PodUID != row.PodUID) {
				return ErrRunnerConflict
			}
			if !row.ReleaseRequested && row.State != sandboxReleased {
				stopSandbox(row, now, "collection_incomplete")
				if *release.CollectionComplete && row.State != sandboxReleased {
					row.RetainUntil, row.State, row.Reason = &now, sandboxPending, "release_pending"
				}
			}
		}
		if stopped != "" {
			stopSandbox(row, now, stopped)
		}
		row.ReconcileAt = now.Add(15 * time.Second)
		if row.State != sandboxReleased && (row.State != sandboxFailed || row.ReleaseRequested) && (row.LeaseUntil == nil || !now.Before(*row.LeaseUntil)) {
			if !row.SlotReserved && !row.ReleaseRequested {
				count, err := tx.Count(ctx, &model.JobSandbox{WorkspaceID: row.WorkspaceID, ExecutionKey: row.ExecutionKey, SlotReserved: true}, nil)
				if err != nil {
					return err
				}
				if count >= int64(auth.evaluation.Traits.Evaluation.Concurrency) {
					row.Reason = "execution_concurrency"
					retained, err := tx.Count(ctx, &model.JobSandbox{WorkspaceID: row.WorkspaceID, ExecutionKey: row.ExecutionKey, SlotReserved: true, State: sandboxRetained}, nil)
					if err != nil {
						return err
					}
					if retained > 0 {
						row.Reason = "retained_capacity"
					}
				} else {
					row.SlotReserved = true
				}
			}
			if row.SlotReserved || row.ReleaseRequested {
				until := now.Add(sandboxLeaseDuration)
				row.LeaseToken, row.LeaseUntil, advance = uuid.NewString(), &until, true
			}
		}
		if create {
			return tx.Add(ctx, row)
		}
		return putSandbox(ctx, tx, row)
	})
	if err != nil {
		return nil, err
	}
	if advance {
		operation, cancel := context.WithTimeout(ctx, sandboxOperationTimeout)
		defer cancel()
		if err := s.advanceSandbox(operation, auth, row); err != nil {
			return nil, err
		}
	} else if row.State == sandboxReady {
		// A concurrent operation owns the lease; a stored ready projection is
		// not sufficient to issue fresh connection coordinates.
		row.State, row.Reason = sandboxPending, "observation_pending"
	}
	response := sandboxResponse(row)
	if auth.evaluation.ResumeCheckpointID != "" {
		point := &model.JobCheckpoint{ID: auth.evaluation.ResumeCheckpointID}
		if err := s.Store.Get(ctx, point); err != nil {
			return nil, err
		}
		members, err := checkpointMembers(point)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			if member.TrialID == trialID {
				response.Restored = true
				break
			}
		}
	}
	if release == nil && row.SandboxUID != "" && row.AdmittedAt != nil {
		// Advance the sampled DB time with this process's monotonic clock.
		// Replica wall clocks do not participate in the Runner's timeout.
		age := databaseNow.Add(time.Since(databaseSampledAt)).Sub(*row.AdmittedAt).Seconds()
		if age < 0 {
			age = 0
		}
		response.AdmissionAgeSeconds = &age
	}
	return response, nil
}

// mutateSandbox uses the same JobInfo lock for slot acquisition and release.
// A stale operation can never overwrite a newer lease or a stopped intent.
func (s *Service) mutateSandbox(ctx context.Context, auth *runnerAuthorization, candidate *model.JobSandbox, fn func(artifacts.Backend, *model.JobSandbox, time.Time) error) error {
	return artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		var now time.Time
		stopped := ""
		if auth != nil {
			var err error
			now, _, stopped, err = lockSandboxRunner(ctx, tx, auth)
			if err != nil {
				return err
			}
		} else {
			if _, _, _, err := lockSandboxMaintenanceOwners(ctx, tx, candidate); err != nil {
				return err
			}
			var err error
			now, err = tx.CurrentDatabaseTime(ctx)
			if err != nil {
				return err
			}
		}
		row := &model.JobSandbox{ID: candidate.ID}
		if err := tx.GetForUpdate(ctx, row); err != nil {
			return err
		}
		if row.WorkspaceID != candidate.WorkspaceID || row.ExecutionKey != candidate.ExecutionKey || row.RunnerUID != candidate.RunnerUID || row.LeaseToken != candidate.LeaseToken {
			return ErrRunnerConflict
		}
		if row.LeaseUntil == nil || !now.Before(*row.LeaseUntil) {
			return ErrRunnerConflict
		}
		if stopped != "" {
			stopSandbox(row, now, stopped)
		}
		hadUID := row.SandboxUID != ""
		if err := fn(tx, row, now); err != nil {
			return err
		}
		if !hadUID && row.SandboxUID != "" {
			row.AdmittedAt = &now
		}
		if err := putSandbox(ctx, tx, row); err != nil {
			return err
		}
		*candidate = *row
		return nil
	})
}

// putSandbox writes zero values too: the generic Put intentionally skips them.
// Callers hold the parent, JobInfo, and (for existing rows) Sandbox locks.
func putSandbox(ctx context.Context, tx datastore.DataStore, row *model.JobSandbox) error {
	updated, err := tx.CompareAndSwap(ctx, row, "id", row.ID, map[string]interface{}{
		"sandbox_uid": row.SandboxUID, "admitted_at": row.AdmittedAt, "pod_name": row.PodName, "pod_uid": row.PodUID,
		"state": row.State, "reason": row.Reason, "retain_until": row.RetainUntil,
		"start_reserved": row.StartReserved, "slot_reserved": row.SlotReserved, "release_requested": row.ReleaseRequested,
		"create_attempts": row.CreateAttempts, "lease_token": row.LeaseToken,
		"lease_until": row.LeaseUntil, "reconcile_at": row.ReconcileAt,
	})
	if err != nil {
		return err
	}
	if !updated {
		return ErrRunnerConflict
	}
	return nil
}

// sandboxStatuses exposes a bounded lifecycle view of the selected execution.
// Live reservations come first, followed by the newest creation time and ID.
// Ready describes resource observation and is not proof of trial execution.
func (s *Service) sandboxStatuses(ctx context.Context, workspaceID, taskID, executionKey string) ([]*SandboxResponse, bool, error) {
	const limit = 100
	rows, err := s.Store.List(ctx, &model.JobSandbox{WorkspaceID: workspaceID, TaskID: taskID, ExecutionKey: executionKey}, &datastore.ListOptions{
		Page: 1, PageSize: limit + 1,
		SortBy: []datastore.SortOption{{Key: "slot_reserved", Order: datastore.SortOrderDescending}, {Key: "create_time", Order: datastore.SortOrderDescending}, {Key: "id", Order: datastore.SortOrderDescending}},
	})
	if err != nil {
		return nil, false, err
	}
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	out := make([]*SandboxResponse, 0, len(rows))
	for _, entity := range rows {
		out = append(out, sandboxResponse(entity.(*model.JobSandbox)))
	}
	return out, truncated, nil
}

package jobs

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/klog/v2"
)

const (
	RunnerProtocolVersion = "v1"
	RunnerHeartbeatPeriod = 15 * time.Second
	RunnerStaleAfter      = 60 * time.Second
)

var (
	ErrRunnerConflict = errors.New("evaluation runner state conflict")
	runnerReasonRE    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
)

type RunnerStopConflictError struct {
	Outcome string
}

func (e *RunnerStopConflictError) Error() string { return ErrRunnerConflict.Error() }
func (e *RunnerStopConflictError) Unwrap() error { return ErrRunnerConflict }

type RunnerProgress struct {
	CompletedTrials int `json:"completedTrials"`
	TotalTrials     int `json:"totalTrials"`
}

type RunnerTerminal struct {
	Outcome            string `json:"outcome"`
	ExitCode           *int   `json:"exitCode,omitempty"`
	Signal             string `json:"signal,omitempty"`
	ArtifactID         string `json:"artifactId"`
	ArtifactDigest     string `json:"artifactDigest"`
	CollectionComplete *bool  `json:"collectionComplete"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
}

type RunnerEvent struct {
	ProtocolVersion string          `json:"protocolVersion"`
	Sequence        uint64          `json:"sequence"`
	Kind            string          `json:"kind"`
	Phase           string          `json:"phase,omitempty"`
	Progress        *RunnerProgress `json:"progress,omitempty"`
	Terminal        *RunnerTerminal `json:"terminal,omitempty"`
}

type RunnerEventAck struct {
	AcceptedSequence uint64 `json:"acceptedSequence"`
	Action           string `json:"action"`
	StopOutcome      string `json:"stopOutcome,omitempty"`
}

type RunnerTerminalStatus struct {
	Outcome            string `json:"outcome"`
	ExitCode           *int   `json:"exitCode,omitempty"`
	Signal             string `json:"signal,omitempty"`
	ArtifactID         string `json:"artifactId"`
	ArtifactDigest     string `json:"artifactDigest"`
	CollectionComplete bool   `json:"collectionComplete"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
}

type RunnerStatus struct {
	Phase           string                `json:"phase"`
	Sequence        uint64                `json:"sequence"`
	LastHeartbeatAt *time.Time            `json:"lastHeartbeatAt,omitempty"`
	Stale           bool                  `json:"stale"`
	Progress        *RunnerProgress       `json:"progress,omitempty"`
	Terminal        *RunnerTerminalStatus `json:"terminal,omitempty"`
}

type evaluationRunnerState struct {
	ProtocolVersion string                `json:"protocolVersion"`
	OwnerPodName    string                `json:"ownerPodName"`
	OwnerPodUID     string                `json:"ownerPodUID"`
	OwnerJobUID     string                `json:"ownerJobUID"`
	ClaimedAt       time.Time             `json:"claimedAt"`
	LastReceivedAt  time.Time             `json:"lastReceivedAt"`
	LastHeartbeatAt *time.Time            `json:"lastHeartbeatAt,omitempty"`
	LastSequence    uint64                `json:"lastSequence"`
	LastEventDigest string                `json:"lastEventDigest"`
	Phase           string                `json:"phase"`
	Progress        *RunnerProgress       `json:"progress,omitempty"`
	Terminal        *RunnerTerminalStatus `json:"terminal,omitempty"`
}

type runnerMetricSet struct {
	events       metric.Int64Counter
	conflicts    metric.Int64Counter
	harborStarts metric.Int64Counter
	receiveLag   metric.Float64Histogram
}

var runnerMetricState struct {
	sync.Once
	value runnerMetricSet
}

func runnerMetrics() runnerMetricSet {
	runnerMetricState.Do(func() {
		meter := otel.Meter("github.com/PixelCores/Eruun/pkg/apiserver/jobs")
		runnerMetricState.value.events, _ = meter.Int64Counter("eruun.job_runner.events")
		runnerMetricState.value.conflicts, _ = meter.Int64Counter("eruun.job_runner.conflicts")
		runnerMetricState.value.harborStarts, _ = meter.Int64Counter("eruun.job_runner.harbor.starts")
		runnerMetricState.value.receiveLag, _ = meter.Float64Histogram("eruun.job_runner.event.receive_latency")
	})
	return runnerMetricState.value
}

func (event RunnerEvent) validate() error {
	if event.ProtocolVersion != RunnerProtocolVersion {
		return fmt.Errorf("protocolVersion must be %q", RunnerProtocolVersion)
	}
	if event.Sequence == 0 {
		return fmt.Errorf("sequence must be positive")
	}
	switch event.Kind {
	case "claim", "heartbeat":
		if event.Phase != "" || event.Progress != nil || event.Terminal != nil {
			return fmt.Errorf("%s event contains unsupported fields", event.Kind)
		}
	case "phase":
		if !validRunnerPhase(event.Phase) || event.Progress != nil || event.Terminal != nil {
			return fmt.Errorf("phase event is invalid")
		}
	case "progress":
		if event.Phase != "" || event.Progress == nil || event.Terminal != nil ||
			event.Progress.CompletedTrials < 0 || event.Progress.TotalTrials < 0 || event.Progress.CompletedTrials > event.Progress.TotalTrials {
			return fmt.Errorf("progress event is invalid")
		}
	case "terminal":
		if event.Phase != "" || event.Progress != nil || event.Terminal == nil {
			return fmt.Errorf("terminal event is invalid")
		}
		if err := event.Terminal.validate(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("kind is not supported")
	}
	return nil
}

func (terminal RunnerTerminal) validate() error {
	switch terminal.Outcome {
	case "succeeded", "failed", "cancelled", "timed_out":
	default:
		return fmt.Errorf("terminal outcome is invalid")
	}
	if terminal.ExitCode != nil && (*terminal.ExitCode < 0 || *terminal.ExitCode > 255) {
		return fmt.Errorf("terminal exitCode is invalid")
	}
	if len(terminal.Signal) > 32 || strings.ContainsAny(terminal.Signal, "\r\n\x00") {
		return fmt.Errorf("terminal signal is invalid")
	}
	if len(terminal.ArtifactID) != 64 || len(terminal.ArtifactDigest) != 64 || terminal.CollectionComplete == nil {
		return fmt.Errorf("terminal artifact confirmation is required")
	}
	if terminal.Reason != "" && !runnerReasonRE.MatchString(terminal.Reason) {
		return fmt.Errorf("terminal reason is invalid")
	}
	if len(terminal.Message) > 512 || strings.ContainsAny(terminal.Message, "\r\n\x00") {
		return fmt.Errorf("terminal message is invalid")
	}
	return nil
}

func validRunnerPhase(phase string) bool {
	return phase == "preparing" || phase == "running" || phase == "finalizing"
}

func runnerPhaseOrder(phase string) int {
	switch phase {
	case "preparing":
		return 1
	case "running":
		return 2
	case "finalizing":
		return 3
	default:
		return 0
	}
}

func runnerEventDigest(event RunnerEvent) (string, error) {
	raw, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func decodeRunnerState(record *model.JobInfo) (*evaluationRunnerState, int64, error) {
	raw, deadline, err := workflowjob.EvaluationRunnerCheckpoint(record)
	if err != nil || len(raw) == 0 {
		return nil, deadline, err
	}
	var state evaluationRunnerState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, deadline, fmt.Errorf("decode evaluation runner state: %w", err)
	}
	if state.ProtocolVersion != RunnerProtocolVersion || state.OwnerPodName == "" || state.OwnerPodUID == "" || state.OwnerJobUID == "" || state.LastSequence == 0 || !validRunnerPhase(state.Phase) {
		return nil, deadline, fmt.Errorf("evaluation runner state is invalid")
	}
	return &state, deadline, nil
}

func runnerOwnerMatches(state *evaluationRunnerState, auth *runnerAuthorization) bool {
	return state != nil && auth != nil && state.OwnerPodName == auth.identity.PodName &&
		state.OwnerPodUID == auth.identity.PodUID && state.OwnerJobUID == auth.jobUID
}

func (s *Service) RunnerEvent(ctx context.Context, identity RunnerIdentity, event RunnerEvent) (*RunnerEventAck, error) {
	started := time.Now()
	if err := event.validate(); err != nil {
		return nil, invalid(err)
	}
	auth, err := s.authorizeRunner(ctx, identity)
	if err != nil {
		return nil, err
	}
	digest, err := runnerEventDigest(event)
	if err != nil {
		return nil, err
	}
	var ack RunnerEventAck
	result := "accepted"
	err = s.Store.(datastore.Transactional).WithTransaction(ctx, func(tx datastore.DataStore) error {
		locker, ok := tx.(datastore.RowLocker)
		if !ok {
			return fmt.Errorf("runner events require row locking")
		}
		task := &model.WorkflowQueue{TaskID: auth.task.TaskID}
		if err := locker.GetForUpdate(ctx, task); err != nil {
			return err
		}
		if err := validateLockedRunnerTask(task, auth); err != nil {
			return err
		}
		record := &model.JobInfo{ID: auth.job.ID}
		if err := locker.GetForUpdate(ctx, record); err != nil {
			return err
		}
		if err := validateLockedRunnerJob(record, auth, task.Status); err != nil {
			return err
		}
		state, deadline, err := decodeRunnerState(record)
		if err != nil {
			return err
		}
		clock, ok := tx.(datastore.DatabaseClock)
		if !ok {
			return fmt.Errorf("runner events require database clock")
		}
		now, err := clock.CurrentDatabaseTime(ctx)
		if err != nil {
			return err
		}
		action := "continue"
		stopOutcome := ""
		if task.Status == config.StatusCancelled {
			action, stopOutcome = "stop", "cancelled"
		} else if deadline > 0 && !now.Before(time.Unix(0, deadline)) {
			action = "stop"
			stopOutcome = "timed_out"
		}
		if state != nil {
			if !runnerOwnerMatches(state, auth) {
				return ErrRunnerConflict
			}
			ack = RunnerEventAck{AcceptedSequence: state.LastSequence, Action: action, StopOutcome: stopOutcome}
			if event.Sequence < state.LastSequence {
				result = "old"
				return nil
			}
			if event.Sequence == state.LastSequence {
				if digest != state.LastEventDigest {
					return ErrRunnerConflict
				}
				result = "replay"
				return nil
			}
			if event.Kind == "claim" || state.Terminal != nil {
				return ErrRunnerConflict
			}
		} else {
			if event.Kind != "claim" || event.Sequence != 1 {
				return ErrRunnerConflict
			}
			state = &evaluationRunnerState{
				ProtocolVersion: RunnerProtocolVersion,
				OwnerPodName:    identity.PodName,
				OwnerPodUID:     identity.PodUID,
				OwnerJobUID:     auth.jobUID,
				ClaimedAt:       now,
				Phase:           "preparing",
			}
		}
		if event.Kind == "terminal" && stopOutcome != "" {
			if event.Terminal.Outcome != stopOutcome {
				return &RunnerStopConflictError{Outcome: stopOutcome}
			}
		}
		if err := s.applyRunnerEvent(ctx, tx, auth, state, event); err != nil {
			return err
		}
		state.LastSequence = event.Sequence
		state.LastEventDigest = digest
		state.LastReceivedAt = now
		if event.Kind == "heartbeat" {
			received := now
			state.LastHeartbeatAt = &received
		}
		raw, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err := workflowjob.SetEvaluationRunnerCheckpoint(record, raw); err != nil {
			return err
		}
		if err := tx.Put(ctx, record); err != nil {
			return err
		}
		ack = RunnerEventAck{AcceptedSequence: state.LastSequence, Action: action, StopOutcome: stopOutcome}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrRunnerConflict) {
			metrics := runnerMetrics()
			if metrics.conflicts != nil {
				metrics.conflicts.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", event.Kind)))
			}
			klog.InfoS("reject conflicting evaluation runner event", "taskID", identity.TaskID, "kind", event.Kind, "sequence", event.Sequence)
		}
		return nil, err
	}
	metrics := runnerMetrics()
	if metrics.events != nil {
		metrics.events.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", event.Kind), attribute.String("result", result)))
	}
	if metrics.receiveLag != nil {
		metrics.receiveLag.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(attribute.String("kind", event.Kind)))
	}
	if result == "accepted" && event.Kind == "phase" && event.Phase == "running" && metrics.harborStarts != nil {
		metrics.harborStarts.Add(ctx, 1)
	}
	klog.InfoS("accepted evaluation runner event", "taskID", identity.TaskID, "kind", event.Kind, "sequence", ack.AcceptedSequence, "result", result, "action", ack.Action)
	return &ack, nil
}

func validateLockedRunnerTask(task *model.WorkflowQueue, auth *runnerAuthorization) error {
	if task == nil || auth == nil || task.Type != auth.task.Type || task.AppID != auth.task.AppID ||
		task.WorkspaceID != auth.task.WorkspaceID {
		return bcode.ErrUnauthorized
	}
	return runnerParentAuthorized(task)
}

func subtleTokenMismatch(left, right string) bool {
	return len(left) != len(right) || subtle.ConstantTimeCompare([]byte(left), []byte(right)) != 1
}

func validateLockedRunnerJob(record *model.JobInfo, auth *runnerAuthorization, parentStatus config.Status) error {
	if record == nil || auth == nil || !runnerJobStatusAuthorized(record, parentStatus) || record.Type != string(config.JobEval) ||
		record.WorkspaceID != auth.job.WorkspaceID || record.TaskID != auth.job.TaskID || record.ExecutionKey == nil || auth.job.ExecutionKey == nil ||
		*record.ExecutionKey != *auth.job.ExecutionKey || record.EvaluationInfo != auth.job.EvaluationInfo || record.RunGeneration != auth.job.RunGeneration || record.Attempt != auth.job.Attempt {
		return bcode.ErrUnauthorized
	}
	if workflowjob.ValidateInstantJobRetryExecution(record, auth.liveJob) != nil {
		return bcode.ErrUnauthorized
	}
	return nil
}

func runnerJobStatusAuthorized(record *model.JobInfo, parentStatus config.Status) bool {
	if record == nil {
		return false
	}
	if !terminal(config.Status(record.Status)) {
		return true
	}
	// Workflow cleanup ownership may expire before Kubernetes finishes deleting
	// the runner Pod. Keep the exact claimed attempt authorized only while its
	// durable cancellation cleanup intent remains pending.
	return parentStatus == config.StatusCancelled && workflowjob.IsCancelledJobCleanupPending(record)
}

func (s *Service) applyRunnerEvent(ctx context.Context, store datastore.DataStore, auth *runnerAuthorization, state *evaluationRunnerState, event RunnerEvent) error {
	switch event.Kind {
	case "phase":
		if runnerPhaseOrder(event.Phase) < runnerPhaseOrder(state.Phase) {
			return ErrRunnerConflict
		}
		state.Phase = event.Phase
	case "progress":
		if state.Progress != nil && (event.Progress.CompletedTrials < state.Progress.CompletedTrials || event.Progress.TotalTrials < state.Progress.TotalTrials) {
			return ErrRunnerConflict
		}
		progress := *event.Progress
		state.Progress = &progress
	case "terminal":
		artifact := &model.JobArtifact{ID: event.Terminal.ArtifactID}
		if err := store.Get(ctx, artifact); err != nil {
			if errors.Is(err, datastore.ErrRecordNotExist) {
				return ErrRunnerConflict
			}
			return err
		}
		var summary struct {
			CollectionComplete *bool  `json:"collectionComplete"`
			ExecutionStatus    string `json:"executionStatus"`
		}
		if artifact.WorkspaceID != auth.task.WorkspaceID || artifact.TaskID != auth.task.TaskID || auth.job.ExecutionKey == nil || artifact.ExecutionKey != *auth.job.ExecutionKey || artifact.Kind != artifacts.KindSource || artifact.Expired || artifact.Digest != event.Terminal.ArtifactDigest ||
			json.Unmarshal(artifact.Summary, &summary) != nil || summary.CollectionComplete == nil || *summary.CollectionComplete != *event.Terminal.CollectionComplete ||
			(summary.ExecutionStatus != "succeeded" && summary.ExecutionStatus != "failed") ||
			(event.Terminal.Outcome == "succeeded" && (!*summary.CollectionComplete || summary.ExecutionStatus != "succeeded")) {
			klog.InfoS("evaluation terminal does not match persisted result", "taskID", auth.task.TaskID, "outcome", event.Terminal.Outcome)
			return ErrRunnerConflict
		}
		state.Terminal = &RunnerTerminalStatus{
			Outcome: event.Terminal.Outcome, ExitCode: event.Terminal.ExitCode, Signal: event.Terminal.Signal,
			ArtifactID: artifact.ID, ArtifactDigest: artifact.Digest, CollectionComplete: *summary.CollectionComplete,
			Reason: event.Terminal.Reason, Message: event.Terminal.Message,
		}
	}
	return nil
}

func runnerStatus(record *model.JobInfo, now time.Time) (*RunnerStatus, error) {
	state, _, err := decodeRunnerState(record)
	if err != nil || state == nil {
		return nil, err
	}
	lastHeartbeat := state.LastHeartbeatAt
	lastReceived := state.LastReceivedAt
	if lastReceived.IsZero() {
		lastReceived = state.ClaimedAt
	}
	status := &RunnerStatus{
		Phase: state.Phase, Sequence: state.LastSequence, LastHeartbeatAt: lastHeartbeat,
		Stale:    state.Terminal == nil && !now.Before(lastReceived.Add(RunnerStaleAfter)),
		Progress: state.Progress, Terminal: state.Terminal,
	}
	return status, nil
}

func latestRunnerStatus(ctx context.Context, store datastore.DataStore, records []*model.JobInfo) (*RunnerStatus, error) {
	var latest *model.JobInfo
	for _, record := range records {
		if record == nil || record.Type != string(config.JobEval) || record.ExecutionKey == nil {
			continue
		}
		if latest == nil || record.RunGeneration > latest.RunGeneration ||
			(record.RunGeneration == latest.RunGeneration && record.Attempt > latest.Attempt) ||
			(record.RunGeneration == latest.RunGeneration && record.Attempt == latest.Attempt && record.ID > latest.ID) {
			latest = record
		}
	}
	if latest == nil {
		return nil, nil
	}
	if latest.InternalInfo == "" {
		return nil, nil
	}
	clock, ok := store.(datastore.DatabaseClock)
	if !ok {
		return nil, fmt.Errorf("runner status requires database clock")
	}
	now, err := clock.CurrentDatabaseTime(ctx)
	if err != nil {
		return nil, err
	}
	return runnerStatus(latest, now)
}

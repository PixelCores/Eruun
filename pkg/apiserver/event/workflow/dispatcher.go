package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

// Worker resilience defaults are defined in workflow/config/runtime.go.

// TaskDispatch is the minimal payload for dispatching a workflow task to a worker.
type TaskDispatch struct {
	Version       int    `json:"version"`
	TaskID        string `json:"taskId"`
	WorkflowID    string `json:"workflowId"`
	ProjectID     string `json:"projectId"`
	AppID         string `json:"appId"`
	RunGeneration uint64 `json:"runGeneration"`
	RunToken      string `json:"runToken"`
}

const taskDispatchVersion = 2

func MarshalTaskDispatch(t TaskDispatch) ([]byte, error) {
	if err := validateTaskDispatch(t); err != nil {
		return nil, err
	}
	return json.Marshal(t)
}

func UnmarshalTaskDispatch(b []byte) (TaskDispatch, error) {
	var t TaskDispatch
	if err := json.Unmarshal(b, &t); err != nil {
		return TaskDispatch{}, err
	}
	return t, validateTaskDispatch(t)
}

func validateTaskDispatch(t TaskDispatch) error {
	if t.Version != taskDispatchVersion || strings.TrimSpace(t.TaskID) == "" || t.RunGeneration == 0 || strings.TrimSpace(t.RunToken) == "" {
		return fmt.Errorf("invalid workflow dispatch envelope")
	}
	return nil
}

// Dispatcher scans waiting tasks and publishes dispatch messages.
func (w *Workflow) Dispatcher(ctx context.Context) {
	ticker := time.NewTicker(w.dispatchPollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			klog.V(3).Info("workflow dispatcher stopped: context cancelled")
			return
		case <-ticker.C:
		}

		waitingTasks, err := w.waitingTasks(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			klog.Errorf("list waiting workflow tasks failed: %v", err)
			continue
		}
		if len(waitingTasks) == 0 {
			continue
		}
		for _, task := range waitingTasks {
			if ctx.Err() != nil {
				return
			}
			w.claimAndProcessTask(ctx, task, func(procCtx context.Context, queuedTask *model.WorkflowQueue) error {
				payload := TaskDispatch{
					Version:       taskDispatchVersion,
					TaskID:        queuedTask.TaskID,
					WorkflowID:    queuedTask.WorkflowID,
					ProjectID:     queuedTask.ProjectID,
					AppID:         queuedTask.AppID,
					RunGeneration: queuedTask.RunGeneration,
					RunToken:      queuedTask.RunToken,
				}
				b, err := MarshalTaskDispatch(payload)
				if err != nil {
					return fmt.Errorf("marshal task dispatch: %w", err)
				}
				id, err := w.enqueueDispatch(procCtx, b)
				if err != nil {
					return fmt.Errorf("enqueue task dispatch: %w", err)
				}
				klog.Infof("dispatched task: %s, streamID: %s", queuedTask.TaskID, id)
				return nil
			})
		}
	}
}

// StartWorker subscribes to task dispatch topic and executes tasks.
// It implements resilient behavior: by default (max failures = 0), it retries indefinitely
// with exponential backoff instead of exiting on transient errors.
// consumerCtx controls message intake; executionCtx owns already-started workflow controllers.
func (w *Workflow) StartWorker(consumerCtx, executionCtx context.Context, errChan chan error, ready, stopped func()) {
	if consumerCtx == nil {
		consumerCtx = context.Background()
	}
	if executionCtx == nil {
		executionCtx = consumerCtx
	}

	group := w.consumerGroup()
	consumer := w.consumerName()

	workerRun := newWorkflowWorkerRun(executionCtx, w.workerConcurrencyLimiter())
	defer func() {
		if err := workerRun.wait(); err != nil {
			w.reportTaskError(err)
		}
	}()

	// Get config values with defaults
	backoffMin := w.workerBackoffMin()
	backoffMax := w.workerBackoffMax()
	maxReadFailures := w.workerMaxReadFailures()
	maxClaimFailures := w.workerMaxClaimFailures()

	klog.Infof("worker starting: stream=%s group=%s consumer=%s maxReadFailures=%d maxClaimFailures=%d",
		w.dispatchTopic(), group, consumer, maxReadFailures, maxClaimFailures)

	// Ensure consumer group exists once on worker start to avoid per-read overhead.
	if err := w.Queue.EnsureGroup(consumerCtx, group); err != nil {
		klog.V(4).Infof("ensure group error: %v", err)
	}
	if ready != nil {
		ready()
	}
	if stopped != nil {
		defer stopped()
	}

	staleTicker := time.NewTicker(w.workerStaleInterval())
	defer staleTicker.Stop()

	currentDelay := backoffMin
	readFailures := 0
	claimFailures := 0

	for {
		select {
		case <-consumerCtx.Done():
			klog.Info("worker shutting down due to context cancellation")
			return
		case <-staleTicker.C:
			mags, err := w.Queue.AutoClaim(consumerCtx, group, consumer, w.workerAutoClaimMinIdle(), w.workerAutoClaimCount())
			if err != nil {
				claimFailures++
				klog.Warningf("auto-claim error (consecutive: %d): %v", claimFailures, err)
				w.reportWorkerError(fmt.Errorf("auto-claim failed (%d consecutive): %w", claimFailures, err))

				// Only exit if maxClaimFailures > 0 and threshold reached
				if maxClaimFailures > 0 && claimFailures >= maxClaimFailures {
					klog.Errorf("max claim failures reached (%d), worker exiting", maxClaimFailures)
					return
				}
				continue
			}
			claimFailures = 0
			currentDelay = backoffMin
			var acknowledgements []dispatchAck
			for _, m := range mags {
				msg.MarkMessageHandlingStart(w.Queue, m.ID)
				if ack, taskID := w.processDispatchMessage(consumerCtx, workerRun, m); ack {
					acknowledgements = append(acknowledgements, dispatchAck{id: m.ID, taskID: taskID, claimed: true})
				} else {
					klog.V(4).Infof("consumer=%s left %s pending id=%s task=%s for retry", consumer, dispatchMessageLogLabel(true), m.ID, taskID)
					msg.MarkMessageHandlingDone(w.Queue, m.ID, false)
				}
			}
			w.ackDispatchMessages(consumerCtx, group, consumer, acknowledgements)
		default:
			mags, err := w.Queue.ReadGroup(consumerCtx, group, consumer, w.workerReadCount(), w.workerReadBlock())
			if err != nil {
				readFailures++
				klog.Warningf("read group error (consecutive: %d): %v", readFailures, err)
				w.reportWorkerError(fmt.Errorf("read group failed (%d consecutive): %w", readFailures, err))

				// Exponential backoff
				wait := w.workerBackoffDelay(currentDelay, backoffMin, backoffMax)
				currentDelay = wait

				select {
				case <-consumerCtx.Done():
					return
				case <-time.After(wait):
				}

				// Only exit if maxReadFailures > 0 and threshold reached
				if maxReadFailures > 0 && readFailures >= maxReadFailures {
					klog.Errorf("max read failures reached (%d), worker exiting", maxReadFailures)
					return
				}
				continue
			}
			readFailures = 0
			currentDelay = backoffMin
			var acknowledgements []dispatchAck
			for _, m := range mags {
				msg.MarkMessageHandlingStart(w.Queue, m.ID)
				if ack, taskID := w.processDispatchMessage(consumerCtx, workerRun, m); ack {
					acknowledgements = append(acknowledgements, dispatchAck{id: m.ID, taskID: taskID, claimed: false})
				} else {
					klog.V(4).Infof("consumer=%s left %s pending id=%s task=%s for retry", consumer, dispatchMessageLogLabel(false), m.ID, taskID)
					msg.MarkMessageHandlingDone(w.Queue, m.ID, false)
				}
			}
			w.ackDispatchMessages(consumerCtx, group, consumer, acknowledgements)
		}
	}
}

func (w *Workflow) claimAndProcessTask(ctx context.Context, task *model.WorkflowQueue, processor func(context.Context, *model.WorkflowQueue) error) {
	var queuedTask *model.WorkflowQueue
	queuedTask, claimed, err := repository.ClaimWorkflowTaskForDispatch(ctx, w.Store, task, w.workflowLeaseDuration())
	if err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		klog.Errorf("mark task %s queued failed: %v", task.TaskID, err)
		return
	}
	if !claimed {
		klog.V(4).Infof("task %s already claimed before mark queued", task.TaskID)
		return
	}
	if err := processor(ctx, queuedTask); err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		klog.Errorf("process task %s failed: %v", task.TaskID, err)
		reverted, revertErr := repository.ReleaseWorkflowDispatchClaim(ctx, w.Store, queuedTask, "dispatch enqueue failed")
		if revertErr != nil {
			if errors.Is(revertErr, context.Canceled) || ctx.Err() != nil {
				return
			}
			klog.Errorf("revert task %s status to waiting failed: %v", task.TaskID, revertErr)
		} else if !reverted {
			klog.V(4).Infof("task %s status already changed before revert", task.TaskID)
		}
	}
}

func (w *Workflow) ackMessage(ctx context.Context, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return w.Queue.Ack(ctx, group, ids...)
}

type dispatchAck struct {
	id      string
	taskID  string
	claimed bool
}

func dispatchMessageLogLabel(claimed bool) string {
	if claimed {
		return "claimed message"
	}
	return "message"
}

func (w *Workflow) ackDispatchMessages(ctx context.Context, group, consumer string, acks []dispatchAck) {
	if len(acks) == 0 {
		return
	}
	ids := make([]string, 0, len(acks))
	for _, ack := range acks {
		ids = append(ids, ack.id)
	}
	if err := w.ackMessage(ctx, group, ids...); err != nil {
		for _, ack := range acks {
			msg.MarkMessageHandlingDone(w.Queue, ack.id, false)
			klog.Errorf("failed to ack %s id=%s task=%s: %v", dispatchMessageLogLabel(ack.claimed), ack.id, ack.taskID, err)
		}
		return
	}
	for _, ack := range acks {
		msg.MarkMessageHandlingDone(w.Queue, ack.id, true)
		klog.Infof("consumer=%s acked %s id=%s task=%s", consumer, dispatchMessageLogLabel(ack.claimed), ack.id, ack.taskID)
	}
}

func (w *Workflow) workerBackoffDelay(current, min, max time.Duration) time.Duration {
	if current < min {
		return min
	}
	next := current * 2
	if next > max {
		return max
	}
	return next
}

func (w *Workflow) reportWorkerError(err error) {
	if err == nil {
		return
	}
	w.reportTaskError(err)
}

// processDispatchMessage processes a single dispatch message.
// Most failures are pass/fail (log + ack). The database execution claim is the
// authoritative ownership fence; duplicate messages that lose that claim are safe
// to acknowledge. Context cancellation leaves the message pending for recovery.
func (w *Workflow) processDispatchMessage(ctx context.Context, workerRun *workflowWorkerRun, m msg.Message) (bool, string) {
	td, err := UnmarshalTaskDispatch(m.Payload)
	if err != nil {
		// Parse error: log only non-sensitive metadata and ack to prevent blocking.
		// A malformed envelope may still contain a valid run token or user input.
		klog.ErrorS(err, "decode workflow dispatch failed", "messageID", m.ID, "payloadBytes", len(m.Payload))
		return true, ""
	}

	task, err := repository.TaskByID(ctx, w.Store, td.TaskID)
	if err != nil {
		if isContextCancellationError(err) {
			klog.V(4).Infof("load task %s interrupted by context cancellation, keep message pending for retry", td.TaskID)
			return false, td.TaskID
		}
		if errors.Is(err, datastore.ErrRecordNotExist) {
			klog.V(4).Infof("discard workflow dispatch for missing task: taskID=%s", td.TaskID)
			return true, td.TaskID
		}
		// Preserve messages across transient datastore failures.
		klog.Errorf("load task %s failed: %v", td.TaskID, err)
		return false, td.TaskID
	}
	if task.Status == config.StatusCancelled && strings.EqualFold(strings.TrimSpace(task.CancelSource), config.CancelSourceUser) {
		klog.V(4).Infof("skip cancelled workflow task by user: taskID=%s", task.TaskID)
		return true, td.TaskID
	}
	if task.RunToken != td.RunToken || task.RunGeneration != td.RunGeneration {
		klog.V(4).Infof("discard stale workflow dispatch: taskID=%s generation=%d", td.TaskID, td.RunGeneration)
		return true, td.TaskID
	}
	claimedTask, claimed, claimErr := repository.ClaimWorkflowTaskForExecution(
		ctx, w.Store, td.TaskID, td.RunGeneration, td.RunToken, w.workerIdentity(), w.workflowLeaseDuration(),
	)
	if claimErr != nil {
		if isContextCancellationError(claimErr) {
			return false, td.TaskID
		}
		klog.Errorf("claim workflow task %s generation %d failed: %v", td.TaskID, td.RunGeneration, claimErr)
		return false, td.TaskID
	}
	if !claimed {
		return true, td.TaskID
	}
	if err := w.runClaimedWorkflowTask(ctx, workerRun, claimedTask, 1); err != nil {
		if isContextCancellationError(err) {
			return false, td.TaskID
		}
		klog.Errorf("run claimed task %s failed: %v", td.TaskID, err)
	}
	return true, td.TaskID
}

func isContextCancellationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (w *Workflow) dispatchTopic() string {
	prefix := ""
	if w.Cfg != nil {
		prefix = w.Cfg.Messaging.ChannelPrefix
	}
	return workflowconfig.DispatchTopic(prefix)
}

func (w *Workflow) consumerGroup() string { return config.WorkflowWorkerQueueGroup }
func (w *Workflow) consumerName() string {
	if w.Cfg != nil {
		return w.Cfg.LeaderConfig.ID
	}
	return "worker"
}

func (w *Workflow) workerIdentity() string {
	identity := strings.TrimSpace(w.consumerName())
	if identity == "" {
		return "worker"
	}
	return identity
}

func (w *Workflow) delayConsumerName() string {
	consumer := w.consumerName()
	if consumer == "" {
		return "delay-dispatcher"
	}
	return fmt.Sprintf("%s-delay", consumer)
}

func (w *Workflow) resultConsumerName() string {
	consumer := w.consumerName()
	if consumer == "" {
		return "result-dispatcher"
	}
	return fmt.Sprintf("%s-result", consumer)
}

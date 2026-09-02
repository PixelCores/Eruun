package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

var errDelayDispatchNoRetry = errors.New("delay dispatch no retry")

const (
	delayRecoveryPollInterval = config.DefaultDispatchPollInterval
	delayRecoveryBatchSize    = 100
)

type delayItem struct {
	executeAt int64
	attempts  int
	msgID     string
	key       string
	payload   *DelayJobPayload
}

type DelayDispatcher struct {
	queue             msg.Queue
	client            kubernetes.Interface
	store             datastore.DataStore
	group             string
	consumer          string
	readCount         int
	readBlock         time.Duration
	autoClaimInterval time.Duration
	autoClaimIdle     time.Duration
	autoClaimCount    int
	recoveryInterval  time.Duration
	recoveryBeforeID  int
	backoffMin        time.Duration
	backoffMax        time.Duration
	mu                sync.Mutex
	pending           map[string]struct{}
	items             []*delayItem
	wake              chan struct{}
	ackFailures       atomic.Int64
	ensureFailures    atomic.Int64
}

func NewDelayDispatcher(queue msg.Queue, client kubernetes.Interface, store datastore.DataStore, group, consumer string) *DelayDispatcher {
	return &DelayDispatcher{
		queue:             queue,
		client:            client,
		store:             store,
		group:             group,
		consumer:          consumer,
		readCount:         config.DefaultWorkerReadCount,
		readBlock:         config.DefaultWorkerReadBlock,
		autoClaimInterval: config.DefaultWorkerStaleInterval,
		autoClaimIdle:     config.DefaultWorkerAutoClaimIdle,
		autoClaimCount:    config.DefaultWorkerAutoClaimCount,
		recoveryInterval:  delayRecoveryPollInterval,
		backoffMin:        config.DefaultWorkerBackoffMin,
		backoffMax:        config.DefaultWorkerBackoffMax,
		pending:           make(map[string]struct{}),
		wake:              make(chan struct{}, 1),
	}
}

func (d *DelayDispatcher) Start(ctx context.Context) {
	if !d.prepare(ctx) {
		return
	}
	go d.runLoops(ctx)
}

func (d *DelayDispatcher) Run(ctx context.Context) {
	if !d.prepare(ctx) {
		return
	}
	d.runLoops(ctx)
}

func (d *DelayDispatcher) prepare(ctx context.Context) bool {
	if d == nil {
		return false
	}
	if d.client == nil || d.store == nil {
		klog.ErrorS(fmt.Errorf("client or store is nil"), "delay dispatcher dependencies missing", "clientNil", d.client == nil, "storeNil", d.store == nil)
		return false
	}
	if d.group == "" {
		d.group = config.DelayQueueGroup
	}
	if d.consumer == "" {
		d.consumer = "delay-dispatcher"
	}
	if d.queue == nil {
		klog.InfoS("delay queue unavailable; database recovery remains active")
		return true
	}
	if err := d.queue.EnsureGroup(ctx, d.group); err != nil {
		failures := d.ensureFailures.Add(1)
		klog.ErrorS(err, "delay dispatcher ensure group failed", "group", d.group, "failureCount", failures)
	}
	return true
}

func (d *DelayDispatcher) runLoops(ctx context.Context) {
	var wg sync.WaitGroup
	if d.queue != nil {
		wg.Add(2)
		go func() {
			defer wg.Done()
			d.readLoop(ctx)
		}()
		go func() {
			defer wg.Done()
			d.claimLoop(ctx)
		}()
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		d.scheduleLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		d.recoveryLoop(ctx)
	}()
	wg.Wait()
}

func (d *DelayDispatcher) readLoop(ctx context.Context) {
	currentDelay := d.backoffMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		messages, err := d.queue.ReadGroup(ctx, d.group, d.consumer, d.readCount, d.readBlock)
		if err != nil {
			wait := d.backoffDelay(currentDelay)
			currentDelay = wait
			klog.ErrorS(err, "delay dispatcher read failed", "group", d.group, "consumer", d.consumer, "retryAfter", wait)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			continue
		}
		currentDelay = d.backoffMin
		for _, m := range messages {
			msg.MarkMessageHandlingStart(d.queue, m.ID)
			d.handleMessage(ctx, m)
		}
	}
}

func (d *DelayDispatcher) claimLoop(ctx context.Context) {
	ticker := time.NewTicker(d.autoClaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		messages, err := d.queue.AutoClaim(ctx, d.group, d.consumer, d.autoClaimIdle, d.autoClaimCount)
		if err != nil {
			klog.ErrorS(err, "delay dispatcher auto-claim failed", "group", d.group, "consumer", d.consumer)
			continue
		}
		for _, m := range messages {
			msg.MarkMessageHandlingStart(d.queue, m.ID)
			d.handleMessage(ctx, m)
		}
	}
}

func (d *DelayDispatcher) handleMessage(ctx context.Context, message msg.Message) {
	if message.ID == "" {
		return
	}
	if len(message.Payload) == 0 {
		d.ackMessage(ctx, message.ID, "empty_payload", true)
		return
	}
	payload, err := d.decodePayload(message.Payload)
	if err != nil {
		klog.ErrorS(err, "delay dispatcher decode payload failed", "msgID", message.ID)
		d.ackMessage(ctx, message.ID, "decode_payload_failed", true)
		return
	}
	if payload.Job == nil {
		klog.ErrorS(fmt.Errorf("job is nil"), "delay dispatcher payload missing job", "msgID", message.ID, "taskID", payload.TaskID)
		d.ackMessage(ctx, message.ID, "missing_job", true)
		return
	}
	executeAt := payload.ExecuteAt
	if executeAt <= 0 {
		executeAt = time.Now().Unix()
	}
	item := &delayItem{
		executeAt: executeAt,
		msgID:     message.ID,
		key:       payload.ExecutionKey,
		payload:   payload,
	}
	if !d.addPending(item) {
		// The existing item owns dispatch; leave this delivery unacked but reclaimable.
		msg.MarkMessageHandlingDone(d.queue, message.ID, false)
		return
	}
	d.notify()
}

func (d *DelayDispatcher) scheduleLoop(ctx context.Context) {
	for {
		item, wait := d.nextItem()
		if item == nil {
			select {
			case <-ctx.Done():
				return
			case <-d.wake:
				continue
			}
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-d.wake:
			timer.Stop()
			d.requeue(item)
			continue
		case <-timer.C:
		}
		if err := d.dispatch(ctx, item); err != nil {
			if errors.Is(err, errDelayDispatchNoRetry) {
				klog.ErrorS(err, "delay dispatcher dispatch failed without retry", "msgID", item.msgID, "attempts", item.attempts)
				d.finish(ctx, item)
				continue
			}
			klog.ErrorS(err, "delay dispatcher dispatch failed", "msgID", item.msgID, "attempts", item.attempts)
			item.attempts++
			item.executeAt = time.Now().Add(d.retryDelay(item.attempts)).Unix()
			d.requeue(item)
			continue
		}
		d.finish(ctx, item)
	}
}

func (d *DelayDispatcher) recoveryLoop(ctx context.Context) {
	interval := d.recoveryInterval
	if interval <= 0 {
		interval = delayRecoveryPollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := d.recoverDueCheckpoints(ctx); err != nil {
			klog.ErrorS(err, "delay dispatcher database recovery failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *DelayDispatcher) recoverDueCheckpoints(ctx context.Context) error {
	if d == nil || d.store == nil {
		return fmt.Errorf("recover delay checkpoints: datastore is nil")
	}
	now := time.Now().Unix()
	opts := &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{
			In: []datastore.InQueryOption{
				{Key: "status", Values: []string{string(config.StatusDistributed)}},
				{Key: "delay_state", Values: []string{string(config.JobDelayStatePending)}},
			},
			LessThan: []datastore.ComparisonQueryOption{{Key: "delay_execute_at", Value: now + 1}},
		},
		Page:     1,
		PageSize: delayRecoveryBatchSize,
		SortBy: []datastore.SortOption{
			{Key: "id", Order: datastore.SortOrderDescending},
		},
	}
	// Descending IDs advance past retries without letting new arrivals extend this pass.
	if d.recoveryBeforeID > 0 {
		opts.FilterOptions.LessThan = append(opts.FilterOptions.LessThan,
			datastore.ComparisonQueryOption{Key: "id", Value: d.recoveryBeforeID})
	}
	entities, err := d.store.List(ctx, &model.JobInfo{}, opts)
	if err != nil {
		return fmt.Errorf("list due delay checkpoints: %w", err)
	}
	var recoveryErrs []error
	for _, entity := range entities {
		record, ok := entity.(*model.JobInfo)
		if !ok || record == nil {
			recoveryErrs = append(recoveryErrs, datastore.ErrEntityInvalid)
			continue
		}
		d.recoveryBeforeID = record.ID
		payload, err := d.decodePayload([]byte(record.DelayPayload))
		if err != nil {
			recoveryErrs = append(recoveryErrs, fmt.Errorf("decode delay checkpoint %d: %w", record.ID, err))
			continue
		}
		item := &delayItem{
			executeAt: payload.ExecuteAt,
			key:       payload.ExecutionKey,
			payload:   payload,
		}
		if d.addPending(item) {
			d.notify()
		}
	}
	if len(entities) < delayRecoveryBatchSize {
		d.recoveryBeforeID = 0
	}
	return errors.Join(recoveryErrs...)
}

func (d *DelayDispatcher) decodePayload(raw []byte) (*DelayJobPayload, error) {
	var payload DelayJobPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if err := validateDelayJobPayload(&payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (d *DelayDispatcher) nextItem() (*delayItem, time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.items) == 0 {
		return nil, 0
	}
	item := d.items[0]
	wait := time.Until(time.Unix(item.executeAt, 0))
	if wait < 0 {
		wait = 0
	}
	d.items = d.items[1:]
	return item, wait
}

func (d *DelayDispatcher) addPending(item *delayItem) bool {
	if item == nil {
		return false
	}
	key := d.itemKey(item)
	if key == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.pending[key]; exists {
		return false
	}
	d.pending[key] = struct{}{}
	d.items = append(d.items, item)
	d.sortItems()
	return true
}

func (d *DelayDispatcher) requeue(item *delayItem) {
	if item == nil {
		return
	}
	d.mu.Lock()
	d.items = append(d.items, item)
	d.sortItems()
	d.mu.Unlock()
	// scheduleLoop immediately recalculates after requeueing. Waking it here would
	// leave a token that causes the loop to requeue and wake itself repeatedly.
}

func (d *DelayDispatcher) finish(ctx context.Context, item *delayItem) {
	if item == nil {
		return
	}
	if err := d.markDelayCheckpointDispatched(ctx, item.payload); err != nil {
		item.attempts++
		item.executeAt = time.Now().Add(d.retryDelay(item.attempts)).Unix()
		d.requeue(item)
		return
	}
	if err := d.ackMessage(ctx, item.msgID, "dispatch_finished", false); err != nil {
		item.attempts++
		item.executeAt = time.Now().Add(d.retryDelay(item.attempts)).Unix()
		d.requeue(item)
		return
	}
	d.mu.Lock()
	delete(d.pending, d.itemKey(item))
	d.mu.Unlock()
}

func (d *DelayDispatcher) itemKey(item *delayItem) string {
	if item == nil {
		return ""
	}
	if key := strings.TrimSpace(item.key); key != "" {
		return key
	}
	if item.payload != nil {
		if key := strings.TrimSpace(item.payload.ExecutionKey); key != "" {
			return key
		}
	}
	return strings.TrimSpace(item.msgID)
}

func (d *DelayDispatcher) ackMessage(ctx context.Context, msgID, reason string, releaseOnFailure bool) error {
	if d == nil || d.queue == nil || msgID == "" {
		return nil
	}
	if err := d.queue.Ack(ctx, d.group, msgID); err != nil {
		if releaseOnFailure {
			msg.MarkMessageHandlingDone(d.queue, msgID, false)
		}
		failures := d.ackFailures.Add(1)
		klog.ErrorS(err, "delay dispatcher ack failed", "group", d.group, "msgID", msgID, "reason", reason, "failureCount", failures)
		return err
	}
	msg.MarkMessageHandlingDone(d.queue, msgID, true)
	klog.V(4).InfoS("delay dispatcher ack succeeded", "group", d.group, "msgID", msgID, "reason", reason)
	return nil
}

func (d *DelayDispatcher) dispatch(ctx context.Context, item *delayItem) error {
	if item == nil || item.payload == nil || item.payload.Job == nil {
		return fmt.Errorf("delay item is nil")
	}
	if err := validateDelayJobPayload(item.payload); err != nil {
		return errors.Join(errDelayDispatchNoRetry, err)
	}
	current, err := d.delayExecutionCurrent(ctx, item.payload)
	if err != nil {
		return err
	}
	if !current {
		return nil
	}
	jobObj := item.payload.Job.DeepCopy()
	namespace := item.payload.Namespace
	if namespace == "" {
		namespace = jobObj.Namespace
	}
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	jobObj.Namespace = namespace
	if jobObj.Name == "" {
		return fmt.Errorf("delay job name is empty")
	}
	stampJobExecutionIdentity(&model.JobTask{
		TaskID:        item.payload.TaskID,
		ExecutionKey:  item.payload.ExecutionKey,
		RunGeneration: item.payload.RunGeneration,
	}, jobObj)

	jobType := config.JobDeployScheduled
	if item.payload.JobType != "" {
		jobType = config.JobType(item.payload.JobType)
	}
	resultPayload := newJobResultPayloadFromDelay(item.payload, jobObj)
	if resultPayload != nil {
		existingOutbox, err := getJobResultOutboxByPayload(ctx, d.store, resultPayload)
		switch {
		case err == nil:
			return d.resumeDelayedResultOutbox(ctx, existingOutbox)
		case !errors.Is(err, datastore.ErrRecordNotExist):
			return err
		}
	}
	if resultPayload != nil {
		current, err := d.delayExecutionCurrent(ctx, item.payload)
		if err != nil {
			return err
		}
		if !current {
			return nil
		}
		existingJob, exists, err := jobExists(ctx, d.client, namespace, jobObj.Name)
		if err != nil {
			return err
		}
		if exists && !jobResultMatchesExecutionIdentity(resultPayload, existingJob) {
			return d.failDelayedJobIdentityMismatch(ctx, resultPayload, existingJob)
		}
	}

	action, err := applyJobRunPolicy(ctx, d.client, d.store, jobObj, jobType)
	if err != nil {
		if _, ok := ExtractStatusError(err); ok {
			if resultPayload != nil {
				updateJobInfoStatus(ctx, d.store, resultPayload, statusFromError(err), jobErrorMessage(err, ""), 0, 0, "")
			}
			return fmt.Errorf("%w: %v", errDelayDispatchNoRetry, err)
		}
		return err
	}
	if action == runPolicyActionSkip {
		if resultPayload != nil {
			updateJobInfoStatus(ctx, d.store, resultPayload, config.StatusSkipped, "", 0, 0, "")
		}
		return nil
	}

	liveJob, _, err := createOrUpdateResource(ctx, func(ctx context.Context) (*batchv1.Job, error) {
		return d.client.BatchV1().Jobs(namespace).Get(ctx, jobObj.Name, metav1.GetOptions{})
	}, func(ctx context.Context) (*batchv1.Job, error) {
		return d.client.BatchV1().Jobs(namespace).Create(ctx, jobObj, metav1.CreateOptions{})
	}, func(_ context.Context, existing *batchv1.Job) error {
		if jobResultMatchesExecutionIdentity(resultPayload, existing) {
			return nil
		}
		return fmt.Errorf("existing delayed job %s/%s belongs to another execution", namespace, jobObj.Name)
	}, k8serrors.IsNotFound, k8serrors.IsAlreadyExists)
	if err != nil {
		if errors.Is(err, errDelayDispatchNoRetry) {
			return err
		}
		if resultPayload != nil {
			return d.handleDelayedJobCreateError(ctx, resultPayload, err)
		}
		return err
	}
	if resultPayload == nil {
		return nil
	}
	if !jobResultMatchesExecutionIdentity(resultPayload, liveJob) {
		return d.failDelayedJobIdentityMismatch(ctx, resultPayload, liveJob)
	}
	return d.ensureDelayedResultOutboxPending(ctx, resultPayload)
}

func (d *DelayDispatcher) delayExecutionCurrent(ctx context.Context, payload *DelayJobPayload) (bool, error) {
	if payload == nil || payload.RunGeneration == 0 {
		return true, nil
	}
	taskID := strings.TrimSpace(payload.TaskID)
	if taskID == "" {
		return false, fmt.Errorf("%w: delayed execution generation requires task ID", errDelayDispatchNoRetry)
	}
	if d == nil || d.store == nil {
		return false, fmt.Errorf("load workflow task for delayed job: datastore is nil")
	}
	committed, settled, err := d.delayedExecutionCommitted(ctx, payload)
	if err != nil {
		return false, err
	}
	if settled {
		return false, nil
	}
	if committed {
		return true, nil
	}
	task, err := repository.TaskByID(ctx, d.store, taskID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			klog.InfoS("discard delayed job for missing workflow task", "taskID", taskID, "runGeneration", payload.RunGeneration)
			return false, nil
		}
		return false, fmt.Errorf("load workflow task for delayed job: %w", err)
	}
	if task.RunGeneration != payload.RunGeneration {
		klog.InfoS("discard stale delayed job generation", "taskID", taskID, "payloadGeneration", payload.RunGeneration, "currentGeneration", task.RunGeneration)
		return false, nil
	}
	payloadToken := strings.TrimSpace(payload.RunToken)
	taskToken := strings.TrimSpace(task.RunToken)
	if taskToken == "" || (payloadToken != "" && taskToken != payloadToken) {
		klog.InfoS("discard stale delayed job token", "taskID", taskID, "runGeneration", payload.RunGeneration)
		return false, nil
	}
	return true, nil
}

func (d *DelayDispatcher) delayedExecutionCommitted(ctx context.Context, payload *DelayJobPayload) (committed, settled bool, err error) {
	if d == nil || d.store == nil || payload == nil || strings.TrimSpace(payload.ExecutionKey) == "" || payload.RunGeneration == 0 {
		return false, false, nil
	}
	jobInfos, err := loadJobInfos(
		ctx,
		d.store,
		strings.TrimSpace(payload.TaskID),
		strings.TrimSpace(payload.JobType),
		strings.TrimSpace(payload.ServiceName),
	)
	if err != nil {
		return false, false, fmt.Errorf("load committed delayed job: %w", err)
	}
	executionKey := strings.TrimSpace(payload.ExecutionKey)
	for _, jobInfo := range jobInfos {
		if jobInfo == nil || jobInfo.ExecutionKey == nil || strings.TrimSpace(*jobInfo.ExecutionKey) != executionKey {
			continue
		}
		if jobInfo.RunGeneration != payload.RunGeneration {
			continue
		}
		status := config.Status(jobInfo.Status)
		return status == config.StatusDistributed, isSettledDelayedExecutionStatus(status), nil
	}
	return false, false, nil
}

func (d *DelayDispatcher) markDelayCheckpointDispatched(ctx context.Context, payload *DelayJobPayload) error {
	if d == nil || d.store == nil || payload == nil {
		return nil
	}
	record, err := d.findDelayCheckpoint(ctx, payload)
	if err != nil {
		return err
	}
	if record == nil || record.DelayState == "" || record.DelayState == config.JobDelayStateDispatched {
		return nil
	}
	if record.DelayState != config.JobDelayStatePending {
		return fmt.Errorf("unexpected delay checkpoint state %q", record.DelayState)
	}
	updated, err := d.store.CompareAndSwap(ctx, record, "delay_state", config.JobDelayStatePending, map[string]interface{}{
		"delay_state": string(config.JobDelayStateDispatched),
	})
	if err != nil {
		return fmt.Errorf("mark delay checkpoint dispatched: %w", err)
	}
	if updated {
		return nil
	}
	latest, err := d.findDelayCheckpoint(ctx, payload)
	if err != nil {
		return err
	}
	if latest == nil || latest.DelayState != config.JobDelayStatePending || config.Status(latest.Status) != config.StatusDistributed {
		return nil
	}
	return fmt.Errorf("mark delay checkpoint dispatched: concurrent state did not converge")
}

func (d *DelayDispatcher) findDelayCheckpoint(ctx context.Context, payload *DelayJobPayload) (*model.JobInfo, error) {
	if d == nil || d.store == nil || payload == nil {
		return nil, nil
	}
	records, err := loadJobInfos(
		ctx,
		d.store,
		strings.TrimSpace(payload.TaskID),
		strings.TrimSpace(payload.JobType),
		strings.TrimSpace(payload.ServiceName),
	)
	if err != nil {
		return nil, fmt.Errorf("load delay checkpoint: %w", err)
	}
	executionKey := strings.TrimSpace(payload.ExecutionKey)
	for _, record := range records {
		if record == nil || record.RunGeneration != payload.RunGeneration || jobInfoExecutionKey(*record) != executionKey {
			continue
		}
		return record, nil
	}
	return nil, nil
}

func isSettledDelayedExecutionStatus(status config.Status) bool {
	switch status {
	case config.StatusCompleted, config.StatusPassed, config.StatusSkipped, config.StatusFailed, config.StatusTimeout, config.StatusCancelled, config.StatusReject:
		return true
	default:
		return false
	}
}

func (d *DelayDispatcher) resumeDelayedResultOutbox(ctx context.Context, outbox *model.JobResultOutbox) error {
	if outbox == nil {
		return nil
	}
	switch outbox.State {
	case config.JobResultOutboxStateResultPending,
		config.JobResultOutboxStateResultDispatching,
		config.JobResultOutboxStateResultQueued,
		config.JobResultOutboxStateResultProcessingQueue,
		config.JobResultOutboxStateResultProcessingLocal:
		return nil
	case config.JobResultOutboxStateFailed:
		message := strings.TrimSpace(outbox.LastError)
		if message == "" {
			message = "result outbox is already marked failed"
		}
		return fmt.Errorf("%w: %s", errDelayDispatchNoRetry, message)
	default:
		return fmt.Errorf("%w: unexpected result outbox state %s", errDelayDispatchNoRetry, outbox.State)
	}
}

func (d *DelayDispatcher) ensureDelayedResultOutboxPending(ctx context.Context, payload *JobResultPayload) error {
	if payload == nil {
		return nil
	}
	outbox, err := createJobResultOutbox(ctx, d.store, payload, config.JobResultOutboxStateResultPending)
	if err != nil {
		return err
	}
	if outbox == nil || outbox.State == config.JobResultOutboxStateResultPending {
		return nil
	}
	return d.resumeDelayedResultOutbox(ctx, outbox)
}

func (d *DelayDispatcher) handleDelayedJobCreateError(ctx context.Context, payload *JobResultPayload, createErr error) error {
	if payload == nil {
		return createErr
	}
	existing, exists, err := jobExists(ctx, d.client, payload.Namespace, payload.Name)
	if err != nil {
		return createErr
	}
	if exists {
		if !jobResultMatchesExecutionIdentity(payload, existing) {
			return d.failDelayedJobIdentityMismatch(ctx, payload, existing)
		}
		return d.ensureDelayedResultOutboxPending(ctx, payload)
	}
	return createErr
}

func (d *DelayDispatcher) failDelayedJobIdentityMismatch(ctx context.Context, payload *JobResultPayload, jobObj *batchv1.Job) error {
	if payload == nil {
		return fmt.Errorf("%w: delayed job result payload is nil", errDelayDispatchNoRetry)
	}
	actualTaskID := ""
	actualExecutionKey := ""
	actualGeneration := ""
	if jobObj != nil {
		actualTaskID = strings.TrimSpace(jobObj.Annotations[config.AnnotationJobTaskID])
		actualExecutionKey = strings.TrimSpace(jobObj.Annotations[config.AnnotationJobExecutionKey])
		actualGeneration = strings.TrimSpace(jobObj.Annotations[config.AnnotationJobRunGeneration])
	}
	message := fmt.Sprintf(
		"job %s/%s belongs to another execution: task %q execution %q generation %q; expected task %q execution %q generation %d",
		payload.Namespace, payload.Name,
		actualTaskID, actualExecutionKey, actualGeneration,
		strings.TrimSpace(payload.TaskID), strings.TrimSpace(payload.ExecutionKey), payload.RunGeneration,
	)
	if err := updateJobInfoStatus(ctx, d.store, payload, config.StatusFailed, message, 0, time.Now().Unix(), ""); err != nil && !errors.Is(err, errResultDispatchNoRetry) {
		return err
	}
	return fmt.Errorf("%w: %s", errDelayDispatchNoRetry, message)
}

func (d *DelayDispatcher) notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *DelayDispatcher) backoffDelay(current time.Duration) time.Duration {
	if current <= 0 {
		current = d.backoffMin
	}
	next := current * 2
	if next > d.backoffMax {
		next = d.backoffMax
	}
	if next < d.backoffMin {
		next = d.backoffMin
	}
	return next
}

func (d *DelayDispatcher) retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		return d.backoffMin
	}
	delay := d.backoffMin
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= d.backoffMax {
			return d.backoffMax
		}
	}
	if delay < d.backoffMin {
		return d.backoffMin
	}
	return delay
}

func (d *DelayDispatcher) sortItems() {
	if len(d.items) < 2 {
		return
	}
	sort.Slice(d.items, func(i, j int) bool {
		return d.items[i].executeAt < d.items[j].executeAt
	})
}

package job

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

const (
	resultOutboxPollInterval  = config.DefaultDispatchPollInterval
	resultOutboxBatchSize     = config.DefaultWorkerReadCount
	resultOutboxProcessGrace  = 30 * time.Second
	resultOutboxDispatchGrace = config.DefaultWorkerAutoClaimIdle
)

var resultOutboxPersistTimeout = 5 * time.Second

type ResultOutboxDispatcher struct {
	queue         msg.Queue
	client        kubernetes.Interface
	store         datastore.DataStore
	pollInterval  time.Duration
	batchSize     int
	dispatchGrace time.Duration
}

func NewResultOutboxDispatcher(queue msg.Queue, client kubernetes.Interface, store datastore.DataStore) *ResultOutboxDispatcher {
	return &ResultOutboxDispatcher{
		queue:         queue,
		client:        client,
		store:         store,
		pollInterval:  resultOutboxPollInterval,
		batchSize:     resultOutboxBatchSize,
		dispatchGrace: resultOutboxDispatchGrace,
	}
}

func (d *ResultOutboxDispatcher) Start(ctx context.Context) {
	if !d.prepare(ctx) {
		return
	}
	go d.loop(ctx)
}

func (d *ResultOutboxDispatcher) Run(ctx context.Context) {
	if !d.prepare(ctx) {
		return
	}
	d.loop(ctx)
}

func (d *ResultOutboxDispatcher) prepare(ctx context.Context) bool {
	if d == nil {
		return false
	}
	if d.queue == nil || d.client == nil || d.store == nil {
		klog.ErrorS(fmt.Errorf("queue, client, or store is nil"), "result outbox dispatcher dependencies missing", "queueNil", d.queue == nil, "clientNil", d.client == nil, "storeNil", d.store == nil)
		return false
	}
	if err := d.recoverLocalProcessing(ctx); err != nil {
		klog.ErrorS(err, "result outbox dispatcher recover local processing failed")
	}
	return true
}

func (d *ResultOutboxDispatcher) loop(ctx context.Context) {
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		if err := d.processOnce(ctx); err != nil {
			klog.ErrorS(err, "result outbox dispatcher process failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *ResultOutboxDispatcher) processOnce(ctx context.Context) error {
	if err := d.processResultDispatching(ctx); err != nil {
		return err
	}
	if err := d.processResultProcessingLocal(ctx); err != nil {
		return err
	}
	if err := d.processResultPending(ctx); err != nil {
		return err
	}
	return nil
}

func (d *ResultOutboxDispatcher) recoverLocalProcessing(ctx context.Context) error {
	return d.recoverOutboxState(ctx, config.JobResultOutboxStateResultProcessingLocal, config.JobResultOutboxStateResultPending)
}

func (d *ResultOutboxDispatcher) recoverOutboxState(ctx context.Context, from, to config.JobResultOutboxState) error {
	for {
		outboxes, err := listJobResultOutboxesByStates(ctx, d.store, []config.JobResultOutboxState{from}, d.batchSize)
		if err != nil {
			return err
		}
		if len(outboxes) == 0 {
			return nil
		}

		updated := 0
		for _, outbox := range outboxes {
			ok, setErr := trySetJobResultOutboxState(ctx, d.store, outbox, from, to, outbox.Attempts, strings.TrimSpace(outbox.LastError), map[string]interface{}{
				"message_id": "",
			})
			if setErr != nil {
				return setErr
			}
			if ok {
				updated++
			}
		}
		if updated == 0 {
			return nil
		}
	}
}

func (d *ResultOutboxDispatcher) processResultPending(ctx context.Context) error {
	outboxes, err := listJobResultOutboxesByStates(ctx, d.store, []config.JobResultOutboxState{config.JobResultOutboxStateResultPending}, d.batchSize)
	if err != nil {
		return err
	}
	for _, outbox := range outboxes {
		if err := d.dispatchPendingOutbox(ctx, outbox); err != nil {
			return err
		}
	}
	return nil
}

func (d *ResultOutboxDispatcher) processResultDispatching(ctx context.Context) error {
	outboxes, err := listJobResultOutboxesByStates(ctx, d.store, []config.JobResultOutboxState{config.JobResultOutboxStateResultDispatching}, d.batchSize)
	if err != nil {
		return err
	}
	if len(outboxes) == 0 {
		return nil
	}

	now := time.Now()
	for _, outbox := range outboxes {
		if !d.shouldRecoverResultDispatching(outbox, now) {
			continue
		}
		message := "result dispatching exceeded recovery grace before enqueue confirmation"
		recovered, setErr := trySetJobResultOutboxState(ctx, d.store, outbox, config.JobResultOutboxStateResultDispatching, config.JobResultOutboxStateResultPending, outbox.Attempts+1, message, map[string]interface{}{
			"message_id": "",
		})
		if setErr != nil {
			return setErr
		}
		if recovered {
			klog.InfoS("result outbox recovered from dispatching to pending after grace period", "outboxID", outbox.ID, "attempts", outbox.Attempts+1)
		}
	}
	return nil
}

func (d *ResultOutboxDispatcher) processResultProcessingLocal(ctx context.Context) error {
	outboxes, err := listJobResultOutboxesByStates(ctx, d.store, []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessingLocal}, d.batchSize)
	if err != nil {
		return err
	}
	if len(outboxes) == 0 {
		return nil
	}

	now := time.Now()
	for _, outbox := range outboxes {
		if !d.shouldRecoverResultProcessingLocal(outbox, now) {
			continue
		}
		message := "result processing local exceeded recovery grace and is retried from pending"
		recovered, setErr := trySetJobResultOutboxState(ctx, d.store, outbox, config.JobResultOutboxStateResultProcessingLocal, config.JobResultOutboxStateResultPending, outbox.Attempts+1, message, map[string]interface{}{
			"message_id": "",
		})
		if setErr != nil {
			return setErr
		}
		if recovered {
			klog.InfoS("result outbox recovered from local processing to pending after grace period", "outboxID", outbox.ID, "attempts", outbox.Attempts+1)
		}
	}
	return nil
}

func (d *ResultOutboxDispatcher) shouldRecoverResultDispatching(outbox *model.JobResultOutbox, now time.Time) bool {
	if outbox == nil {
		return false
	}
	grace := d.dispatchGrace
	if grace <= 0 {
		return true
	}
	lastUpdate := outbox.UpdateTime
	if lastUpdate.IsZero() {
		lastUpdate = outbox.CreateTime
	}
	if lastUpdate.IsZero() {
		return true
	}
	return !lastUpdate.Add(grace).After(now)
}

func (d *ResultOutboxDispatcher) shouldRecoverResultProcessingLocal(outbox *model.JobResultOutbox, now time.Time) bool {
	if outbox == nil {
		return false
	}
	timeoutSeconds := outbox.TimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = int64(config.DefaultJobTaskTimeout.Seconds())
	}
	grace := time.Duration(timeoutSeconds)*time.Second + resultOutboxProcessGrace + resultOutboxPersistTimeout + d.pollInterval
	if grace <= 0 {
		grace = d.pollInterval
	}
	lastUpdate := outbox.UpdateTime
	if lastUpdate.IsZero() {
		lastUpdate = outbox.CreateTime
	}
	if lastUpdate.IsZero() {
		return true
	}
	return !lastUpdate.Add(grace).After(now)
}

func (d *ResultOutboxDispatcher) dispatchPendingOutbox(ctx context.Context, outbox *model.JobResultOutbox) error {
	if outbox == nil {
		return nil
	}
	payload := jobResultPayloadFromOutbox(outbox)
	if payload == nil {
		return markJobResultOutboxFailed(ctx, d.store, outbox, "result outbox payload is invalid")
	}

	claimed, err := trySetJobResultOutboxState(ctx, d.store, outbox, config.JobResultOutboxStateResultPending, config.JobResultOutboxStateResultDispatching, outbox.Attempts, strings.TrimSpace(outbox.LastError), map[string]interface{}{
		"message_id": "",
	})
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}

	messageID, err := enqueueResultJob(ctx, d.queue, payload)
	if err == nil {
		persistCtx, cancel := resultOutboxPersistenceContext()
		defer cancel()

		queued, setErr := trySetJobResultOutboxState(persistCtx, d.store, outbox, config.JobResultOutboxStateResultDispatching, config.JobResultOutboxStateResultQueued, outbox.Attempts, "", map[string]interface{}{
			"message_id": strings.TrimSpace(messageID),
		})
		if setErr != nil {
			return setErr
		}
		outbox.MessageID = strings.TrimSpace(messageID)
		if queued {
			outbox.State = config.JobResultOutboxStateResultQueued
			outbox.LastError = ""
		}
		return nil
	}

	requeued, setErr := trySetJobResultOutboxState(ctx, d.store, outbox, config.JobResultOutboxStateResultDispatching, config.JobResultOutboxStateResultPending, outbox.Attempts+1, fmt.Sprintf("result enqueue failed: %v", err), map[string]interface{}{
		"message_id": "",
	})
	if setErr != nil {
		return setErr
	}
	if requeued {
		klog.Warningf("result enqueue failed; outbox returned to pending outboxID=%s attempts=%d err=%v", outbox.ID, outbox.Attempts, err)
	}
	return nil
}

func enqueueResultJob(ctx context.Context, queue msg.Queue, payload *JobResultPayload) (string, error) {
	if err := validateJobResultPayload(payload); err != nil {
		return "", err
	}
	if queue == nil {
		return "", ErrResultQueueUnavailable
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal result payload: %w", err)
	}
	return queue.Enqueue(ctx, raw)
}

func buildJobResultOutbox(payload *JobResultPayload, state config.JobResultOutboxState) *model.JobResultOutbox {
	if validateJobResultPayload(payload) != nil {
		return nil
	}
	return &model.JobResultOutbox{
		ID:             jobResultOutboxID(payload),
		TaskID:         strings.TrimSpace(payload.TaskID),
		ExecutionKey:   strings.TrimSpace(payload.ExecutionKey),
		RunGeneration:  payload.RunGeneration,
		JobType:        strings.TrimSpace(payload.JobType),
		Namespace:      strings.TrimSpace(payload.Namespace),
		Name:           strings.TrimSpace(payload.Name),
		ServiceName:    strings.TrimSpace(payload.ServiceName),
		TimeoutSeconds: payload.TimeoutSeconds,
		State:          state,
	}
}

func jobResultOutboxID(payload *JobResultPayload) string {
	if validateJobResultPayload(payload) != nil {
		return ""
	}
	identity := fmt.Sprintf("%s\n%s\n%s\n%s\n%d", strings.TrimSpace(payload.TaskID), strings.TrimSpace(payload.Namespace), strings.TrimSpace(payload.Name), strings.TrimSpace(payload.ExecutionKey), payload.RunGeneration)
	sum := sha1.Sum([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func jobResultPayloadFromOutbox(outbox *model.JobResultOutbox) *JobResultPayload {
	if outbox == nil {
		return nil
	}
	payload := &JobResultPayload{
		OutboxID:       strings.TrimSpace(outbox.ID),
		TaskID:         strings.TrimSpace(outbox.TaskID),
		ExecutionKey:   strings.TrimSpace(outbox.ExecutionKey),
		RunGeneration:  outbox.RunGeneration,
		JobType:        strings.TrimSpace(outbox.JobType),
		Namespace:      strings.TrimSpace(outbox.Namespace),
		Name:           strings.TrimSpace(outbox.Name),
		ServiceName:    strings.TrimSpace(outbox.ServiceName),
		TimeoutSeconds: outbox.TimeoutSeconds,
	}
	if !isResultPayloadProcessable(payload) {
		return nil
	}
	return payload
}

func getJobResultOutboxByPayload(ctx context.Context, store datastore.DataStore, payload *JobResultPayload) (*model.JobResultOutbox, error) {
	return getJobResultOutboxByID(ctx, store, jobResultOutboxID(payload))
}

func getJobResultOutboxByID(ctx context.Context, store datastore.DataStore, id string) (*model.JobResultOutbox, error) {
	if store == nil {
		return nil, fmt.Errorf("datastore is nil")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, datastore.ErrPrimaryEmpty
	}
	outbox := &model.JobResultOutbox{ID: id}
	if err := store.Get(ctx, outbox); err != nil {
		return nil, err
	}
	return outbox, nil
}

func createJobResultOutbox(ctx context.Context, store datastore.DataStore, payload *JobResultPayload, state config.JobResultOutboxState) (*model.JobResultOutbox, error) {
	if store == nil {
		return nil, fmt.Errorf("datastore is nil")
	}
	outbox := buildJobResultOutbox(payload, state)
	if outbox == nil {
		return nil, fmt.Errorf("result payload is nil")
	}
	if err := store.Add(ctx, outbox); err != nil {
		if errors.Is(err, datastore.ErrRecordExist) {
			return getJobResultOutboxByID(ctx, store, outbox.ID)
		}
		return nil, err
	}
	return outbox, nil
}

func deleteJobResultOutbox(ctx context.Context, store datastore.DataStore, id string) error {
	if store == nil || strings.TrimSpace(id) == "" {
		return nil
	}
	err := store.Delete(ctx, &model.JobResultOutbox{ID: strings.TrimSpace(id)})
	if errors.Is(err, datastore.ErrRecordNotExist) {
		return nil
	}
	return err
}

func listJobResultOutboxesByStates(ctx context.Context, store datastore.DataStore, states []config.JobResultOutboxState, pageSize int) ([]*model.JobResultOutbox, error) {
	if store == nil {
		return nil, fmt.Errorf("datastore is nil")
	}
	values := make([]string, 0, len(states))
	for _, state := range states {
		if state == "" {
			continue
		}
		values = append(values, string(state))
	}
	if len(values) == 0 {
		return nil, nil
	}
	if pageSize <= 0 {
		pageSize = resultOutboxBatchSize
	}
	entities, err := store.List(ctx, &model.JobResultOutbox{}, &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{
			In: []datastore.InQueryOption{{Key: "state", Values: values}},
		},
		SortBy: []datastore.SortOption{
			{Key: "update_time", Order: datastore.SortOrderAscending},
			{Key: "create_time", Order: datastore.SortOrderAscending},
		},
		Page:     1,
		PageSize: pageSize,
	})
	if err != nil {
		return nil, err
	}
	outboxes := make([]*model.JobResultOutbox, 0, len(entities))
	for _, entity := range entities {
		outbox, ok := entity.(*model.JobResultOutbox)
		if !ok || outbox == nil {
			return nil, datastore.ErrEntityInvalid
		}
		outboxes = append(outboxes, outbox)
	}
	return outboxes, nil
}

func setJobResultOutboxState(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, from, to config.JobResultOutboxState, attempts int, lastError string) error {
	_, err := trySetJobResultOutboxState(ctx, store, outbox, from, to, attempts, lastError, nil)
	return err
}

func trySetJobResultOutboxState(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, from, to config.JobResultOutboxState, attempts int, lastError string, extraUpdates map[string]interface{}) (bool, error) {
	if outbox == nil {
		return false, nil
	}
	updates := map[string]interface{}{
		"state":      to,
		"attempts":   attempts,
		"last_error": strings.TrimSpace(lastError),
	}
	for key, value := range extraUpdates {
		updates[key] = value
	}
	ok, err := compareAndSwapJobResultOutbox(ctx, store, outbox, from, updates)
	if err != nil {
		return false, err
	}
	if ok {
		outbox.State = to
		outbox.Attempts = attempts
		outbox.LastError = strings.TrimSpace(lastError)
		if value, exists := updates["message_id"]; exists {
			outbox.MessageID = strings.TrimSpace(fmt.Sprint(value))
		}
	}
	return ok, nil
}

func moveQueueResultOutboxToQueued(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, lastError string, attempts int) error {
	if outbox == nil {
		return nil
	}
	_, err := trySetJobResultOutboxState(ctx, store, outbox, config.JobResultOutboxStateResultProcessingQueue, config.JobResultOutboxStateResultQueued, attempts, lastError, map[string]interface{}{
		"message_id": strings.TrimSpace(outbox.MessageID),
	})
	return err
}

func markJobResultOutboxFailed(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, message string) error {
	if outbox == nil {
		return nil
	}
	return setJobResultOutboxState(ctx, store, outbox, outbox.State, config.JobResultOutboxStateFailed, outbox.Attempts+1, message)
}

func compareAndSwapJobResultOutbox(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, from config.JobResultOutboxState, updates map[string]interface{}) (bool, error) {
	return compareAndSwapJobResultOutboxWithConditions(ctx, store, outbox, map[string]interface{}{
		"state": string(from),
	}, updates)
}

func compareAndSwapJobResultOutboxWithConditions(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("datastore is nil")
	}
	if outbox == nil || strings.TrimSpace(outbox.ID) == "" {
		return false, datastore.ErrPrimaryEmpty
	}
	entity := &model.JobResultOutbox{ID: outbox.ID}
	guardedConditions := map[string]interface{}{
		"id": strings.TrimSpace(outbox.ID),
	}
	for key, value := range conditions {
		guardedConditions[key] = value
	}
	if casStore, ok := store.(datastore.ConditionalCompareAndSwap); ok {
		return casStore.CompareAndSwapWithConditions(ctx, entity, guardedConditions, updates)
	}
	if len(guardedConditions) != 1 {
		return false, fmt.Errorf("datastore does not support multi-condition compare-and-swap")
	}
	for key, value := range guardedConditions {
		return store.CompareAndSwap(ctx, entity, key, value, updates)
	}
	return false, fmt.Errorf("compare-and-swap requires at least one condition")
}

func resultOutboxPersistenceContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), resultOutboxPersistTimeout)
}

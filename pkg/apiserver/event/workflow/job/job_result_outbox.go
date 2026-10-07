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

	"github.com/google/uuid"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

const (
	resultOutboxPollInterval      = workflowconfig.DefaultDispatchPollInterval
	resultOutboxBatchSize         = workflowconfig.DefaultWorkerReadCount
	resultOutboxProcessGrace      = 30 * time.Second
	resultOutboxHeartbeatInterval = 10 * time.Second
	resultOutboxDispatchGrace     = workflowconfig.DefaultWorkerAutoClaimIdle
)

var resultOutboxPersistTimeout = 5 * time.Second

type ResultOutboxDispatcher struct {
	queue        msg.Queue
	store        datastore.DataStore
	pollInterval time.Duration
	batchSize    int
}

func NewResultOutboxDispatcher(queue msg.Queue, store datastore.DataStore) *ResultOutboxDispatcher {
	return &ResultOutboxDispatcher{
		queue:        queue,
		store:        store,
		pollInterval: resultOutboxPollInterval,
		batchSize:    resultOutboxBatchSize,
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
	if d.queue == nil || d.store == nil {
		klog.ErrorS(fmt.Errorf("queue or store is nil"), "result outbox dispatcher dependencies missing", "queueNil", d.queue == nil, "storeNil", d.store == nil)
		return false
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
	if err := d.recoverResultOutboxes(ctx, []config.JobResultOutboxState{
		config.JobResultOutboxStateResultDispatching, config.JobResultOutboxStateResultQueued,
		config.JobResultOutboxStateResultProcessingQueue,
	}); err != nil {
		return err
	}
	return d.processResultPending(ctx)
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

func (d *ResultOutboxDispatcher) recoverResultOutboxes(ctx context.Context, states []config.JobResultOutboxState) error {
	outboxes, err := listJobResultOutboxesByStates(ctx, d.store, states, d.batchSize)
	if err != nil {
		return err
	}
	now, err := resultOutboxDatabaseTime(ctx, d.store)
	if err != nil {
		return err
	}
	for _, outbox := range outboxes {
		conditions := map[string]interface{}{"state": string(outbox.State), "message_id": outbox.MessageID, "lease_expires_at": outbox.LeaseExpiresAt}
		if outbox.LeaseExpiresAt == nil {
			// A typed nil pointer is not a nil SQL condition value.
			conditions["lease_expires_at"] = nil
			message := "active result outbox has no lease; automatic recovery rejected"
			failed, err := compareAndSwapJobResultOutboxWithConditions(ctx, d.store, outbox, conditions, map[string]interface{}{
				"state": config.JobResultOutboxStateFailed, "last_error": message,
			})
			if err != nil {
				return err
			}
			if failed {
				klog.ErrorS(errors.New(message), "reject invalid result outbox", "outboxID", outbox.ID, "state", outbox.State)
			}
			continue
		}
		if outbox.LeaseExpiresAt.After(now) {
			continue
		}
		_, err := compareAndSwapJobResultOutboxWithConditions(ctx, d.store, outbox, conditions, map[string]interface{}{
			"state": config.JobResultOutboxStateResultPending, "message_id": "", "lease_expires_at": nil,
			"attempts": outbox.Attempts + 1, "last_error": "result notification or processing exceeded recovery grace",
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func resultOutboxDatabaseTime(ctx context.Context, store datastore.DataStore) (time.Time, error) {
	clock, ok := store.(datastore.DatabaseClock)
	if !ok {
		return time.Time{}, fmt.Errorf("result outbox requires database clock")
	}
	now, err := clock.CurrentDatabaseTime(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("read result outbox database clock: %w", err)
	}
	if now.IsZero() {
		return time.Time{}, fmt.Errorf("result outbox database clock is zero")
	}
	return now.UTC(), nil
}

func (d *ResultOutboxDispatcher) dispatchPendingOutbox(ctx context.Context, outbox *model.JobResultOutbox) error {
	if outbox == nil {
		return nil
	}
	payload := jobResultPayloadFromOutbox(outbox)
	if payload == nil {
		return markJobResultOutboxFailed(ctx, d.store, outbox, "result outbox payload is invalid")
	}

	now, err := resultOutboxDatabaseTime(ctx, d.store)
	if err != nil {
		return err
	}
	deadline := now.Add(resultOutboxDispatchGrace)
	claimed, err := trySetJobResultOutboxState(ctx, d.store, outbox, config.JobResultOutboxStateResultPending, config.JobResultOutboxStateResultDispatching, outbox.Attempts, strings.TrimSpace(outbox.LastError), map[string]interface{}{
		"message_id": uuid.NewString(), "lease_expires_at": &deadline,
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
		now, clockErr := resultOutboxDatabaseTime(persistCtx, d.store)
		if clockErr != nil {
			return clockErr
		}
		deadline = now.Add(resultOutboxDispatchGrace)

		queued, setErr := trySetJobResultOutboxState(persistCtx, d.store, outbox, config.JobResultOutboxStateResultDispatching, config.JobResultOutboxStateResultQueued, outbox.Attempts, "", map[string]interface{}{
			"message_id": strings.TrimSpace(messageID), "lease_expires_at": &deadline,
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
		"message_id": "", "lease_expires_at": nil,
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
	if payload == nil || strings.TrimSpace(payload.OutboxID) == "" {
		return "", fmt.Errorf("result payload outbox ID is required")
	}
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
		// Invalid NULL active leases are rejected first, then expired leases.
		// Live processing must not hide newer expired rows.
		SortBy: []datastore.SortOption{
			{Key: "lease_expires_at", Order: datastore.SortOrderAscending},
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
		if value, exists := updates["lease_expires_at"]; exists {
			outbox.LeaseExpiresAt, _ = value.(*time.Time)
		}
	}
	return ok, nil
}

func retryResultOutbox(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, lastError string) error {
	_, err := trySetJobResultOutboxState(ctx, store, outbox, config.JobResultOutboxStateResultProcessingQueue, config.JobResultOutboxStateResultPending, outbox.Attempts+1, lastError, map[string]interface{}{"message_id": "", "lease_expires_at": nil})
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
		"state": string(from), "message_id": outbox.MessageID,
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

var errResultOutboxOwnershipLost = errors.New("result outbox ownership lost")

// Token verification and result writes share a transaction. There is no K8s I/O
// in this callback. The CAS takes the row lock before checking the DB-timed lease.
func withResultOutboxOwnership(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, persist func(datastore.DataStore, *model.JobResultOutbox) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if outbox == nil || strings.TrimSpace(outbox.ID) == "" || strings.TrimSpace(outbox.MessageID) == "" {
		return errResultOutboxOwnershipLost
	}
	transactional, ok := store.(datastore.ReadCommittedTransactional)
	if !ok {
		return fmt.Errorf("result outbox requires read committed transactions")
	}
	ctx, cancel := context.WithTimeout(ctx, resultOutboxPersistTimeout)
	defer cancel()
	return transactional.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		owned, err := compareAndSwapJobResultOutbox(ctx, tx, outbox, config.JobResultOutboxStateResultProcessingQueue, map[string]interface{}{})
		if err != nil {
			return err
		}
		if !owned {
			return errResultOutboxOwnershipLost
		}
		current, err := getJobResultOutboxByID(ctx, tx, outbox.ID)
		if err != nil {
			return err
		}
		now, err := resultOutboxDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if current.LeaseExpiresAt == nil || !current.LeaseExpiresAt.After(now) {
			return errResultOutboxOwnershipLost
		}
		return persist(tx, current)
	})
}

func renewResultOutboxLease(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox) error {
	return withResultOutboxOwnership(ctx, store, outbox, func(tx datastore.DataStore, current *model.JobResultOutbox) error {
		now, err := resultOutboxDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		deadline := now.Add(resultOutboxProcessGrace)
		renewed, err := compareAndSwapJobResultOutbox(ctx, tx, outbox, config.JobResultOutboxStateResultProcessingQueue, map[string]interface{}{"lease_expires_at": &deadline})
		if err != nil {
			return err
		}
		if !renewed {
			return errResultOutboxOwnershipLost
		}
		return nil
	})
}

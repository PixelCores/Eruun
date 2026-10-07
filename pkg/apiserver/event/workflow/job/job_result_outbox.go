package job

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

const (
	resultOutboxBatchSize         = 100
	resultOutboxProcessGrace      = 30 * time.Second
	resultOutboxHeartbeatInterval = 10 * time.Second
)

var resultOutboxPersistTimeout = 5 * time.Second

func (d *ResultDispatcher) recoverResultOutboxes(ctx context.Context) error {
	outboxes, err := listJobResultOutboxesByStates(ctx, d.store, []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessing}, d.recoveryBatchSize)
	if err != nil {
		return err
	}
	now, err := resultOutboxDatabaseTime(ctx, d.store)
	if err != nil {
		return err
	}
	for _, outbox := range outboxes {
		conditions := map[string]interface{}{"state": string(outbox.State), "claim_token": outbox.ClaimToken, "lease_expires_at": outbox.LeaseExpiresAt}
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
			"state": config.JobResultOutboxStateResultPending, "claim_token": "", "lease_expires_at": nil,
			"attempts": outbox.Attempts + 1, "last_error": "result processing lease expired",
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

// The process-local cursor bounds each scan while rotating past retries and
// concurrent claim losers. Correctness still comes from the database claim.
func listPendingResultOutboxes(ctx context.Context, store datastore.DataStore, beforeID string, limit int) ([]*model.JobResultOutbox, error) {
	opts := &datastore.ListOptions{Page: 1, PageSize: limit, SortBy: []datastore.SortOption{{Key: "id", Order: datastore.SortOrderDescending}}}
	if beforeID != "" {
		opts.LessThan = []datastore.ComparisonQueryOption{{Key: "id", Value: beforeID}}
	}
	entities, err := store.List(ctx, &model.JobResultOutbox{State: config.JobResultOutboxStateResultPending}, opts)
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
			{Key: "id", Order: datastore.SortOrderAscending},
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
		if value, exists := updates["claim_token"]; exists {
			outbox.ClaimToken = strings.TrimSpace(fmt.Sprint(value))
		}
		if value, exists := updates["lease_expires_at"]; exists {
			outbox.LeaseExpiresAt, _ = value.(*time.Time)
		}
	}
	return ok, nil
}

func retryResultOutbox(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, lastError string) error {
	_, err := trySetJobResultOutboxState(ctx, store, outbox, config.JobResultOutboxStateResultProcessing, config.JobResultOutboxStateResultPending, outbox.Attempts+1, lastError, map[string]interface{}{"claim_token": "", "lease_expires_at": nil})
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
		"state": string(from), "claim_token": outbox.ClaimToken,
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
	if outbox == nil || strings.TrimSpace(outbox.ID) == "" || strings.TrimSpace(outbox.ClaimToken) == "" {
		return errResultOutboxOwnershipLost
	}
	transactional, ok := store.(datastore.ReadCommittedTransactional)
	if !ok {
		return fmt.Errorf("result outbox requires read committed transactions")
	}
	ctx, cancel := context.WithTimeout(ctx, resultOutboxPersistTimeout)
	defer cancel()
	return transactional.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		owned, err := compareAndSwapJobResultOutbox(ctx, tx, outbox, config.JobResultOutboxStateResultProcessing, map[string]interface{}{})
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
		renewed, err := compareAndSwapJobResultOutbox(ctx, tx, outbox, config.JobResultOutboxStateResultProcessing, map[string]interface{}{"lease_expires_at": &deadline})
		if err != nil {
			return err
		}
		if !renewed {
			return errResultOutboxOwnershipLost
		}
		return nil
	})
}

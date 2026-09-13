package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func scopedDelivery(ctx context.Context, db datastore.DataStore, workspaceID, taskID, target string, lock bool) (*model.JobDelivery, error) {
	if err := requireWorkspace(workspaceID); err != nil {
		return nil, err
	}
	d := &model.JobDelivery{ID: deliveryID(workspaceID, taskID, target)}
	var err error
	if lock {
		err = locked(ctx, db, d)
	} else {
		err = db.Get(ctx, d)
	}
	if err != nil {
		return nil, err
	}
	if d.WorkspaceID != workspaceID || d.TaskID != taskID || d.Target != target {
		return nil, datastore.ErrRecordNotExist
	}
	return d, nil
}

func (s *Store) Deliveries(ctx context.Context, workspaceID, taskID string) ([]*model.JobDelivery, error) {
	if err := requireWorkspace(workspaceID); err != nil {
		return nil, err
	}
	if taskID == "" {
		return nil, fmt.Errorf("taskId is required")
	}
	records, err := s.db.List(ctx, &model.JobDelivery{WorkspaceID: workspaceID, TaskID: taskID}, nil)
	if err != nil {
		return nil, err
	}
	result := make([]*model.JobDelivery, 0, len(records))
	for _, record := range records {
		result = append(result, record.(*model.JobDelivery))
	}
	return result, nil
}

func (s *Store) Retry(ctx context.Context, workspaceID, taskID, target string) error {
	return transaction(ctx, s.db, func(tx datastore.DataStore) error {
		if err := requireWorkspace(workspaceID); err != nil {
			return err
		}
		if err := locked(ctx, tx, &model.Workspace{ID: workspaceID}); err != nil {
			return err
		}
		d, err := scopedDelivery(ctx, tx, workspaceID, taskID, target, true)
		if err != nil {
			return err
		}
		if d.State == DeliverySucceeded {
			return nil
		}
		source, err := scopedArtifact(ctx, tx, workspaceID, d.SourceID, false)
		if err != nil {
			return err
		}
		if err := sourceAvailable(ctx, tx, source); err != nil {
			return err
		}
		now, err := clock(ctx, tx)
		if err != nil {
			return err
		}
		if d.State == DeliveryRunning && d.LeaseUntil != nil && now.Before(*d.LeaseUntil) {
			return nil
		}
		_, err = tx.CompareAndSwap(ctx, d, "state", d.State, map[string]interface{}{"state": DeliveryPending, "last_error": "", "lease_token": "", "lease_until": nil})
		return err
	})
}

// ReconcilePending is called by the existing server maintenance loop. Failed
// destinations require an explicit Retry; interrupted leases can be recovered.
func (s *Store) ReconcilePending(ctx context.Context, limit int) error {
	if limit < 1 || limit > 1000 {
		return fmt.Errorf("delivery limit must be 1..1000")
	}
	now, err := clock(ctx, s.db)
	if err != nil {
		return err
	}
	pending, err := s.db.List(ctx, &model.JobDelivery{State: DeliveryPending}, &datastore.ListOptions{Page: 1, PageSize: limit, SortBy: []datastore.SortOption{{Key: "create_time", Order: datastore.SortOrderAscending}}})
	if err != nil {
		return err
	}
	recovering, err := s.db.List(ctx, &model.JobDelivery{State: DeliveryRunning}, &datastore.ListOptions{Page: 1, PageSize: limit, FilterOptions: datastore.FilterOptions{LessThan: []datastore.ComparisonQueryOption{{Key: "lease_until", Value: now}}}, SortBy: []datastore.SortOption{{Key: "lease_until", Order: datastore.SortOrderAscending}}})
	if err != nil {
		return err
	}
	records := append(pending, recovering...)
	// Resolve MinIO before its metadata-only database reference when both are ready.
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].(*model.JobDelivery).Target > records[j].(*model.JobDelivery).Target
	})
	var failures []error
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		d := record.(*model.JobDelivery)
		if err := s.deliver(ctx, d); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (s *Store) deliver(ctx context.Context, candidate *model.JobDelivery) error {
	var d *model.JobDelivery
	claimed := false
	err := transaction(ctx, s.db, func(tx datastore.DataStore) error {
		var err error
		if err := locked(ctx, tx, &model.Workspace{ID: candidate.WorkspaceID}); err != nil {
			return err
		}
		d, err = scopedDelivery(ctx, tx, candidate.WorkspaceID, candidate.TaskID, candidate.Target, true)
		if err != nil {
			return err
		}
		now, err := clock(ctx, tx)
		if err != nil {
			return err
		}
		if d.State != DeliveryPending && (d.State != DeliveryRunning || d.LeaseUntil == nil || now.Before(*d.LeaseUntil)) {
			return nil
		}
		if d.Mode == "metadata" {
			full, err := scopedDelivery(ctx, tx, d.WorkspaceID, d.TaskID, "minio", false)
			if err == nil && (full.State == DeliveryPending || full.State == DeliveryRunning) {
				return nil
			}
		}
		d.LeaseToken = uuid.NewString()
		until := now.Add(3 * time.Minute)
		d.LeaseUntil = &until
		claimed, err = tx.CompareAndSwap(ctx, d, "state", d.State, map[string]interface{}{"state": DeliveryRunning, "lease_token": d.LeaseToken, "lease_until": until, "attempts": d.Attempts + 1, "last_error": ""})
		d.State = DeliveryRunning
		return err
	})
	if errors.Is(err, datastore.ErrRecordNotExist) {
		return nil
	}
	if err != nil || !claimed {
		return err
	}
	transferCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var reference string
	switch d.Target {
	case "database":
		reference, err = s.copyDatabase(transferCtx, d)
	case "minio":
		reference, err = s.copyMinIO(transferCtx, d)
	default:
		err = fmt.Errorf("unsupported storage destination")
	}
	state, lastError := DeliverySucceeded, ""
	if err != nil {
		state = DeliveryFailed
		lastError = err.Error()
	}
	// If cancellation prevented a final update, the durable lease remains
	// recoverable. A stale worker can never replace a newer worker's outcome.
	updated, saveErr := s.db.(datastore.ConditionalCompareAndSwap).CompareAndSwapWithConditions(ctx, d, map[string]interface{}{"state": DeliveryRunning, "lease_token": d.LeaseToken}, map[string]interface{}{"state": state, "last_error": lastError, "reference": reference, "lease_token": "", "lease_until": nil})
	if saveErr != nil {
		return fmt.Errorf("record delivery outcome: %w", saveErr)
	}
	if !updated {
		return nil
	}
	// A transfer failure is recorded for the user, not a maintenance-loop failure.
	return nil
}

func (s *Store) copyMinIO(ctx context.Context, d *model.JobDelivery) (string, error) {
	if s.objects == nil {
		return "", ErrDestinationUnavailable
	}
	file, err := os.CreateTemp("", "eruun-delivery-*")
	if err != nil {
		return "", fmt.Errorf("stage result delivery: %w", err)
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	if err := s.Download(ctx, d.WorkspaceID, d.SourceID, file); err != nil {
		return "", err
	}
	source, err := s.Get(ctx, d.WorkspaceID, d.SourceID)
	if err != nil {
		return "", err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	key := fmt.Sprintf("workspaces/%s/jobs/%s/%s.tar.gz", stableID(d.WorkspaceID), stableID(d.TaskID), source.Digest)
	return s.objects.Put(ctx, key, file, source.Size, source.Digest)
}

func (s *Store) copyDatabase(ctx context.Context, d *model.JobDelivery) (string, error) {
	id := databaseID(d.WorkspaceID, d.TaskID)
	err := transaction(ctx, s.db, func(tx datastore.DataStore) error {
		// Lock in the same order as Retry. Confirm the lease before publishing a copy.
		current, err := scopedDelivery(ctx, tx, d.WorkspaceID, d.TaskID, d.Target, true)
		if err != nil {
			return err
		}
		if current.State != DeliveryRunning || current.LeaseToken != d.LeaseToken {
			return fmt.Errorf("delivery lease changed")
		}
		source, err := scopedArtifact(ctx, tx, d.WorkspaceID, d.SourceID, true)
		if err != nil {
			return err
		}
		if err := sourceAvailable(ctx, tx, source); err != nil {
			return err
		}
		existing, getErr := scopedArtifact(ctx, tx, d.WorkspaceID, id, false)
		if getErr == nil {
			if existing.Digest != source.Digest {
				return ErrConflict
			}
			return nil
		}
		if !errors.Is(getErr, datastore.ErrRecordNotExist) {
			return getErr
		}
		copy := *source
		copy.ID = id
		copy.Kind = KindDatabase
		copy.ExpiresAt = nil
		copy.Expired = false
		copy.BaseModel = model.BaseModel{}
		if d.Mode == "metadata" {
			full, err := scopedDelivery(ctx, tx, d.WorkspaceID, d.TaskID, "minio", false)
			if err != nil || full.State != DeliverySucceeded || full.SourceID != source.ID {
				return fmt.Errorf("metadata destination requires a successful MinIO copy")
			}
			copy.Reference = full.Reference
			copy.Chunks = 0
		}
		if err := tx.Add(ctx, &copy); err != nil {
			return err
		}
		if d.Mode == "metadata" {
			return nil
		}
		for i := 0; i < source.Chunks; i++ {
			chunk := &model.ArtifactChunk{ID: fmt.Sprintf("%s-%06d", source.ID, i)}
			if err := tx.Get(ctx, chunk); err != nil {
				return err
			}
			if chunk.WorkspaceID != d.WorkspaceID || chunk.ArtifactID != source.ID || chunk.Ordinal != i {
				return fmt.Errorf("archive chunk identity mismatch")
			}
			chunk.ID = fmt.Sprintf("%s-%06d", copy.ID, i)
			chunk.ArtifactID = copy.ID
			chunk.BaseModel = model.BaseModel{}
			if err := tx.Add(ctx, chunk); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) DownloadDelivery(ctx context.Context, workspaceID, taskID, target string, w io.Writer) error {
	d, err := scopedDelivery(ctx, s.db, workspaceID, taskID, target, false)
	if err != nil {
		return err
	}
	if d.State != DeliverySucceeded {
		return fmt.Errorf("%w: destination has no successful saved result", ErrInvalidInput)
	}
	if d.Target == "database" {
		a, err := s.Get(ctx, workspaceID, d.Reference)
		if err != nil {
			return err
		}
		if a.Chunks > 0 {
			return s.Download(ctx, workspaceID, a.ID, w)
		}
		if s.objects == nil {
			return ErrDestinationUnavailable
		}
		return s.downloadObject(ctx, a.Reference, a.Digest, w)
	}
	if s.objects == nil {
		return ErrDestinationUnavailable
	}
	source, err := s.Get(ctx, workspaceID, d.SourceID)
	if err != nil {
		return err
	}
	return s.downloadObject(ctx, d.Reference, source.Digest, w)
}

func (s *Store) downloadObject(ctx context.Context, reference, digest string, w io.Writer) error {
	hash := sha256.New()
	if err := s.objects.Get(ctx, reference, io.MultiWriter(w, hash)); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != digest {
		return fmt.Errorf("saved result integrity mismatch")
	}
	return nil
}

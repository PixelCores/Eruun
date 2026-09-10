package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

const (
	KindDataset       = "dataset"
	KindSource        = "source"
	KindDatabase      = "database"
	DeliveryPending   = "pending"
	DeliveryRunning   = "running"
	DeliverySucceeded = "succeeded"
	DeliveryFailed    = "failed"
)

// Store uses the server datastore. Callers authorize workspace membership; all
// artifact references are additionally checked against that explicit workspace.
type Store struct {
	db      datastore.DataStore
	objects objectStore
}

func New(db datastore.DataStore, cfg *spec.MinIOConfig) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("artifact datastore is required")
	}
	if _, ok := db.(datastore.Transactional); !ok {
		return nil, fmt.Errorf("artifact datastore requires transactions")
	}
	if _, ok := db.(datastore.DatabaseClock); !ok {
		return nil, fmt.Errorf("artifact datastore requires database clock")
	}
	if _, ok := db.(datastore.ConditionalCompareAndSwap); !ok {
		return nil, fmt.Errorf("artifact datastore requires conditional updates")
	}
	s := &Store{db: db}
	if cfg != nil {
		object, err := newMinIO(cfg)
		if err != nil {
			return nil, err
		}
		s.objects = object
	}
	return s, nil
}

func stableID(parts ...string) string {
	hash := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(hash[:])
}
func sourceID(workspaceID, taskID string) string { return stableID(KindSource, workspaceID, taskID) }
func deliveryID(workspaceID, taskID, target string) string {
	return stableID("delivery", workspaceID, taskID, target)
}
func databaseID(workspaceID, taskID string) string {
	return stableID(KindDatabase, workspaceID, taskID)
}
func requireWorkspace(workspaceID string) error {
	if workspaceID == "" {
		return fmt.Errorf("%w: workspaceId is required", ErrInvalidInput)
	}
	return nil
}
func clock(ctx context.Context, db datastore.DataStore) (time.Time, error) {
	return db.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
}
func transaction(ctx context.Context, db datastore.DataStore, fn func(datastore.DataStore) error) error {
	return db.(datastore.Transactional).WithTransaction(ctx, fn)
}
func locked(ctx context.Context, db datastore.DataStore, e datastore.Entity) error {
	locker, ok := db.(datastore.RowLocker)
	if !ok {
		return fmt.Errorf("artifact datastore requires row locks")
	}
	return locker.GetForUpdate(ctx, e)
}
func scopedArtifact(ctx context.Context, db datastore.DataStore, workspaceID, id string, lock bool) (*model.JobArtifact, error) {
	if err := requireWorkspace(workspaceID); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, datastore.ErrRecordNotExist
	}
	a := &model.JobArtifact{ID: id}
	var err error
	if lock {
		err = locked(ctx, db, a)
	} else {
		err = db.Get(ctx, a)
	}
	if err != nil {
		return nil, err
	}
	if a.WorkspaceID != workspaceID {
		return nil, datastore.ErrRecordNotExist
	}
	return a, nil
}
func sourceAvailable(ctx context.Context, db datastore.DataStore, a *model.JobArtifact) error {
	if a.Expired {
		return ErrSourceExpired
	}
	if a.ExpiresAt != nil {
		now, err := clock(ctx, db)
		if err != nil {
			return err
		}
		if !now.Before(*a.ExpiresAt) {
			return ErrSourceExpired
		}
	}
	return nil
}

func (s *Store) UploadDataset(ctx context.Context, workspaceID, name string, r io.Reader) (*model.JobArtifact, error) {
	if err := requireWorkspace(workspaceID); err != nil {
		return nil, err
	}
	if strings.TrimSpace(name) == "" || len(name) > 255 {
		return nil, fmt.Errorf("%w: dataset name must contain 1..255 bytes", ErrInvalidInput)
	}
	archive, err := readArchive(ctx, r, true)
	if err != nil {
		return nil, err
	}
	defer archive.close()
	a := &model.JobArtifact{ID: uuid.NewString(), WorkspaceID: workspaceID, Kind: KindDataset, Name: name, Digest: archive.digest, Size: archive.size, Manifest: archive.manifest}
	err = transaction(ctx, s.db, func(tx datastore.DataStore) error {
		if err := locked(ctx, tx, &model.Workspace{ID: workspaceID}); err != nil {
			return err
		}
		return saveArchive(ctx, tx, a, archive.file)
	})
	if err != nil {
		return nil, fmt.Errorf("store dataset: %w", err)
	}
	return a, nil
}

// PutResult publishes the source and all selected delivery records atomically.
// The source is immutable per task; duplicate identical uploads return it.
func (s *Store) PutResult(ctx context.Context, workspaceID, taskID string, policy spec.JobResultPolicy, r io.Reader) (*model.JobArtifact, error) {
	return s.PutResultGuarded(ctx, workspaceID, taskID, policy, r, nil)
}

// PutResultGuarded locks the workspace before invoking guard in the publishing
// transaction, allowing the runtime to atomically reject obsolete executions.
func (s *Store) PutResultGuarded(ctx context.Context, workspaceID, taskID string, policy spec.JobResultPolicy, r io.Reader, guard func(datastore.DataStore) error) (*model.JobArtifact, error) {
	if err := requireWorkspace(workspaceID); err != nil {
		return nil, err
	}
	if taskID == "" {
		return nil, fmt.Errorf("taskId is required")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	archive, err := readArchive(ctx, r, false)
	if err != nil {
		return nil, err
	}
	defer archive.close()
	a := &model.JobArtifact{ID: sourceID(workspaceID, taskID), WorkspaceID: workspaceID, TaskID: taskID, Kind: KindSource, Name: "results.tar.gz", Digest: archive.digest, Size: archive.size, Manifest: archive.manifest, Summary: archive.summary}
	err = transaction(ctx, s.db, func(tx datastore.DataStore) error {
		if err := locked(ctx, tx, &model.Workspace{ID: workspaceID}); err != nil {
			return err
		}
		if guard != nil {
			if err := guard(tx); err != nil {
				return err
			}
		}
		// The existing task row serializes duplicate publication and workspace deletion.
		task := &model.WorkflowQueue{TaskID: taskID}
		if err := locked(ctx, tx, task); err != nil {
			return err
		}
		if task.WorkspaceID != workspaceID {
			return datastore.ErrRecordNotExist
		}
		existing, getErr := scopedArtifact(ctx, tx, workspaceID, a.ID, false)
		if getErr == nil {
			if existing.Digest != a.Digest {
				return ErrConflict
			}
			a = existing
			return nil
		}
		if !errors.Is(getErr, datastore.ErrRecordNotExist) {
			return getErr
		}
		now, err := clock(ctx, tx)
		if err != nil {
			return err
		}
		expiry := now.Add(time.Duration(policy.RetentionDays) * 24 * time.Hour)
		a.ExpiresAt = &expiry
		if err := saveArchive(ctx, tx, a, archive.file); err != nil {
			return err
		}
		for _, target := range policy.Targets {
			d := &model.JobDelivery{ID: deliveryID(workspaceID, taskID, target.Type), WorkspaceID: workspaceID, TaskID: taskID, SourceID: a.ID, Target: target.Type, Mode: target.Mode, State: DeliveryPending}
			if err := tx.Add(ctx, d); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("publish evaluation result: %w", err)
	}
	return a, nil
}

func saveArchive(ctx context.Context, db datastore.DataStore, a *model.JobArtifact, r io.Reader) error {
	a.Chunks = int((a.Size + ChunkSize - 1) / ChunkSize)
	if err := db.Add(ctx, a); err != nil {
		return err
	}
	buf := make([]byte, ChunkSize)
	for i := 0; i < a.Chunks; i++ {
		n, err := io.ReadFull(contextReader{ctx, r}, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("read archive chunk: %w", err)
		}
		chunk := &model.ArtifactChunk{ID: fmt.Sprintf("%s-%06d", a.ID, i), WorkspaceID: a.WorkspaceID, ArtifactID: a.ID, Ordinal: i, Data: buf[:n]}
		if err := db.Add(ctx, chunk); err != nil {
			return fmt.Errorf("save archive chunk: %w", err)
		}
	}
	return nil
}

func (s *Store) Get(ctx context.Context, workspaceID, id string) (*model.JobArtifact, error) {
	return scopedArtifact(ctx, s.db, workspaceID, id, false)
}

// List returns the first page for a task's small source/destination result set.
// Dataset listings should use ListPage so every uploaded task package is reachable.
func (s *Store) List(ctx context.Context, workspaceID, kind, taskID string) ([]*model.JobArtifact, error) {
	return s.ListPage(ctx, workspaceID, kind, taskID, 1, 100)
}

func (s *Store) ListPage(ctx context.Context, workspaceID, kind, taskID string, page, pageSize int) ([]*model.JobArtifact, error) {
	if err := requireWorkspace(workspaceID); err != nil {
		return nil, err
	}
	if page < 1 || pageSize < 1 || pageSize > 100 || page > 10000000 {
		return nil, fmt.Errorf("%w: page must be positive and pageSize must be 1..100", ErrInvalidInput)
	}
	records, err := s.db.List(ctx, &model.JobArtifact{WorkspaceID: workspaceID, Kind: kind, TaskID: taskID}, &datastore.ListOptions{Page: page, PageSize: pageSize, SortBy: []datastore.SortOption{{Key: "create_time", Order: datastore.SortOrderDescending}, {Key: "id", Order: datastore.SortOrderDescending}}})
	if err != nil {
		return nil, err
	}
	result := make([]*model.JobArtifact, 0, len(records))
	for _, record := range records {
		result = append(result, record.(*model.JobArtifact))
	}
	return result, nil
}

func (s *Store) Download(ctx context.Context, workspaceID, id string, w io.Writer) error {
	return transaction(ctx, s.db, func(tx datastore.DataStore) error {
		if err := requireWorkspace(workspaceID); err != nil {
			return err
		}
		if err := locked(ctx, tx, &model.Workspace{ID: workspaceID}); err != nil {
			return err
		}
		a, err := scopedArtifact(ctx, tx, workspaceID, id, true)
		if err != nil {
			return err
		}
		if err := sourceAvailable(ctx, tx, a); err != nil {
			return err
		}
		if a.Chunks == 0 {
			return fmt.Errorf("%w: artifact contains metadata only; download its full destination", ErrInvalidInput)
		}
		return copyChunks(ctx, tx, a, w)
	})
}

func copyChunks(ctx context.Context, db datastore.DataStore, a *model.JobArtifact, w io.Writer) error {
	hash := sha256.New()
	total := int64(0)
	for i := 0; i < a.Chunks; i++ {
		chunk := &model.ArtifactChunk{ID: fmt.Sprintf("%s-%06d", a.ID, i)}
		if err := db.Get(ctx, chunk); err != nil {
			return fmt.Errorf("read archive chunk: %w", err)
		}
		if chunk.WorkspaceID != a.WorkspaceID || chunk.ArtifactID != a.ID || chunk.Ordinal != i {
			return fmt.Errorf("archive chunk identity mismatch")
		}
		n, err := io.Copy(io.MultiWriter(w, hash), bytes.NewReader(chunk.Data))
		total += n
		if err != nil {
			return fmt.Errorf("write archive chunk: %w", err)
		}
	}
	if total != a.Size || hex.EncodeToString(hash.Sum(nil)) != a.Digest {
		return fmt.Errorf("archive integrity mismatch")
	}
	return nil
}

func (s *Store) SetRetention(ctx context.Context, workspaceID, taskID string, days int) error {
	if days < 1 || days > 3650 {
		return fmt.Errorf("%w: retentionDays must be 1..3650", ErrInvalidInput)
	}
	return transaction(ctx, s.db, func(tx datastore.DataStore) error {
		if err := requireWorkspace(workspaceID); err != nil {
			return err
		}
		if err := locked(ctx, tx, &model.Workspace{ID: workspaceID}); err != nil {
			return err
		}
		a, err := scopedArtifact(ctx, tx, workspaceID, sourceID(workspaceID, taskID), true)
		if err != nil {
			return err
		}
		if err := sourceAvailable(ctx, tx, a); err != nil {
			return err
		}
		now, err := clock(ctx, tx)
		if err != nil {
			return err
		}
		expiry := now.Add(time.Duration(days) * 24 * time.Hour)
		_, err = tx.CompareAndSwap(ctx, a, "expired", false, map[string]interface{}{"expires_at": expiry})
		return err
	})
}

func (s *Store) CleanupExpired(ctx context.Context, limit int) error {
	if limit < 1 || limit > 1000 {
		return fmt.Errorf("cleanup limit must be 1..1000")
	}
	now, err := clock(ctx, s.db)
	if err != nil {
		return err
	}
	records, err := s.db.List(ctx, &model.JobArtifact{Kind: KindSource}, &datastore.ListOptions{Page: 1, PageSize: limit, FilterOptions: datastore.FilterOptions{LessThan: []datastore.ComparisonQueryOption{{Key: "expires_at", Value: now}}, NotEqual: []datastore.ComparisonQueryOption{{Key: "expired", Value: true}}}, SortBy: []datastore.SortOption{{Key: "expires_at", Order: datastore.SortOrderAscending}}})
	if err != nil {
		return err
	}
	for _, record := range records {
		candidate := record.(*model.JobArtifact)
		if err := transaction(ctx, s.db, func(tx datastore.DataStore) error {
			if err := locked(ctx, tx, &model.Workspace{ID: candidate.WorkspaceID}); err != nil {
				return err
			}
			a, err := scopedArtifact(ctx, tx, candidate.WorkspaceID, candidate.ID, true)
			if err != nil {
				return err
			}
			current, err := clock(ctx, tx)
			if err != nil {
				return err
			}
			if a.Expired || a.ExpiresAt == nil || current.Before(*a.ExpiresAt) {
				return nil
			}
			if err := tx.DeleteByFilter(ctx, &model.ArtifactChunk{ArtifactID: a.ID, WorkspaceID: a.WorkspaceID}, nil); err != nil {
				return err
			}
			_, err = tx.CompareAndSwap(ctx, a, "expired", false, map[string]interface{}{"expired": true, "chunks": 0})
			return err
		}); err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
			return fmt.Errorf("expire original result: %w", err)
		}
	}
	return nil
}

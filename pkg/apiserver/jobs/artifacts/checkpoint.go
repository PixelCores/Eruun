package artifacts

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

const KindCheckpoint = "checkpoint"

// PutCheckpoint stores bounded, validated material independently of result
// delivery. The caller binds its manifest and artifact atomically under its
// execution locks. Duplicate IDs must contain byte-identical archives.
func (s *Store) PutCheckpoint(ctx context.Context, workspaceID, taskID, executionKey, id string, input io.Reader, bind func(datastore.DataStore, *model.JobArtifact, json.RawMessage) error) error {
	if err := requireWorkspace(workspaceID); err != nil {
		return err
	}
	a, err := readArchive(ctx, input, false)
	if err != nil {
		return err
	}
	defer a.close()
	gz, err := gzip.NewReader(a.file)
	if err != nil {
		return fmt.Errorf("read checkpoint material: %w", err)
	}
	tr := tar.NewReader(gz)
	var manifest json.RawMessage
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = gz.Close()
			return fmt.Errorf("read checkpoint material: %w", err)
		}
		if strings.TrimPrefix(h.Name, "./") != "checkpoint.json" {
			continue
		}
		if (h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA) || h.Size > metadataLimit {
			_ = gz.Close()
			return fmt.Errorf("%w: invalid checkpoint.json", ErrInvalidArchive)
		}
		manifest, err = io.ReadAll(tr)
		if err != nil {
			_ = gz.Close()
			return err
		}
	}
	if err := gz.Close(); err != nil {
		return err
	}
	if !json.Valid(manifest) {
		return fmt.Errorf("%w: missing checkpoint.json", ErrInvalidArchive)
	}
	var envelope struct {
		Files []ManifestEntry `json:"files"`
	}
	var entries []ManifestEntry
	if json.Unmarshal(manifest, &envelope) != nil || json.Unmarshal(a.manifest, &entries) != nil {
		return fmt.Errorf("%w: invalid checkpoint file manifest", ErrInvalidArchive)
	}
	declared := make(map[string]ManifestEntry, len(envelope.Files))
	for _, entry := range envelope.Files {
		if _, exists := declared[entry.Path]; exists || entry.Path == "checkpoint.json" || entry.Digest == "" || entry.LinkTarget != "" {
			return fmt.Errorf("%w: invalid checkpoint file declaration", ErrInvalidArchive)
		}
		declared[entry.Path] = entry
	}
	if len(entries) != len(declared)+1 {
		return fmt.Errorf("%w: checkpoint files are incomplete", ErrInvalidArchive)
	}
	for _, entry := range entries {
		if entry.Path == "checkpoint.json" {
			continue
		}
		value, ok := declared[entry.Path]
		if !ok || value.Size != entry.Size || value.Digest != entry.Digest || entry.LinkTarget != "" {
			return fmt.Errorf("%w: checkpoint file digest mismatch", ErrInvalidArchive)
		}
	}
	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	artifact := &model.JobArtifact{ID: stableID(KindCheckpoint, workspaceID, executionKey, id), WorkspaceID: workspaceID, TaskID: taskID, ExecutionKey: executionKey, Kind: KindCheckpoint, Name: "checkpoint.tar.gz", Digest: a.digest, Size: a.size, Manifest: a.manifest}
	return transaction(ctx, s.db, func(tx datastore.DataStore) error {
		if err := locked(ctx, tx, &model.Workspace{ID: workspaceID}); err != nil {
			return err
		}
		if err := bind(tx, artifact, manifest); err != nil {
			return err
		}
		existing, err := scopedArtifact(ctx, tx, workspaceID, artifact.ID, false)
		if err == nil {
			if existing.Kind != KindCheckpoint || existing.Digest != artifact.Digest || existing.Expired {
				return ErrConflict
			}
			return nil
		}
		if !errors.Is(err, datastore.ErrRecordNotExist) {
			return err
		}
		return saveArchive(ctx, tx, artifact, a.file)
	})
}

// DeleteCheckpoint removes material only in the transaction that fenced and
// expired its checkpoint; this kind is deliberately outside result retention.
func DeleteCheckpoint(ctx context.Context, tx datastore.DataStore, workspaceID, id string) error {
	a, err := scopedArtifact(ctx, tx, workspaceID, id, true)
	if errors.Is(err, datastore.ErrRecordNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if a.Kind != KindCheckpoint {
		return ErrInvalidInput
	}
	if err := tx.DeleteByFilter(ctx, &model.ArtifactChunk{WorkspaceID: workspaceID, ArtifactID: id}, nil); err != nil {
		return err
	}
	return tx.Delete(ctx, a)
}

// VerifyCheckpoint checks every immutable chunk before the caller commits a
// complete point. Callers hold that checkpoint's lifecycle lock.
func VerifyCheckpoint(ctx context.Context, tx datastore.DataStore, workspaceID, id, executionKey string) error {
	a, err := scopedArtifact(ctx, tx, workspaceID, id, false)
	if err != nil {
		return err
	}
	if a.Kind != KindCheckpoint || a.ExecutionKey != executionKey || a.Expired || a.Chunks < 1 {
		return ErrInvalidInput
	}
	return copyChunks(ctx, tx, a, io.Discard)
}

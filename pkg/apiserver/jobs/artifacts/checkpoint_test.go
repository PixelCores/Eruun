package artifacts

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"
)

func TestCheckpointMaterialFileManifest(t *testing.T) {
	content := "session state"
	digest := sha256.Sum256([]byte(content))
	entry := ManifestEntry{Path: "outputs/session.json", Size: int64(len(content)), Digest: hex.EncodeToString(digest[:])}
	for _, scenario := range []string{"valid", "manifest last", "omitted", "missing file", "wrong digest", "duplicate"} {
		t.Run(scenario, func(t *testing.T) {
			store, db := testStore(t)
			files := []ManifestEntry{entry}
			if scenario == "omitted" {
				files = nil
			}
			if scenario == "wrong digest" {
				files[0].Digest = "wrong"
			}
			if scenario == "duplicate" {
				files = append(files, entry)
			}
			manifest, err := json.Marshal(map[string]interface{}{"version": 1, "files": files})
			require.NoError(t, err)
			entries := []testEntry{{name: "checkpoint.json", content: string(manifest)}}
			if scenario != "missing file" {
				entries = append(entries, testEntry{name: entry.Path, content: content})
			}
			if scenario == "manifest last" {
				entries[0], entries[1] = entries[1], entries[0]
			}
			data := archiveBytes(t, entries...)
			bound := false
			err = store.PutCheckpoint(context.Background(), "space-a", "task-a", "exec-a", "point", bytes.NewReader(data), func(_ Backend, artifact *model.JobArtifact, raw json.RawMessage) error {
				bound = true
				require.Equal(t, KindCheckpoint, artifact.Kind)
				require.JSONEq(t, string(manifest), string(raw))
				return nil
			})
			if scenario == "valid" || scenario == "manifest last" {
				require.NoError(t, err)
				require.True(t, bound)
			} else {
				require.ErrorIs(t, err, ErrInvalidArchive)
				require.False(t, bound)
				count, err := db.Count(context.Background(), &model.JobArtifact{Kind: KindCheckpoint}, nil)
				require.NoError(t, err)
				require.Zero(t, count)
			}
		})
	}
}

func TestCheckpointManifestExtractionValidatesCompleteArchive(t *testing.T) {
	valid := archiveBytes(t, testEntry{name: "checkpoint.json", content: `{"files":[]}`})
	corrupt := append([]byte(nil), valid...)
	corrupt[len(corrupt)-6] ^= 1
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"missing manifest", archiveBytes(t, testEntry{name: "other.json", content: `{}`})},
		{"invalid manifest", archiveBytes(t, testEntry{name: "checkpoint.json", content: `{`})},
		{"duplicate manifest", archiveBytes(t, testEntry{name: "checkpoint.json", content: `{"files":[]}`}, testEntry{name: "./checkpoint.json", content: `{"files":[]}`})},
		{"manifest symlink", archiveBytes(t, testEntry{name: "other.json", content: `{"files":[]}`}, testEntry{name: "checkpoint.json", kind: tar.TypeSymlink, link: "other.json"})},
		{"gzip checksum after manifest", corrupt},
		{"second archive after manifest", append(append([]byte(nil), valid...), valid...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := testStore(t)
			bound := false
			err := store.PutCheckpoint(context.Background(), "space-a", "task-a", "exec-a", "point", bytes.NewReader(tc.data), func(Backend, *model.JobArtifact, json.RawMessage) error { bound = true; return nil })
			require.ErrorIs(t, err, ErrInvalidArchive)
			require.False(t, bound)
		})
	}
}

type checkpointZeros struct{}

func (checkpointZeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestCheckpointArchiveSizeBoundaries(t *testing.T) {
	t.Run("compressed limit", func(t *testing.T) {
		store, _ := testStore(t)
		err := store.PutCheckpoint(context.Background(), "space-a", "task-a", "exec-a", "point", io.LimitReader(checkpointZeros{}, (64<<20)+1), nil)
		require.ErrorContains(t, err, "compressed size exceeds 67108864 bytes")
	})
	// Large zero padding compresses to a small fixture while exercising the
	// complete tar stream, including bytes after its end-of-archive blocks.
	for _, extra := range []int64{0, 1} {
		t.Run(fmt.Sprintf("expanded limit plus %d", extra), func(t *testing.T) {
			store, _ := testStore(t)
			base := archiveBytes(t, testEntry{name: "checkpoint.json", content: `{"files":[]}`})
			reader, err := gzip.NewReader(bytes.NewReader(base))
			require.NoError(t, err)
			var compressed bytes.Buffer
			writer := gzip.NewWriter(&compressed)
			n, err := io.Copy(writer, reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			_, err = io.CopyN(writer, checkpointZeros{}, (256<<20)+extra-n)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			bound := false
			err = store.PutCheckpoint(context.Background(), "space-a", "task-a", "exec-a", "point", &compressed, func(Backend, *model.JobArtifact, json.RawMessage) error { bound = true; return nil })
			if extra == 0 {
				require.NoError(t, err)
				require.True(t, bound)
			} else {
				require.ErrorIs(t, err, ErrInvalidArchive)
				require.False(t, bound)
			}
		})
	}
	for _, extra := range []int{0, 1} {
		t.Run(fmt.Sprintf("manifest limit plus %d", extra), func(t *testing.T) {
			store, _ := testStore(t)
			content := `{"files":[],"padding":"` + strings.Repeat("x", (1<<20)+extra-len(`{"files":[],"padding":""}`)) + `"}`
			bound := false
			err := store.PutCheckpoint(context.Background(), "space-a", "task-a", "exec-a", "point", bytes.NewReader(archiveBytes(t, testEntry{name: "checkpoint.json", content: content})), func(Backend, *model.JobArtifact, json.RawMessage) error { bound = true; return nil })
			if extra == 0 {
				require.NoError(t, err)
				require.True(t, bound)
			} else {
				require.ErrorIs(t, err, ErrInvalidArchive)
				require.False(t, bound)
			}
		})
	}
}

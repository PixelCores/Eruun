package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"
)

func TestCheckpointMaterialFileManifest(t *testing.T) {
	content := "session state"
	digest := sha256.Sum256([]byte(content))
	entry := ManifestEntry{Path: "outputs/session.json", Size: int64(len(content)), Digest: hex.EncodeToString(digest[:])}
	for _, scenario := range []string{"valid", "omitted", "missing file", "wrong digest", "duplicate"} {
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
			data := archiveBytes(t, entries...)
			bound := false
			err = store.PutCheckpoint(context.Background(), "space-a", "task-a", "exec-a", "point", bytes.NewReader(data), func(_ Backend, artifact *model.JobArtifact, raw json.RawMessage) error {
				bound = true
				require.Equal(t, KindCheckpoint, artifact.Kind)
				require.JSONEq(t, string(manifest), string(raw))
				return nil
			})
			if scenario == "valid" {
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

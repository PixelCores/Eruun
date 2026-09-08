package artifacts

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type testEntry struct {
	name, content, link string
	kind                byte
}

func archiveBytes(t *testing.T, entries ...testEntry) []byte {
	t.Helper()
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tr := tar.NewWriter(gz)
	for _, entry := range entries {
		kind := entry.kind
		if kind == 0 {
			kind = tar.TypeReg
		}
		header := &tar.Header{Name: entry.name, Mode: 0600, Typeflag: kind, Linkname: entry.link}
		if kind == tar.TypeReg {
			header.Size = int64(len(entry.content))
		}
		require.NoError(t, tr.WriteHeader(header))
		if header.Size > 0 {
			_, err := io.WriteString(tr, entry.content)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tr.Close())
	require.NoError(t, gz.Close())
	return out.Bytes()
}
func taskEntries(prefix string) []testEntry {
	return []testEntry{{name: prefix + "instruction.md", content: "Build the requested artifact"}, {name: prefix + "task.toml", content: "version = \"1.0\"\n[environment]\ndocker_image = \"example/task:1.0\"\n"}, {name: prefix + "environment/Dockerfile", content: "FROM example/task:1.0\n"}, {name: prefix + "tests/test.sh", content: "#!/bin/sh\necho 1 > /logs/verifier/reward.txt\n"}}
}

func TestArchiveNativeTasksAndCompleteManifest(t *testing.T) {
	for _, prefix := range []string{"", "dataset/task/", "./dataset/task/"} {
		t.Run(prefix, func(t *testing.T) {
			data := archiveBytes(t, taskEntries(prefix)...)
			got, err := readArchive(context.Background(), bytes.NewReader(data), true)
			require.NoError(t, err)
			defer got.close()
			require.Equal(t, int64(len(data)), got.size)
			require.Len(t, got.digest, 64)
			var files []ManifestEntry
			require.NoError(t, json.Unmarshal(got.manifest, &files))
			require.Len(t, files, 4)
		})
	}
}

func TestArchiveRejectsUnsafeAndInvalidInputs(t *testing.T) {
	tests := []struct {
		name    string
		entries []testEntry
		dataset bool
	}{
		{"traversal", []testEntry{{name: "../outside", content: "secret"}}, false},
		{"absolute", []testEntry{{name: "/outside", content: "x"}}, false},
		{"backslash", []testEntry{{name: "a\\b", content: "x"}}, false},
		{"duplicate", []testEntry{{name: "a", content: "1"}, {name: "./a", content: "2"}}, false},
		{"file then directory", []testEntry{{name: "a", content: "1"}, {name: "a/b", content: "2"}}, false},
		{"directory then file", []testEntry{{name: "a/b", content: "2"}, {name: "a", content: "1"}}, false},
		{"hardlink", []testEntry{{name: "x", kind: tar.TypeLink, link: "a"}}, false},
		{"dataset symlink", append(taskEntries(""), testEntry{name: "link", kind: tar.TypeSymlink, link: "instruction.md"}), true},
		{"escaping result symlink", []testEntry{{name: "a", kind: tar.TypeSymlink, link: "../out"}}, false},
		{"dangling symlink", []testEntry{{name: "a", kind: tar.TypeSymlink, link: "missing"}}, false},
		{"chained symlink", []testEntry{{name: "file", content: "x"}, {name: "a", kind: tar.TypeSymlink, link: "file"}, {name: "b", kind: tar.TypeSymlink, link: "a"}}, false},
		{"entry through symlink", []testEntry{{name: "a", kind: tar.TypeSymlink, link: "."}, {name: "a/b", content: "x"}}, false},
		{"empty", nil, false},
		{"missing verifier", taskEntries("")[:3], true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := readArchive(context.Background(), bytes.NewReader(archiveBytes(t, tt.entries...)), tt.dataset)
			if result != nil {
				result.close()
			}
			require.ErrorIs(t, err, ErrInvalidArchive)
		})
	}
	for _, image := range []string{"example/task", "example/task:latest", "example/task:", "example/task@sha256:bad"} {
		t.Run(image, func(t *testing.T) {
			entries := taskEntries("")
			entries[1].content = "[environment]\ndocker_image = \"" + image + "\"\n"
			result, err := readArchive(context.Background(), bytes.NewReader(archiveBytes(t, entries...)), true)
			if result != nil {
				result.close()
			}
			require.ErrorIs(t, err, ErrInvalidArchive)
		})
	}
}

func TestArchiveResultSummaryLinksAndIntegrity(t *testing.T) {
	data := archiveBytes(t, testEntry{name: "result.json", content: `{"collectionComplete":true,"evaluationStatus":"succeeded"}`}, testEntry{name: "outputs/trial/trajectory.json", content: `[1,2,3]`}, testEntry{name: "outputs/current", kind: tar.TypeSymlink, link: "trial/trajectory.json"})
	result, err := readArchive(context.Background(), bytes.NewReader(data), false)
	require.NoError(t, err)
	defer result.close()
	require.JSONEq(t, `{"collectionComplete":true,"evaluationStatus":"succeeded"}`, string(result.summary))
	require.Contains(t, string(result.manifest), `"linkTarget":"trial/trajectory.json"`)
	data[len(data)-6] ^= 1
	_, err = readArchive(context.Background(), bytes.NewReader(data), false)
	require.ErrorIs(t, err, ErrInvalidArchive)
	var combined bytes.Buffer
	combined.Write(archiveBytes(t, testEntry{name: "first", content: "x"}))
	combined.Write(archiveBytes(t, testEntry{name: "second", content: "y"}))
	_, err = readArchive(context.Background(), &combined, false)
	require.ErrorIs(t, err, ErrInvalidArchive)
}

func TestArchiveBoundsAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := readArchive(ctx, bytes.NewReader(nil), false)
	require.ErrorIs(t, err, context.Canceled)
	entries := make([]testEntry, MaxArchiveFiles+1)
	for i := range entries {
		entries[i] = testEntry{name: string(rune(0x1000 + i)), content: "x"}
	}
	_, err = readArchive(context.Background(), bytes.NewReader(archiveBytes(t, entries...)), false)
	require.ErrorIs(t, err, ErrInvalidArchive)
}

// Package artifacts stores workspace-scoped native task packages and evaluation
// output archives, and copies results to independently tracked destinations.
package artifacts

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/pelletier/go-toml/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

const (
	DatasetArchiveLimit  int64 = 64 << 20
	DatasetExpandedLimit int64 = 256 << 20
	ResultArchiveLimit   int64 = 512 << 20
	ResultExpandedLimit  int64 = 2 << 30
	MaxArchiveFiles            = 10000
	ChunkSize                  = 1 << 20
	metadataLimit              = 1 << 20
)

var (
	ErrInvalidArchive         = errors.New("invalid archive")
	ErrInvalidInput           = errors.New("invalid artifact input")
	ErrSourceExpired          = errors.New("original result has expired")
	ErrConflict               = errors.New("immutable result already exists with different content")
	ErrDestinationUnavailable = errors.New("storage destination is unavailable")
)

type ManifestEntry struct {
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	Digest     string `json:"digest,omitempty"`
	LinkTarget string `json:"linkTarget,omitempty"`
}

type archive struct {
	file     *os.File
	size     int64
	digest   string
	manifest json.RawMessage
	summary  json.RawMessage
}

func (a *archive) close() { name := a.file.Name(); _ = a.file.Close(); _ = os.Remove(name) }

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func readArchive(ctx context.Context, input io.Reader, dataset bool) (_ *archive, err error) {
	compressed, expanded := ResultArchiveLimit, ResultExpandedLimit
	if dataset {
		compressed, expanded = DatasetArchiveLimit, DatasetExpandedLimit
	}
	file, err := os.CreateTemp("", "eruun-archive-*")
	if err != nil {
		return nil, fmt.Errorf("stage archive: %w", err)
	}
	result := &archive{file: file}
	defer func() {
		if err != nil {
			result.close()
		}
	}()
	hash := sha256.New()
	result.size, err = io.Copy(io.MultiWriter(file, hash), io.LimitReader(contextReader{ctx, input}, compressed+1))
	if err != nil {
		return nil, fmt.Errorf("read archive: %w", err)
	}
	if result.size > compressed {
		return nil, fmt.Errorf("%w: compressed size exceeds %d bytes", ErrInvalidArchive, compressed)
	}
	result.digest = hex.EncodeToString(hash.Sum(nil))
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("%w: gzip header", ErrInvalidArchive)
	}
	defer gz.Close()
	limited := &io.LimitedReader{R: contextReader{ctx, gz}, N: expanded + 1}
	tr := tar.NewReader(limited)
	seen := make(map[string]bool)
	files := make(map[string]bool)
	parents := make(map[string]bool)
	links := make(map[string]string)
	configs := make(map[string][]byte)
	entries := make([]ManifestEntry, 0)
	summaryPath := ""
	for count := 0; ; count++ {
		hdr, nextErr := tr.Next()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			return nil, fmt.Errorf("%w: tar entry: %v", ErrInvalidArchive, nextErr)
		}
		if count >= MaxArchiveFiles {
			return nil, fmt.Errorf("%w: too many archive entries", ErrInvalidArchive)
		}
		name := strings.TrimSuffix(hdr.Name, "/")
		// Common tar writers emit a harmless leading ./; normalize it exactly once.
		name = strings.TrimPrefix(name, "./")
		if name == "." && hdr.Typeflag == tar.TypeDir {
			continue
		}
		if name == "" || strings.ContainsAny(name, "\\\x00") || strings.HasPrefix(name, "/") || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") || len(name) > 1024 {
			return nil, fmt.Errorf("%w: unsafe entry path", ErrInvalidArchive)
		}
		if seen[name] {
			return nil, fmt.Errorf("%w: duplicate path %q", ErrInvalidArchive, name)
		}
		seen[name] = true
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if files[parent] {
				return nil, fmt.Errorf("%w: file/directory collision", ErrInvalidArchive)
			}
			parents[parent] = true
		}
		if hdr.Typeflag == tar.TypeDir {
			continue
		}
		if !dataset && hdr.Typeflag == tar.TypeSymlink {
			target := hdr.Linkname
			resolved := path.Clean(path.Join(path.Dir(name), target))
			if target == "" || strings.HasPrefix(target, "/") || strings.ContainsAny(target, "\\\x00") || resolved == ".." || strings.HasPrefix(resolved, "../") || parents[name] {
				return nil, fmt.Errorf("%w: unsafe result link", ErrInvalidArchive)
			}
			files[name] = true
			links[name] = resolved
			entries = append(entries, ManifestEntry{Path: name, LinkTarget: target})
			continue
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			return nil, fmt.Errorf("%w: links and special files are not supported", ErrInvalidArchive)
		}
		if parents[name] {
			return nil, fmt.Errorf("%w: file/directory collision", ErrInvalidArchive)
		}
		if hdr.Size < 0 || hdr.Size > expanded {
			return nil, fmt.Errorf("%w: entry too large", ErrInvalidArchive)
		}
		files[name] = true
		hash := sha256.New()
		isConfig := dataset && path.Base(name) == "task.toml"
		isSummary := !dataset && path.Base(name) == "result.json" && (summaryPath == "" || strings.Count(name, "/") < strings.Count(summaryPath, "/"))
		var content []byte
		if isConfig || isSummary {
			if hdr.Size > metadataLimit && isConfig {
				return nil, fmt.Errorf("%w: task.toml too large", ErrInvalidArchive)
			}
			if hdr.Size <= metadataLimit {
				content, err = io.ReadAll(io.TeeReader(tr, hash))
			} else {
				_, err = io.Copy(hash, tr)
			}
		} else {
			_, err = io.Copy(hash, tr)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: read file: %v", ErrInvalidArchive, err)
		}
		if limited.N <= 0 {
			return nil, fmt.Errorf("%w: expanded size exceeds limit", ErrInvalidArchive)
		}
		if isConfig {
			configs[name] = content
		}
		if isSummary && len(content) > 0 && json.Valid(content) {
			result.summary = content
			summaryPath = name
		}
		entries = append(entries, ManifestEntry{Path: name, Size: hdr.Size, Digest: hex.EncodeToString(hash.Sum(nil))})
	}
	// Consume padding and verify the gzip checksum. Nonzero trailing data would
	// let another extractor see content absent from our validated manifest.
	padding := make([]byte, 32*1024)
	for {
		n, readErr := limited.Read(padding)
		for _, b := range padding[:n] {
			if b != 0 {
				return nil, fmt.Errorf("%w: trailing archive content", ErrInvalidArchive)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("%w: gzip integrity: %v", ErrInvalidArchive, readErr)
		}
		if limited.N <= 0 {
			return nil, fmt.Errorf("%w: expanded size exceeds limit", ErrInvalidArchive)
		}
	}
	if limited.N <= 0 {
		return nil, fmt.Errorf("%w: expanded size exceeds limit", ErrInvalidArchive)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: archive contains no files", ErrInvalidArchive)
	}
	for _, target := range links {
		if target != "." && !seen[target] && !parents[target] {
			return nil, fmt.Errorf("%w: dangling result link", ErrInvalidArchive)
		}
		// Reject chained links; resolving arbitrary chains while extracting is
		// unnecessary for preserving files and makes containment ambiguous.
		if _, ok := links[target]; ok {
			return nil, fmt.Errorf("%w: chained result link", ErrInvalidArchive)
		}
		for parent := path.Dir(target); parent != "."; parent = path.Dir(parent) {
			if _, ok := links[parent]; ok {
				return nil, fmt.Errorf("%w: link target traverses another link", ErrInvalidArchive)
			}
		}
	}
	if dataset {
		if len(configs) == 0 {
			return nil, fmt.Errorf("%w: no Harbor task.toml found", ErrInvalidArchive)
		}
		for name, data := range configs {
			var task struct {
				Environment struct {
					DockerImage string `toml:"docker_image"`
				} `toml:"environment"`
			}
			if err := toml.Unmarshal(data, &task); err != nil {
				return nil, fmt.Errorf("%w: parse task.toml", ErrInvalidArchive)
			}
			image := task.Environment.DockerImage
			if !spec.ExplicitJobImage(image) {
				return nil, fmt.Errorf("%w: task requires a prebuilt docker_image with explicit tag or digest", ErrInvalidArchive)
			}
			dir := path.Dir(name)
			for _, required := range []string{"instruction.md", "environment/Dockerfile", "tests/test.sh"} {
				if !files[path.Join(dir, required)] {
					return nil, fmt.Errorf("%w: Harbor task missing %s", ErrInvalidArchive, required)
				}
			}
		}
	}
	result.manifest, err = json.Marshal(entries)
	if err != nil {
		return nil, fmt.Errorf("encode archive manifest: %w", err)
	}
	if len(result.manifest) > 2*metadataLimit {
		return nil, fmt.Errorf("%w: archive manifest exceeds 2 MiB", ErrInvalidArchive)
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return result, nil
}

package kube

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube/kubetest"
)

var tarProbeCommand = []string{"/bin/sh", "-c", "command -v tar >/dev/null 2>&1"}

func isShellCommand(command []string, contains string) bool {
	return len(command) >= 3 && command[0] == "/bin/sh" && command[1] == "-c" && strings.Contains(command[2], contains)
}

func writeTar(t *testing.T, w io.Writer, write func(*tar.Writer)) {
	t.Helper()
	tw := tar.NewWriter(w)
	write(tw)
	require.NoError(t, tw.Close())
}

func TestExecPodShellScriptBuildsShellCommandAndCapturesOutput(t *testing.T) {
	var got kubetest.ExecRequest
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		got = req
		_, _ = io.WriteString(req.Stdout, "hello")
		_, _ = io.WriteString(req.Stderr, "warn")
		return nil
	})

	result, err := ExecPodShellScript(context.Background(), config, "default", "pod-api", "api", "echo hello")
	require.NoError(t, err)
	require.Equal(t, []string{"/bin/sh", "-c", "echo hello"}, got.Command)
	require.Equal(t, "api", got.Container)
	require.Equal(t, "default", got.Namespace)
	require.Equal(t, "pod-api", got.Pod)
	require.Equal(t, "hello", result.Stdout)
	require.Equal(t, "warn", result.Stderr)
	require.Equal(t, 0, result.ExitCode)
	require.True(t, result.Succeeded())
}

func TestExecPodShellScriptCapsOutput(t *testing.T) {
	stdoutRaw := strings.Repeat("o", syncExecOutputLimitBytes+128)
	stderrRaw := strings.Repeat("e", syncExecOutputLimitBytes+256)
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		_, _ = io.WriteString(req.Stdout, stdoutRaw)
		_, _ = io.WriteString(req.Stderr, stderrRaw)
		return nil
	})

	result, err := ExecPodShellScript(context.Background(), config, "default", "pod-api", "api", "echo hello")
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("o", syncExecOutputLimitBytes)+syncExecOutputTruncatedSuffix, result.Stdout)
	require.Equal(t, strings.Repeat("e", syncExecOutputLimitBytes)+syncExecOutputTruncatedSuffix, result.Stderr)
}

func TestExecPodShellScriptReturnsExitCodeOnCommandFailure(t *testing.T) {
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		_, _ = io.WriteString(req.Stderr, "boom")
		return kubetest.ExitCode(17)
	})

	result, err := ExecPodShellScript(context.Background(), config, "default", "pod-api", "api", "exit 17")
	require.NoError(t, err)
	require.Equal(t, 17, result.ExitCode)
	require.Equal(t, "boom", result.Stderr)
	require.False(t, result.Succeeded())
}

func TestStreamPodShellScriptEmitsEventsAndExit(t *testing.T) {
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		_, _ = io.WriteString(req.Stdout, "hello\n")
		_, _ = io.WriteString(req.Stderr, "warn\n")
		return kubetest.ExitCode(5)
	})

	events, err := StreamPodShellScript(context.Background(), config, "default", "pod-api", "api", "echo hello")
	require.NoError(t, err)
	got := make([]PodShellStreamEvent, 0, 3)
	for event := range events {
		got = append(got, event)
	}
	require.Len(t, got, 3)
	// stdout and stderr arrive on separate streams, so only the exit is ordered.
	chunks := map[PodShellStreamEventType]string{got[0].Type: got[0].Chunk, got[1].Type: got[1].Chunk}
	require.Equal(t, map[PodShellStreamEventType]string{PodShellStreamEventStdout: "hello\n", PodShellStreamEventStderr: "warn\n"}, chunks)
	require.Equal(t, PodShellStreamEventExit, got[2].Type)
	require.Equal(t, 5, got[2].ExitCode)
	require.False(t, got[2].Succeeded)
}

func TestArchivePodPathAsZipCreatesZipStream(t *testing.T) {
	var gotTarCommand []string
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		if reflect.DeepEqual(req.Command, tarProbeCommand) {
			return nil
		}
		gotTarCommand = append([]string(nil), req.Command...)
		writeTar(t, req.Stdout, func(tw *tar.Writer) {
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out", Mode: 0755, Typeflag: tar.TypeDir}))
			data := []byte("payload")
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out/file.txt", Mode: 0644, Size: int64(len(data)), Typeflag: tar.TypeReg}))
			_, _ = tw.Write(data)
		})
		return nil
	})

	stream, err := ArchivePodPathAsZip(context.Background(), config, "default", "pod-api", "api", "/tmp/out")
	require.NoError(t, err)
	require.Equal(t, "out.zip", stream.FileName)
	require.Equal(t, "application/zip", stream.ContentType)
	defer stream.Close()

	archiveBytes, err := io.ReadAll(stream.Reader)
	require.NoError(t, err)
	require.Equal(t, []string{"tar", "-C", "/tmp", "-cf", "-", "--", "out"}, gotTarCommand)

	zr, err := zip.NewReader(bytes.NewReader(archiveBytes), int64(len(archiveBytes)))
	require.NoError(t, err)
	require.Len(t, zr.File, 2)
	require.Equal(t, "out/", zr.File[0].Name)
	require.Equal(t, "out/file.txt", zr.File[1].Name)
	fileReader, err := zr.File[1].Open()
	require.NoError(t, err)
	defer fileReader.Close()
	payload, err := io.ReadAll(fileReader)
	require.NoError(t, err)
	require.Equal(t, "payload", string(payload))
}

func TestArchivePodPathAsZipFallsBackToMultipartWhenTarMissing(t *testing.T) {
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		switch {
		case reflect.DeepEqual(req.Command, tarProbeCommand):
			return kubetest.ExitCode(1)
		case isShellCommand(req.Command, "find"):
			_, _ = io.WriteString(req.Stdout, "./out/a.txt\n./out/nested/b.txt\n")
			return nil
		case isShellCommand(req.Command, "cat"):
			switch target := req.Command[len(req.Command)-1]; target {
			case "out/a.txt", "./out/a.txt":
				_, _ = io.WriteString(req.Stdout, "A")
			case "out/nested/b.txt", "./out/nested/b.txt":
				_, _ = io.WriteString(req.Stdout, "B")
			default:
				return fmt.Errorf("unexpected cat target: %s", target)
			}
			return nil
		default:
			return fmt.Errorf("unexpected command sequence: %v", req.Command)
		}
	})

	stream, err := ArchivePodPathAsZip(context.Background(), config, "default", "pod-api", "api", "/tmp/out")
	require.NoError(t, err)
	require.Equal(t, "out.multipart", stream.FileName)
	mediaType, params, err := mime.ParseMediaType(stream.ContentType)
	require.NoError(t, err)
	require.Equal(t, "multipart/mixed", mediaType)
	defer stream.Close()

	body, err := io.ReadAll(stream.Reader)
	require.NoError(t, err)
	mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	parts := map[string]string{}
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		pathHeader := part.Header.Get("X-Eruun-Path")
		payload, readErr := io.ReadAll(part)
		require.NoError(t, readErr)
		parts[pathHeader] = string(payload)
	}
	require.Equal(t, map[string]string{
		"out/a.txt":        "A",
		"out/nested/b.txt": "B",
	}, parts)
}

func TestArchivePodPathAsZipReturnsReadErrorOnMalformedTar(t *testing.T) {
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		if reflect.DeepEqual(req.Command, tarProbeCommand) {
			return nil
		}
		_, _ = req.Stdout.Write([]byte("not-a-tar"))
		return nil
	})

	stream, err := ArchivePodPathAsZip(context.Background(), config, "default", "pod-api", "api", "/tmp/out")
	require.NoError(t, err)
	defer stream.Close()
	_, err = io.ReadAll(stream.Reader)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read tar stream")
}

func TestArchivePodPathAsZipMarksLookupErrors(t *testing.T) {
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		if reflect.DeepEqual(req.Command, tarProbeCommand) {
			return nil
		}
		_, _ = io.WriteString(req.Stderr, "tar: out: cannot stat: no such file or directory")
		return kubetest.ExitCode(2)
	})

	stream, err := ArchivePodPathAsZip(context.Background(), config, "default", "pod-api", "api", "/tmp/out")
	require.NoError(t, err)
	defer stream.Close()

	_, err = bufio.NewReader(stream.Reader).Peek(1)
	require.Error(t, err)
	require.True(t, IsArchivePathLookupError(err))
}

func TestIsArchivePathLookupErrorIgnoresToolNotFound(t *testing.T) {
	err := fmt.Errorf("read pod file content: exit error: /bin/sh: cat: not found")
	require.False(t, IsArchivePathLookupError(err))
}

func TestSanitizeArchiveEntryName(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		want       string
		expectFail bool
	}{
		{name: "normal relative path", input: "out/a.txt", want: "out/a.txt"},
		{name: "trim and dot prefix", input: " ./out/b.txt ", want: "out/b.txt"},
		{name: "normalize windows separators", input: "out\\c.txt", want: "out/c.txt"},
		{name: "empty input", input: "", want: ""},
		{name: "dot input", input: ".", want: ""},
		{name: "dot slash input", input: "./", want: ""},
		{name: "parent relative", input: "../secret", expectFail: true},
		{name: "parent relative windows", input: "..\\secret", expectFail: true},
		{name: "parent segment in middle", input: "a/../b", expectFail: true},
		{name: "multiple parent segments", input: "a/../../b", expectFail: true},
		{name: "single parent segment", input: "..", expectFail: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := sanitizeArchiveEntryName(tt.input)
			if tt.expectFail {
				require.Error(t, err)
				require.Contains(t, err.Error(), "archive entry path escapes destination")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestArchivePodPathAsZipRejectsParentRelativePath(t *testing.T) {
	config := &rest.Config{Host: "https://example.invalid"}

	for _, targetPath := range []string{"..", "../..", "../out", "a/../../b"} {
		targetPath := targetPath
		t.Run(targetPath, func(t *testing.T) {
			stream, err := ArchivePodPathAsZip(context.Background(), config, "default", "pod-api", "api", targetPath)
			require.Nil(t, stream)
			require.Error(t, err)
			require.True(t, IsArchivePathInvalidError(err))
		})
	}
}

func TestArchivePodPathAsZipSupportsHardLinkEntry(t *testing.T) {
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		if reflect.DeepEqual(req.Command, tarProbeCommand) {
			return nil
		}
		writeTar(t, req.Stdout, func(tw *tar.Writer) {
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out", Mode: 0755, Typeflag: tar.TypeDir}))
			regular := []byte("base")
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out/base.txt", Mode: 0644, Size: int64(len(regular)), Typeflag: tar.TypeReg}))
			_, _ = tw.Write(regular)
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out/hard.txt", Mode: 0644, Typeflag: tar.TypeLink, Linkname: "out/base.txt"}))
		})
		return nil
	})

	stream, err := ArchivePodPathAsZip(context.Background(), config, "default", "pod-api", "api", "/tmp/out")
	require.NoError(t, err)
	defer stream.Close()

	archiveBytes, err := io.ReadAll(stream.Reader)
	require.NoError(t, err)
	zr, err := zip.NewReader(bytes.NewReader(archiveBytes), int64(len(archiveBytes)))
	require.NoError(t, err)
	contents := map[string]string{}
	for _, f := range zr.File {
		reader, openErr := f.Open()
		require.NoError(t, openErr)
		payload, readErr := io.ReadAll(reader)
		_ = reader.Close()
		require.NoError(t, readErr)
		contents[f.Name] = string(payload)
	}
	require.Equal(t, "base", contents["out/base.txt"])
	require.Equal(t, "base", contents["out/hard.txt"])
}

func TestArchivePodPathAsZipMultipartFailureCancelsFindCommand(t *testing.T) {
	findCanceled := make(chan struct{}, 1)
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		switch {
		case reflect.DeepEqual(req.Command, tarProbeCommand):
			return kubetest.ExitCode(1)
		case isShellCommand(req.Command, "find"):
			if _, err := io.WriteString(req.Stdout, "./out/a.txt\n"); err != nil {
				return err
			}
			select {
			case <-req.Context.Done():
				select {
				case findCanceled <- struct{}{}:
				default:
				}
				return req.Context.Err()
			case <-time.After(2 * time.Second):
				return fmt.Errorf("find command not canceled")
			}
		case isShellCommand(req.Command, "cat"):
			return fmt.Errorf("permission denied")
		default:
			return fmt.Errorf("unexpected command sequence: %v", req.Command)
		}
	})

	stream, err := ArchivePodPathAsZip(context.Background(), config, "default", "pod-api", "api", "/tmp/out")
	require.NoError(t, err)
	defer stream.Close()

	_, err = io.ReadAll(stream.Reader)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "read pod file content") || strings.Contains(err.Error(), "list pod files"))

	select {
	case <-findCanceled:
	case <-time.After(1 * time.Second):
		t.Fatalf("find command was not canceled after multipart failure")
	}
}

func TestCopyTarHardLinkToZipUsesResolverOnCacheMiss(t *testing.T) {
	zipBuffer := &bytes.Buffer{}
	zw := zip.NewWriter(zipBuffer)
	cache := newTarHardLinkCache(maxHardLinkCacheTotalBytes)
	hdr := &tar.Header{
		Name:     "out/hard.txt",
		Mode:     0644,
		Typeflag: tar.TypeLink,
		Linkname: "out/base.txt",
	}
	resolveCalls := 0
	err := copyTarHardLinkToZip(zw, hdr, "out/hard.txt", cache, func(targetName string, writer io.Writer) error {
		resolveCalls++
		require.Equal(t, "out/base.txt", targetName)
		_, writeErr := io.WriteString(writer, "fallback-payload")
		return writeErr
	})
	require.NoError(t, err)
	require.Equal(t, 1, resolveCalls)
	require.NoError(t, zw.Close())

	zr, err := zip.NewReader(bytes.NewReader(zipBuffer.Bytes()), int64(zipBuffer.Len()))
	require.NoError(t, err)
	require.Len(t, zr.File, 1)
	require.Equal(t, "out/hard.txt", zr.File[0].Name)
	reader, err := zr.File[0].Open()
	require.NoError(t, err)
	payload, err := io.ReadAll(reader)
	_ = reader.Close()
	require.NoError(t, err)
	require.Equal(t, "fallback-payload", string(payload))
	_, exists := cache.Get("out/base.txt")
	require.False(t, exists)
}

func TestBuildPodExecURLPreservesHostPathPrefix(t *testing.T) {
	config := &rest.Config{
		Host:    "https://example.test/k8s-proxy",
		APIPath: "/api",
	}
	execURL, err := buildPodExecURL(config, "default", "pod-api", "api", []string{"/bin/sh", "-c", "echo hello"}, false, true, true, false)
	require.NoError(t, err)
	require.Equal(t, "/k8s-proxy/api/v1/namespaces/default/pods/pod-api/exec", execURL.Path)
	require.Equal(t, "api", execURL.Query().Get("container"))
}

func TestBuildPodExecURLAvoidsDuplicatingPrefixedAPIPath(t *testing.T) {
	config := &rest.Config{
		Host:    "https://example.test/k8s-proxy",
		APIPath: "/k8s-proxy/api",
	}
	execURL, err := buildPodExecURL(config, "default", "pod-api", "", []string{"ls"}, false, true, true, false)
	require.NoError(t, err)
	require.Equal(t, "/k8s-proxy/api/v1/namespaces/default/pods/pod-api/exec", execURL.Path)
}

func TestBuildPodExecURLAvoidsDuplicateDefaultAPIPathWhenHostHasAPI(t *testing.T) {
	config := &rest.Config{
		Host:    "https://example.test/k8s-proxy/api",
		APIPath: "/api",
	}
	execURL, err := buildPodExecURL(config, "default", "pod-api", "", []string{"ls"}, false, true, true, false)
	require.NoError(t, err)
	require.Equal(t, "/k8s-proxy/api/v1/namespaces/default/pods/pod-api/exec", execURL.Path)
}

// blockingWriter models a pipe whose reader has stopped reading.
type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.release
	return len(p), nil
}

func TestStreamPodCommandReturnsOnCancelWhileOutputIsBlocked(t *testing.T) {
	config := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		_, _ = io.WriteString(req.Stdout, strings.Repeat("x", 1024))
		<-req.Context.Done()
		return req.Context.Err()
	})
	stdout := &blockingWriter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	t.Cleanup(func() { close(stdout.release) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- streamPodCommand(ctx, config, "default", "pod-api", "api", []string{"cat"}, nil, stdout, io.Discard, false)
	}()

	<-stdout.entered
	cancel()
	// Callers such as archivePodPathWithTar close their pipe only after this
	// returns; cancellation alone must be enough to get here.
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("streamPodCommand did not return after cancellation while stdout was blocked")
	}
}

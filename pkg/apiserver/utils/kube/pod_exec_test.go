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
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/utils/exec"
)

type fakeExecutor struct {
	streamWithContext func(context.Context, remotecommand.StreamOptions) error
}

func (f fakeExecutor) Stream(options remotecommand.StreamOptions) error {
	return f.streamWithContext(context.Background(), options)
}

func (f fakeExecutor) StreamWithContext(ctx context.Context, options remotecommand.StreamOptions) error {
	return f.streamWithContext(ctx, options)
}

type fakeExitError struct {
	status int
}

func (e fakeExitError) Error() string   { return "exit error" }
func (e fakeExitError) String() string  { return e.Error() }
func (e fakeExitError) Exited() bool    { return true }
func (e fakeExitError) ExitStatus() int { return e.status }

var _ utilexec.ExitError = fakeExitError{}

func TestExecPodShellScriptBuildsShellCommandAndCapturesOutput(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	var gotCommands []string
	var gotContainer string
	newRemoteExecutor = func(_ *rest.Config, _ string, execURL *url.URL) (remotecommand.Executor, error) {
		gotCommands = append([]string(nil), execURL.Query()["command"]...)
		gotContainer = execURL.Query().Get("container")
		return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
			_, _ = io.WriteString(options.Stdout, "hello")
			_, _ = io.WriteString(options.Stderr, "warn")
			return nil
		}}, nil
	}

	result, err := ExecPodShellScript(context.Background(), client, config, "default", "pod-api", "api", "echo hello")
	require.NoError(t, err)
	require.Equal(t, []string{"/bin/sh", "-c", "echo hello"}, gotCommands)
	require.Equal(t, "api", gotContainer)
	require.Equal(t, "hello", result.Stdout)
	require.Equal(t, "warn", result.Stderr)
	require.Equal(t, 0, result.ExitCode)
	require.True(t, result.Succeeded())
}

func TestExecPodShellScriptCapsOutput(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	stdoutRaw := strings.Repeat("o", syncExecOutputLimitBytes+128)
	stderrRaw := strings.Repeat("e", syncExecOutputLimitBytes+256)
	newRemoteExecutor = func(_ *rest.Config, _ string, _ *url.URL) (remotecommand.Executor, error) {
		return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
			_, _ = io.WriteString(options.Stdout, stdoutRaw)
			_, _ = io.WriteString(options.Stderr, stderrRaw)
			return nil
		}}, nil
	}

	result, err := ExecPodShellScript(context.Background(), client, config, "default", "pod-api", "api", "echo hello")
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(result.Stdout, syncExecOutputTruncatedSuffix))
	require.True(t, strings.HasSuffix(result.Stderr, syncExecOutputTruncatedSuffix))
	require.Equal(t, strings.Repeat("o", syncExecOutputLimitBytes)+syncExecOutputTruncatedSuffix, result.Stdout)
	require.Equal(t, strings.Repeat("e", syncExecOutputLimitBytes)+syncExecOutputTruncatedSuffix, result.Stderr)
}

func TestExecPodShellScriptReturnsExitCodeOnCommandFailure(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	newRemoteExecutor = func(_ *rest.Config, _ string, _ *url.URL) (remotecommand.Executor, error) {
		return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
			_, _ = io.WriteString(options.Stderr, "boom")
			return fakeExitError{status: 17}
		}}, nil
	}

	result, err := ExecPodShellScript(context.Background(), client, config, "default", "pod-api", "api", "exit 17")
	require.NoError(t, err)
	require.Equal(t, 17, result.ExitCode)
	require.Equal(t, "boom", result.Stderr)
	require.False(t, result.Succeeded())
}

func TestStreamPodShellScriptEmitsEventsAndExit(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	newRemoteExecutor = func(_ *rest.Config, _ string, _ *url.URL) (remotecommand.Executor, error) {
		return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
			_, _ = io.WriteString(options.Stdout, "hello\n")
			_, _ = io.WriteString(options.Stderr, "warn\n")
			return fakeExitError{status: 5}
		}}, nil
	}

	events, err := StreamPodShellScript(context.Background(), client, config, "default", "pod-api", "api", "echo hello")
	require.NoError(t, err)
	got := make([]PodShellStreamEvent, 0, 3)
	for event := range events {
		got = append(got, event)
	}
	require.Len(t, got, 3)
	require.Equal(t, PodShellStreamEventStdout, got[0].Type)
	require.Equal(t, "hello\n", got[0].Chunk)
	require.Equal(t, PodShellStreamEventStderr, got[1].Type)
	require.Equal(t, "warn\n", got[1].Chunk)
	require.Equal(t, PodShellStreamEventExit, got[2].Type)
	require.Equal(t, 5, got[2].ExitCode)
	require.False(t, got[2].Succeeded)
}

func TestArchivePodPathAsZipCreatesZipStream(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	var gotTarCommand []string
	newRemoteExecutor = func(_ *rest.Config, _ string, execURL *url.URL) (remotecommand.Executor, error) {
		commands := append([]string(nil), execURL.Query()["command"]...)
		if reflect.DeepEqual(commands, []string{"/bin/sh", "-c", "command -v tar >/dev/null 2>&1"}) {
			return fakeExecutor{streamWithContext: func(_ context.Context, _ remotecommand.StreamOptions) error {
				return nil
			}}, nil
		}
		gotTarCommand = append([]string(nil), commands...)
		return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
			buf := &bytes.Buffer{}
			tw := tar.NewWriter(buf)
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out", Mode: 0755, Typeflag: tar.TypeDir}))
			data := []byte("payload")
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out/file.txt", Mode: 0644, Size: int64(len(data)), Typeflag: tar.TypeReg}))
			_, _ = tw.Write(data)
			require.NoError(t, tw.Close())
			_, _ = options.Stdout.Write(buf.Bytes())
			return nil
		}}, nil
	}

	stream, err := ArchivePodPathAsZip(context.Background(), client, config, "default", "pod-api", "api", "/tmp/out")
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
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	newRemoteExecutor = func(_ *rest.Config, _ string, execURL *url.URL) (remotecommand.Executor, error) {
		commands := append([]string(nil), execURL.Query()["command"]...)
		if reflect.DeepEqual(commands, []string{"/bin/sh", "-c", "command -v tar >/dev/null 2>&1"}) {
			return fakeExecutor{streamWithContext: func(_ context.Context, _ remotecommand.StreamOptions) error {
				return fakeExitError{status: 1}
			}}, nil
		}
		if len(commands) >= 3 && commands[0] == "/bin/sh" && commands[1] == "-c" && strings.Contains(commands[2], "find") {
			return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
				_, _ = io.WriteString(options.Stdout, "./out/a.txt\n./out/nested/b.txt\n")
				return nil
			}}, nil
		}
		if len(commands) >= 3 && commands[0] == "/bin/sh" && commands[1] == "-c" && strings.Contains(commands[2], "cat") {
			target := commands[len(commands)-1]
			return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
				switch target {
				case "out/a.txt", "./out/a.txt":
					_, _ = io.WriteString(options.Stdout, "A")
				case "out/nested/b.txt", "./out/nested/b.txt":
					_, _ = io.WriteString(options.Stdout, "B")
				default:
					return fmt.Errorf("unexpected cat target: %s", target)
				}
				return nil
			}}, nil
		}
		return nil, fmt.Errorf("unexpected command sequence: %v", commands)
	}

	stream, err := ArchivePodPathAsZip(context.Background(), client, config, "default", "pod-api", "api", "/tmp/out")
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
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	newRemoteExecutor = func(_ *rest.Config, _ string, execURL *url.URL) (remotecommand.Executor, error) {
		commands := append([]string(nil), execURL.Query()["command"]...)
		if reflect.DeepEqual(commands, []string{"/bin/sh", "-c", "command -v tar >/dev/null 2>&1"}) {
			return fakeExecutor{streamWithContext: func(_ context.Context, _ remotecommand.StreamOptions) error {
				return nil
			}}, nil
		}
		return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
			_, _ = options.Stdout.Write([]byte("not-a-tar"))
			return nil
		}}, nil
	}

	stream, err := ArchivePodPathAsZip(context.Background(), client, config, "default", "pod-api", "api", "/tmp/out")
	require.NoError(t, err)
	defer stream.Close()
	_, err = io.ReadAll(stream.Reader)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read tar stream")
}

func TestArchivePodPathAsZipMarksLookupErrors(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	newRemoteExecutor = func(_ *rest.Config, _ string, execURL *url.URL) (remotecommand.Executor, error) {
		commands := append([]string(nil), execURL.Query()["command"]...)
		if reflect.DeepEqual(commands, []string{"/bin/sh", "-c", "command -v tar >/dev/null 2>&1"}) {
			return fakeExecutor{streamWithContext: func(_ context.Context, _ remotecommand.StreamOptions) error {
				return nil
			}}, nil
		}
		return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
			_, _ = io.WriteString(options.Stderr, "tar: out: cannot stat: no such file or directory")
			return fakeExitError{status: 2}
		}}, nil
	}

	stream, err := ArchivePodPathAsZip(context.Background(), client, config, "default", "pod-api", "api", "/tmp/out")
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
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	for _, targetPath := range []string{"..", "../..", "../out", "a/../../b"} {
		targetPath := targetPath
		t.Run(targetPath, func(t *testing.T) {
			stream, err := ArchivePodPathAsZip(context.Background(), client, config, "default", "pod-api", "api", targetPath)
			require.Nil(t, stream)
			require.Error(t, err)
			require.True(t, IsArchivePathInvalidError(err))
		})
	}
}

func TestArchivePodPathAsZipSupportsHardLinkEntry(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	newRemoteExecutor = func(_ *rest.Config, _ string, execURL *url.URL) (remotecommand.Executor, error) {
		commands := append([]string(nil), execURL.Query()["command"]...)
		if reflect.DeepEqual(commands, []string{"/bin/sh", "-c", "command -v tar >/dev/null 2>&1"}) {
			return fakeExecutor{streamWithContext: func(_ context.Context, _ remotecommand.StreamOptions) error {
				return nil
			}}, nil
		}
		return fakeExecutor{streamWithContext: func(_ context.Context, options remotecommand.StreamOptions) error {
			buf := &bytes.Buffer{}
			tw := tar.NewWriter(buf)
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out", Mode: 0755, Typeflag: tar.TypeDir}))
			regular := []byte("base")
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out/base.txt", Mode: 0644, Size: int64(len(regular)), Typeflag: tar.TypeReg}))
			_, _ = tw.Write(regular)
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out/hard.txt", Mode: 0644, Typeflag: tar.TypeLink, Linkname: "out/base.txt"}))
			require.NoError(t, tw.Close())
			_, _ = options.Stdout.Write(buf.Bytes())
			return nil
		}}, nil
	}

	stream, err := ArchivePodPathAsZip(context.Background(), client, config, "default", "pod-api", "api", "/tmp/out")
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

func TestArchivePodPathAsZipMultipartFailureCancelsFindCommand(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	config := &rest.Config{Host: "https://example.test"}

	findCanceled := make(chan struct{}, 1)
	orig := newRemoteExecutor
	defer func() { newRemoteExecutor = orig }()
	newRemoteExecutor = func(_ *rest.Config, _ string, execURL *url.URL) (remotecommand.Executor, error) {
		commands := append([]string(nil), execURL.Query()["command"]...)
		if reflect.DeepEqual(commands, []string{"/bin/sh", "-c", "command -v tar >/dev/null 2>&1"}) {
			return fakeExecutor{streamWithContext: func(_ context.Context, _ remotecommand.StreamOptions) error {
				return fakeExitError{status: 1}
			}}, nil
		}
		if len(commands) >= 3 && commands[0] == "/bin/sh" && commands[1] == "-c" && strings.Contains(commands[2], "find") {
			return fakeExecutor{streamWithContext: func(ctx context.Context, options remotecommand.StreamOptions) error {
				if _, err := io.WriteString(options.Stdout, "./out/a.txt\n"); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					select {
					case findCanceled <- struct{}{}:
					default:
					}
					return ctx.Err()
				case <-time.After(2 * time.Second):
					return fmt.Errorf("find command not canceled")
				}
			}}, nil
		}
		if len(commands) >= 3 && commands[0] == "/bin/sh" && commands[1] == "-c" && strings.Contains(commands[2], "cat") {
			return fakeExecutor{streamWithContext: func(_ context.Context, _ remotecommand.StreamOptions) error {
				return fmt.Errorf("permission denied")
			}}, nil
		}
		return nil, fmt.Errorf("unexpected command sequence: %v", commands)
	}

	stream, err := ArchivePodPathAsZip(context.Background(), client, config, "default", "pod-api", "api", "/tmp/out")
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

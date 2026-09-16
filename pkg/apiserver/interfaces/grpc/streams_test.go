package grpcapi

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeArchiveSender struct {
	chunks [][]byte
	delay  time.Duration
	onSend func()
}

func (s *fakeArchiveSender) Send(item *eruunv1.ArchiveChunk) error {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.chunks = append(s.chunks, append([]byte(nil), item.Data...))
	if s.onSend != nil {
		s.onSend()
	}
	return nil
}

func TestArchiveStreamingChunkBoundSlowClientAndCleanup(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 3*archiveChunkSize+19)
	sender := &fakeArchiveSender{delay: time.Millisecond}
	var staged string
	err := streamArchive(context.Background(), sender, func(_ context.Context, writer io.Writer) error {
		staged = writer.(*os.File).Name()
		_, err := writer.Write(data)
		return err
	})
	require.NoError(t, err)
	require.Len(t, sender.chunks, 4)
	for _, chunk := range sender.chunks {
		require.LessOrEqual(t, len(chunk), archiveChunkSize)
	}
	require.Equal(t, data, bytes.Join(sender.chunks, nil))
	_, statErr := os.Stat(staged)
	require.True(t, errors.Is(statErr, os.ErrNotExist), "staged file must be removed")
}

func TestArchiveStreamingCancellationClosesStagedFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := &fakeArchiveSender{onSend: cancel}
	var staged string
	err := streamArchive(ctx, sender, func(_ context.Context, writer io.Writer) error {
		staged = writer.(*os.File).Name()
		_, err := writer.Write(bytes.Repeat([]byte("x"), 2*archiveChunkSize))
		return err
	})
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Len(t, sender.chunks, 1)
	_, statErr := os.Stat(staged)
	require.True(t, errors.Is(statErr, os.ErrNotExist))
}

type fakeUploadStream struct {
	grpc.ServerStream
	ctx      context.Context
	parts    []*eruunv1.UploadDatasetPart
	position int
	block    <-chan struct{}
	sent     *eruunv1.JobArtifact
}

func (s *fakeUploadStream) Context() context.Context { return s.ctx }
func (s *fakeUploadStream) Recv() (*eruunv1.UploadDatasetPart, error) {
	if s.position < len(s.parts) {
		item := s.parts[s.position]
		s.position++
		return item, nil
	}
	if s.block != nil {
		<-s.block
	}
	return nil, io.EOF
}
func (s *fakeUploadStream) SendAndClose(item *eruunv1.JobArtifact) error { s.sent = item; return nil }

func scopedUploadContext(parent context.Context) context.Context {
	return account.WithScope(parent, account.Scope{UserID: "user", WorkspaceID: "workspace", Namespace: "ns", Role: "member"})
}

func TestDatasetUploadFrameBoundTotalLimitAndCancellation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	header := &eruunv1.UploadDatasetPart{Part: &eruunv1.UploadDatasetPart_Header{Header: &eruunv1.UploadDatasetHeader{Name: "dataset"}}}
	oversize := &fakeUploadStream{ctx: scopedUploadContext(context.Background()), parts: []*eruunv1.UploadDatasetPart{
		header, {Part: &eruunv1.UploadDatasetPart_Chunk{Chunk: make([]byte, archiveChunkSize+1)}},
	}}
	err := (&JobsServer{}).UploadJobDataset(oversize)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	files, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, files)

	chunk := make([]byte, archiveChunkSize)
	parts := make([]*eruunv1.UploadDatasetPart, 0, archiveMaxSize/archiveChunkSize+2)
	parts = append(parts, header)
	for i := 0; i <= archiveMaxSize/archiveChunkSize; i++ {
		parts = append(parts, &eruunv1.UploadDatasetPart{Part: &eruunv1.UploadDatasetPart_Chunk{Chunk: chunk}})
	}
	tooLarge := &fakeUploadStream{ctx: scopedUploadContext(context.Background()), parts: parts}
	err = (&JobsServer{}).UploadJobDataset(tooLarge)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	files, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, files)

	ctx, cancel := context.WithCancel(context.Background())
	blocked := make(chan struct{})
	stream := &fakeUploadStream{ctx: scopedUploadContext(ctx), parts: []*eruunv1.UploadDatasetPart{header}, block: blocked}
	done := make(chan error, 1)
	go func() { done <- (&JobsServer{}).UploadJobDataset(stream) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	err = <-done
	close(blocked)
	require.Equal(t, codes.Canceled, status.Code(err))
	files, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, files)
}

type trackedReadCloser struct {
	io.Reader
	closed atomic.Int32
}

func (r *trackedReadCloser) Close() error { r.closed.Add(1); return nil }

func TestComponentFileStreamChunkLimitAndReaderClose(t *testing.T) {
	reader := &trackedReadCloser{Reader: bytes.NewReader(bytes.Repeat([]byte("z"), 2*archiveChunkSize+7))}
	var chunks []*eruunv1.ComponentFileChunk
	err := streamComponentFile(context.Background(), &service.ComponentFileArchiveStream{
		Reader: reader, FileName: "archive.zip", ContentType: "application/zip",
	}, func(item *eruunv1.ComponentFileChunk) error { chunks = append(chunks, item); return nil })
	require.NoError(t, err)
	require.Equal(t, int32(1), reader.closed.Load())
	require.Equal(t, "archive.zip", chunks[0].FileName)
	require.Empty(t, chunks[0].Data)
	for _, item := range chunks[1:] {
		require.LessOrEqual(t, len(item.Data), archiveChunkSize)
	}
	var collected [][]byte
	for _, item := range chunks[1:] {
		collected = append(collected, item.Data)
	}
	require.Equal(t, 2*archiveChunkSize+7, len(bytes.Join(collected, nil)))
}

type fakeDatasetDownloadStream struct {
	grpc.ServerStream
	ctx    context.Context
	chunks [][]byte
}

func (s *fakeDatasetDownloadStream) Context() context.Context { return s.ctx }
func (s *fakeDatasetDownloadStream) Send(item *eruunv1.ArchiveChunk) error {
	s.chunks = append(s.chunks, append([]byte(nil), item.Data...))
	return nil
}

func validDatasetArchive(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	zip := gzip.NewWriter(&out)
	archive := tar.NewWriter(zip)
	entries := map[string]string{
		"instruction.md":         "Build the requested artifact",
		"task.toml":              "version = \"1.0\"\n[environment]\ndocker_image = \"example/task:1.0\"\n",
		"environment/Dockerfile": "FROM example/task:1.0\n",
		"tests/test.sh":          "#!/bin/sh\necho 1 > /logs/verifier/reward.txt\n",
	}
	for name, body := range entries {
		require.NoError(t, archive.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(body))}))
		_, err := io.WriteString(archive, body)
		require.NoError(t, err)
	}
	require.NoError(t, archive.Close())
	require.NoError(t, zip.Close())
	return out.Bytes()
}

func TestDatasetUploadAndDownloadUseExistingArtifactStore(t *testing.T) {
	accounts, delivery := grpcTestAccounts(t)
	_, p := grpcRegisterTestUser(t, accounts, delivery, "grpc-dataset@example.com")
	space, err := accounts.Workspace(context.Background(), p, "")
	require.NoError(t, err)
	store, err := artifacts.New(accounts.Repo.Store, nil)
	require.NoError(t, err)
	adapter := &JobsServer{Jobs: &jobs.Service{Artifacts: store}}
	ctx := account.WithScope(context.Background(), account.Scope{
		UserID: p.User.ID, WorkspaceID: space.Workspace.ID, Namespace: space.Workspace.Namespace, Role: "owner",
	})
	data := validDatasetArchive(t)
	upload := &fakeUploadStream{ctx: ctx, parts: []*eruunv1.UploadDatasetPart{
		{Part: &eruunv1.UploadDatasetPart_Header{Header: &eruunv1.UploadDatasetHeader{Name: "task package"}}},
		{Part: &eruunv1.UploadDatasetPart_Chunk{Chunk: data}},
	}}
	require.NoError(t, adapter.UploadJobDataset(upload))
	require.NotNil(t, upload.sent)
	require.Equal(t, space.Workspace.ID, upload.sent.WorkspaceId)
	require.Equal(t, int64(len(data)), upload.sent.Size)
	item, err := adapter.GetJobDataset(ctx, &eruunv1.JobDatasetRequest{DatasetId: upload.sent.Id})
	require.NoError(t, err)
	require.Equal(t, upload.sent.Digest, item.Digest)
	download := &fakeDatasetDownloadStream{ctx: ctx}
	require.NoError(t, adapter.DownloadJobDataset(&eruunv1.JobDatasetRequest{DatasetId: upload.sent.Id}, download))
	for _, chunk := range download.chunks {
		require.LessOrEqual(t, len(chunk), archiveChunkSize)
	}
	require.Equal(t, data, bytes.Join(download.chunks, nil))
}

type fakeStreamApplicationService struct {
	service.ApplicationsService
	logs  *service.ComponentLogStream
	shell *service.ComponentShellScriptStream
}

func (f *fakeStreamApplicationService) StreamComponentLogs(context.Context, string, string, string) (*service.ComponentLogStream, error) {
	return f.logs, nil
}
func (f *fakeStreamApplicationService) StreamComponentShellScript(context.Context, string, string, apis.ExecComponentShellScriptRequest) (*service.ComponentShellScriptStream, error) {
	return f.shell, nil
}

type fakeLogStream struct {
	grpc.ServerStream
	ctx   context.Context
	items []*eruunv1.ComponentLogLine
}

func (s *fakeLogStream) Context() context.Context { return s.ctx }
func (s *fakeLogStream) Send(item *eruunv1.ComponentLogLine) error {
	s.items = append(s.items, item)
	return nil
}

type fakeShellStream struct {
	grpc.ServerStream
	ctx   context.Context
	items []*eruunv1.ComponentShellEvent
}

func (s *fakeShellStream) Context() context.Context { return s.ctx }
func (s *fakeShellStream) Send(item *eruunv1.ComponentShellEvent) error {
	s.items = append(s.items, item)
	return nil
}

func TestLogAndShellStreamsBoundChunksAndCloseResources(t *testing.T) {
	longLine := bytes.Repeat([]byte("x"), 2*archiveChunkSize+9)
	reader := &trackedReadCloser{Reader: bytes.NewReader(append(append([]byte(nil), longLine...), '\n'))}
	adapter := &ApplicationsServer{Applications: &fakeStreamApplicationService{
		logs: &service.ComponentLogStream{Reader: reader, PodName: "pod", ContainerName: "api"},
	}}
	logs := &fakeLogStream{ctx: context.Background()}
	require.NoError(t, adapter.StreamComponentLogs(&eruunv1.ComponentLogsRequest{AppId: "app", ComponentName: "api"}, logs))
	require.Equal(t, int32(1), reader.closed.Load())
	require.Greater(t, len(logs.items), 1)
	var fragments [][]byte
	for i, item := range logs.items {
		require.LessOrEqual(t, len(item.Data), archiveChunkSize)
		if i < len(logs.items)-1 {
			require.True(t, item.Continuation)
		} else {
			require.False(t, item.Continuation)
		}
		fragments = append(fragments, item.Data)
	}
	require.Equal(t, longLine, bytes.Join(fragments, nil))

	data := append(bytes.Repeat([]byte("z"), 2*archiveChunkSize+3), 0xff)
	events := make(chan kube.PodShellStreamEvent, 2)
	events <- kube.PodShellStreamEvent{Type: kube.PodShellStreamEventStdout, Chunk: string(data)}
	events <- kube.PodShellStreamEvent{Type: kube.PodShellStreamEventExit, ExitCode: 0, Succeeded: true}
	close(events)
	adapter.Applications = &fakeStreamApplicationService{shell: &service.ComponentShellScriptStream{Events: events, PodName: "pod", ContainerName: "api"}}
	shell := &fakeShellStream{ctx: context.Background()}
	require.NoError(t, adapter.StreamComponentShell(&eruunv1.ComponentShellRequest{
		AppId: "app", ComponentName: "api", Request: &eruunv1.AppDTOExecComponentShellScriptRequest{Script: "echo hi"},
	}, shell))
	require.Equal(t, 4, len(shell.items))
	var emitted [][]byte
	for _, item := range shell.items[:len(shell.items)-1] {
		require.LessOrEqual(t, len(item.Chunk), archiveChunkSize)
		emitted = append(emitted, item.Chunk)
	}
	require.Equal(t, data, bytes.Join(emitted, nil))
	require.Equal(t, "exit", shell.items[len(shell.items)-1].Type)
	require.True(t, shell.items[len(shell.items)-1].GetSucceeded())
}

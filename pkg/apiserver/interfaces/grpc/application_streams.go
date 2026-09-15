package grpcapi

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

func (s *ApplicationsServer) StreamComponentLogs(req *eruunv1.ComponentLogsRequest, stream eruunv1.ApplicationService_StreamComponentLogsServer) error {
	if req == nil || appID(req.AppId) != nil || strings.TrimSpace(req.ComponentName) == "" {
		return rpcError(bcode.ErrApplicationConfig)
	}
	logs, err := s.Applications.StreamComponentLogs(stream.Context(), req.AppId, req.ComponentName, strings.TrimSpace(req.Container))
	if err != nil {
		return rpcError(err)
	}
	if logs == nil || logs.Reader == nil {
		return rpcError(bcode.ErrComponentLogUnavailable)
	}
	defer logs.Close()
	stopClose := context.AfterFunc(stream.Context(), func() { _ = logs.Close() })
	defer stopClose()
	reader := bufio.NewReaderSize(logs.Reader, archiveChunkSize)
	for {
		if err := stream.Context().Err(); err != nil {
			return rpcError(err)
		}
		line, err := reader.ReadSlice('\n')
		if len(line) > 0 {
			payload := line
			if !errors.Is(err, bufio.ErrBufferFull) {
				payload = []byte(strings.TrimRight(string(line), "\r\n"))
			}
			item := &eruunv1.ComponentLogLine{PodName: logs.PodName, ContainerName: logs.ContainerName, Data: append([]byte(nil), payload...), Continuation: errors.Is(err, bufio.ErrBufferFull)}
			if sendErr := stream.Send(item); sendErr != nil {
				return rpcError(sendErr)
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return rpcError(fmt.Errorf("read component logs: %w", err))
		}
	}
}

func streamComponentFile(ctx context.Context, archive *service.ComponentFileArchiveStream, send func(*eruunv1.ComponentFileChunk) error) error {
	if archive == nil || archive.Reader == nil {
		return rpcError(fmt.Errorf("component file archive is empty"))
	}
	defer archive.Close()
	stopClose := context.AfterFunc(ctx, func() { _ = archive.Close() })
	defer stopClose()
	buffered := bufio.NewReader(archive.Reader)
	if _, err := buffered.Peek(1); err != nil {
		if kube.IsArchivePathInvalidError(err) || kube.IsArchivePathLookupError(err) {
			return rpcError(bcode.ErrComponentFilePathInvalid)
		}
		return rpcError(fmt.Errorf("read component file archive: %w", err))
	}
	if err := send(&eruunv1.ComponentFileChunk{
		FileName: archive.FileName, ContentType: archive.ContentType,
		PodName: archive.PodName, ContainerName: archive.ContainerName,
	}); err != nil {
		return rpcError(err)
	}
	buf := make([]byte, archiveChunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return rpcError(err)
		}
		n, readErr := buffered.Read(buf)
		if n > 0 {
			if err := send(&eruunv1.ComponentFileChunk{Data: append([]byte(nil), buf[:n]...)}); err != nil {
				return rpcError(err)
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return rpcError(fmt.Errorf("read component file archive: %w", readErr))
		}
	}
}

func (s *ApplicationsServer) ExportComponentFiles(req *eruunv1.ComponentFilesRequest, stream eruunv1.ApplicationService_ExportComponentFilesServer) error {
	if req == nil || appID(req.AppId) != nil || req.ComponentName == "" {
		return rpcError(bcode.ErrApplicationConfig)
	}
	input, err := decodeTypedRequest[apis.ExportComponentFilesRequest](req.Request)
	if err != nil {
		return rpcError(bcode.ErrApplicationConfig)
	}
	if strings.TrimSpace(input.Path) == "" {
		return rpcError(bcode.ErrComponentFilePathInvalid)
	}
	archive, err := s.Applications.ExportComponentFilesZip(stream.Context(), req.AppId, req.ComponentName, input)
	if err != nil {
		return rpcError(err)
	}
	return streamComponentFile(stream.Context(), archive, stream.Send)
}

func (s *ApplicationsServer) ExecComponentShell(ctx context.Context, req *eruunv1.ComponentShellRequest) (*eruunv1.AppDTOExecComponentShellScriptResponse, error) {
	if req == nil || appID(req.AppId) != nil || req.ComponentName == "" {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input, err := decodeTypedRequest[apis.ExecComponentShellScriptRequest](req.Request)
	if err != nil || strings.TrimSpace(input.Script) == "" {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.Applications.ExecComponentShellScript(ctx, req.AppId, req.ComponentName, input)
	return typedResult(resp, err, &eruunv1.AppDTOExecComponentShellScriptResponse{})
}

func (s *ApplicationsServer) StreamComponentShell(req *eruunv1.ComponentShellRequest, stream eruunv1.ApplicationService_StreamComponentShellServer) error {
	if req == nil || appID(req.AppId) != nil || req.ComponentName == "" {
		return rpcError(bcode.ErrApplicationConfig)
	}
	input, err := decodeTypedRequest[apis.ExecComponentShellScriptRequest](req.Request)
	if err != nil || strings.TrimSpace(input.Script) == "" {
		return rpcError(bcode.ErrApplicationConfig)
	}
	output, err := s.Applications.StreamComponentShellScript(stream.Context(), req.AppId, req.ComponentName, input)
	if err != nil {
		return rpcError(err)
	}
	if output == nil || output.Events == nil {
		return rpcError(fmt.Errorf("shell stream is empty"))
	}
	for {
		select {
		case <-stream.Context().Done():
			return rpcError(stream.Context().Err())
		case event, ok := <-output.Events:
			if !ok {
				return nil
			}
			item := &eruunv1.ComponentShellEvent{
				PodName: output.PodName, ContainerName: output.ContainerName,
				Type: string(event.Type), Message: event.Message,
			}
			if event.Type == kube.PodShellStreamEventExit || event.Type == kube.PodShellStreamEventError {
				code := int32(event.ExitCode)
				item.ExitCode = &code
				result := event.Succeeded
				item.Succeeded = &result
			}
			data := []byte(event.Chunk)
			if len(data) == 0 {
				if err := stream.Send(item); err != nil {
					return rpcError(err)
				}
				continue
			}
			for len(data) > 0 {
				n := len(data)
				if n > archiveChunkSize {
					n = archiveChunkSize
				}
				part := &eruunv1.ComponentShellEvent{
					PodName: item.PodName, ContainerName: item.ContainerName,
					Type: item.Type, Message: item.Message,
					ExitCode: item.ExitCode, Succeeded: item.Succeeded,
					Chunk: append([]byte(nil), data[:n]...),
				}
				if err := stream.Send(part); err != nil {
					return rpcError(err)
				}
				data = data[n:]
			}
		}
	}
}

func (s *ApplicationsServer) DownloadLogArchive(req *eruunv1.LogArchiveRPCRequest, stream eruunv1.ApplicationService_DownloadLogArchiveServer) error {
	if req == nil || appID(req.AppId) != nil {
		return rpcError(bcode.ErrApplicationConfig)
	}
	input, err := decodeTypedRequest[apis.LogArchiveDownloadRequest](req.Request)
	if err != nil {
		return rpcError(bcode.ErrApplicationConfig)
	}
	if input.JobType != "" && input.JobType != config.JobLogArchiveUpload {
		return rpcError(bcode.ErrApplicationConfig)
	}
	if len(input.Components) != 1 || strings.TrimSpace(input.Components[0]) == "" {
		return rpcError(bcode.ErrApplicationConfig)
	}
	if strings.TrimSpace(input.Path) == "" {
		return rpcError(bcode.ErrComponentFilePathInvalid)
	}
	archive, err := s.Applications.DownloadLogArchive(stream.Context(), req.AppId, input)
	if err != nil {
		return rpcError(err)
	}
	return streamComponentFile(stream.Context(), archive, stream.Send)
}

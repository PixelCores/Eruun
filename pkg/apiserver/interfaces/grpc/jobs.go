package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	corev1 "k8s.io/api/core/v1"
)

const archiveChunkSize = 64 << 10
const archiveMaxSize = 64 << 20

type JobsServer struct {
	eruunv1.UnimplementedJobsServiceServer
	Jobs     *jobs.Service           `inject:""`
	Workflow service.WorkflowService `inject:""`
}

func jobPolicyInput(p *eruunv1.JobResultPolicy) spec.JobResultPolicy {
	result := spec.JobResultPolicy{}
	if p == nil {
		return result
	}
	result.RetentionDays = int(p.RetentionDays)
	for _, target := range p.Targets {
		if target != nil {
			result.Targets = append(result.Targets, spec.JobResultTarget{Type: target.Type, Mode: target.Mode})
		}
	}
	return result
}

func jobPolicyOutput(p spec.JobResultPolicy) *eruunv1.JobResultPolicy {
	result := &eruunv1.JobResultPolicy{RetentionDays: int32(p.RetentionDays)}
	for _, target := range p.Targets {
		result.Targets = append(result.Targets, &eruunv1.JobResultTarget{Type: target.Type, Mode: target.Mode})
	}
	return result
}

func jobTraitsInput(p *eruunv1.JobTraits) (spec.JobTraits, error) {
	result := spec.JobTraits{}
	if p == nil {
		return result, nil
	}
	for _, storage := range p.Storage {
		if storage == nil {
			continue
		}
		result.Storage = append(result.Storage, spec.StorageTraitSpec{
			Name: storage.Name, Type: storage.Type, MountPath: storage.MountPath, SubPath: storage.SubPath,
			SubPathExpr: storage.SubPathExpr, ReadOnly: storage.ReadOnly, SourceName: storage.SourceName,
			TmpCreate: storage.TmpCreate, Size: storage.Size, ClaimName: storage.ClaimName, StorageClass: storage.StorageClass,
		})
	}
	for _, source := range p.EnvFrom {
		if source != nil {
			result.EnvFrom = append(result.EnvFrom, spec.EnvFromSourceSpec{Type: source.Type, SourceName: source.SourceName})
		}
	}
	for _, env := range p.Envs {
		if env == nil {
			continue
		}
		value := spec.ValueSource{}
		if env.ValueFrom != nil {
			if env.ValueFrom.Static != nil {
				s := env.ValueFrom.GetStatic()
				value.Static = &s
			}
			if env.ValueFrom.Field != nil {
				s := env.ValueFrom.GetField()
				value.Field = &s
			}
			if env.ValueFrom.Secret != nil {
				value.Secret = &spec.SecretSelectorSpec{Name: env.ValueFrom.Secret.Name, Key: env.ValueFrom.Secret.Key}
			}
			if env.ValueFrom.Config != nil {
				value.Config = &spec.ConfigMapSelectorSpec{Name: env.ValueFrom.Config.Name, Key: env.ValueFrom.Config.Key}
			}
		}
		result.Envs = append(result.Envs, spec.SimplifiedEnvSpec{Name: env.Name, ValueFrom: value})
	}
	// target_work_env stays in the proto for field-number stability, but Jobs
	// have never accepted it; reject it rather than dropping it silently.
	if len(p.TargetWorkEnv) > 0 {
		return spec.JobTraits{}, fmt.Errorf("unsupported standalone Job trait")
	}
	if p.Resources != nil {
		result.Resources = &spec.ResourceTraitsSpec{CPU: p.Resources.Cpu, Memory: p.Resources.Memory, GPU: p.Resources.Gpu, CPULimit: p.Resources.CpuLimit, MemoryLimit: p.Resources.MemoryLimit}
	}
	if p.SecurityPolicy != nil {
		policy, err := decodeTypedRequest[corev1.SecurityContext](p.SecurityPolicy)
		if err != nil {
			return spec.JobTraits{}, fmt.Errorf("decode Job security policy: %w", err)
		}
		result.SecurityPolicy = &policy
	}
	if p.Eval != nil {
		evaluation, err := decodeTypedRequest[spec.EvaluationTraitSpec](p.Eval)
		if err != nil {
			return spec.JobTraits{}, fmt.Errorf("decode evaluation trait: %w", err)
		}
		result.Evaluation = &evaluation
	}
	return result, nil
}

func jobTraitsOutput(p spec.JobTraits) (*eruunv1.JobTraits, error) {
	result := &eruunv1.JobTraits{}
	for _, storage := range p.Storage {
		result.Storage = append(result.Storage, &eruunv1.JobStorageTrait{
			Name: storage.Name, Type: storage.Type, MountPath: storage.MountPath, SubPath: storage.SubPath,
			SubPathExpr: storage.SubPathExpr, ReadOnly: storage.ReadOnly, SourceName: storage.SourceName,
			TmpCreate: storage.TmpCreate, Size: storage.Size, ClaimName: storage.ClaimName, StorageClass: storage.StorageClass,
		})
	}
	for _, source := range p.EnvFrom {
		result.EnvFrom = append(result.EnvFrom, &eruunv1.JobEnvFrom{Type: source.Type, SourceName: source.SourceName})
	}
	for _, env := range p.Envs {
		value := &eruunv1.JobValueSource{}
		if env.ValueFrom.Static != nil {
			s := *env.ValueFrom.Static
			value.Static = &s
		}
		if env.ValueFrom.Field != nil {
			s := *env.ValueFrom.Field
			value.Field = &s
		}
		if env.ValueFrom.Secret != nil {
			value.Secret = &eruunv1.JobSecretSelector{Name: env.ValueFrom.Secret.Name, Key: env.ValueFrom.Secret.Key}
		}
		if env.ValueFrom.Config != nil {
			value.Config = &eruunv1.JobConfigSelector{Name: env.ValueFrom.Config.Name, Key: env.ValueFrom.Config.Key}
		}
		result.Envs = append(result.Envs, &eruunv1.JobEnv{Name: env.Name, ValueFrom: value})
	}
	if p.Resources != nil {
		result.Resources = &eruunv1.JobResourceTrait{Cpu: p.Resources.CPU, Memory: p.Resources.Memory, Gpu: p.Resources.GPU, CpuLimit: p.Resources.CPULimit, MemoryLimit: p.Resources.MemoryLimit}
	}
	if p.SecurityPolicy != nil {
		policy, err := encodeTypedResponse(p.SecurityPolicy, &eruunv1.AppKubeCoreSecurityContext{})
		if err != nil {
			return nil, fmt.Errorf("encode Job security policy: %w", err)
		}
		result.SecurityPolicy = policy
	}
	if p.Evaluation != nil {
		evaluation, err := encodeTypedResponse(p.Evaluation, &eruunv1.EvaluationTrait{})
		if err != nil {
			return nil, fmt.Errorf("encode evaluation trait: %w", err)
		}
		result.Eval = evaluation
	}
	return result, nil
}

func jsonValue(raw []byte) (*structpb.Value, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	result := &structpb.Value{}
	if err := protojson.Unmarshal(raw, result); err != nil {
		return nil, fmt.Errorf("decode JSON value: %w", err)
	}
	return result, nil
}

func jobSpecOutput(p spec.JobSpec) (*eruunv1.JobSpec, error) {
	value, err := jsonValue(p.Spec)
	if err != nil {
		return nil, err
	}
	traits, err := jobTraitsOutput(p.Traits)
	if err != nil {
		return nil, err
	}
	return &eruunv1.JobSpec{Name: p.Name, Type: p.Type, Spec: value, Traits: traits}, nil
}

func jobArtifactOutput(item *model.JobArtifact) (*eruunv1.JobArtifact, error) {
	if item == nil {
		return nil, fmt.Errorf("job artifact is nil")
	}
	manifest, err := jsonValue(item.Manifest)
	if err != nil {
		return nil, err
	}
	summary, err := jsonValue(item.Summary)
	if err != nil {
		return nil, err
	}
	result := &eruunv1.JobArtifact{
		Id: item.ID, WorkspaceId: item.WorkspaceID, TaskId: item.TaskID, ExecutionKey: item.ExecutionKey, Kind: item.Kind,
		Name: item.Name, Digest: item.Digest, Size: item.Size, Manifest: manifest, Summary: summary,
		Reference: item.Reference, Expired: item.Expired,
		CreateTime: timeMessage(item.CreateTime), UpdateTime: timeMessage(item.UpdateTime),
	}
	if item.ExpiresAt != nil {
		result.ExpiresAt = timeMessage(*item.ExpiresAt)
	}
	return result, nil
}

func jobArtifactsOutput(items []*model.JobArtifact) ([]*eruunv1.JobArtifact, error) {
	result := make([]*eruunv1.JobArtifact, 0, len(items))
	for _, item := range items {
		value, err := jobArtifactOutput(item)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func jobDeliveriesOutput(items []*model.JobDelivery) []*eruunv1.JobDelivery {
	result := make([]*eruunv1.JobDelivery, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		result = append(result, &eruunv1.JobDelivery{
			Id: item.ID, WorkspaceId: item.WorkspaceID, TaskId: item.TaskID, ExecutionKey: item.ExecutionKey, SourceId: item.SourceID,
			Target: item.Target, Mode: item.Mode, State: item.State, Attempts: int32(item.Attempts),
			Error: item.LastError, Reference: item.Reference,
			CreateTime: timeMessage(item.CreateTime), UpdateTime: timeMessage(item.UpdateTime),
		})
	}
	return result
}

func jobSubmitInput(req *eruunv1.SubmitJobRequest) (jobs.SubmitRequest, error) {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return jobs.SubmitRequest{}, bcode.ErrJobInput
	}
	var raw []byte
	if req.Spec != nil {
		var err error
		raw, err = protojson.Marshal(req.Spec)
		if err != nil {
			return jobs.SubmitRequest{}, bcode.ErrJobInput
		}
	}
	traits, err := jobTraitsInput(req.Traits)
	if err != nil {
		return jobs.SubmitRequest{}, bcode.ErrJobInput
	}
	return jobs.SubmitRequest{WorkspaceID: req.WorkspaceId, JobSpec: spec.JobSpec{Name: req.Name, Type: req.Type, Spec: raw, Traits: traits}}, nil
}

func (s *JobsServer) SubmitJob(ctx context.Context, req *eruunv1.SubmitJobRequest) (*eruunv1.JobAccepted, error) {
	input, err := jobSubmitInput(req)
	if err != nil {
		return nil, rpcError(err)
	}
	accepted, err := s.Jobs.Submit(ctx, input)
	if err != nil {
		return nil, rpcError(jobFailure(err))
	}
	return &eruunv1.JobAccepted{TaskId: accepted.TaskID, WorkspaceId: accepted.WorkspaceID, Type: accepted.Type, Status: string(accepted.Status)}, nil
}

func (s *JobsServer) GetJob(ctx context.Context, req *eruunv1.JobTaskRequest) (*eruunv1.JobDetail, error) {
	if req == nil || req.TaskId == "" {
		return nil, rpcError(bcode.ErrJobInput)
	}
	detail, err := s.Jobs.Get(ctx, req.TaskId, req.ExecutionKey)
	if err != nil {
		return nil, rpcError(jobFailure(err))
	}
	job, err := jobSpecOutput(detail.Job)
	if err != nil {
		return nil, rpcError(err)
	}
	results, err := jobArtifactsOutput(detail.Results)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &eruunv1.JobDetail{
		Accepted: &eruunv1.JobAccepted{TaskId: detail.TaskID, WorkspaceId: detail.WorkspaceID, Type: detail.Type, Status: string(detail.Status)},
		Job:      job, Results: results, Deliveries: jobDeliveriesOutput(detail.Deliveries), CollectionState: detail.CollectionState, ExecutionKey: detail.ExecutionKey, FrameworkVersion: detail.FrameworkVersion, SandboxesTruncated: detail.SandboxesTruncated,
	}
	for _, sandbox := range detail.Sandboxes {
		item := &eruunv1.JobSandboxStatus{TrialId: sandbox.TrialID, State: sandbox.State, Admitted: sandbox.Admitted, Namespace: sandbox.Namespace, SandboxName: sandbox.SandboxName, SandboxUid: sandbox.SandboxUID, PodName: sandbox.PodName, PodUid: sandbox.PodUID, ContainerName: sandbox.ContainerName, Reason: sandbox.Reason}
		if sandbox.RetainUntil != nil {
			item.RetainUntil = timeMessage(*sandbox.RetainUntil)
		}
		resp.Sandboxes = append(resp.Sandboxes, item)
	}
	for _, execution := range detail.Executions {
		if execution == nil {
			continue
		}
		out := &eruunv1.JobExecution{
			Id: int64(execution.ID), Type: execution.Type, WorkflowId: execution.WorkflowID,
			ProductId: execution.ProductID, WorkspaceId: execution.WorkspaceID, AppId: execution.AppID,
			TaskId: execution.TaskID, Status: execution.Status, StartTime: execution.StartTime, EndTime: execution.EndTime,
			Info: execution.Info, ServiceName: execution.ServiceName, Error: execution.Error,
			Production: execution.Production, TargetEnv: execution.TargetEnv, ExecutionKey: execution.ExecutionKey,
			RunGeneration: execution.RunGeneration, Attempt: uint32(execution.Attempt),
			SchedulingState: execution.SchedulingState, SchedulingClass: execution.SchedulingClass,
			SchedulingPriority: int32(execution.SchedulingPriority), SchedulingReason: execution.SchedulingReason,
			CreateTime: timeMessage(execution.CreateTime), UpdateTime: timeMessage(execution.UpdateTime),
		}
		if execution.SchedulingQueuedAt != nil {
			out.SchedulingQueuedAt = timeMessage(*execution.SchedulingQueuedAt)
		}
		resp.Executions = append(resp.Executions, out)
	}
	if detail.RunnerStatus != nil {
		runner := detail.RunnerStatus
		resp.RunnerStatus = &eruunv1.JobRunnerStatus{Phase: runner.Phase, Sequence: runner.Sequence, Stale: runner.Stale}
		if runner.LastHeartbeatAt != nil {
			resp.RunnerStatus.LastHeartbeatAt = timeMessage(*runner.LastHeartbeatAt)
		}
		if runner.Progress != nil {
			resp.RunnerStatus.Progress = &eruunv1.JobRunnerProgress{CompletedTrials: int32(runner.Progress.CompletedTrials), TotalTrials: int32(runner.Progress.TotalTrials)}
		}
		if runner.Terminal != nil {
			t := runner.Terminal
			resp.RunnerStatus.Terminal = &eruunv1.JobRunnerTerminal{
				Outcome: t.Outcome, Signal: t.Signal, ArtifactId: t.ArtifactID, ArtifactDigest: t.ArtifactDigest,
				CollectionComplete: t.CollectionComplete, Reason: t.Reason, Message: t.Message,
			}
			if t.ExitCode != nil {
				code := int32(*t.ExitCode)
				resp.RunnerStatus.Terminal.ExitCode = &code
			}
		}
	}
	return resp, nil
}

func (s *JobsServer) CancelJob(ctx context.Context, req *eruunv1.JobTaskRequest) (*emptypb.Empty, error) {
	if req == nil || req.TaskId == "" || req.ExecutionKey != "" {
		return nil, rpcError(bcode.ErrJobInput)
	}
	scope, err := jobs.Scope(ctx, true)
	if err == nil {
		var task *model.WorkflowQueue
		task, err = s.Jobs.Task(ctx, req.TaskId)
		if err == nil && (task.Type != config.WorkflowTaskTypeJob || task.AppID != "") {
			err = bcode.ErrNotFound
		}
	}
	if err == nil {
		err = s.Workflow.CancelWorkflowTask(ctx, scope.UserID, req.TaskId, "cancelled by user")
	}
	return &emptypb.Empty{}, rpcError(jobFailure(err))
}

func (s *JobsServer) GetJobResults(ctx context.Context, req *eruunv1.JobTaskRequest) (*eruunv1.JobResults, error) {
	if req == nil || req.TaskId == "" {
		return nil, rpcError(bcode.ErrJobInput)
	}
	detail, err := s.Jobs.Get(ctx, req.TaskId, req.ExecutionKey)
	if err != nil {
		return nil, rpcError(jobFailure(err))
	}
	items, err := jobArtifactsOutput(detail.Results)
	return &eruunv1.JobResults{CollectionState: detail.CollectionState, Artifacts: items, Deliveries: jobDeliveriesOutput(detail.Deliveries), ExecutionKey: detail.ExecutionKey}, rpcError(err)
}

func (s *JobsServer) resultTask(ctx context.Context, taskID, requestedKey string) (*model.WorkflowQueue, string, error) {
	return s.Jobs.ResolveResultTask(ctx, taskID, requestedKey)
}

func (s *JobsServer) RetryJobDelivery(ctx context.Context, req *eruunv1.JobDeliveryRequest) (*emptypb.Empty, error) {
	if req == nil || req.TaskId == "" || req.Target == "" {
		return nil, rpcError(bcode.ErrJobInput)
	}
	_, err := jobs.Scope(ctx, true)
	if err == nil {
		var task *model.WorkflowQueue
		var key string
		task, key, err = s.resultTask(ctx, req.TaskId, req.ExecutionKey)
		if err == nil {
			err = s.Jobs.Artifacts.Retry(ctx, task.WorkspaceID, task.TaskID, req.Target, key)
		}
	}
	return &emptypb.Empty{}, rpcError(jobFailure(err))
}

func (s *JobsServer) SetJobRetention(ctx context.Context, req *eruunv1.SetJobRetentionRequest) (*emptypb.Empty, error) {
	if req == nil || req.TaskId == "" || req.RetentionDays < 1 || req.RetentionDays > 3650 {
		return nil, rpcError(bcode.ErrJobInput)
	}
	_, err := jobs.Scope(ctx, true)
	if err == nil {
		var task *model.WorkflowQueue
		var key string
		task, key, err = s.resultTask(ctx, req.TaskId, req.ExecutionKey)
		if err == nil {
			err = s.Jobs.Artifacts.SetRetention(ctx, task.WorkspaceID, task.TaskID, int(req.RetentionDays), key)
		}
	}
	return &emptypb.Empty{}, rpcError(jobFailure(err))
}

func (s *JobsServer) GetJobStoragePolicy(ctx context.Context, _ *emptypb.Empty) (*eruunv1.JobStoragePolicy, error) {
	policy, err := s.Jobs.Policy(ctx)
	if err != nil {
		return nil, rpcError(jobFailure(err))
	}
	return &eruunv1.JobStoragePolicy{AvailableTargets: policy.AvailableTargets, Policy: jobPolicyOutput(policy.Policy)}, nil
}

func (s *JobsServer) SetJobStoragePolicy(ctx context.Context, req *eruunv1.JobResultPolicy) (*emptypb.Empty, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrJobInput)
	}
	return &emptypb.Empty{}, rpcError(jobFailure(s.Jobs.SetPolicy(ctx, jobPolicyInput(req))))
}

func (s *JobsServer) ListJobDatasets(ctx context.Context, req *eruunv1.ListJobDatasetsRequest) (*eruunv1.JobArtifacts, error) {
	scope, err := jobs.Scope(ctx, false)
	if err != nil {
		return nil, rpcError(err)
	}
	page, size := 1, 20
	if req != nil {
		if req.Page != nil {
			page = int(*req.Page)
		}
		if req.PageSize != nil {
			size = int(*req.PageSize)
		}
	}
	items, err := s.Jobs.Artifacts.ListPage(ctx, scope.WorkspaceID, artifacts.KindDataset, "", page, size)
	if err != nil {
		return nil, rpcError(jobFailure(err))
	}
	out, err := jobArtifactsOutput(items)
	return &eruunv1.JobArtifacts{Artifacts: out}, rpcError(err)
}

func (s *JobsServer) GetJobDataset(ctx context.Context, req *eruunv1.JobDatasetRequest) (*eruunv1.JobArtifact, error) {
	if req == nil || req.DatasetId == "" {
		return nil, rpcError(bcode.ErrJobInput)
	}
	scope, err := jobs.Scope(ctx, false)
	if err != nil {
		return nil, rpcError(err)
	}
	item, err := s.Jobs.Artifacts.Get(ctx, scope.WorkspaceID, req.DatasetId)
	if err != nil {
		return nil, rpcError(jobFailure(err))
	}
	if item.Kind != artifacts.KindDataset {
		return nil, rpcError(bcode.ErrNotFound)
	}
	out, err := jobArtifactOutput(item)
	return out, rpcError(err)
}

func jobFailure(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, artifacts.ErrInvalidInput), errors.Is(err, artifacts.ErrInvalidArchive):
		// Archive parsers may include uploaded file names or bytes in their
		// errors. The gRPC detail must not echo attacker-controlled content.
		return bcode.ErrJobInput
	case errors.Is(err, artifacts.ErrSourceExpired):
		return bcode.ErrJobResultExpired
	case errors.Is(err, artifacts.ErrConflict):
		return bcode.ErrJobResultConflict
	case errors.Is(err, artifacts.ErrDestinationUnavailable):
		return bcode.ErrServiceUnavailable
	case errors.Is(err, jobs.ErrRunnerConflict):
		return bcode.ErrJobRunnerConflict
	default:
		return err
	}
}

func (s *JobsServer) UploadJobDataset(stream eruunv1.JobsService_UploadJobDatasetServer) error {
	// The first frame names the dataset; subsequent frames are bounded 64 KiB
	// data. Staging on disk avoids holding the 64 MiB archive in Go heap.
	ctx, cancel := context.WithTimeout(stream.Context(), spec.JobArchiveTimeoutSeconds*time.Second)
	defer cancel()
	scope, err := jobs.Scope(ctx, true)
	if err != nil {
		return rpcError(err)
	}
	file, err := os.CreateTemp("", "eruun-grpc-upload-*")
	if err != nil {
		return rpcError(fmt.Errorf("stage dataset: %w", err))
	}
	defer os.Remove(file.Name())
	defer file.Close()
	first, err := recvUploadPart(ctx, stream)
	if err != nil {
		return rpcError(jobFailure(err))
	}
	if first.GetHeader() == nil || first.GetHeader().Name == "" {
		return rpcError(bcode.ErrJobInput)
	}
	var total int64
	for {
		part, err := recvUploadPart(ctx, stream)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return rpcError(jobFailure(err))
		}
		if part.GetHeader() != nil || part.GetChunk() == nil {
			return rpcError(bcode.ErrJobInput)
		}
		data := part.GetChunk()
		if len(data) > archiveChunkSize {
			return rpcError(bcode.ErrJobTooLarge)
		}
		total += int64(len(data))
		if total > archiveMaxSize {
			return rpcError(bcode.ErrJobTooLarge)
		}
		if _, err := file.Write(data); err != nil {
			return rpcError(fmt.Errorf("stage dataset chunk: %w", err))
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return rpcError(fmt.Errorf("rewind dataset: %w", err))
	}
	item, err := s.Jobs.Artifacts.UploadDataset(ctx, scope.WorkspaceID, first.GetHeader().Name, file)
	if err != nil {
		return rpcError(jobFailure(err))
	}
	out, err := jobArtifactOutput(item)
	if err != nil {
		return rpcError(err)
	}
	return stream.SendAndClose(out)
}

func recvUploadPart(ctx context.Context, stream eruunv1.JobsService_UploadJobDatasetServer) (*eruunv1.UploadDatasetPart, error) {
	type result struct {
		part *eruunv1.UploadDatasetPart
		err  error
	}
	ch := make(chan result, 1)
	go func() { part, err := stream.Recv(); ch <- result{part, err} }()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case value := <-ch:
		return value.part, value.err
	}
}

func streamArchive(ctx context.Context, stream interface {
	Send(*eruunv1.ArchiveChunk) error
}, copyArchive func(context.Context, io.Writer) error) error {
	ctx, cancel := context.WithTimeout(ctx, spec.JobArchiveTimeoutSeconds*time.Second)
	defer cancel()
	file, err := os.CreateTemp("", "eruun-grpc-download-*")
	if err != nil {
		return rpcError(fmt.Errorf("stage archive: %w", err))
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := copyArchive(ctx, file); err != nil {
		return rpcError(jobFailure(err))
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return rpcError(fmt.Errorf("rewind archive: %w", err))
	}
	buf := make([]byte, archiveChunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return rpcError(err)
		}
		n, readErr := file.Read(buf)
		if n > 0 {
			if err := sendArchivePart(ctx, stream, &eruunv1.ArchiveChunk{Data: append([]byte(nil), buf[:n]...)}); err != nil {
				return rpcError(err)
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return rpcError(fmt.Errorf("read archive: %w", readErr))
		}
	}
}

func sendArchivePart(ctx context.Context, stream interface {
	Send(*eruunv1.ArchiveChunk) error
}, part *eruunv1.ArchiveChunk) error {
	ch := make(chan error, 1)
	go func() { ch <- stream.Send(part) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

func (s *JobsServer) DownloadJobDataset(req *eruunv1.JobDatasetRequest, stream eruunv1.JobsService_DownloadJobDatasetServer) error {
	if req == nil || req.DatasetId == "" {
		return rpcError(bcode.ErrJobInput)
	}
	scope, err := jobs.Scope(stream.Context(), false)
	if err != nil {
		return rpcError(err)
	}
	return streamArchive(stream.Context(), stream, func(ctx context.Context, w io.Writer) error {
		item, err := s.Jobs.Artifacts.Get(ctx, scope.WorkspaceID, req.DatasetId)
		if err != nil {
			return err
		}
		if item.Kind != artifacts.KindDataset {
			return bcode.ErrNotFound
		}
		return s.Jobs.Artifacts.Download(ctx, scope.WorkspaceID, item.ID, w)
	})
}

func (s *JobsServer) DownloadJobResult(req *eruunv1.JobArtifactRequest, stream eruunv1.JobsService_DownloadJobResultServer) error {
	if req == nil || req.TaskId == "" || req.ArtifactId == "" {
		return rpcError(bcode.ErrJobInput)
	}
	return streamArchive(stream.Context(), stream, func(ctx context.Context, w io.Writer) error {
		task, key, err := s.resultTask(ctx, req.TaskId, req.ExecutionKey)
		if err != nil {
			return err
		}
		item, err := s.Jobs.Artifacts.Get(ctx, task.WorkspaceID, req.ArtifactId)
		if err != nil {
			return err
		}
		if item.TaskID != task.TaskID || item.ExecutionKey != key || item.Kind == artifacts.KindDataset {
			return bcode.ErrNotFound
		}
		return s.Jobs.Artifacts.Download(ctx, task.WorkspaceID, item.ID, w)
	})
}

func (s *JobsServer) DownloadJobDelivery(req *eruunv1.JobDeliveryRequest, stream eruunv1.JobsService_DownloadJobDeliveryServer) error {
	if req == nil || req.TaskId == "" || req.Target == "" {
		return rpcError(bcode.ErrJobInput)
	}
	return streamArchive(stream.Context(), stream, func(ctx context.Context, w io.Writer) error {
		task, key, err := s.resultTask(ctx, req.TaskId, req.ExecutionKey)
		if err != nil {
			return err
		}
		return s.Jobs.Artifacts.DownloadDelivery(ctx, task.WorkspaceID, task.TaskID, req.Target, w, key)
	})
}

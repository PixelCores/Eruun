package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	wfcloudcontract "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type CloudJobCtl struct {
	job      *model.JobTask
	store    datastore.DataStore
	waitFunc func(ctx context.Context, d time.Duration) error
}

type CloudJobRecord struct {
	Provider     string                 `json:"provider,omitempty"`
	Action       string                 `json:"action,omitempty"`
	Params       map[string]interface{} `json:"params,omitempty"`
	ExecutionKey string                 `json:"executionKey,omitempty"`
	Status       config.Status          `json:"status,omitempty"`
	Request      *CloudJobRequest       `json:"request,omitempty"`
	State        map[string]interface{} `json:"state,omitempty"`
	Result       *CloudJobResult        `json:"result,omitempty"`
	Error        string                 `json:"error,omitempty"`
}

func NewCloudJobCtl(job *model.JobTask, store datastore.DataStore) *CloudJobCtl {
	if job == nil {
		klog.Errorf("CloudJobCtl: job is nil")
		return nil
	}
	if store == nil {
		klog.Errorf("CloudJobCtl: store is nil")
		return nil
	}
	return &CloudJobCtl{
		job:      job,
		store:    store,
		waitFunc: waitWithContext,
	}
}

func (c *CloudJobCtl) Clean(_ context.Context) {}

func (c *CloudJobCtl) SaveInfo(ctx context.Context) error {
	ensurePublicCloudJobInfo(c.job)
	return saveOrUpdateJobInfo(ctx, c.store, c.job)
}

func (c *CloudJobCtl) Run(ctx context.Context) error {
	runCtx, cancel := cloudRunContext(ctx, c.job)
	defer cancel()
	runCtx = wfcloudcontract.WithDataStore(runCtx, c.store)

	info, err := cloudInfoFromJobInfo(c.job)
	if err != nil {
		return fmt.Errorf("cloud job info is invalid: %w", err)
	}
	ensurePublicCloudJobInfo(c.job)
	checkpoint, err := c.loadCheckpoint(runCtx)
	if err != nil {
		return fmt.Errorf("load cloud job checkpoint: %w", err)
	}
	request := buildCloudJobRequest(c.job, info, checkpoint)

	providerName := normalizeProviderName(request.Provider)
	if providerName == "" {
		err := fmt.Errorf("cloud job provider is required")
		setCloudJobCheckpoint(c.job, info, nil, nil, nil, config.StatusFailed, err)
		c.job.Status = config.StatusFailed
		return err
	}
	action := strings.TrimSpace(request.Action)
	if action == "" {
		err := fmt.Errorf("cloud job action is required")
		setCloudJobCheckpoint(c.job, info, nil, nil, nil, config.StatusFailed, err)
		c.job.Status = config.StatusFailed
		return err
	}

	provider, exists := getCloudProvider(providerName)
	if !exists {
		err := fmt.Errorf("%w: %s", errCloudProviderNotFound, providerName)
		setCloudJobCheckpoint(c.job, info, request, nil, nil, config.StatusFailed, err)
		c.job.Status = config.StatusFailed
		return err
	}
	cloudAction, found := provider.ResolveAction(action)
	if !found || cloudAction == nil {
		err := fmt.Errorf("%w: %s/%s", errCloudActionNotFound, providerName, action)
		setCloudJobCheckpoint(c.job, info, request, nil, nil, config.StatusFailed, err)
		c.job.Status = config.StatusFailed
		return err
	}
	if err := cloudAction.Validate(request); err != nil {
		setCloudJobCheckpoint(c.job, info, request, nil, nil, config.StatusFailed, err)
		c.job.Status = config.StatusFailed
		return err
	}
	state := loadCheckpointState(checkpoint, c.job.Info)

	runtime, err := provider.NewRuntime(runCtx, request)
	if err != nil {
		setCloudJobCheckpoint(c.job, info, request, nil, state, config.StatusFailed, err)
		c.job.Status = config.StatusFailed
		return err
	}
	runCtx = attachCloudJobRuntimeProviderSnapshot(runCtx, request)

	if err := c.persistCheckpoint(runCtx, info, request, nil, state, config.StatusRunning, nil); err != nil {
		return fmt.Errorf("persist cloud job checkpoint: %w", err)
	}
	for {
		progress, runErr := cloudAction.Run(runCtx, runtime, request, cloneCloudParams(state))
		if runErr != nil {
			status := statusForCloudActionErr(runErr)
			setCloudJobCheckpoint(c.job, info, request, nil, state, status, runErr)
			c.job.Status = status
			return NewStatusError(status, runErr)
		}
		if progress == nil {
			err := fmt.Errorf("cloud action returned nil progress")
			setCloudJobCheckpoint(c.job, info, request, nil, state, config.StatusFailed, err)
			c.job.Status = config.StatusFailed
			return err
		}
		if progress.State != nil {
			state = cloneCloudParams(progress.State)
		}

		if progress.Done {
			setCloudJobCheckpoint(c.job, info, request, progress.Result, state, config.StatusCompleted, nil)
			c.job.Status = config.StatusCompleted
			return nil
		}
		if progress.RequeueAfter <= 0 {
			err := fmt.Errorf("cloud action did not complete and did not provide requeue delay")
			setCloudJobCheckpoint(c.job, info, request, progress.Result, state, config.StatusFailed, err)
			c.job.Status = config.StatusFailed
			return err
		}

		if err := c.persistCheckpoint(runCtx, info, request, progress.Result, state, config.StatusRunning, nil); err != nil {
			return fmt.Errorf("persist cloud job checkpoint: %w", err)
		}
		if waitErr := c.waitFunc(runCtx, progress.RequeueAfter); waitErr != nil {
			status := statusForCloudActionErr(waitErr)
			setCloudJobCheckpoint(c.job, info, request, progress.Result, state, status, waitErr)
			c.job.Status = status
			return NewStatusError(status, waitErr)
		}
	}
}

func cloudRunContext(ctx context.Context, job *model.JobTask) (context.Context, context.CancelFunc) {
	timeout := int64(0)
	if job != nil {
		timeout = job.Timeout
	}
	if timeout <= 0 {
		timeout = int64(config.DefaultJobTaskTimeout.Seconds())
	}
	return context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
}

func recordCloudJobResult(
	info *CloudJobInfo,
	request *CloudJobRequest,
	result *CloudJobResult,
	state map[string]interface{},
	status config.Status,
	err error,
) string {
	if status == "" {
		status = config.StatusCompleted
	}
	record := CloudJobRecord{
		Status: status,
	}
	if info != nil {
		record.Provider = normalizeProviderName(info.Provider)
		record.Action = strings.TrimSpace(info.Action)
		record.Params = cloneCloudParams(info.Params)
		record.ExecutionKey = cloudJobExecutionKey(info)
	}
	if request != nil {
		record.Request = cloneCloudJobRequest(request)
	}
	if state != nil {
		record.State = cloneCloudParams(state)
	}
	if result != nil {
		record.Result = &CloudJobResult{
			RequestID: strings.TrimSpace(result.RequestID),
			Message:   strings.TrimSpace(result.Message),
			Output:    cloneCloudParams(result.Output),
		}
	}
	if err != nil {
		if status == config.StatusCompleted {
			record.Status = statusForCloudActionErr(err)
		}
		record.Error = strings.TrimSpace(err.Error())
	}
	data, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		return ""
	}
	return string(data)
}

func setCloudJobCheckpoint(
	job *model.JobTask,
	info *CloudJobInfo,
	request *CloudJobRequest,
	result *CloudJobResult,
	state map[string]interface{},
	status config.Status,
	err error,
) {
	if job == nil {
		return
	}
	ensurePublicCloudJobInfo(job)
	job.InternalInfo = recordCloudJobResult(info, request, result, state, status, err)
}

func cloneCloudJobRequest(src *CloudJobRequest) *CloudJobRequest {
	if src == nil {
		return nil
	}
	dst := *src
	dst.Provider = normalizeProviderName(src.Provider)
	dst.Action = strings.TrimSpace(src.Action)
	dst.Params = cloneCloudParams(src.Params)
	dst.RuntimeProviderSnapshot = nil
	dst.ResumeFromPersistedState = false
	return &dst
}

func cloneCloudParams(src map[string]interface{}) map[string]interface{} {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func buildCloudJobRequest(job *model.JobTask, info *CloudJobInfo, checkpoint *CloudJobRecord) *CloudJobRequest {
	request := &CloudJobRequest{
		Name:         job.Name,
		Namespace:    job.Namespace,
		WorkflowID:   job.WorkflowID,
		ProjectID:    job.ProjectID,
		AppID:        job.AppID,
		TaskID:       job.TaskID,
		ExecutionKey: job.ExecutionKey,
	}
	if info != nil {
		request.Provider = normalizeProviderName(info.Provider)
		request.Action = strings.TrimSpace(info.Action)
		request.Params = cloneCloudParams(info.Params)
	}
	if checkpoint != nil && checkpoint.Request != nil {
		snapshot := cloneCloudJobRequest(checkpoint.Request)
		if snapshot != nil {
			if snapshot.Provider != "" {
				request.Provider = snapshot.Provider
			}
			if snapshot.Action != "" {
				request.Action = snapshot.Action
			}
			if snapshot.Params != nil {
				request.Params = snapshot.Params
			}
		}
	}
	request.ResumeFromPersistedState = cloudCheckpointRequiresRuntimeProviderSnapshot(checkpoint)
	return request
}

func cloudJobExecutionKey(info *CloudJobInfo) string {
	if info == nil {
		return ""
	}
	return strings.TrimSpace(info.ExecutionKey)
}

func cloudJobExecutionKeyFromJob(job *model.JobTask) string {
	if job == nil {
		return ""
	}
	info, ok := optionalJobInfo[*CloudJobInfo](job)
	if !ok {
		return ""
	}
	return cloudJobExecutionKey(info)
}

func cloudJobExecutionKeyFromRecord(record *CloudJobRecord) string {
	if record == nil {
		return ""
	}
	return strings.TrimSpace(record.ExecutionKey)
}

func parseCloudJobRecord(raw string) *CloudJobRecord {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var record CloudJobRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil
	}
	return &record
}

func parseCloudCheckpointFromJobInfo(jobInfo *model.JobInfo) *CloudJobRecord {
	if jobInfo == nil {
		return nil
	}
	if record := parseCloudJobRecord(jobInfo.InternalInfo); record != nil {
		return record
	}
	return parseCloudJobRecord(jobInfo.Info)
}

func loadCheckpointState(checkpoint *CloudJobRecord, raw string) map[string]interface{} {
	if checkpoint != nil && checkpoint.State != nil {
		return cloneCloudParams(checkpoint.State)
	}
	return loadCloudActionState(raw)
}

func loadCloudActionState(raw string) map[string]interface{} {
	record := parseCloudJobRecord(raw)
	if record == nil {
		return nil
	}
	return cloneCloudParams(record.State)
}

func (c *CloudJobCtl) loadCheckpoint(ctx context.Context) (*CloudJobRecord, error) {
	if c == nil || c.job == nil {
		return nil, nil
	}
	jobInfos, err := loadJobInfos(ctx, c.store, c.job.TaskID, c.job.JobType, resolveJobServiceName(c.job))
	if err != nil {
		return nil, err
	}
	if jobInfo := selectCloudCheckpointJobInfo(c.job, jobInfos); jobInfo != nil {
		if record := parseCloudCheckpointFromJobInfo(jobInfo); record != nil {
			return record, nil
		}
	}
	if record := parseCloudJobRecord(c.job.InternalInfo); record != nil {
		return record, nil
	}
	return parseCloudJobRecord(c.job.Info), nil
}

func (c *CloudJobCtl) persistCheckpoint(
	ctx context.Context,
	info *CloudJobInfo,
	request *CloudJobRequest,
	result *CloudJobResult,
	state map[string]interface{},
	status config.Status,
	err error,
) error {
	if c == nil || c.job == nil {
		return nil
	}
	c.job.Status = status
	setCloudJobCheckpoint(c.job, info, request, result, state, status, err)
	return c.SaveInfo(ctx)
}

func attachCloudJobRuntimeProviderSnapshot(ctx context.Context, request *CloudJobRequest) context.Context {
	if request == nil || request.RuntimeProviderSnapshot == nil {
		return ctx
	}
	providerName := normalizeProviderName(request.Provider)
	if providerName == "" {
		request.RuntimeProviderSnapshot = nil
		return ctx
	}
	ctx = wfcloudcontract.WithRuntimeProviderSnapshot(ctx, providerName, request.RuntimeProviderSnapshot)
	request.RuntimeProviderSnapshot = nil
	return ctx
}

func ensurePublicCloudJobInfo(job *model.JobTask) {
	if job == nil || strings.TrimSpace(job.Info) != "" {
		return
	}
	kind := strings.TrimSpace(string(domainspec.ResourceCloudJob))
	if kind == "" {
		kind = "cloudjob"
	}
	name := strings.TrimSpace(job.Name)
	if name == "" {
		job.Info = kind
		return
	}
	namespace := strings.TrimSpace(job.Namespace)
	if namespace == "" {
		job.Info = fmt.Sprintf("%s: %s", kind, name)
		return
	}
	job.Info = fmt.Sprintf("%s: %s/%s", kind, namespace, name)
}

func PublicCloudJobInfoMessage(raw string) string {
	raw = strings.TrimSpace(raw)
	record := parseCloudJobRecord(raw)
	if record == nil {
		return raw
	}
	return summarizeCloudJobRecord(record)
}

func summarizeCloudJobRecord(record *CloudJobRecord) string {
	if record == nil {
		return ""
	}
	provider := normalizeProviderName(record.Provider)
	action := strings.TrimSpace(record.Action)
	if record.Request != nil {
		if provider == "" {
			provider = normalizeProviderName(record.Request.Provider)
		}
		if action == "" {
			action = strings.TrimSpace(record.Request.Action)
		}
	}
	parts := []string{"cloudjob checkpoint (redacted)"}
	if provider != "" {
		parts = append(parts, fmt.Sprintf("provider=%s", provider))
	}
	if action != "" {
		parts = append(parts, fmt.Sprintf("action=%s", action))
	}
	if status := strings.TrimSpace(string(record.Status)); status != "" {
		parts = append(parts, fmt.Sprintf("status=%s", status))
	}
	return strings.Join(parts, "; ")
}

func selectCloudCheckpointJobInfo(job *model.JobTask, candidates []*model.JobInfo) *model.JobInfo {
	if len(candidates) == 0 {
		return nil
	}
	executionKey := cloudJobExecutionKeyFromJob(job)
	if executionKey == "" {
		return candidates[0]
	}
	for _, candidate := range candidates {
		record := parseCloudCheckpointFromJobInfo(candidate)
		if record == nil {
			continue
		}
		if candidateKey := cloudJobExecutionKeyFromRecord(record); candidateKey == executionKey {
			return candidate
		}
	}
	return nil
}

func cloudCheckpointRequiresRuntimeProviderSnapshot(record *CloudJobRecord) bool {
	if record == nil {
		return false
	}
	return record.State != nil || record.Result != nil
}

func statusForCloudActionErr(err error) config.Status {
	switch {
	case errors.Is(err, context.Canceled):
		return config.StatusCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return config.StatusTimeout
	default:
		return config.StatusFailed
	}
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

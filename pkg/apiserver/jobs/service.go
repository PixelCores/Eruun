// Package jobs exposes standalone workspace Jobs through the existing workflow queue.
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type Service struct {
	Store           artifacts.Backend
	Artifacts       *artifacts.Store
	Kube            kubernetes.Interface
	Config          *config.Config
	SandboxClient   dynamic.Interface
	SandboxObserver informer.SandboxObserver
}

func New(store datastore.DataStore, kube kubernetes.Interface, cfg *config.Config) (*Service, error) {
	backend, err := artifacts.RequireBackend(store)
	if err != nil {
		return nil, err
	}
	var minio *spec.MinIOConfig
	if cfg != nil && cfg.Jobs != nil {
		minio = cfg.Jobs.MinIO
	}
	data, err := artifacts.New(store, minio)
	if err != nil {
		return nil, err
	}
	return &Service{Store: backend, Artifacts: data, Kube: kube, Config: cfg}, nil
}

type SubmitRequest struct {
	WorkspaceID string `json:"workspaceId,omitempty"`
	spec.JobSpec
}

type Accepted struct {
	TaskID      string        `json:"taskId"`
	WorkspaceID string        `json:"workspaceId"`
	Type        string        `json:"type"`
	Status      config.Status `json:"status"`
}

type Detail struct {
	Accepted
	ExecutionKey       string               `json:"executionKey,omitempty"`
	FrameworkVersion   string               `json:"frameworkVersion,omitempty"`
	Job                spec.JobSpec         `json:"job"`
	Executions         []*model.JobInfo     `json:"executions"`
	Results            []*model.JobArtifact `json:"results"`
	Deliveries         []*model.JobDelivery `json:"deliveries"`
	CollectionState    string               `json:"collectionState,omitempty"`
	Sandboxes          []*SandboxResponse   `json:"sandboxes,omitempty"`
	SandboxesTruncated bool                 `json:"sandboxesTruncated"`
	RunnerStatus       *RunnerStatus        `json:"runnerStatus,omitempty"`
}

type StoragePolicy struct {
	AvailableTargets []string             `json:"availableTargets"`
	Policy           spec.JobResultPolicy `json:"policy"`
}

func Scope(ctx context.Context, write bool) (account.Scope, error) {
	scope, ok := account.FromContext(ctx)
	if !ok || scope.WorkspaceID == "" || scope.Namespace == "" || (write && scope.Role == "viewer") {
		return account.Scope{}, bcode.ErrForbidden
	}
	return scope, nil
}

func invalid(err error) error { return bcode.WithSafeClientMessage(bcode.ErrJobInput, err.Error()) }

func (s *Service) Policy(ctx context.Context) (*StoragePolicy, error) {
	scope, err := Scope(ctx, false)
	if err != nil {
		return nil, err
	}
	space := &model.Workspace{ID: scope.WorkspaceID}
	if err := s.Store.Get(ctx, space); err != nil {
		return nil, err
	}
	if space.Namespace != scope.Namespace {
		return nil, bcode.ErrForbidden
	}
	p := spec.DefaultJobResultPolicy()
	if len(space.JobResultPolicy) > 0 {
		if err := spec.DecodeJobJSON(space.JobResultPolicy, &p); err != nil {
			return nil, fmt.Errorf("read workspace Job policy: %w", err)
		}
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("stored workspace Job policy: %w", err)
	}
	targets := []string{"database"}
	if s.Config != nil && s.Config.Jobs != nil && s.Config.Jobs.MinIO != nil {
		targets = append(targets, "minio")
	}
	return &StoragePolicy{AvailableTargets: targets, Policy: p}, nil
}

func (s *Service) validatePolicy(p spec.JobResultPolicy) error {
	if err := p.Validate(); err != nil {
		return invalid(err)
	}
	for _, target := range p.Targets {
		if target.Type == "minio" && (s.Config == nil || s.Config.Jobs == nil || s.Config.Jobs.MinIO == nil) {
			return bcode.WithSafeClientMessage(bcode.ErrJobInput, "MinIO has not been configured by the administrator")
		}
	}
	return nil
}

func (s *Service) SetPolicy(ctx context.Context, p spec.JobResultPolicy) error {
	scope, err := Scope(ctx, true)
	if err != nil {
		return err
	}
	if err = s.validatePolicy(p); err != nil {
		return err
	}
	data, _ := json.Marshal(p)
	return artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		space := &model.Workspace{ID: scope.WorkspaceID}

		if err := tx.GetForUpdate(ctx, space); err != nil {
			return err
		}
		if space.Namespace != scope.Namespace {
			return bcode.ErrForbidden
		}
		space.JobResultPolicy = data
		return tx.Put(ctx, space)
	})
}

func (s *Service) Submit(ctx context.Context, request SubmitRequest) (*Accepted, error) {
	scope, err := Scope(ctx, true)
	if err != nil {
		return nil, err
	}
	if request.WorkspaceID != "" && request.WorkspaceID != scope.WorkspaceID {
		return nil, bcode.ErrForbidden
	}
	if err = request.JobSpec.Normalize(); err != nil {
		return nil, invalid(err)
	}
	if s.Config == nil || s.Config.Accounts == nil {
		return nil, bcode.ErrServiceUnavailable
	}
	if request.Traits.Evaluation != nil {
		if s.Config.Jobs == nil {
			return nil, bcode.WithSafeClientMessage(bcode.ErrServiceUnavailable, "Harbor Runner is not configured")
		}
		if request.Traits.Evaluation.ResultPolicy == nil {
			p, err := s.Policy(ctx)
			if err != nil {
				return nil, err
			}
			request.Traits.Evaluation.ResultPolicy = &p.Policy
		}
		if err = s.validatePolicy(*request.Traits.Evaluation.ResultPolicy); err != nil {
			return nil, err
		}
		// Normalize already restricted evaluation envs to Secret references;
		// confirm each one resolves in this workspace before accepting the Job.
		for _, env := range request.Traits.Envs {
			if env.ValueFrom.Secret == nil {
				continue
			}
			if s.Kube == nil {
				return nil, bcode.ErrServiceUnavailable
			}
			secret, err := s.Kube.CoreV1().Secrets(scope.Namespace).Get(ctx, env.ValueFrom.Secret.Name, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("resolve evaluation credential: %w", err)
			}
			if _, ok := secret.Data[env.ValueFrom.Secret.Key]; !ok {
				return nil, invalid(fmt.Errorf("credential Secret key is missing"))
			}
		}
	}
	random := make([]byte, 12)
	if _, err = rand.Read(random); err != nil {
		return nil, err
	}
	taskID := hex.EncodeToString(random)
	declaration, err := json.Marshal(request.JobSpec)
	if err != nil {
		return nil, err
	}
	task := &model.WorkflowQueue{TaskID: taskID, WorkspaceID: scope.WorkspaceID, WorkflowName: request.Name, WorkflowDisplayName: request.Name, TaskCreator: scope.UserID, Type: config.WorkflowTaskTypeJob, Status: config.StatusWaiting, JobSpec: string(declaration)}
	// Validate the exact renderer and workspace policy before enqueueing. Building
	// does not create namespace, application, component, or Kubernetes resources.
	job, err := BuildTask(ctx, s.Store, s.Config, task, scope.Namespace)
	if err != nil {
		return nil, invalid(err)
	}
	space := &model.Workspace{ID: scope.WorkspaceID, Namespace: scope.Namespace}
	if request.Traits.Evaluation != nil {
		if err := BuildEvaluationTask(ctx, s.Store, s.Config, job, spec.JobTraits{}); err != nil {
			if errors.Is(err, errEvaluationPolicyUnavailable) {
				return nil, bcode.ErrServiceUnavailable
			}
			return nil, invalid(err)
		}
		err = PrepareEvaluationTask(job, space, s.Config.Accounts.Workspace, s.Config.Jobs.RunnerImage)
	} else {
		_, err = workflowjob.PrepareTask(job, "", space, s.Config.Accounts.Workspace)
	}
	if err != nil {
		return nil, err
	}
	err = artifacts.WithTransaction(ctx, s.Store, func(tx artifacts.Backend) error {
		locked := &model.Workspace{ID: scope.WorkspaceID}

		if err := tx.GetForUpdate(ctx, locked); err != nil {
			return err
		}
		if locked.Namespace != scope.Namespace {
			return bcode.ErrForbidden
		}
		return tx.Add(ctx, task)
	})
	if err != nil {
		return nil, fmt.Errorf("submit Job: %w", err)
	}
	return &Accepted{TaskID: taskID, WorkspaceID: scope.WorkspaceID, Type: request.Type, Status: task.Status}, nil
}

// scopedTask authorizes only the parent workspace; public App reads additionally
// select a concrete evaluation execution below.
func (s *Service) scopedTask(ctx context.Context, taskID string) (*model.WorkflowQueue, error) {
	scope, err := Scope(ctx, false)
	if err != nil {
		return nil, err
	}
	task := &model.WorkflowQueue{TaskID: taskID}
	if err := s.Store.Get(ctx, task); err != nil {
		return nil, err
	}
	if task.WorkspaceID != scope.WorkspaceID {
		return nil, bcode.ErrNotFound
	}
	return task, nil
}

func (s *Service) evaluationExecution(ctx context.Context, task *model.WorkflowQueue, executionKey string) (*model.JobInfo, error) {
	options := &datastore.ListOptions{FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{{Key: "type", Values: []string{string(config.JobEval)}}}}}
	if executionKey != "" {
		options.In = append(options.In, datastore.InQueryOption{Key: "execution_key", Values: []string{executionKey}})
	} else {
		options.Page, options.PageSize = 1, 2
		// Historical NULL identities are ignored, while a non-NULL empty identity
		// retains its existing result-storage and ambiguity semantics.
		options.NotEqual = []datastore.ComparisonQueryOption{{Key: "execution_key", Value: nil}}
	}
	rows, err := s.Store.List(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, AppID: task.AppID}, options)
	if err != nil {
		return nil, err
	}
	var selected *model.JobInfo
	for _, row := range rows {
		record := row.(*model.JobInfo)
		if record.Type != string(config.JobEval) || record.ExecutionKey == nil || (executionKey != "" && *record.ExecutionKey != executionKey) {
			continue
		}
		if selected != nil {
			return nil, invalid(fmt.Errorf("executionKey is required to select one evaluation Job"))
		}
		selected = record
	}
	if selected == nil && executionKey != "" {
		return nil, bcode.ErrNotFound
	}
	return selected, nil
}

func (s *Service) Task(ctx context.Context, taskID string, executionKey ...string) (*model.WorkflowQueue, error) {
	task, err := s.scopedTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	key := ""
	if len(executionKey) > 0 {
		key = executionKey[0]
	}
	if key == "" {
		if task.Type != config.WorkflowTaskTypeJob || task.AppID != "" {
			return nil, bcode.ErrNotFound
		}
		return task, nil
	}
	if _, err := s.evaluationExecution(ctx, task, key); err != nil {
		return nil, err
	}
	return task, nil
}

// resolveExecution authorizes the parent before selecting a bounded execution set.
func (s *Service) resolveExecution(ctx context.Context, taskID, executionKey string) (*model.WorkflowQueue, *model.JobInfo, error) {
	task, err := s.scopedTask(ctx, taskID)
	if err != nil {
		return nil, nil, err
	}
	if executionKey == "" && (task.Type != config.WorkflowTaskTypeJob || task.AppID != "") {
		return nil, nil, bcode.ErrNotFound
	}
	record, err := s.evaluationExecution(ctx, task, executionKey)
	if err != nil {
		return nil, nil, err
	}
	return task, record, nil
}

// ResolveResultTask returns the authorized parent and its selected evaluation
// identity together, so result operations do not repeat execution selection.
func (s *Service) ResolveResultTask(ctx context.Context, taskID, executionKey string) (*model.WorkflowQueue, string, error) {
	task, record, err := s.resolveExecution(ctx, taskID, executionKey)
	if err != nil {
		return nil, "", err
	}
	if record == nil {
		return task, "", nil
	}
	return task, *record.ExecutionKey, nil
}

func (s *Service) Get(ctx context.Context, taskID string, executionKey ...string) (*Detail, error) {
	key := ""
	if len(executionKey) > 0 {
		key = executionKey[0]
	}
	task, record, err := s.resolveExecution(ctx, taskID, key)
	if err != nil {
		return nil, err
	}
	var declaration spec.JobSpec
	status := task.Status
	if task.AppID != "" {
		if record == nil {
			return nil, bcode.ErrNotFound
		}
		info, err := decodeEvaluationInfo(record.EvaluationInfo)
		if err != nil {
			return nil, err
		}
		declaration = spec.JobSpec{Name: record.ServiceName, Type: "job", Traits: info.Traits}
		status = config.Status(record.Status)
	} else if err := spec.DecodeJobJSON([]byte(task.JobSpec), &declaration); err != nil {
		return nil, err
	}
	out := &Detail{Accepted: Accepted{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Type: declaration.Type, Status: status}, Job: declaration, Executions: []*model.JobInfo{}}
	if record != nil {
		out.ExecutionKey = *record.ExecutionKey
		out.Executions = append(out.Executions, record)
		if record.EvaluationInfo != "" {
			info, err := decodeEvaluationInfo(record.EvaluationInfo)
			if err != nil {
				return nil, err
			}
			out.Job.Traits = info.Traits
			out.FrameworkVersion = info.FrameworkVersion
		}
	} else if declaration.Type == string(config.JobCommand) {
		rows, err := s.Store.List(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, AppID: task.AppID}, &datastore.ListOptions{})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			out.Executions = append(out.Executions, row.(*model.JobInfo))
		}
	}
	out.Results, err = s.Artifacts.List(ctx, task.WorkspaceID, "", task.TaskID, out.ExecutionKey)
	if err != nil {
		return nil, err
	}
	out.Deliveries, err = s.Artifacts.Deliveries(ctx, task.WorkspaceID, task.TaskID, out.ExecutionKey)
	if err != nil {
		return nil, err
	}
	if declaration.Traits.Evaluation != nil {
		if out.ExecutionKey != "" {
			out.Sandboxes, out.SandboxesTruncated, err = s.sandboxStatuses(ctx, task.WorkspaceID, task.TaskID, out.ExecutionKey)
			if err != nil {
				return nil, err
			}
		}
		out.RunnerStatus, err = latestRunnerStatus(ctx, s.Store, out.Executions)
		if err != nil {
			return nil, err
		}
		out.CollectionState = "pending"
		if terminal(status) {
			out.CollectionState = "unavailable"
		}
		for _, result := range out.Results {
			if result.Kind != artifacts.KindSource {
				continue
			}
			out.CollectionState = "collected"
			var summary struct {
				Complete bool `json:"collectionComplete"`
			}
			if json.Unmarshal(result.Summary, &summary) != nil || !summary.Complete {
				out.CollectionState = "incomplete"
			}
			if result.Expired {
				out.CollectionState = "expired"
			}
			break
		}
	}
	return out, nil
}

func terminal(status config.Status) bool {
	switch status {
	case config.StatusCompleted, config.StatusPassed, config.StatusSkipped, config.StatusFailed,
		config.StatusTimeout, config.StatusCancelled, config.StatusReject, config.StatusNotRun:
		return true
	}
	return false
}

type RunnerIdentity struct{ TaskID, Token, PodName, PodUID string }
type runnerAuthorization struct {
	task       *model.WorkflowQueue
	job        *model.JobInfo
	liveJob    *batchv1.Job
	namespace  string
	jobUID     string
	identity   RunnerIdentity
	evaluation *evaluationInfo
}

func (s *Service) authorizeRunner(ctx context.Context, identity RunnerIdentity) (*runnerAuthorization, error) {
	if len(identity.Token) != 64 || identity.TaskID == "" || identity.PodName == "" || identity.PodUID == "" || s.Kube == nil {
		return nil, bcode.ErrUnauthorized
	}
	task := &model.WorkflowQueue{TaskID: identity.TaskID}
	if err := s.Store.Get(ctx, task); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrUnauthorized
		}
		return nil, err
	}
	if task.WorkspaceID == "" {
		return nil, bcode.ErrUnauthorized
	}
	space := &model.Workspace{ID: task.WorkspaceID}
	if err := s.Store.Get(ctx, space); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrUnauthorized
		}
		return nil, err
	}
	rows, err := s.Store.List(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, AppID: task.AppID}, &datastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	scoped := account.WithScope(ctx, account.Scope{WorkspaceID: space.ID, Namespace: space.Namespace, Role: "member"})
	pod, err := s.Kube.CoreV1().Pods(space.Namespace).Get(scoped, identity.PodName, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, bcode.ErrUnauthorized
		}
		return nil, err
	}
	if string(pod.UID) != identity.PodUID || pod.Spec.ServiceAccountName != workspace.EvaluationRunnerName || len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Name != "runner" || pod.Annotations[config.AnnotationJobTaskID] != task.TaskID {
		return nil, bcode.ErrUnauthorized
	}
	for _, row := range rows {
		job := row.(*model.JobInfo)
		if !runnerJobStatusAuthorized(job, task.Status) || job.Type != string(config.JobEval) || job.ExecutionKey == nil || job.InternalInfo == "" {
			continue
		}
		evaluation, err := decodeEvaluationInfo(job.EvaluationInfo)
		if err != nil || subtleTokenMismatch(evaluation.RunnerToken, identity.Token) {
			continue
		}
		if pod.Annotations[config.AnnotationJobExecutionKey] != *job.ExecutionKey || pod.Annotations[config.AnnotationJobRunGeneration] != strconv.FormatUint(job.RunGeneration, 10) {
			continue
		}
		for _, owner := range pod.OwnerReferences {
			if owner.Kind == "Job" && owner.Name != "" && owner.Controller != nil && *owner.Controller {
				live, err := s.Kube.BatchV1().Jobs(space.Namespace).Get(scoped, owner.Name, metav1.GetOptions{})
				if err != nil {
					if k8serrors.IsNotFound(err) {
						return nil, bcode.ErrUnauthorized
					}
					return nil, err
				}
				if live.UID != owner.UID {
					return nil, bcode.ErrUnauthorized
				}
				if workflowjob.ValidateInstantJobRetryExecution(job, live) != nil {
					return nil, bcode.ErrUnauthorized
				}
				auth := &runnerAuthorization{task: task, job: job, liveJob: live, namespace: space.Namespace, jobUID: string(live.UID), identity: identity, evaluation: evaluation}
				if err := runnerParentAuthorized(ctx, s.Store, task.Status, job, auth); err != nil {
					return nil, err
				}
				return auth, nil
			}
		}
	}
	return nil, bcode.ErrUnauthorized
}

func (s *Service) RunnerDataset(ctx context.Context, identity RunnerIdentity, w io.Writer) error {
	auth, err := s.authorizeRunner(ctx, identity)
	if err != nil {
		return err
	}
	state, _, err := decodeRunnerState(auth.job)
	if err != nil || !runnerOwnerMatches(state, auth) {
		return bcode.ErrUnauthorized
	}
	return s.Artifacts.Download(ctx, auth.task.WorkspaceID, auth.evaluation.Traits.Evaluation.TaskPackageID, w)
}

func (s *Service) RunnerResult(ctx context.Context, identity RunnerIdentity, r io.Reader) (*model.JobArtifact, error) {
	auth, err := s.authorizeRunner(ctx, identity)
	if err != nil {
		return nil, err
	}
	state, _, err := decodeRunnerState(auth.job)
	if err != nil || !runnerOwnerMatches(state, auth) {
		return nil, bcode.ErrUnauthorized
	}
	policy := auth.evaluation.Traits.Evaluation.ResultPolicy
	if policy == nil {
		return nil, bcode.ErrJobInput
	}
	return s.Artifacts.PutResultGuarded(ctx, auth.task.WorkspaceID, auth.task.TaskID, *policy, r, func(tx artifacts.Backend) error {
		task := &model.WorkflowQueue{TaskID: auth.task.TaskID}

		if err := tx.GetForUpdate(ctx, task); err != nil {
			return err
		}
		if err := validateLockedRunnerTask(task, auth); err != nil {
			return err
		}
		// A recovered owner may adopt the same immutable execution checkpoint.
		// A replacement execution may never publish under the old Pod identity.
		job := &model.JobInfo{ID: auth.job.ID}
		if err := tx.GetForUpdate(ctx, job); err != nil {
			return err
		}
		if err := validateLockedRunnerJob(ctx, tx, job, auth, task.Status); err != nil {
			return err
		}
		state, _, err := decodeRunnerState(job)
		if err != nil || !runnerOwnerMatches(state, auth) {
			return bcode.ErrUnauthorized
		}
		return nil
	}, *auth.job.ExecutionKey)
}

func runnerParentAuthorized(ctx context.Context, store artifacts.Backend, status config.Status, record *model.JobInfo, auth *runnerAuthorization) error {
	if status == config.StatusRunning || status == config.StatusCancelled {
		return nil
	}
	if status != config.StatusWaiting && status != config.StatusQueued {
		return bcode.ErrUnauthorized
	}
	// Reaping an expired workflow lease can temporarily queue the same live
	// execution. Only its already claimed and fully fenced Runner may wait for
	// recovery; no operation or new claim is accepted in this parent state.
	state, deadline, err := decodeRunnerState(record)
	if err != nil || !runnerOwnerMatches(state, auth) || deadline <= 0 {
		return bcode.ErrUnauthorized
	}

	now, err := store.CurrentDatabaseTime(ctx)
	if err != nil {
		return err
	}
	if !now.Before(time.Unix(0, deadline)) {
		return bcode.ErrUnauthorized
	}
	return bcode.ErrServiceUnavailable
}

// Maintain runs within the controller leader's existing lifecycle. Delivery
// rows carry their own leases; no second Job execution scheduler is introduced.
func (s *Service) Maintain(ctx context.Context) {
	var group errgroup.Group
	run := func(interval time.Duration, operation string, work func(context.Context) error) {
		group.Go(func() error {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				if err := work(ctx); err != nil && ctx.Err() == nil {
					klog.ErrorS(err, operation)
				}
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
				}
			}
		})
	}
	run(time.Second, "reconcile Job result destinations", func(ctx context.Context) error { return s.Artifacts.ReconcilePending(ctx, 100) })
	run(15*time.Second, "expire original Job results", func(ctx context.Context) error { return s.Artifacts.CleanupExpired(ctx, 100) })
	run(5*time.Second, "reconcile evaluation Sandboxes", func(ctx context.Context) error { return s.reconcileSandboxes(ctx, 100) })
	run(5*time.Second, "reconcile evaluation checkpoints", func(ctx context.Context) error { return s.reconcileCheckpoints(ctx, 100) })
	_ = group.Wait() // Each loop exits without error only after ctx cancellation.
}

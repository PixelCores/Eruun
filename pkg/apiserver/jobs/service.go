// Package jobs exposes standalone workspace Jobs through the existing workflow queue.
package jobs

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
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
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type Service struct {
	Store     datastore.DataStore
	Artifacts *artifacts.Store
	Kube      kubernetes.Interface
	Config    *config.Config
}

func New(store datastore.DataStore, kube kubernetes.Interface, cfg *config.Config) (*Service, error) {
	var minio *spec.MinIOConfig
	if cfg != nil && cfg.Jobs != nil {
		minio = cfg.Jobs.MinIO
	}
	data, err := artifacts.New(store, minio)
	if err != nil {
		return nil, err
	}
	return &Service{Store: store, Artifacts: data, Kube: kube, Config: cfg}, nil
}

type SubmitRequest struct {
	WorkspaceID string `json:"workspaceId"`
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
	Job             spec.JobSpec         `json:"job"`
	Executions      []*model.JobInfo     `json:"executions"`
	Results         []*model.JobArtifact `json:"results"`
	Deliveries      []*model.JobDelivery `json:"deliveries"`
	CollectionState string               `json:"collectionState,omitempty"`
	RunnerStatus    *RunnerStatus        `json:"runnerStatus,omitempty"`
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
	return s.Store.(datastore.Transactional).WithTransaction(ctx, func(tx datastore.DataStore) error {
		space := &model.Workspace{ID: scope.WorkspaceID}
		locker, ok := tx.(datastore.RowLocker)
		if !ok {
			return fmt.Errorf("Job policy requires row locking")
		}
		if err := locker.GetForUpdate(ctx, space); err != nil {
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
	if request.WorkspaceID != scope.WorkspaceID {
		return nil, bcode.ErrForbidden
	}
	if err = request.JobSpec.Normalize(); err != nil {
		return nil, invalid(err)
	}
	if s.Config == nil || s.Config.Accounts == nil {
		return nil, bcode.ErrServiceUnavailable
	}
	if request.Type == string(config.JobAgentEvaluation) {
		if s.Config.Jobs == nil {
			return nil, bcode.WithSafeClientMessage(bcode.ErrServiceUnavailable, "Harbor Runner is not configured")
		}
		if request.ResultPolicy == nil {
			p, err := s.Policy(ctx)
			if err != nil {
				return nil, err
			}
			request.ResultPolicy = &p.Policy
		}
		if err = s.validatePolicy(*request.ResultPolicy); err != nil {
			return nil, err
		}
		var evaluation spec.AgentEvaluationSpec
		if err = spec.DecodeJobJSON(request.Spec, &evaluation); err != nil {
			return nil, invalid(err)
		}
		for _, credential := range evaluation.Agent.Credentials {
			if s.Kube == nil {
				return nil, bcode.ErrServiceUnavailable
			}
			secret, err := s.Kube.CoreV1().Secrets(scope.Namespace).Get(ctx, credential.SecretKeyRef.Name, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("resolve evaluation credential: %w", err)
			}
			if _, ok := secret.Data[credential.SecretKeyRef.Key]; !ok {
				return nil, invalid(fmt.Errorf("credential Secret key is missing"))
			}
		}
	}
	random := make([]byte, 44)
	if _, err = rand.Read(random); err != nil {
		return nil, err
	}
	taskID := hex.EncodeToString(random[:12])
	token := hex.EncodeToString(random[12:])
	declaration, err := json.Marshal(request.JobSpec)
	if err != nil {
		return nil, err
	}
	task := &model.WorkflowQueue{TaskID: taskID, WorkspaceID: scope.WorkspaceID, WorkflowName: request.Name, WorkflowDisplayName: request.Name, TaskCreator: scope.UserID, Type: config.WorkflowTaskTypeJob, Status: config.StatusWaiting, JobSpec: string(declaration), JobToken: token}
	// Validate the exact renderer and workspace policy before enqueueing. Building
	// does not create namespace, application, component, or Kubernetes resources.
	job, err := BuildTask(ctx, s.Store, s.Config, task, scope.Namespace)
	if err != nil {
		return nil, invalid(err)
	}
	space := &model.Workspace{ID: scope.WorkspaceID, Namespace: scope.Namespace}
	if request.Type == string(config.JobAgentEvaluation) {
		err = workspace.PrepareEvaluationTask(job, space, s.Config.Accounts.Workspace, s.Config.Jobs.RunnerImage)
	} else {
		_, err = workspace.PrepareTask(job, "", space, s.Config.Accounts.Workspace)
	}
	if err != nil {
		return nil, err
	}
	err = s.Store.(datastore.Transactional).WithTransaction(ctx, func(tx datastore.DataStore) error {
		locked := &model.Workspace{ID: scope.WorkspaceID}
		locker, ok := tx.(datastore.RowLocker)
		if !ok {
			return fmt.Errorf("Job submission requires row locking")
		}
		if err := locker.GetForUpdate(ctx, locked); err != nil {
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

func (s *Service) Task(ctx context.Context, taskID string) (*model.WorkflowQueue, error) {
	scope, err := Scope(ctx, false)
	if err != nil {
		return nil, err
	}
	task := &model.WorkflowQueue{TaskID: taskID}
	if err = s.Store.Get(ctx, task); err != nil {
		return nil, err
	}
	if task.Type != config.WorkflowTaskTypeJob || task.AppID != "" || task.WorkspaceID != scope.WorkspaceID {
		return nil, bcode.ErrNotFound
	}
	return task, nil
}

func (s *Service) Get(ctx context.Context, taskID string) (*Detail, error) {
	task, err := s.Task(ctx, taskID)
	if err != nil {
		return nil, err
	}
	var declaration spec.JobSpec
	if err = spec.DecodeJobJSON([]byte(task.JobSpec), &declaration); err != nil {
		return nil, err
	}
	out := &Detail{Accepted: Accepted{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, Type: declaration.Type, Status: task.Status}, Job: declaration, Executions: []*model.JobInfo{}}
	rows, err := s.Store.List(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID}, &datastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		out.Executions = append(out.Executions, row.(*model.JobInfo))
	}
	out.Results, err = s.Artifacts.List(ctx, task.WorkspaceID, "", task.TaskID)
	if err != nil {
		return nil, err
	}
	out.Deliveries, err = s.Artifacts.Deliveries(ctx, task.WorkspaceID, task.TaskID)
	if err != nil {
		return nil, err
	}
	if declaration.Type == string(config.JobAgentEvaluation) {
		out.RunnerStatus, err = latestRunnerStatus(ctx, s.Store, out.Executions)
		if err != nil {
			return nil, err
		}
		out.CollectionState = "pending"
		if terminal(task.Status) {
			out.CollectionState = "unavailable"
		}
		for _, result := range out.Results {
			if result.Kind == artifacts.KindSource {
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
	task      *model.WorkflowQueue
	job       *model.JobInfo
	liveJob   *batchv1.Job
	namespace string
	jobUID    string
	identity  RunnerIdentity
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
	if task.Type != config.WorkflowTaskTypeJob || task.AppID != "" || subtle.ConstantTimeCompare([]byte(task.JobToken), []byte(identity.Token)) != 1 {
		return nil, bcode.ErrUnauthorized
	}
	if err := runnerParentAuthorized(ctx, s.Store, task); err != nil {
		return nil, err
	}
	if !validateRunnerDeclaration(task.JobSpec) {
		return nil, bcode.ErrUnauthorized
	}
	space := &model.Workspace{ID: task.WorkspaceID}
	if err := s.Store.Get(ctx, space); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrUnauthorized
		}
		return nil, err
	}
	rows, err := s.Store.List(ctx, &model.JobInfo{TaskID: task.TaskID, WorkspaceID: task.WorkspaceID}, &datastore.ListOptions{})
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
		if terminal(config.Status(job.Status)) || job.Type != string(config.JobAgentEvaluation) || job.ExecutionKey == nil || job.InternalInfo == "" {
			continue
		}
		if pod.Annotations[config.AnnotationJobExecutionKey] != *job.ExecutionKey || pod.Annotations[config.AnnotationJobRunGeneration] != strconv.FormatUint(job.RunGeneration, 10) {
			continue
		}
		for _, owner := range pod.OwnerReferences {
			if owner.Kind == "Job" && owner.Name == job.ServiceName && owner.Controller != nil && *owner.Controller {
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
				return &runnerAuthorization{task: task, job: job, liveJob: live, namespace: space.Namespace, jobUID: string(live.UID), identity: identity}, nil
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
	var declared spec.JobSpec
	var evaluation spec.AgentEvaluationSpec
	if err = json.Unmarshal([]byte(auth.task.JobSpec), &declared); err != nil {
		return err
	}
	if err = json.Unmarshal(declared.Spec, &evaluation); err != nil {
		return err
	}
	return s.Artifacts.Download(ctx, auth.task.WorkspaceID, evaluation.DatasetID, w)
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
	var declared spec.JobSpec
	if err = json.Unmarshal([]byte(auth.task.JobSpec), &declared); err != nil || declared.ResultPolicy == nil {
		return nil, bcode.ErrJobInput
	}
	return s.Artifacts.PutResultGuarded(ctx, auth.task.WorkspaceID, auth.task.TaskID, *declared.ResultPolicy, r, func(tx datastore.DataStore) error {
		task := &model.WorkflowQueue{TaskID: auth.task.TaskID}
		locker, ok := tx.(datastore.RowLocker)
		if !ok {
			return fmt.Errorf("result publication requires row locking")
		}
		if err := locker.GetForUpdate(ctx, task); err != nil {
			return err
		}
		if task.WorkspaceID != auth.task.WorkspaceID || task.JobToken != auth.task.JobToken || task.JobSpec != auth.task.JobSpec {
			return bcode.ErrUnauthorized
		}
		if err := runnerParentAuthorized(ctx, tx, task); err != nil {
			return err
		}
		// A recovered owner may adopt the same immutable execution checkpoint.
		// A replacement execution may never publish under the old Pod identity.
		job := &model.JobInfo{ID: auth.job.ID}
		if err := locker.GetForUpdate(ctx, job); err != nil {
			return err
		}
		if err := validateLockedRunnerJob(job, auth); err != nil {
			return err
		}
		state, _, err := decodeRunnerState(job)
		if err != nil || !runnerOwnerMatches(state, auth) {
			return bcode.ErrUnauthorized
		}
		return nil
	})
}

func runnerParentAuthorized(ctx context.Context, store datastore.DataStore, task *model.WorkflowQueue) error {
	if task == nil {
		return bcode.ErrUnauthorized
	}
	if task.Status == config.StatusRunning {
		return nil
	}
	if task.Status != config.StatusCancelled || task.RunToken == "" || task.WorkerID == "" || task.LeaseExpiresAt == nil {
		return bcode.ErrUnauthorized
	}
	clock, ok := store.(datastore.DatabaseClock)
	if !ok {
		return bcode.ErrUnauthorized
	}
	now, err := clock.CurrentDatabaseTime(ctx)
	if err != nil {
		return err
	}
	if !task.LeaseExpiresAt.After(now) {
		return bcode.ErrUnauthorized
	}
	return nil
}

// Maintain runs within the controller leader's existing lifecycle. Delivery
// rows carry their own leases; no second Job execution scheduler is introduced.
func (s *Service) Maintain(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.Artifacts.ReconcilePending(ctx, 20); err != nil && ctx.Err() == nil {
			klog.ErrorS(err, "reconcile Job result destinations")
		}
		if err := s.Artifacts.CleanupExpired(ctx, 100); err != nil && ctx.Err() == nil {
			klog.ErrorS(err, "expire original Job results")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

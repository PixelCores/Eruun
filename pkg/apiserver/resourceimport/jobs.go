package resourceimport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	maxResourceImportScanRules   = 32
	maxResourceImportRegexLength = 512
	resourceImportCandidate      = "candidate"
	resourceImportCheckpointV1   = 1
)

type compiledScanRule struct {
	kinds         map[string]struct{}
	name          *regexp.Regexp
	labelSelector labels.Selector
}

type resourceImportManageCheckpoint struct {
	Version      int                                       `json:"version"`
	ScanTaskID   string                                    `json:"scanTaskId"`
	ApplyRequest apisv1.ImportNamespaceApplicationsRequest `json:"applyRequest"`
}

func (s *serviceImpl) SubmitScanJob(
	ctx context.Context,
	req apisv1.ResourceImportScanJobRequest,
) (*apisv1.ResourceImportJobAcceptedResponse, error) {
	namespace := strings.TrimSpace(req.Namespace)
	if err := validateResourceImportNamespace(ctx, namespace); err != nil {
		return nil, err
	}
	if _, _, err := compileScanRules(req.Rules); err != nil {
		return nil, err
	}
	return s.submitJob(ctx, config.WorkflowTaskTypeResourceImportScan, namespace, req)
}

func (s *serviceImpl) SubmitManageJob(
	ctx context.Context,
	req apisv1.ResourceImportManageJobRequest,
) (*apisv1.ResourceImportJobAcceptedResponse, error) {
	req.ScanTaskID = strings.TrimSpace(req.ScanTaskID)
	if req.ScanTaskID == "" || len(req.Applications) != 1 {
		return nil, bcode.ErrApplicationConfig
	}
	scan, err := s.loadScanResult(ctx, req.ScanTaskID)
	if err != nil {
		return nil, err
	}
	if err := validateResourceImportNamespace(ctx, scan.Namespace); err != nil {
		return nil, err
	}
	if err := validateSelectedScanResources(scan, req.Applications); err != nil {
		return nil, err
	}
	return s.submitJob(ctx, config.WorkflowTaskTypeResourceImportManage, scan.Namespace, req)
}

func (s *serviceImpl) GetJob(ctx context.Context, taskID string) (*apisv1.ResourceImportJobResponse, error) {
	task, jobInfo, err := s.loadJob(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !isResourceImportTaskType(task.Type) {
		return nil, bcode.ErrWorkflowTaskNotExist
	}
	response := &apisv1.ResourceImportJobResponse{
		TaskID: task.TaskID,
		Type:   task.Type,
		Status: string(task.Status),
	}
	if jobInfo != nil {
		if strings.TrimSpace(jobInfo.Error) != "" {
			response.Error = importcontract.ExecutionFailureReason
		}
		if raw := strings.TrimSpace(jobInfo.Info); raw != "" && json.Valid([]byte(raw)) {
			response.Result = json.RawMessage(raw)
		}
	}
	if response.Error == "" &&
		task.Status == config.StatusFailed &&
		strings.TrimSpace(task.SchedulingReason) == importcontract.PreExecutionFailureReason {
		response.Error = importcontract.PreExecutionFailureReason
	}
	return response, nil
}

// ExecuteResourceImportJob runs inside the durable workflow worker after the
// controller has validated the persisted workspace and namespace. Resource
// import is a system operation and uses this module's Kubernetes client because
// dependency discovery includes namespace RBAC and filtered cluster resources
// that ordinary workspace clients intentionally cannot read.
func (s *serviceImpl) ExecuteResourceImportJob(
	ctx context.Context,
	taskType config.WorkflowTaskType,
	request json.RawMessage,
	checkpoint json.RawMessage,
) (json.RawMessage, error) {
	if s.KubeClient == nil {
		return nil, fmt.Errorf("resource import Kubernetes client is nil")
	}
	switch taskType {
	case config.WorkflowTaskTypeResourceImportScan:
		var req apisv1.ResourceImportScanJobRequest
		if err := decodeResourceImportRequest(request, &req); err != nil {
			return nil, err
		}
		result, err := s.executeScan(ctx, req)
		return marshalResourceImportResult(result, err)
	case config.WorkflowTaskTypeResourceImportManage:
		var req apisv1.ResourceImportManageJobRequest
		if err := decodeResourceImportRequest(request, &req); err != nil {
			return nil, err
		}
		result, err := s.executeManage(ctx, req, checkpoint)
		return marshalResourceImportResult(result, err)
	default:
		return nil, fmt.Errorf("unsupported resource import task type %q", taskType)
	}
}

func (s *serviceImpl) PrepareResourceImportJob(
	ctx context.Context,
	taskType config.WorkflowTaskType,
	request json.RawMessage,
) (json.RawMessage, error) {
	if taskType != config.WorkflowTaskTypeResourceImportManage {
		return nil, nil
	}
	var req apisv1.ResourceImportManageJobRequest
	if err := decodeResourceImportRequest(request, &req); err != nil {
		return nil, err
	}
	checkpoint, err := s.prepareManageCheckpoint(ctx, req)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, fmt.Errorf("marshal resource import management checkpoint: %w", err)
	}
	return raw, nil
}

func (s *serviceImpl) submitJob(
	ctx context.Context,
	taskType config.WorkflowTaskType,
	namespace string,
	request any,
) (*apisv1.ResourceImportJobAcceptedResponse, error) {
	if s.Store == nil {
		return nil, fmt.Errorf("resource import datastore is nil")
	}
	scope, ok := access.FromContext(ctx)
	if !ok || strings.TrimSpace(scope.WorkspaceID) == "" || namespace != scope.Namespace {
		return nil, bcode.ErrForbidden
	}
	rawRequest, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal resource import job request: %w", err)
	}
	envelope, err := json.Marshal(importcontract.TaskEnvelope{
		Version:   importcontract.TaskEnvelopeVersion,
		Namespace: namespace,
		Request:   rawRequest,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal resource import task info: %w", err)
	}
	taskID := utils.RandStringByNumLowercase(24)
	task := &model.WorkflowQueue{
		TaskID:              taskID,
		WorkspaceID:         scope.WorkspaceID,
		WorkflowName:        string(taskType),
		WorkflowDisplayName: string(taskType),
		Status:              config.StatusWaiting,
		TaskCreator:         scope.UserID,
		Type:                taskType,
		ResourceActionInfo:  string(envelope),
	}
	transactional, ok := s.Store.(datastore.Transactional)
	if !ok {
		return nil, fmt.Errorf("resource import datastore does not support transactions")
	}
	if err := transactional.WithTransaction(ctx, func(tx datastore.DataStore) error {
		locker, ok := tx.(datastore.RowLocker)
		if !ok {
			return fmt.Errorf("resource import datastore does not support row locking")
		}
		if err := locker.GetForUpdate(ctx, &model.Workspace{ID: scope.WorkspaceID}); err != nil {
			return fmt.Errorf("lock resource import workspace: %w", err)
		}
		return tx.Add(ctx, task)
	}); err != nil {
		return nil, fmt.Errorf("create resource import job: %w", err)
	}
	return &apisv1.ResourceImportJobAcceptedResponse{
		TaskID: taskID,
		Type:   taskType,
		Status: string(config.StatusWaiting),
	}, nil
}

func (s *serviceImpl) executeScan(
	ctx context.Context,
	req apisv1.ResourceImportScanJobRequest,
) (*apisv1.ResourceImportScanResult, error) {
	namespace := strings.TrimSpace(req.Namespace)
	if err := validateResourceImportNamespace(ctx, namespace); err != nil {
		return nil, err
	}
	rules, includeKinds, err := compileScanRules(req.Rules)
	if err != nil {
		return nil, err
	}
	resources, warnings, err := s.scanNamespaceResources(ctx, namespace, includeKinds)
	if err != nil {
		return nil, fmt.Errorf("scan resource import candidates: %w", err)
	}
	result := &apisv1.ResourceImportScanResult{Namespace: namespace, Warnings: warnings}
	for _, resource := range resources {
		if resource == nil || resource.object == nil || !matchesScanRules(resource, rules) {
			continue
		}
		digest, err := importcontract.DigestObject(resource.object)
		if err != nil {
			return nil, fmt.Errorf("digest resource import candidate %s/%s: %w", resource.kind, resource.name, err)
		}
		result.Resources = append(result.Resources, apisv1.ImportNamespaceResourceResult{
			Kind:      resource.kind,
			Namespace: resource.namespace,
			Name:      resource.name,
			Source: &apisv1.ImportNamespaceResourceIdentity{
				APIVersion:      resource.object.GetAPIVersion(),
				Kind:            resource.object.GetKind(),
				Namespace:       resource.object.GetNamespace(),
				Name:            resource.object.GetName(),
				UID:             string(resource.object.GetUID()),
				ResourceVersion: resource.object.GetResourceVersion(),
				SpecDigest:      digest,
			},
			Status: resourceImportCandidate,
		})
	}
	sort.Slice(result.Resources, func(i, j int) bool {
		left, right := result.Resources[i], result.Resources[j]
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		return left.Name < right.Name
	})
	return result, nil
}

func (s *serviceImpl) prepareManageCheckpoint(
	ctx context.Context,
	req apisv1.ResourceImportManageJobRequest,
) (*resourceImportManageCheckpoint, error) {
	scan, err := s.loadScanResult(ctx, strings.TrimSpace(req.ScanTaskID))
	if err != nil {
		return nil, err
	}
	if err := validateResourceImportNamespace(ctx, scan.Namespace); err != nil {
		return nil, err
	}
	if err := validateSelectedScanResources(scan, req.Applications); err != nil {
		return nil, err
	}
	dryRunRequest := apisv1.ImportNamespaceApplicationsRequest{
		Namespace:      scan.Namespace,
		Mode:           importModeDryRun,
		ManagementMode: config.ManagementModeAdopted,
		Applications:   req.Applications,
	}
	dryRun, err := s.ImportNamespaceResources(ctx, dryRunRequest)
	if err != nil {
		return nil, fmt.Errorf("plan resource import management: %w", err)
	}
	if err := validateResourceImportManagementResult(dryRun, false); err != nil {
		return nil, fmt.Errorf("plan resource import management: %w", err)
	}
	if err := validateScanIdentityDrift(scan, dryRun, req.Applications); err != nil {
		return nil, err
	}
	applyRequest := dryRunRequest
	applyRequest.Mode = importModeApply
	applyRequest.PlanFingerprint = dryRun.PlanFingerprint
	return &resourceImportManageCheckpoint{
		Version:      resourceImportCheckpointV1,
		ScanTaskID:   strings.TrimSpace(req.ScanTaskID),
		ApplyRequest: applyRequest,
	}, nil
}

func (s *serviceImpl) executeManage(
	ctx context.Context,
	req apisv1.ResourceImportManageJobRequest,
	rawCheckpoint json.RawMessage,
) (*apisv1.ImportNamespaceApplicationsResponse, error) {
	scan, err := s.loadScanResult(ctx, strings.TrimSpace(req.ScanTaskID))
	if err != nil {
		return nil, err
	}
	if err := validateResourceImportNamespace(ctx, scan.Namespace); err != nil {
		return nil, err
	}
	if err := validateSelectedScanResources(scan, req.Applications); err != nil {
		return nil, err
	}
	checkpoint, err := decodeResourceImportManageCheckpoint(rawCheckpoint, req, scan.Namespace)
	if err != nil {
		return nil, err
	}
	result, err := s.ImportNamespaceResources(ctx, checkpoint.ApplyRequest)
	if err != nil {
		return nil, fmt.Errorf("apply resource import management: %w", err)
	}
	if err := validateResourceImportManagementResult(result, true); err != nil {
		return nil, fmt.Errorf("apply resource import management: %w", err)
	}
	return result, nil
}

func decodeResourceImportManageCheckpoint(
	raw json.RawMessage,
	req apisv1.ResourceImportManageJobRequest,
	namespace string,
) (*resourceImportManageCheckpoint, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, fmt.Errorf("resource import management checkpoint is missing or invalid")
	}
	var checkpoint resourceImportManageCheckpoint
	if err := json.Unmarshal(raw, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode resource import management checkpoint: %w", err)
	}
	applyRequest := checkpoint.ApplyRequest
	if checkpoint.Version != resourceImportCheckpointV1 ||
		strings.TrimSpace(checkpoint.ScanTaskID) != strings.TrimSpace(req.ScanTaskID) ||
		strings.TrimSpace(applyRequest.Namespace) != strings.TrimSpace(namespace) ||
		!strings.EqualFold(strings.TrimSpace(applyRequest.Mode), importModeApply) ||
		applyRequest.ManagementMode != config.ManagementModeAdopted ||
		strings.TrimSpace(applyRequest.PlanFingerprint) == "" ||
		!reflect.DeepEqual(applyRequest.Applications, req.Applications) {
		return nil, fmt.Errorf("resource import management checkpoint does not match the task request")
	}
	return &checkpoint, nil
}

func validateResourceImportManagementResult(result *apisv1.ImportNamespaceApplicationsResponse, applied bool) error {
	if result == nil || len(result.Apps) != 1 {
		return fmt.Errorf("resource import management returned an invalid application result")
	}
	if message := strings.TrimSpace(result.Apps[0].Error); message != "" {
		return fmt.Errorf("application %q failed: %s", result.Apps[0].Name, message)
	}
	for _, resource := range result.ResourceResults {
		if strings.EqualFold(strings.TrimSpace(resource.Status), importResourceStatusFailed) {
			message := strings.TrimSpace(resource.Error)
			if message == "" {
				message = "resource operation failed"
			}
			return fmt.Errorf("resource %s/%s failed: %s", resource.Kind, resource.Name, message)
		}
	}
	if applied && result.Summary.AppsApplied != 1 {
		return fmt.Errorf("resource import management did not apply the selected application")
	}
	return nil
}

func (s *serviceImpl) loadScanResult(ctx context.Context, taskID string) (*apisv1.ResourceImportScanResult, error) {
	task, jobInfo, err := s.loadJob(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if task.Type != config.WorkflowTaskTypeResourceImportScan || task.Status != config.StatusCompleted || jobInfo == nil {
		return nil, fmt.Errorf("%w: resource import scan job is not completed", bcode.ErrApplicationConfig)
	}
	var result apisv1.ResourceImportScanResult
	if err := json.Unmarshal([]byte(jobInfo.Info), &result); err != nil {
		return nil, fmt.Errorf("decode resource import scan result: %w", err)
	}
	if strings.TrimSpace(result.Namespace) == "" {
		return nil, fmt.Errorf("resource import scan result has no namespace")
	}
	return &result, nil
}

func (s *serviceImpl) loadJob(ctx context.Context, taskID string) (*model.WorkflowQueue, *model.JobInfo, error) {
	if s.Store == nil {
		return nil, nil, fmt.Errorf("resource import datastore is nil")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, nil, bcode.ErrWorkflowTaskNotExist
	}
	task := &model.WorkflowQueue{TaskID: taskID}
	if err := s.Store.Get(ctx, task); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, nil, bcode.ErrWorkflowTaskNotExist
		}
		return nil, nil, err
	}
	entities, err := s.Store.List(ctx, &model.JobInfo{TaskID: taskID, WorkspaceID: task.WorkspaceID}, &datastore.ListOptions{
		SortBy: []datastore.SortOption{{Key: "update_time", Order: datastore.SortOrderDescending}},
	})
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, nil, err
	}
	for _, entity := range entities {
		if info, ok := entity.(*model.JobInfo); ok && info != nil {
			return task, info, nil
		}
	}
	return task, nil, nil
}

func compileScanRules(rules []apisv1.ResourceImportScanRule) ([]compiledScanRule, map[string]struct{}, error) {
	if len(rules) == 0 || len(rules) > maxResourceImportScanRules {
		return nil, nil, bcode.ErrApplicationConfig
	}
	compiled := make([]compiledScanRule, 0, len(rules))
	includeKinds := make(map[string]struct{})
	for _, input := range rules {
		namePattern := strings.TrimSpace(input.NameRegex)
		labelPattern := strings.TrimSpace(input.LabelSelector)
		if len(input.Kinds) == 0 && namePattern == "" && labelPattern == "" {
			return nil, nil, bcode.ErrApplicationConfig
		}
		if len(namePattern) > maxResourceImportRegexLength {
			return nil, nil, bcode.ErrApplicationConfig
		}
		rule := compiledScanRule{}
		if len(input.Kinds) > 0 {
			kinds, _, err := normalizeImportKinds(input.Kinds)
			if err != nil {
				return nil, nil, err
			}
			rule.kinds = kinds
			for kind := range kinds {
				includeKinds[kind] = struct{}{}
			}
		} else {
			for _, kind := range allImportKinds {
				includeKinds[kind] = struct{}{}
			}
		}
		if namePattern != "" {
			name, err := regexp.Compile(namePattern)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: invalid resource name regex: %v", bcode.ErrApplicationConfig, err)
			}
			rule.name = name
		}
		if labelPattern != "" {
			selector, err := labels.Parse(labelPattern)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: invalid Kubernetes label selector: %v", bcode.ErrApplicationConfig, err)
			}
			rule.labelSelector = selector
		}
		compiled = append(compiled, rule)
	}
	return compiled, includeKinds, nil
}

func matchesScanRules(resource *importResource, rules []compiledScanRule) bool {
	for _, rule := range rules {
		if len(rule.kinds) > 0 {
			if _, ok := rule.kinds[resource.kindKey]; !ok {
				continue
			}
		}
		if rule.name != nil && !rule.name.MatchString(resource.name) {
			continue
		}
		if rule.labelSelector != nil && !rule.labelSelector.Matches(labels.Set(resource.labels)) {
			continue
		}
		return true
	}
	return false
}

func validateSelectedScanResources(
	scan *apisv1.ResourceImportScanResult,
	applications []apisv1.ImportNamespaceApplicationMapping,
) error {
	if scan == nil || len(applications) == 0 {
		return bcode.ErrApplicationConfig
	}
	candidates := make(map[string]struct{}, len(scan.Resources))
	for _, resource := range scan.Resources {
		if resource.Source == nil {
			continue
		}
		candidates[resourceImportIdentityKey(resource.Source.APIVersion, resource.Source.Kind, resource.Source.Name)] = struct{}{}
	}
	for _, application := range applications {
		if len(application.Components) == 0 {
			return bcode.ErrApplicationConfig
		}
		for _, component := range application.Components {
			workload := component.Workload
			if _, ok := candidates[resourceImportIdentityKey(workload.APIVersion, workload.Kind, workload.Name)]; !ok {
				return fmt.Errorf("%w: selected workload %s/%s was not returned by scan job", bcode.ErrApplicationConfig, workload.Kind, workload.Name)
			}
		}
	}
	return nil
}

func validateScanIdentityDrift(
	scan *apisv1.ResourceImportScanResult,
	plan *apisv1.ImportNamespaceApplicationsResponse,
	applications []apisv1.ImportNamespaceApplicationMapping,
) error {
	if scan == nil || plan == nil {
		return fmt.Errorf("resource import scan or plan is missing")
	}
	scanned := make(map[string]*apisv1.ImportNamespaceResourceIdentity, len(scan.Resources))
	for _, resource := range scan.Resources {
		if resource.Source != nil {
			scanned[resourceImportIdentityKey(resource.Source.APIVersion, resource.Source.Kind, resource.Source.Name)] = resource.Source
		}
	}
	planned := make(map[string]*apisv1.ImportNamespaceResourceIdentity, len(plan.ResourceResults))
	for _, resource := range plan.ResourceResults {
		if resource.Source != nil {
			planned[resourceImportIdentityKey(resource.Source.APIVersion, resource.Source.Kind, resource.Source.Name)] = resource.Source
		}
	}
	for _, application := range applications {
		for _, component := range application.Components {
			workload := component.Workload
			key := resourceImportIdentityKey(workload.APIVersion, workload.Kind, workload.Name)
			before, after := scanned[key], planned[key]
			if before == nil || after == nil || before.UID != after.UID || before.ResourceVersion != after.ResourceVersion || before.SpecDigest != after.SpecDigest {
				return fmt.Errorf("%w: selected workload %s/%s changed after scan", bcode.ErrNamespaceImportPlanDrift, workload.Kind, workload.Name)
			}
		}
	}
	return nil
}

func validateResourceImportNamespace(ctx context.Context, namespace string) error {
	if namespace == "" || strings.EqualFold(namespace, config.DefaultNamespace) {
		return bcode.ErrApplicationConfig
	}
	if scope, ok := access.FromContext(ctx); !ok || namespace != scope.Namespace {
		return bcode.ErrForbidden
	}
	return nil
}

func resourceImportIdentityKey(apiVersion, kind, name string) string {
	return strings.ToLower(strings.TrimSpace(apiVersion)) + "/" +
		strings.ToLower(strings.TrimSpace(kind)) + "/" + strings.ToLower(strings.TrimSpace(name))
}

func decodeResourceImportRequest(raw json.RawMessage, target any) error {
	if len(raw) == 0 || !json.Valid(raw) {
		return fmt.Errorf("resource import job request is invalid")
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode resource import job request: %w", err)
	}
	return nil
}

func marshalResourceImportResult(value any, runErr error) (json.RawMessage, error) {
	if runErr != nil {
		return nil, runErr
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal resource import job result: %w", err)
	}
	return raw, nil
}

func isResourceImportTaskType(taskType config.WorkflowTaskType) bool {
	return taskType == config.WorkflowTaskTypeResourceImportScan ||
		taskType == config.WorkflowTaskTypeResourceImportManage
}

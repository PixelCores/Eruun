package grpcapi

import (
	"context"
	"errors"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type applicationRuntimeReader interface {
	ListApplicationRuntimeComponents(context.Context, string) ([]*model.ApplicationComponent, error)
}
type applicationNamespaceImporter interface {
	ImportNamespaceResources(context.Context, apis.ImportNamespaceApplicationsRequest) (*apis.ImportNamespaceApplicationsResponse, error)
	TryImportNamespaceResources(context.Context, apis.TryImportNamespaceApplicationsRequest) (*apis.TryImportNamespaceApplicationsResponse, error)
}

// ApplicationsServer composes the same domain services as the HTTP adapter.
type ApplicationsServer struct {
	eruunv1.UnimplementedApplicationServiceServer
	Applications service.ApplicationsService  `inject:""`
	Runtime      applicationRuntimeReader     `inject:""`
	Workflow     service.WorkflowService      `inject:""`
	Validation   service.ValidationService    `inject:""`
	Conversion   service.ConversionService    `inject:""`
	Import       applicationNamespaceImporter `inject:""`
}

func typedResult[P proto.Message](value any, err error, target P) (P, error) {
	if err != nil {
		return target, rpcError(err)
	}
	result, err := encodeTypedResponse(value, target)
	return result, rpcError(err)
}

func appID(id string) error {
	if strings.TrimSpace(id) == "" {
		return bcode.ErrApplicationConfig
	}
	return nil
}

func (s *ApplicationsServer) GetCanonicalJSONSchema(ctx context.Context, _ *emptypb.Empty) (*eruunv1.CanonicalJSONSchema, error) {
	schema, err := apis.CanonicalJSONSchema()
	if err != nil {
		return nil, rpcError(err)
	}
	return &eruunv1.CanonicalJSONSchema{JsonSchema: schema}, nil
}

func listApplicationOptions(req *eruunv1.ApplicationListRequest) service.ListApplicationsOptions {
	result := service.ListApplicationsOptions{}
	if req != nil {
		if req.Page != nil {
			result.Page = int(*req.Page)
		}
		if req.PageSize != nil {
			result.PageSize = int(*req.PageSize)
		}
	}
	return result
}

func (s *ApplicationsServer) ListApplications(ctx context.Context, req *eruunv1.ApplicationListRequest) (*eruunv1.AppDTOListApplicationResponse, error) {
	items, err := s.Applications.ListApplications(ctx, listApplicationOptions(req))
	if err != nil {
		return nil, rpcError(err)
	}
	if scope, ok := access.FromContext(ctx); ok && scope.Role == "viewer" {
		limited := make([]*apis.ApplicationBase, 0, len(items))
		for _, item := range items {
			if item != nil {
				limited = append(limited, &apis.ApplicationBase{ID: item.ID, Name: item.Name, Namespace: item.Namespace, WorkspaceID: item.WorkspaceID, Version: item.Version})
			}
		}
		items = limited
	}
	return typedResult(apis.ListApplicationResponse{Applications: items}, nil, &eruunv1.AppDTOListApplicationResponse{})
}

func (s *ApplicationsServer) ListTemplateApplications(ctx context.Context, req *eruunv1.ApplicationListRequest) (*eruunv1.AppDTOListApplicationResponse, error) {
	items, err := s.Applications.ListTemplateApplications(ctx, listApplicationOptions(req))
	return typedResult(apis.ListApplicationResponse{Applications: items}, err, &eruunv1.AppDTOListApplicationResponse{})
}

func (s *ApplicationsServer) ListCronJobs(ctx context.Context, _ *emptypb.Empty) (*eruunv1.ApplicationCronJobs, error) {
	items, err := s.Applications.ListCronJobs(ctx)
	return typedResult(struct {
		Jobs []*apis.CronJobInfo `json:"jobs"`
	}{items}, err, &eruunv1.ApplicationCronJobs{})
}

func (s *ApplicationsServer) ListScheduledJobs(ctx context.Context, _ *emptypb.Empty) (*eruunv1.ApplicationScheduledJobs, error) {
	items, err := s.Applications.ListScheduledJobs(ctx)
	return typedResult(struct {
		Jobs []*apis.ScheduledJobInfo `json:"jobs"`
	}{items}, err, &eruunv1.ApplicationScheduledJobs{})
}

func (s *ApplicationsServer) CreateApplications(ctx context.Context, req *eruunv1.AppDTOCreateApplicationsRequest) (*eruunv1.AppDTOApplicationBase, error) {
	input, err := decodeTypedRequest[apis.CreateApplicationsRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	item, err := s.Applications.CreateApplications(ctx, input)
	return typedResult(item, err, &eruunv1.AppDTOApplicationBase{})
}

func (s *ApplicationsServer) BatchGetApplications(ctx context.Context, req *eruunv1.AppDTOBatchGetApplicationsRequest) (*eruunv1.AppDTOBatchGetApplicationsResponse, error) {
	input, err := decodeTypedRequest[apis.BatchGetApplicationsRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.Applications.BatchGetApplications(ctx, input.AppIDs)
	return typedResult(resp, err, &eruunv1.AppDTOBatchGetApplicationsResponse{})
}

func (s *ApplicationsServer) ConvertApplications(ctx context.Context, req *eruunv1.AppDTOConvertApplicationsRequest) (*eruunv1.AppDTOConvertApplicationsResponse, error) {
	input, err := decodeTypedRequest[apis.ConvertApplicationsRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.Conversion.ConvertKubeResources(ctx, input)
	return typedResult(resp, err, &eruunv1.AppDTOConvertApplicationsResponse{})
}

func importNamespaceCheck(namespace string) error {
	if strings.EqualFold(strings.TrimSpace(namespace), config.DefaultNamespace) {
		return bcode.WithSafeClientMessage(bcode.ErrApplicationConfig, "import from default namespace is not allowed")
	}
	return nil
}

func (s *ApplicationsServer) ImportNamespaceApplications(ctx context.Context, req *eruunv1.AppDTOImportNamespaceApplicationsRequest) (*eruunv1.AppDTOImportNamespaceApplicationsResponse, error) {
	input, err := decodeTypedRequest[apis.ImportNamespaceApplicationsRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	if err := importNamespaceCheck(input.Namespace); err != nil {
		return nil, rpcError(err)
	}
	resp, err := s.Import.ImportNamespaceResources(ctx, input)
	return typedResult(resp, err, &eruunv1.AppDTOImportNamespaceApplicationsResponse{})
}

func (s *ApplicationsServer) TryImportNamespaceApplications(ctx context.Context, req *eruunv1.AppDTOTryImportNamespaceApplicationsRequest) (*eruunv1.AppDTOTryImportNamespaceApplicationsResponse, error) {
	input, err := decodeTypedRequest[apis.TryImportNamespaceApplicationsRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	if err := importNamespaceCheck(input.Namespace); err != nil {
		return nil, rpcError(err)
	}
	resp, err := s.Import.TryImportNamespaceResources(ctx, input)
	return typedResult(resp, err, &eruunv1.AppDTOTryImportNamespaceApplicationsResponse{})
}

func (s *ApplicationsServer) ListApplicationWorkflows(ctx context.Context, req *eruunv1.ApplicationIDRequest) (*eruunv1.AppDTOListApplicationWorkflowsResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	items, err := s.Applications.ListApplicationWorkflows(ctx, req.AppId)
	if err != nil {
		return nil, rpcError(err)
	}
	result := make([]*apis.ApplicationWorkflow, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		converted, err := assembler.ConvertWorkflowModelToDTO(item)
		if err != nil {
			return nil, rpcError(err)
		}
		result = append(result, converted)
	}
	return typedResult(apis.ListApplicationWorkflowsResponse{Workflows: result}, nil, &eruunv1.AppDTOListApplicationWorkflowsResponse{})
}

func (s *ApplicationsServer) GetApplicationSpec(ctx context.Context, req *eruunv1.ApplicationIDRequest) (*eruunv1.AppDTOCreateApplicationsRequest, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.Applications.GetApplicationSpec(ctx, req.AppId)
	return typedResult(resp, err, &eruunv1.AppDTOCreateApplicationsRequest{})
}

func (s *ApplicationsServer) ListApplicationComponents(ctx context.Context, req *eruunv1.ApplicationIDRequest) (*eruunv1.AppDTOListApplicationComponentsResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	items, err := s.Applications.ListApplicationComponents(ctx, req.AppId)
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := assembler.ConvertComponentModelsToDTO(items)
	return typedResult(apis.ListApplicationComponentsResponse{Components: converted}, err, &eruunv1.AppDTOListApplicationComponentsResponse{})
}

func (s *ApplicationsServer) ListComponentContainers(ctx context.Context, req *eruunv1.ApplicationComponentRequest) (*eruunv1.AppDTOComponentContainersResponse, error) {
	if req == nil || appID(req.AppId) != nil || req.ComponentName == "" {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.Applications.ListComponentContainers(ctx, req.AppId, req.ComponentName)
	return typedResult(resp, err, &eruunv1.AppDTOComponentContainersResponse{})
}

func (s *ApplicationsServer) DeleteApplication(ctx context.Context, req *eruunv1.DeleteApplicationRPCRequest) (*eruunv1.AppDTODeleteApplicationResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input := apis.DeleteApplicationRequest{}
	if req.Request != nil {
		var err error
		input, err = decodeTypedRequest[apis.DeleteApplicationRequest](req.Request)
		if err != nil {
			return nil, rpcError(bcode.ErrApplicationConfig)
		}
	}
	if input.WaitSeconds != nil && *input.WaitSeconds < 0 {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.Applications.DeleteApplicationCascade(ctx, req.AppId, input)
	if resp != nil && err != nil {
		return typedResult(resp, nil, &eruunv1.AppDTODeleteApplicationResponse{})
	}
	return typedResult(resp, err, &eruunv1.AppDTODeleteApplicationResponse{})
}

func (s *ApplicationsServer) UpdateApplicationWorkflow(ctx context.Context, req *eruunv1.UpdateApplicationWorkflowRPCRequest) (*eruunv1.AppDTOUpdateWorkflowResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	input, err := decodeTypedRequest[apis.UpdateApplicationWorkflowRequest](req.Request)
	if err != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	input.WorkflowType = config.WorkflowTaskType(strings.ToLower(strings.TrimSpace(string(input.WorkflowType))))
	apis.NormalizeWorkflowSteps(input.Workflow)
	resp, err := s.Applications.UpdateApplicationWorkflow(ctx, req.AppId, input)
	return typedResult(resp, err, &eruunv1.AppDTOUpdateWorkflowResponse{})
}

func (s *ApplicationsServer) ListWorkflowSchedules(ctx context.Context, req *eruunv1.ApplicationIDRequest) (*eruunv1.AppDTOListWorkflowSchedulesResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	items, err := s.Workflow.ListWorkflowSchedules(ctx, req.AppId)
	return typedResult(apis.ListWorkflowSchedulesResponse{Schedules: items}, err, &eruunv1.AppDTOListWorkflowSchedulesResponse{})
}

func (s *ApplicationsServer) UpsertWorkflowSchedule(ctx context.Context, req *eruunv1.UpsertWorkflowScheduleRPCRequest) (*eruunv1.AppDTOUpsertWorkflowScheduleResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	input, err := decodeTypedRequest[apis.UpsertWorkflowScheduleRequest](req.Request)
	if err != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	resp, err := s.Workflow.UpsertWorkflowSchedule(ctx, req.AppId, input)
	return typedResult(resp, err, &eruunv1.AppDTOUpsertWorkflowScheduleResponse{})
}

func (s *ApplicationsServer) DeleteWorkflowSchedule(ctx context.Context, req *eruunv1.ApplicationWorkflowRequest) (*eruunv1.AppDTODeleteWorkflowScheduleResponse, error) {
	if req == nil || appID(req.AppId) != nil || req.WorkflowId == "" {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	err := s.Workflow.DeleteWorkflowSchedule(ctx, req.AppId, req.WorkflowId)
	return typedResult(apis.DeleteWorkflowScheduleResponse{WorkflowID: req.WorkflowId}, err, &eruunv1.AppDTODeleteWorkflowScheduleResponse{})
}

func (s *ApplicationsServer) PlanApplicationResourceCleanup(ctx context.Context, req *eruunv1.ApplicationIDRequest) (*eruunv1.AppDTOCleanupApplicationResourcesPlanResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.Applications.PlanApplicationResourceCleanup(ctx, req.AppId)
	return typedResult(resp, err, &eruunv1.AppDTOCleanupApplicationResourcesPlanResponse{})
}

func (s *ApplicationsServer) ApplyApplicationResourceCleanup(ctx context.Context, req *eruunv1.CleanupApplicationRPCRequest) (*eruunv1.AppDTOCleanupApplicationResourcesResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input := apis.CleanupApplicationResourcesRequest{}
	if req.Request != nil {
		var err error
		input, err = decodeTypedRequest[apis.CleanupApplicationResourcesRequest](req.Request)
		if err != nil {
			return nil, rpcError(bcode.ErrApplicationConfig)
		}
	}
	resp, err := s.Applications.ApplyApplicationResourceCleanup(ctx, req.AppId, input)
	if resp != nil && err != nil {
		return typedResult(resp, nil, &eruunv1.AppDTOCleanupApplicationResourcesResponse{})
	}
	return typedResult(resp, err, &eruunv1.AppDTOCleanupApplicationResourcesResponse{})
}

func (s *ApplicationsServer) ResetApplicationDatabases(ctx context.Context, req *eruunv1.ResetApplicationDatabasesRPCRequest) (*eruunv1.AppDTODatabaseResetResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input, err := decodeTypedRequest[apis.DatabaseResetRequest](req.Request)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.Applications.ResetApplicationDatabases(ctx, req.AppId, input)
	return typedResult(resp, err, &eruunv1.AppDTODatabaseResetResponse{})
}

func (s *ApplicationsServer) RestartApplicationWorkloads(ctx context.Context, req *eruunv1.ApplicationLifecycleRPCRequest) (*eruunv1.AppDTORestartApplicationWorkloadsResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input := apis.ApplicationLifecycleRequest{}
	if req.Request != nil {
		var err error
		input, err = decodeTypedRequest[apis.ApplicationLifecycleRequest](req.Request)
		if err != nil {
			return nil, rpcError(bcode.ErrApplicationConfig)
		}
	}
	resp, err := s.Applications.RestartApplicationWorkloads(ctx, req.AppId, input)
	if resp != nil && err != nil {
		return typedResult(resp, nil, &eruunv1.AppDTORestartApplicationWorkloadsResponse{})
	}
	return typedResult(resp, err, &eruunv1.AppDTORestartApplicationWorkloadsResponse{})
}

func (s *ApplicationsServer) StopApplicationDeployments(ctx context.Context, req *eruunv1.ApplicationLifecycleRPCRequest) (*eruunv1.AppDTOStopApplicationDeploymentsResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input := apis.ApplicationLifecycleRequest{}
	if req.Request != nil {
		var err error
		input, err = decodeTypedRequest[apis.ApplicationLifecycleRequest](req.Request)
		if err != nil {
			return nil, rpcError(bcode.ErrApplicationConfig)
		}
	}
	resp, err := s.Applications.StopApplicationDeployments(ctx, req.AppId, input)
	if resp != nil && err != nil {
		return typedResult(resp, nil, &eruunv1.AppDTOStopApplicationDeploymentsResponse{})
	}
	return typedResult(resp, err, &eruunv1.AppDTOStopApplicationDeploymentsResponse{})
}

func (s *ApplicationsServer) StartApplicationDeployments(ctx context.Context, req *eruunv1.ApplicationLifecycleRPCRequest) (*eruunv1.AppDTOStartApplicationDeploymentsResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input := apis.ApplicationLifecycleRequest{}
	if req.Request != nil {
		var err error
		input, err = decodeTypedRequest[apis.ApplicationLifecycleRequest](req.Request)
		if err != nil {
			return nil, rpcError(bcode.ErrApplicationConfig)
		}
	}
	resp, err := s.Applications.StartApplicationDeployments(ctx, req.AppId, input)
	if resp != nil && err != nil {
		return typedResult(resp, nil, &eruunv1.AppDTOStartApplicationDeploymentsResponse{})
	}
	return typedResult(resp, err, &eruunv1.AppDTOStartApplicationDeploymentsResponse{})
}

func (s *ApplicationsServer) UpdateApplicationVersion(ctx context.Context, req *eruunv1.UpdateVersionRPCRequest) (*eruunv1.AppDTOUpdateVersionResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input, err := decodeTypedRequest[apis.UpdateVersionRequest](req.Request)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	for i := range input.Components {
		input.Components[i].Name = strings.ToLower(strings.TrimSpace(input.Components[i].Name))
	}
	resp, err := s.Applications.UpdateVersion(ctx, req.AppId, input)
	return typedResult(resp, err, &eruunv1.AppDTOUpdateVersionResponse{})
}

func (s *ApplicationsServer) DiffUpdateApplicationVersion(ctx context.Context, req *eruunv1.DiffUpdateVersionRPCRequest) (*eruunv1.AppDTODiffUpdateVersionResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input, err := decodeTypedRequest[apis.DiffUpdateVersionRequest](req.Request)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.Applications.DiffUpdateVersion(ctx, req.AppId, input)
	return typedResult(resp, err, &eruunv1.AppDTODiffUpdateVersionResponse{})
}

func (s *ApplicationsServer) TryApplication(ctx context.Context, req *eruunv1.AppDTOCreateApplicationsRequest) (*eruunv1.AppDTOTryApplicationResponse, error) {
	input, err := decodeTypedRequest[apis.TryApplicationRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	for i := range input.Components {
		input.Components[i].Name = strings.ToLower(strings.TrimSpace(input.Components[i].Name))
	}
	apis.NormalizeWorkflowSteps(input.Workflow)
	resp := s.Validation.TryApplication(ctx, input)
	return typedResult(resp, nil, &eruunv1.AppDTOTryApplicationResponse{})
}

func (s *ApplicationsServer) TryWorkflow(ctx context.Context, req *eruunv1.UpdateApplicationWorkflowRPCRequest) (*eruunv1.AppDTOTryWorkflowResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	input, err := decodeTypedRequest[apis.UpdateApplicationWorkflowRequest](req.Request)
	if err != nil {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	input.WorkflowType = config.WorkflowTaskType(strings.ToLower(strings.TrimSpace(string(input.WorkflowType))))
	apis.NormalizeWorkflowSteps(input.Workflow)
	tryReq := apis.TryWorkflowRequest{
		WorkflowID: input.WorkflowID, Name: input.Name, Alias: input.Alias, WorkflowType: input.WorkflowType,
		Callback: input.Callback, FailurePolicy: input.FailurePolicy, FailurePolicySet: input.FailurePolicySet, Workflow: input.Workflow,
	}
	resp := s.Validation.TryWorkflow(ctx, req.AppId, tryReq)
	return typedResult(resp, nil, &eruunv1.AppDTOTryWorkflowResponse{})
}

// Create-and-exec remains a successful typed result if creation succeeded but
// the subsequent execution failed, matching the existing partial outcome.
func (s *ApplicationsServer) CreateAndExecApplications(ctx context.Context, req *eruunv1.AppDTOCreateAndExecApplicationRequest) (*eruunv1.AppDTOCreateAndExecApplicationResponse, error) {
	input, err := decodeTypedRequest[apis.CreateAndExecApplicationRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	idempotencyKey, err := requestIdempotencyKey(ctx, bcode.ErrApplicationConfig)
	if err != nil {
		return nil, rpcError(err)
	}
	resp, err := service.ExecuteCreateAndExecApplication(
		ctx, s.Applications, s.Workflow, input, idempotencyKey, safeBusinessMessage,
	)
	return typedResult(resp, err, &eruunv1.AppDTOCreateAndExecApplicationResponse{})
}

func safeBusinessMessage(err error) string {
	var code *bcode.Bcode
	if errors.As(err, &code) {
		return code.Message
	}
	return "workflow execution failed"
}

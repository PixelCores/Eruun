package grpcapi

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/ratelimit"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/go-playground/validator/v10"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const maxUnaryMessageBytes = 24 << 20

type principalContextKey struct{}

var publicAccountMethods = map[string]bool{
	eruunv1.AccountService_GetAuthMethods_FullMethodName: true,
	eruunv1.AccountService_SendAuthCode_FullMethodName:   true,
	eruunv1.AccountService_Register_FullMethodName:       true,
	eruunv1.AccountService_Login_FullMethodName:          true,
	eruunv1.AccountService_Refresh_FullMethodName:        true,
	eruunv1.AccountService_ResetPassword_FullMethodName:  true,
}

var adminAccountMethods = map[string]bool{
	eruunv1.AccountService_ListAdminUsers_FullMethodName:       true,
	eruunv1.AccountService_SetAdminUserDisabled_FullMethodName: true,
}

var authAccountMethods = map[string]bool{
	eruunv1.AccountService_GetAuthMethods_FullMethodName: true,
	eruunv1.AccountService_SendAuthCode_FullMethodName:   true,
	eruunv1.AccountService_Register_FullMethodName:       true,
	eruunv1.AccountService_Login_FullMethodName:          true,
	eruunv1.AccountService_Refresh_FullMethodName:        true,
	eruunv1.AccountService_Logout_FullMethodName:         true,
	eruunv1.AccountService_GetMe_FullMethodName:          true,
	eruunv1.AccountService_ChangePassword_FullMethodName: true,
	eruunv1.AccountService_ResetPassword_FullMethodName:  true,
	eruunv1.AccountService_ListIdentities_FullMethodName: true,
	eruunv1.AccountService_BindIdentity_FullMethodName:   true,
	eruunv1.AccountService_UnbindIdentity_FullMethodName: true,
}

var administrationMethodPolicies = map[string]string{
	eruunv1.SettingsService_ListSettings_FullMethodName:                          "system",
	eruunv1.SettingsService_GetSetting_FullMethodName:                            "system",
	eruunv1.SettingsService_CreateSetting_FullMethodName:                         "system",
	eruunv1.SettingsService_UpdateSetting_FullMethodName:                         "system",
	eruunv1.SettingsService_DeleteSetting_FullMethodName:                         "system",
	eruunv1.ProgrammingLanguagesService_ListProgrammingLanguages_FullMethodName:  "member",
	eruunv1.ProgrammingLanguagesService_GetProgrammingLanguage_FullMethodName:    "member",
	eruunv1.ProgrammingLanguagesService_CreateProgrammingLanguage_FullMethodName: "system",
	eruunv1.ProgrammingLanguagesService_UpdateProgrammingLanguage_FullMethodName: "system",
	eruunv1.ProgrammingLanguagesService_DeleteProgrammingLanguage_FullMethodName: "system",
	eruunv1.ResourceImportService_SubmitResourceImportScan_FullMethodName:        "system_workspace",
	eruunv1.ResourceImportService_SubmitResourceImportManage_FullMethodName:      "system_workspace",
	eruunv1.ResourceImportService_GetResourceImportJob_FullMethodName:            "system_workspace",
}

var jobsMethodPolicies = map[string]string{
	eruunv1.JobsService_SubmitJob_FullMethodName:           "member",
	eruunv1.JobsService_GetJob_FullMethodName:              "viewer",
	eruunv1.JobsService_CancelJob_FullMethodName:           "member",
	eruunv1.JobsService_GetJobResults_FullMethodName:       "viewer",
	eruunv1.JobsService_DownloadJobResult_FullMethodName:   "viewer",
	eruunv1.JobsService_DownloadJobDelivery_FullMethodName: "viewer",
	eruunv1.JobsService_RetryJobDelivery_FullMethodName:    "member",
	eruunv1.JobsService_SetJobRetention_FullMethodName:     "member",
	eruunv1.JobsService_GetJobStoragePolicy_FullMethodName: "viewer",
	eruunv1.JobsService_SetJobStoragePolicy_FullMethodName: "member",
	eruunv1.JobsService_UploadJobDataset_FullMethodName:    "member",
	eruunv1.JobsService_ListJobDatasets_FullMethodName:     "viewer",
	eruunv1.JobsService_GetJobDataset_FullMethodName:       "viewer",
	eruunv1.JobsService_DownloadJobDataset_FullMethodName:  "viewer",
}

var applicationMethodPolicies = map[string]string{
	eruunv1.ApplicationService_GetCanonicalJSONSchema_FullMethodName:          "viewer",
	eruunv1.ApplicationService_ListApplications_FullMethodName:                "viewer",
	eruunv1.ApplicationService_ListTemplateApplications_FullMethodName:        "member",
	eruunv1.ApplicationService_ListCronJobs_FullMethodName:                    "member",
	eruunv1.ApplicationService_ListScheduledJobs_FullMethodName:               "member",
	eruunv1.ApplicationService_CreateApplications_FullMethodName:              "member",
	eruunv1.ApplicationService_CreateAndExecApplications_FullMethodName:       "member",
	eruunv1.ApplicationService_BatchGetApplications_FullMethodName:            "member",
	eruunv1.ApplicationService_ConvertApplications_FullMethodName:             "member",
	eruunv1.ApplicationService_ImportNamespaceApplications_FullMethodName:     "system_workspace",
	eruunv1.ApplicationService_TryImportNamespaceApplications_FullMethodName:  "system_workspace",
	eruunv1.ApplicationService_ListApplicationWorkflows_FullMethodName:        "member",
	eruunv1.ApplicationService_GetApplicationSpec_FullMethodName:              "member",
	eruunv1.ApplicationService_GetApplicationStatus_FullMethodName:            "viewer",
	eruunv1.ApplicationService_ListApplicationComponents_FullMethodName:       "member",
	eruunv1.ApplicationService_GetApplicationComponentStatus_FullMethodName:   "viewer",
	eruunv1.ApplicationService_ListComponentContainers_FullMethodName:         "member",
	eruunv1.ApplicationService_BatchApplicationComponentStatus_FullMethodName: "member",
	eruunv1.ApplicationService_StreamComponentLogs_FullMethodName:             "member",
	eruunv1.ApplicationService_ExportComponentFiles_FullMethodName:            "member",
	eruunv1.ApplicationService_ExecComponentShell_FullMethodName:              "member",
	eruunv1.ApplicationService_StreamComponentShell_FullMethodName:            "member",
	eruunv1.ApplicationService_DeleteApplication_FullMethodName:               "member",
	eruunv1.ApplicationService_UpdateApplicationWorkflow_FullMethodName:       "member",
	eruunv1.ApplicationService_ListWorkflowSchedules_FullMethodName:           "member",
	eruunv1.ApplicationService_UpsertWorkflowSchedule_FullMethodName:          "member",
	eruunv1.ApplicationService_DeleteWorkflowSchedule_FullMethodName:          "member",
	eruunv1.ApplicationService_PlanApplicationResourceCleanup_FullMethodName:  "member",
	eruunv1.ApplicationService_ApplyApplicationResourceCleanup_FullMethodName: "member",
	eruunv1.ApplicationService_ResetApplicationDatabases_FullMethodName:       "member",
	eruunv1.ApplicationService_DownloadLogArchive_FullMethodName:              "member",
	eruunv1.ApplicationService_RestartApplicationWorkloads_FullMethodName:     "member",
	eruunv1.ApplicationService_StopApplicationDeployments_FullMethodName:      "member",
	eruunv1.ApplicationService_StartApplicationDeployments_FullMethodName:     "member",
	eruunv1.ApplicationService_ExecApplicationWorkflow_FullMethodName:         "member",
	eruunv1.ApplicationService_CancelApplicationWorkflow_FullMethodName:       "member",
	eruunv1.ApplicationService_CancelAllApplicationWorkflows_FullMethodName:   "member",
	eruunv1.ApplicationService_ListApplicationTasks_FullMethodName:            "member",
	eruunv1.ApplicationService_ApproveWorkflowTask_FullMethodName:             "member",
	eruunv1.ApplicationService_GetWorkflowTaskStatus_FullMethodName:           "member",
	eruunv1.ApplicationService_GetWorkflowTaskStages_FullMethodName:           "member",
	eruunv1.ApplicationService_UpdateApplicationVersion_FullMethodName:        "member",
	eruunv1.ApplicationService_DiffUpdateApplicationVersion_FullMethodName:    "member",
	eruunv1.ApplicationService_CancelDelayedVersionUpdate_FullMethodName:      "member",
	eruunv1.ApplicationService_TryApplication_FullMethodName:                  "member",
	eruunv1.ApplicationService_TryWorkflow_FullMethodName:                     "member",
}

// NewServer registers only methods with an explicit authorization policy.
func NewServer(accounts *account.Service, namespaces *workspace.Manager, administration *AdministrationServer, jobsAdapter *JobsServer, applications *ApplicationsServer, limiter *ratelimit.Limiter) *grpc.Server {
	interceptor := accountAuthInterceptor(accounts, limiter)
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxUnaryMessageBytes),
		grpc.UnaryInterceptor(interceptor),
		grpc.StreamInterceptor(accountStreamAuthInterceptor(accounts, limiter)),
	)
	eruunv1.RegisterAccountServiceServer(s, &AccountServer{Accounts: accounts, Namespaces: namespaces})
	eruunv1.RegisterSettingsServiceServer(s, administration)
	eruunv1.RegisterProgrammingLanguagesServiceServer(s, administration)
	eruunv1.RegisterResourceImportServiceServer(s, administration)
	eruunv1.RegisterJobsServiceServer(s, jobsAdapter)
	eruunv1.RegisterApplicationServiceServer(s, applications)
	return s
}

func accountMethodKnown(method string) bool {
	for _, descriptor := range eruunv1.AccountService_ServiceDesc.Methods {
		if method == "/"+eruunv1.AccountService_ServiceDesc.ServiceName+"/"+descriptor.MethodName {
			return true
		}
	}
	return false
}

func accountAuthInterceptor(accounts *account.Service, limiter *ratelimit.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info == nil {
			return nil, rpcError(bcode.ErrForbidden)
		}
		ctx, err := authorizeRPC(ctx, info.FullMethod, accounts, limiter)
		if err != nil {
			return nil, err
		}
		if err := guardRPCResource(ctx, info.FullMethod, req, accounts); err != nil {
			return nil, rpcError(err)
		}
		return handler(ctx, req)
	}
}

type scopedServerStream struct {
	grpc.ServerStream
	ctx      context.Context
	method   string
	accounts *account.Service
}

func (s *scopedServerStream) Context() context.Context { return s.ctx }

func (s *scopedServerStream) RecvMsg(request any) error {
	if err := s.ServerStream.RecvMsg(request); err != nil {
		return err
	}
	if err := guardRPCResource(s.ctx, s.method, request, s.accounts); err != nil {
		return rpcError(err)
	}
	return nil
}

func accountStreamAuthInterceptor(accounts *account.Service, limiter *ratelimit.Limiter) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info == nil {
			return rpcError(bcode.ErrForbidden)
		}
		ctx, err := authorizeRPC(stream.Context(), info.FullMethod, accounts, limiter)
		if err != nil {
			return err
		}
		return handler(srv, &scopedServerStream{ServerStream: stream, ctx: ctx, method: info.FullMethod, accounts: accounts})
	}
}

func guardRPCResource(ctx context.Context, method string, request any, accounts *account.Service) error {
	if _, known := applicationMethodPolicies[method]; !known {
		return nil
	}
	if accounts == nil || accounts.Repo.Store == nil {
		return bcode.ErrServiceUnavailable
	}
	guard := account.NewStore(accounts.Repo.Store)
	if item, ok := request.(interface{ GetAppId() string }); ok && item.GetAppId() != "" {
		if err := guard.Get(ctx, &model.Applications{ID: item.GetAppId()}); err != nil {
			return err
		}
	}
	if item, ok := request.(interface{ GetTaskId() string }); ok && item.GetTaskId() != "" {
		if err := guard.Get(ctx, &model.WorkflowQueue{TaskID: item.GetTaskId()}); err != nil {
			return err
		}
	}
	return nil
}

func authorizeRPC(ctx context.Context, method string, accounts *account.Service, limiter *ratelimit.Limiter) (context.Context, error) {
	minimum := administrationMethodPolicies[method]
	if minimum == "" {
		minimum = jobsMethodPolicies[method]
	}
	if minimum == "" {
		minimum = applicationMethodPolicies[method]
	}
	if !accountMethodKnown(method) && minimum == "" {
		return nil, rpcError(bcode.ErrForbidden)
	}
	if !limiter.Allow(expensiveRPC(method)) {
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}
	if accounts == nil {
		return nil, rpcError(bcode.ErrServiceUnavailable)
	}
	if authAccountMethods[method] {
		ip, err := clientIP(ctx)
		if err != nil {
			return nil, rpcError(err)
		}
		if err := accounts.RateLimit(ctx, "auth-ip:"+ip, 60, time.Minute); err != nil {
			return nil, rpcError(err)
		}
	}
	token, err := bearerToken(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	var p *account.Principal
	if token != "" {
		p, err = accounts.Authenticate(ctx, token)
		if err != nil {
			return nil, rpcError(err)
		}
		ctx = context.WithValue(ctx, principalContextKey{}, p)
	}
	if !publicAccountMethods[method] {
		if p == nil {
			return nil, rpcError(bcode.ErrUnauthorized)
		}
		if p.User.MustChangePassword &&
			method != eruunv1.AccountService_GetMe_FullMethodName &&
			method != eruunv1.AccountService_ChangePassword_FullMethodName &&
			method != eruunv1.AccountService_Logout_FullMethodName {
			return nil, rpcError(bcode.ErrAccountPasswordChange)
		}
		if adminAccountMethods[method] && !p.User.SystemAdmin {
			return nil, rpcError(bcode.ErrForbidden)
		}
		if minimum != "" {
			if minimum == "system" {
				if !p.User.SystemAdmin {
					return nil, rpcError(bcode.ErrForbidden)
				}
			} else {
				if minimum == "system_workspace" && !p.User.SystemAdmin {
					return nil, rpcError(bcode.ErrForbidden)
				}
				workspaceID, metadataErr := selectedWorkspaceID(ctx)
				if metadataErr != nil {
					return nil, rpcError(metadataErr)
				}
				access, workspaceErr := accounts.Workspace(ctx, p, workspaceID)
				if workspaceErr != nil {
					return nil, rpcError(workspaceErr)
				}
				if access.Role == "viewer" && minimum != "viewer" {
					return nil, rpcError(bcode.ErrForbidden)
				}
				ctx = account.WithScope(ctx, account.Scope{
					UserID: p.User.ID, WorkspaceID: access.Workspace.ID,
					Namespace: access.Workspace.Namespace, Role: access.Role,
					SystemAdmin: p.User.SystemAdmin, ClusterOperation: minimum == "system_workspace",
				})
			}
		}
	}
	return ctx, nil
}

func expensiveRPC(method string) bool {
	for route, rpc := range routeRPC {
		if rpc != method {
			continue
		}
		if !strings.HasPrefix(route, "GET ") {
			return true
		}
		path := strings.ToLower(route)
		return strings.Contains(path, "/exec") || strings.Contains(path, "/logs") ||
			strings.Contains(path, "/shell/stream") || strings.Contains(path, "/files/export")
	}
	return true
}

func selectedWorkspaceID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", nil
	}
	values := md.Get("x-eruun-workspace-id")
	if len(values) > 1 {
		return "", bcode.ErrForbidden
	}
	if len(values) == 0 {
		return "", nil
	}
	return values[0], nil
}

func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", nil
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 {
		return "", bcode.ErrUnauthorized
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", bcode.ErrUnauthorized
	}
	return parts[1], nil
}

func clientIP(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "", bcode.ErrForbidden
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil || net.ParseIP(host) == nil {
		return "", bcode.ErrForbidden
	}
	return host, nil
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request cancelled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	}
	var bc *bcode.Bcode
	if !errors.As(err, &bc) {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			bc = bcode.ErrNotFound
		} else {
			var validationErr validator.ValidationErrors
			if errors.As(err, &validationErr) {
				bc = bcode.ErrApplicationConfig
			} else {
				bc = bcode.ErrServer
			}
		}
	}
	message := bcode.SafeClientMessage(err)
	if message == "" {
		message = bc.Message
	}
	code := grpcCode(bc)
	st := status.New(code, message)
	withDetail, detailErr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: "ERUUN_BUSINESS_ERROR", Domain: "eruun.io",
		Metadata: map[string]string{"business_code": strconv.FormatInt(int64(bc.BusinessCode), 10)},
	})
	if detailErr != nil {
		return status.Error(codes.Internal, "The service has lapsed.")
	}
	return withDetail.Err()
}

func grpcCode(bc *bcode.Bcode) codes.Code {
	if bc == nil {
		return codes.Internal
	}
	switch bc.HTTPCode {
	case 200:
		if bc.BusinessCode == bcode.ErrApplicationNotExist.BusinessCode {
			return codes.NotFound
		}
		return codes.FailedPrecondition
	case 400, 422:
		return codes.InvalidArgument
	case 401:
		return codes.Unauthenticated
	case 403:
		return codes.PermissionDenied
	case 404:
		return codes.NotFound
	case 409:
		return codes.FailedPrecondition
	case 429:
		return codes.ResourceExhausted
	case 413:
		return codes.ResourceExhausted
	case 501:
		return codes.Unimplemented
	case 502, 503:
		return codes.Unavailable
	case 504:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}

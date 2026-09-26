package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	apiresponse "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/response"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

func (app *applications) createApplications(c *gin.Context) {
	handleBoundResult(
		c,
		validatedStrictJSONBody[apis.CreateApplicationsRequest](bcode.ErrApplicationConfig, true),
		func(ctx context.Context, req *apis.CreateApplicationsRequest) (*apis.ApplicationBase, error) {
			return app.ApplicationService.CreateApplications(ctx, *req)
		},
	)
}

func (app *applications) createAndExecApplications(c *gin.Context) {
	idempotencyKey, ok := bindIdempotencyKey(c, bcode.ErrApplicationConfig)
	if !ok {
		return
	}
	req, ok := bindAndValidateStrictJSON[apis.CreateAndExecApplicationRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}

	resp, err := service.ExecuteCreateAndExecApplication(
		c.Request.Context(), app.ApplicationService, app.WorkflowService,
		*req, idempotencyKey, func(err error) string { return err.Error() },
	)
	respondWithResult(c, resp, err)
}

func shouldMarkCreateAndExecDeploying(executeAt int64, now time.Time) bool {
	if executeAt < 0 {
		return false
	}
	return executeAt == 0 || executeAt <= now.Unix()
}

func (app *applications) convertApplications(c *gin.Context) {
	handleBoundResult(
		c,
		validatedRequestBody[apis.ConvertApplicationsRequest](bcode.ErrApplicationConfig, true),
		func(ctx context.Context, req *apis.ConvertApplicationsRequest) (*apis.ConvertApplicationsResponse, error) {
			return app.ConversionService.ConvertKubeResources(ctx, *req)
		},
	)
}

func (app *applications) importNamespaceApplications(c *gin.Context) {
	req, ok := bindStrictJSON[apis.ImportNamespaceApplicationsRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	if !app.validateNamespaceImportPreconditions(c, req.Namespace) {
		return
	}
	resp, err := app.ImportService.ImportNamespaceResources(c.Request.Context(), *req)
	respondWithResult(c, resp, err)
}

func (app *applications) tryImportNamespaceApplications(c *gin.Context) {
	req, ok := bindRequest[apis.TryImportNamespaceApplicationsRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	if !app.validateNamespaceImportPreconditions(c, req.Namespace) {
		return
	}
	resp, err := app.ImportService.TryImportNamespaceResources(c.Request.Context(), *req)
	respondWithResult(c, resp, err)
}

func (app *applications) validateNamespaceImportPreconditions(c *gin.Context, namespace string) bool {
	if strings.EqualFold(strings.TrimSpace(namespace), config.DefaultNamespace) {
		apiresponse.ReturnErrorWithMessage(c, bcode.ErrApplicationConfig, "import from default namespace is not allowed")
		return false
	}
	if app.ImportService == nil {
		apiresponse.ReturnError(c, errors.New("import service is not initialized"))
		return false
	}
	return true
}

func (app *applications) listApplications(c *gin.Context) {
	opts, ok := bindListApplicationsOptions(c)
	if !ok {
		return
	}
	apps, err := app.ApplicationService.ListApplications(c.Request.Context(), opts)
	if scope, ok := access.FromContext(c.Request.Context()); ok && scope.Role == "viewer" {
		summaries := make([]apis.ApplicationSummary, 0, len(apps))
		for _, a := range apps {
			summaries = append(summaries, apis.ApplicationSummary{ID: a.ID, Name: a.Name, Namespace: a.Namespace, WorkspaceID: a.WorkspaceID, Version: a.Version})
		}
		respondWithResult(c, gin.H{"applications": summaries}, err)
		return
	}
	respondWithResult(c, apis.ListApplicationResponse{Applications: apps}, err)
}

func (app *applications) batchGetApplications(c *gin.Context) {
	req, ok := bindRequest[apis.BatchGetApplicationsRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	resp, err := app.ApplicationService.BatchGetApplications(c.Request.Context(), req.AppIDs)
	respondWithResult(c, resp, err)
}

func (app *applications) listTemplateApplications(c *gin.Context) {
	opts, ok := bindListApplicationsOptions(c)
	if !ok {
		return
	}
	apps, err := app.ApplicationService.ListTemplateApplications(c.Request.Context(), opts)
	respondWithResult(c, apis.ListApplicationResponse{Applications: apps}, err)
}

func bindListApplicationsOptions(c *gin.Context) (service.ListApplicationsOptions, bool) {
	var query apis.ListApplicationsQuery
	if err := c.ShouldBindQuery(&query); err != nil {
		apiresponse.ReturnError(c, bcode.ErrApplicationConfig)
		return service.ListApplicationsOptions{}, false
	}
	return service.ListApplicationsOptions{
		Page:     query.Page,
		PageSize: query.PageSize,
	}, true
}

func (app *applications) listCronJobs(c *gin.Context) {
	handleContextResult(c, app.ApplicationService.ListCronJobs)
}

func (app *applications) listScheduledJobs(c *gin.Context) {
	handleContextResult(c, app.ApplicationService.ListScheduledJobs)
}

func (app *applications) deleteApplicationResources(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	req, ok := bindJSONAllowEOF[apis.CleanupApplicationResourcesRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	resp, err := app.ApplicationService.ApplyApplicationResourceCleanup(c.Request.Context(), appID, *req)
	if err != nil {
		if resp != nil {
			klog.ErrorS(err, "application resource cleanup reported partial failures", "appID", appID)
			apiresponse.ReturnSuccess(c, resp)
			return
		}
		apiresponse.ReturnError(c, err)
		return
	}
	apiresponse.ReturnSuccess(c, resp)
}

func (app *applications) planApplicationResourceCleanup(c *gin.Context) {
	handlePathResult(c, appIDPathParam, app.ApplicationService.PlanApplicationResourceCleanup)
}

func (app *applications) resetApplicationDatabases(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	req, ok := bindAndValidateStrictJSON[apis.DatabaseResetRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	resp, err := app.ApplicationService.ResetApplicationDatabases(c.Request.Context(), appID, *req)
	respondWithResult(c, resp, err)
}

func (app *applications) downloadLogArchive(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	req, ok := bindStrictJSON[apis.LogArchiveDownloadRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	if !validateLogArchiveDownloadRequest(c, req) {
		return
	}

	archive, err := app.ApplicationService.DownloadLogArchive(c.Request.Context(), appID, *req)
	if err != nil {
		apiresponse.ReturnError(c, err)
		return
	}
	writeComponentArchiveStream(c, archive, appID, strings.TrimSpace(req.Components[0]), "log-archive")
}

func validateLogArchiveDownloadRequest(c *gin.Context, req *apis.LogArchiveDownloadRequest) bool {
	if req == nil {
		apiresponse.ReturnError(c, bcode.ErrApplicationConfig)
		return false
	}
	if req.JobType != "" && req.JobType != config.JobLogArchiveUpload {
		apiresponse.ReturnError(c, bcode.ErrApplicationConfig)
		return false
	}
	if len(req.Components) != 1 {
		apiresponse.ReturnError(c, bcode.ErrApplicationConfig)
		return false
	}
	if strings.TrimSpace(req.Components[0]) == "" {
		apiresponse.ReturnError(c, bcode.ErrApplicationConfig)
		return false
	}
	if strings.TrimSpace(req.Path) == "" {
		apiresponse.ReturnError(c, bcode.ErrComponentFilePathInvalid)
		return false
	}
	return true
}

func (app *applications) deleteApplication(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}

	req, ok := bindJSONAllowEOF[apis.DeleteApplicationRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	if req.WaitSeconds != nil && *req.WaitSeconds < 0 {
		apiresponse.ReturnError(c, bcode.ErrApplicationConfig)
		return
	}

	resp, err := app.ApplicationService.DeleteApplicationCascade(c.Request.Context(), appID, *req)
	if err != nil {
		if resp != nil {
			klog.ErrorS(err, "delete application reported partial failures", "appID", appID)
			apiresponse.ReturnSuccess(c, resp)
			return
		}
		apiresponse.ReturnError(c, err)
		return
	}
	apiresponse.ReturnSuccess(c, resp)
}

// restartApplicationWorkloads triggers a rollout restart for app workloads.
func (app *applications) restartApplicationWorkloads(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	req, ok := bindStrictJSONAllowEOF[apis.ApplicationLifecycleRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	resp, err := app.ApplicationService.RestartApplicationWorkloads(c.Request.Context(), appID, *req)
	if err != nil {
		if resp != nil {
			klog.ErrorS(err, "restart reported partial failures", "appID", appID)
			apiresponse.ReturnSuccess(c, resp)
			return
		}
		apiresponse.ReturnError(c, err)
		return
	}
	apiresponse.ReturnSuccess(c, resp)
}

// stopApplicationDeployments scales all application Deployment components to zero replicas.
func (app *applications) stopApplicationDeployments(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	req, ok := bindStrictJSONAllowEOF[apis.ApplicationLifecycleRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	resp, err := app.ApplicationService.StopApplicationDeployments(c.Request.Context(), appID, *req)
	if err != nil {
		if resp != nil {
			klog.ErrorS(err, "stop reported partial failures", "appID", appID)
			apiresponse.ReturnSuccess(c, resp)
			return
		}
		apiresponse.ReturnError(c, err)
		return
	}
	apiresponse.ReturnSuccess(c, resp)
}

// startApplicationDeployments restores all application Deployment components to their stored replica counts.
func (app *applications) startApplicationDeployments(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}
	req, ok := bindStrictJSONAllowEOF[apis.ApplicationLifecycleRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	resp, err := app.ApplicationService.StartApplicationDeployments(c.Request.Context(), appID, *req)
	if err != nil {
		if resp != nil {
			klog.ErrorS(err, "start reported partial failures", "appID", appID)
			apiresponse.ReturnSuccess(c, resp)
			return
		}
		apiresponse.ReturnError(c, err)
		return
	}
	apiresponse.ReturnSuccess(c, resp)
}

// updateVersion 更新应用版本
func (app *applications) updateVersion(c *gin.Context) {
	appID, ok := appIDPathParam(c)
	if !ok {
		return
	}

	req, ok := bindStrictJSON[apis.UpdateVersionRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}

	for i := range req.Components {
		req.Components[i].Name = strings.ToLower(strings.TrimSpace(req.Components[i].Name))
	}

	if err := validate.Struct(req); err != nil {
		apiresponse.ReturnError(c, bcode.ErrApplicationConfig)
		return
	}

	ctx := c.Request.Context()
	klog.InfoS("update version request received", "appID", appID, "version", req.Version, "strategy", req.Strategy, "components", len(req.Components))

	resp, err := app.ApplicationService.UpdateVersion(ctx, appID, *req)
	if err != nil {
		klog.ErrorS(err, "update version failed", "appID", appID)
		apiresponse.ReturnError(c, err)
		return
	}

	klog.InfoS("update version succeeded", "appID", appID, "newVersion", resp.Version, "taskID", resp.TaskID)
	apiresponse.ReturnSuccess(c, resp)
}

// diffUpdateVersion compares a source app version snapshot with the target app
// and optionally applies the generated update.
func (app *applications) diffUpdateVersion(c *gin.Context) {
	targetAppID, ok := appIDPathParam(c)
	if !ok {
		return
	}

	req, ok := bindStrictJSON[apis.DiffUpdateVersionRequest](c, bcode.ErrApplicationConfig, true)
	if !ok {
		return
	}
	if err := validate.Struct(req); err != nil {
		apiresponse.ReturnError(c, bcode.ErrApplicationConfig)
		return
	}

	ctx := c.Request.Context()
	klog.InfoS("diff update version request received", "targetAppID", targetAppID, "sourceAppID", req.SourceAppID, "dryRun", req.DryRun, "targetOnlyStrategy", req.TargetOnlyStrategy, "strategy", req.Strategy)

	resp, err := app.ApplicationService.DiffUpdateVersion(ctx, targetAppID, *req)
	if err != nil {
		klog.ErrorS(err, "diff update version failed", "targetAppID", targetAppID, "sourceAppID", req.SourceAppID)
		apiresponse.ReturnError(c, err)
		return
	}

	klog.InfoS("diff update version succeeded", "targetAppID", targetAppID, "sourceAppID", req.SourceAppID, "targetVersion", resp.TargetVersion, "executable", resp.Executable, "dryRun", resp.DryRun)
	apiresponse.ReturnSuccess(c, resp)
}

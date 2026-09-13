package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/gin-gonic/gin"
)

type workspaceJobs struct {
	Service  *jobs.Service           `inject:""`
	Workflow service.WorkflowService `inject:""`
}

func NewWorkspaceJobs() Interface { return &workspaceJobs{} }

func (a *workspaceJobs) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/jobs", a.submit)
	group.GET("/jobs/:taskID", a.get)
	group.POST("/jobs/:taskID/cancel", a.cancel)
	group.GET("/jobs/:taskID/results", a.results)
	group.GET("/jobs/:taskID/results/:artifactID/download", a.downloadResult)
	group.GET("/jobs/:taskID/deliveries/:target/download", a.downloadDelivery)
	group.POST("/jobs/:taskID/deliveries/:target/retry", a.retry)
	group.PUT("/jobs/:taskID/retention", a.retention)
	group.GET("/job-storage-policy", a.policy)
	group.PUT("/job-storage-policy", a.setPolicy)
	group.POST("/job-datasets", a.uploadDataset)
	group.GET("/job-datasets", a.datasets)
	group.GET("/job-datasets/:datasetID", a.dataset)
	group.GET("/job-datasets/:datasetID/download", a.downloadDataset)
	// These two routes authenticate only the task-bound Runner capability and
	// current Kubernetes execution identity in the service, never user sessions.
	group.GET("/job-runners/:taskID/dataset", a.runnerDataset)
	group.POST("/job-runners/:taskID/results", a.runnerResult)
	group.POST("/job-runners/:taskID/events", a.runnerEvent)
}

func jobError(err error) error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return bcode.ErrJobTooLarge
	}
	switch {
	case errors.Is(err, artifacts.ErrInvalidInput):
		return bcode.WithSafeClientMessage(bcode.ErrJobInput, err.Error())
	case errors.Is(err, artifacts.ErrInvalidArchive):
		return bcode.WithSafeClientMessage(bcode.ErrJobInput, err.Error())
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

func jobResponse(c *gin.Context, status int, result any, err error) {
	if err != nil {
		bcode.ReturnError(c, jobError(err))
		return
	}
	bcode.ReturnResponse(c, status, bcode.SuccessCode, "", result)
}

func (a *workspaceJobs) submit(c *gin.Context) {
	request, ok := bindStrictJSON[jobs.SubmitRequest](c, bcode.ErrJobInput, true)
	if !ok {
		return
	}
	result, err := a.Service.Submit(c.Request.Context(), *request)
	jobResponse(c, http.StatusAccepted, result, err)
}
func (a *workspaceJobs) get(c *gin.Context) {
	result, err := a.Service.Get(c.Request.Context(), c.Param("taskID"))
	jobResponse(c, http.StatusOK, result, err)
}
func (a *workspaceJobs) cancel(c *gin.Context) {
	scope, err := jobs.Scope(c.Request.Context(), true)
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	if _, err = a.Service.Task(c.Request.Context(), c.Param("taskID")); err == nil {
		err = a.Workflow.CancelWorkflowTask(c.Request.Context(), scope.UserID, c.Param("taskID"), "cancelled by user")
	}
	jobResponse(c, http.StatusAccepted, nil, err)
}
func (a *workspaceJobs) results(c *gin.Context) {
	detail, err := a.Service.Get(c.Request.Context(), c.Param("taskID"))
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	jobResponse(c, http.StatusOK, gin.H{"collectionState": detail.CollectionState, "artifacts": detail.Results, "deliveries": detail.Deliveries}, nil)
}
func (a *workspaceJobs) policy(c *gin.Context) {
	result, err := a.Service.Policy(c.Request.Context())
	jobResponse(c, http.StatusOK, result, err)
}
func (a *workspaceJobs) setPolicy(c *gin.Context) {
	p, ok := bindStrictJSON[spec.JobResultPolicy](c, bcode.ErrJobInput, true)
	if !ok {
		return
	}
	jobResponse(c, http.StatusOK, nil, a.Service.SetPolicy(c.Request.Context(), *p))
}
func (a *workspaceJobs) uploadDataset(c *gin.Context) {
	scope, err := jobs.Scope(c.Request.Context(), true)
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	if strings.Split(c.GetHeader("Content-Type"), ";")[0] != "application/gzip" {
		jobResponse(c, 0, nil, bcode.ErrJobInput)
		return
	}
	result, err := a.Service.Artifacts.UploadDataset(c.Request.Context(), scope.WorkspaceID, c.Query("name"), c.Request.Body)
	jobResponse(c, http.StatusCreated, result, err)
}
func (a *workspaceJobs) datasets(c *gin.Context) {
	scope, err := jobs.Scope(c.Request.Context(), false)
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	page, size := 1, 20
	if raw := c.Query("page"); raw != "" {
		page, err = strconv.Atoi(raw)
		if err != nil {
			jobResponse(c, 0, nil, bcode.ErrJobInput)
			return
		}
	}
	if raw := c.Query("pageSize"); raw != "" {
		size, err = strconv.Atoi(raw)
		if err != nil {
			jobResponse(c, 0, nil, bcode.ErrJobInput)
			return
		}
	}
	result, err := a.Service.Artifacts.ListPage(c.Request.Context(), scope.WorkspaceID, artifacts.KindDataset, "", page, size)
	jobResponse(c, http.StatusOK, result, err)
}
func (a *workspaceJobs) dataset(c *gin.Context) {
	scope, err := jobs.Scope(c.Request.Context(), false)
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	result, err := a.Service.Artifacts.Get(c.Request.Context(), scope.WorkspaceID, c.Param("datasetID"))
	if err == nil && result.Kind != artifacts.KindDataset {
		err = bcode.ErrNotFound
	}
	jobResponse(c, http.StatusOK, result, err)
}

// Stage before sending headers so storage/auth failures remain ordinary API
// errors; bounded archives are streamed through disk rather than process memory.
func downloadArchive(c *gin.Context, copy func(io.Writer) error) {
	file, err := os.CreateTemp("", "eruun-download-*")
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err = copy(file); err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	info, err := file.Stat()
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	c.Header("Content-Disposition", `attachment; filename="archive.tar.gz"`)
	c.Header("Cache-Control", "no-store")
	c.Header("Content-Type", "application/gzip")
	http.ServeContent(c.Writer, c.Request, "archive.tar.gz", info.ModTime(), file)
}
func (a *workspaceJobs) downloadDataset(c *gin.Context) {
	scope, err := jobs.Scope(c.Request.Context(), false)
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	downloadArchive(c, func(w io.Writer) error {
		item, err := a.Service.Artifacts.Get(c.Request.Context(), scope.WorkspaceID, c.Param("datasetID"))
		if err != nil {
			return err
		}
		if item.Kind != artifacts.KindDataset {
			return bcode.ErrNotFound
		}
		return a.Service.Artifacts.Download(c.Request.Context(), scope.WorkspaceID, item.ID, w)
	})
}
func (a *workspaceJobs) downloadResult(c *gin.Context) {
	task, err := a.Service.Task(c.Request.Context(), c.Param("taskID"))
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	downloadArchive(c, func(w io.Writer) error {
		item, err := a.Service.Artifacts.Get(c.Request.Context(), task.WorkspaceID, c.Param("artifactID"))
		if err != nil {
			return err
		}
		if item.TaskID != task.TaskID {
			return bcode.ErrNotFound
		}
		return a.Service.Artifacts.Download(c.Request.Context(), task.WorkspaceID, item.ID, w)
	})
}
func (a *workspaceJobs) downloadDelivery(c *gin.Context) {
	task, err := a.Service.Task(c.Request.Context(), c.Param("taskID"))
	if err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	downloadArchive(c, func(w io.Writer) error {
		return a.Service.Artifacts.DownloadDelivery(c.Request.Context(), task.WorkspaceID, task.TaskID, c.Param("target"), w)
	})
}
func (a *workspaceJobs) retry(c *gin.Context) {
	if _, err := jobs.Scope(c.Request.Context(), true); err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	task, err := a.Service.Task(c.Request.Context(), c.Param("taskID"))
	if err == nil {
		err = a.Service.Artifacts.Retry(c.Request.Context(), task.WorkspaceID, task.TaskID, c.Param("target"))
	}
	jobResponse(c, http.StatusAccepted, nil, err)
}
func (a *workspaceJobs) retention(c *gin.Context) {
	if _, err := jobs.Scope(c.Request.Context(), true); err != nil {
		jobResponse(c, 0, nil, err)
		return
	}
	request, ok := bindStrictJSON[struct {
		RetentionDays int `json:"retentionDays"`
	}](c, bcode.ErrJobInput, true)
	if !ok {
		return
	}
	if request.RetentionDays < 1 || request.RetentionDays > 3650 {
		jobResponse(c, 0, nil, bcode.ErrJobInput)
		return
	}
	task, err := a.Service.Task(c.Request.Context(), c.Param("taskID"))
	if err == nil {
		err = a.Service.Artifacts.SetRetention(c.Request.Context(), task.WorkspaceID, task.TaskID, request.RetentionDays)
	}
	jobResponse(c, http.StatusOK, nil, err)
}
func runnerIdentity(c *gin.Context) jobs.RunnerIdentity {
	parts := strings.Fields(c.GetHeader("Authorization"))
	token := ""
	if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		token = parts[1]
	}
	return jobs.RunnerIdentity{TaskID: c.Param("taskID"), Token: token, PodName: c.GetHeader("X-Eruun-Runner-Pod-Name"), PodUID: c.GetHeader("X-Eruun-Runner-Pod-UID")}
}
func (a *workspaceJobs) runnerDataset(c *gin.Context) {
	downloadArchive(c, func(w io.Writer) error { return a.Service.RunnerDataset(c.Request.Context(), runnerIdentity(c), w) })
}
func (a *workspaceJobs) runnerResult(c *gin.Context) {
	if strings.Split(c.GetHeader("Content-Type"), ";")[0] != "application/gzip" {
		jobResponse(c, 0, nil, bcode.ErrJobInput)
		return
	}
	result, err := a.Service.RunnerResult(c.Request.Context(), runnerIdentity(c), c.Request.Body)
	jobResponse(c, http.StatusCreated, result, err)
}
func (a *workspaceJobs) runnerEvent(c *gin.Context) {
	var request jobs.RunnerEvent
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			jobResponse(c, 0, nil, bcode.ErrJobTooLarge)
		} else {
			jobResponse(c, 0, nil, bcode.ErrJobInput)
		}
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		jobResponse(c, 0, nil, bcode.ErrJobInput)
		return
	}
	result, err := a.Service.RunnerEvent(c.Request.Context(), runnerIdentity(c), request)
	jobResponse(c, http.StatusOK, result, err)
}

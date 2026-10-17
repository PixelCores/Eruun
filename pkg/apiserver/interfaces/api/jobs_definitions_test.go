package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
)

func jobDefinitionsRouter(t *testing.T) *gin.Engine {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "jobs.db")), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(&model.Workspace{}, &model.Applications{}, &model.JobDefinition{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.JobArtifact{}, &model.ArtifactChunk{}, &model.JobDelivery{}))
	raw := &sqlstore.Driver{Client: *db}
	require.NoError(t, raw.Add(context.Background(), &model.Workspace{ID: "space", Namespace: "space-ns"}))
	service, err := jobs.New(account.NewStore(raw), fake.NewSimpleClientset(), &config.Config{Accounts: &spec.AccountConfig{}})
	require.NoError(t, err)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		role := c.GetHeader("Test-Role")
		if role == "" {
			role = "member"
		}
		space := c.GetHeader("Test-Workspace")
		if space == "" {
			space = "space"
		}
		c.Request = c.Request.WithContext(account.WithScope(c.Request.Context(), account.Scope{
			WorkspaceID: space, Namespace: space + "-ns", Role: role, UserID: "creator",
		}))
		c.Next()
	})
	(&workspaceJobs{Service: service}).RegisterRoutes(router.Group("/api/v1"))
	return router
}

func TestJobDefinitionsHTTPContract(t *testing.T) {
	router := jobDefinitionsRouter(t)
	declaration := `{"name":"daily","stages":[{"name":"main","steps":[{"name":"hello","type":"command","spec":{"image":"busybox:1.37.0","command":["echo","hello"]}}]}]}`
	call := func(method, path, body string, status int) json.RawMessage {
		t.Helper()
		request := httptest.NewRequest(method, "/api/v1"+path, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		require.Equal(t, status, recorder.Code, recorder.Body.String())
		var envelope struct {
			Code int             `json:"code"`
			Data json.RawMessage `json:"data"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
		if status < 400 {
			require.Zero(t, envelope.Code)
		}
		return envelope.Data
	}

	created := call(http.MethodPost, "/job-definitions", declaration, http.StatusCreated)
	var definition jobs.Definition
	require.NoError(t, json.Unmarshal(created, &definition))
	require.Len(t, definition.ID, 24)
	require.EqualValues(t, 1, definition.Revision)
	require.Equal(t, "creator", definition.Creator)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(created, &fields))
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	require.ElementsMatch(t, []string{"id", "name", "revision", "spec", "creator", "createTime", "updateTime"}, keys)
	path := "/job-definitions/" + definition.ID
	call(http.MethodGet, path, "", http.StatusOK)
	call(http.MethodPost, "/job-definitions", declaration, http.StatusConflict)

	var definitions jobs.DefinitionPage
	require.NoError(t, json.Unmarshal(call(http.MethodGet, "/job-definitions?page=1&pageSize=1", "", http.StatusOK), &definitions))
	require.EqualValues(t, 1, definitions.Total)
	require.Len(t, definitions.Items, 1)
	require.Equal(t, definition.ID, definitions.Items[0].ID)

	future := time.Now().Add(time.Hour).Unix()
	var accepted jobs.Accepted
	require.NoError(t, json.Unmarshal(call(http.MethodPost, path+"/runs", fmt.Sprintf(`{"executeAt":%d}`, future), http.StatusAccepted), &accepted))
	require.Equal(t, "space", accepted.WorkspaceID)
	require.Equal(t, "job", accepted.Type)
	require.Equal(t, config.StatusWaiting, accepted.Status)
	changed := strings.Replace(declaration, `"daily"`, `"updated"`, 1)
	var updated jobs.Definition
	require.NoError(t, json.Unmarshal(call(http.MethodPut, path, changed, http.StatusOK), &updated))
	require.EqualValues(t, 2, updated.Revision)
	require.Equal(t, "updated", updated.Spec.Name)

	// A one-off run is omitted by definitionId filtering, and executeAt reaches
	// the same queue field as definition-triggered runs.
	inline := strings.Replace(declaration, `{"name":`, fmt.Sprintf(`{"executeAt":%d,"name":`, future), 1)
	call(http.MethodPost, "/jobs", inline, http.StatusAccepted)
	var runs jobs.RunPage
	require.NoError(t, json.Unmarshal(call(http.MethodGet, "/jobs?definitionId="+definition.ID+"&page=1&pageSize=1", "", http.StatusOK), &runs))
	require.EqualValues(t, 1, runs.Total)
	require.Len(t, runs.Items, 1)
	require.Equal(t, accepted.TaskID, runs.Items[0].TaskID)
	require.Equal(t, future, runs.Items[0].ExecuteAt)
	require.EqualValues(t, 1, runs.Items[0].JobDefinitionRevision)
	require.Equal(t, definition.ID, runs.Items[0].JobDefinitionID)
	require.Equal(t, "daily", runs.Items[0].Name)
	require.NoError(t, json.Unmarshal(call(http.MethodGet, "/jobs?page=2&pageSize=1", "", http.StatusOK), &runs))
	require.EqualValues(t, 2, runs.Total)
	require.Len(t, runs.Items, 1)

	call(http.MethodPost, path+"/runs", `{}`, http.StatusAccepted)
	call(http.MethodPost, path+"/runs", `{"executeAt":-1}`, http.StatusBadRequest)
	call(http.MethodDelete, path, "", http.StatusOK)
	call(http.MethodGet, path, "", http.StatusNotFound)
	var detail jobs.Detail
	require.NoError(t, json.Unmarshal(call(http.MethodGet, "/jobs/"+accepted.TaskID, "", http.StatusOK), &detail))
	require.Equal(t, "daily", detail.Job.Name)
	require.NoError(t, json.Unmarshal(call(http.MethodGet, "/jobs?definitionId="+definition.ID, "", http.StatusOK), &runs))
	require.EqualValues(t, 2, runs.Total, "deleted definitions retain their run history")

	for _, endpoint := range []string{"/jobs", "/job-definitions"} {
		for _, query := range []string{"?page=0", "?page=-1", "?page=no", "?pageSize=0", "?pageSize=101", "?pageSize=no"} {
			call(http.MethodGet, endpoint+query, "", http.StatusBadRequest)
		}
	}
	call(http.MethodPost, "/job-definitions", strings.Replace(declaration, `{"name":`, `{"cron":"* * * * *","name":`, 1), http.StatusBadRequest)
	call(http.MethodPost, "/job-definitions", declaration+`{}`, http.StatusBadRequest)
}

func TestJobDefinitionsHTTPAuthorization(t *testing.T) {
	router := jobDefinitionsRouter(t)
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodPost, "/job-definitions", `{}`},
		{http.MethodPut, "/job-definitions/missing", `{}`},
		{http.MethodDelete, "/job-definitions/missing", ""},
		{http.MethodPost, "/job-definitions/missing/runs", `{}`},
	} {
		request := httptest.NewRequest(tc.method, "/api/v1"+tc.path, strings.NewReader(tc.body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Test-Role", "viewer")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
	}
}

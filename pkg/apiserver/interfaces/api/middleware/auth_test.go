package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/access"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func middlewareAccounts(t *testing.T) (*account.Service, *access.Store) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "auth.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, db.AutoMigrate(&model.User{}, &model.Session{}, &model.Workspace{}, &model.WorkspaceMember{}, &model.Applications{}, &model.WorkflowQueue{}, &model.ApplicationComponent{}))
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	raw := &sqlstore.Driver{Client: *db}
	s := account.New(raw, &spec.AccountConfig{Origins: []string{"https://console.example.com"}}, client, nil)
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		u := &model.User{ID: id}
		token := strings.Repeat(id, 43)
		sum := sha256.Sum256([]byte(token))
		refresh := sha256.Sum256([]byte("refresh" + token))
		uid := u.ID
		for _, e := range []datastore.Entity{u, &model.Session{ID: id, UserID: id, AccessHash: hex.EncodeToString(sum[:]), RefreshHash: hex.EncodeToString(refresh[:]), AccessExpiresAt: time.Now().Add(time.Hour), ExpiresAt: time.Now().Add(time.Hour), AuthenticatedAt: time.Now()}, &model.Workspace{ID: "personal-" + id, OwnerID: id, PersonalUserID: &uid, Kind: "personal", Namespace: "ns-" + id}, &model.WorkspaceMember{ID: "personal-" + id, WorkspaceID: "personal-" + id, UserID: id, Role: "admin"}, &model.Workspace{ID: "team-" + id, OwnerID: id, Kind: "team", Namespace: "team-ns-" + id}, &model.WorkspaceMember{ID: "team-" + id, WorkspaceID: "team-" + id, UserID: id, Role: "admin"}, &model.Applications{ID: "app-" + id, Name: "app-" + id, WorkspaceID: "personal-" + id, Namespace: "ns-" + id}, &model.WorkflowQueue{TaskID: "task-" + id, AppID: "app-" + id}} {
			require.NoError(t, raw.Add(ctx, e))
		}
	}
	return s, access.NewStore(raw)
}

func TestAuthRouteMatrixAndImmediateMembershipChanges(t *testing.T) {
	s, _ := middlewareAccounts(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Auth(AuthOptions{Accounts: s}))
	called := 0
	for _, path := range []string{"/api/v1/health", "/api/v1/auth/methods", "/api/v1/applications", "/api/v1/applications/:appID/status", "/api/v1/applications/:appID/components/:componentName/logs", "/api/v1/workflow/tasks/:taskID/status", "/api/v1/settings", "/api/v1/new-unclassified-route"} {
		r.GET(path, func(c *gin.Context) { called++; c.Status(200) })
	}
	r.POST("/api/v1/auth/refresh", func(c *gin.Context) { called++; c.Status(200) })
	request := func(method, path, token, workspace, origin string) int {
		req := httptest.NewRequest(method, path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if workspace != "" {
			req.Header.Set("X-Eruun-Workspace-ID", workspace)
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec.Code
	}
	a := strings.Repeat("a", 43)
	for _, tc := range []struct {
		path, token, space string
		want               int
	}{{"/api/v1/health", "", "", 200}, {"/api/v1/auth/methods", "", "", 200}, {"/api/v1/applications", "", "", 401}, {"/api/v1/applications", a, "", 200}, {"/api/v1/applications/app-b/status", a, "", 403}, {"/api/v1/applications/app-b/components/web/logs", a, "", 403}, {"/api/v1/workflow/tasks/task-b/status", a, "", 403}, {"/api/v1/applications", a, "team-b", 403}, {"/api/v1/settings", a, "", 403}, {"/api/v1/new-unclassified-route", a, "", 403}} {
		before := called
		require.Equal(t, tc.want, request("GET", tc.path, tc.token, tc.space, ""), tc.path)
		if tc.want != 200 {
			require.Equal(t, before, called, "denied request reached handler")
		}
	}
	require.Equal(t, 403, request("POST", "/api/v1/auth/refresh", "", "", "https://attacker.example"))
	require.Equal(t, 403, request("POST", "/api/v1/auth/refresh", "", "", ""))
	require.Equal(t, 200, request("POST", "/api/v1/auth/refresh", "", "", "https://console.example.com"))
	ctx := context.Background()
	member := &model.WorkspaceMember{ID: "a-in-b", WorkspaceID: "team-b", UserID: "a", Role: "viewer"}
	require.NoError(t, s.Repo.Store.Add(ctx, member))
	require.Equal(t, 200, request("GET", "/api/v1/applications", a, "team-b", ""))
	require.Equal(t, 403, request("GET", "/api/v1/applications/app-b/components/web/logs", a, "team-b", ""))
	require.NoError(t, s.Repo.Store.Delete(ctx, member))
	require.Equal(t, 403, request("GET", "/api/v1/applications", a, "team-b", ""))
	user := &model.User{ID: "a"}
	require.NoError(t, s.Repo.Update(ctx, user, map[string]interface{}{"systemadmin": true}))
	require.Equal(t, 403, request("GET", "/api/v1/new-unclassified-route", a, "", ""))
	require.Equal(t, 200, request("GET", "/api/v1/settings", a, "", ""))
}

func TestScopedStoreFiltersBeforePaginationAndDeniesWrites(t *testing.T) {
	s, store := middlewareAccounts(t)
	ctx := access.WithScope(context.Background(), access.Scope{UserID: "a", WorkspaceID: "personal-a", Namespace: "ns-a", Role: "owner"})
	rows, err := store.List(ctx, &model.Applications{}, &datastore.ListOptions{Page: 1, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "app-a", rows[0].(*model.Applications).ID)
	rows, err = store.List(ctx, &model.Applications{}, &datastore.ListOptions{Page: 2, PageSize: 1})
	require.NoError(t, err)
	require.Empty(t, rows)
	rows, err = store.List(ctx, &model.Applications{}, &datastore.ListOptions{FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{{Key: "id", Values: []string{"app-a", "app-b"}}}}})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	for _, e := range []datastore.Entity{&model.Applications{ID: "app-b"}, &model.WorkflowQueue{TaskID: "task-b"}} {
		require.Error(t, store.Get(ctx, e))
		require.Error(t, store.Delete(ctx, e))
	}
	require.Error(t, store.Add(ctx, &model.ApplicationComponent{Name: "bad", AppID: "app-b", Namespace: "ns-b"}))
	require.Error(t, store.Add(ctx, &model.ApplicationComponent{Name: "bad", AppID: "app-a", Namespace: "ns-b"}))
	_, err = store.CompareAndSwap(ctx, &model.Applications{ID: "app-a"}, "id", "app-a", map[string]interface{}{"workspaceid": "personal-b"})
	require.ErrorIs(t, err, bcode.ErrForbidden)
	app := &model.Applications{ID: "app-a"}
	require.NoError(t, s.Repo.Store.Get(context.Background(), app))
	require.Equal(t, "personal-a", app.WorkspaceID)
	n, err := s.Repo.Store.Count(context.Background(), &model.Applications{}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
	n, err = s.Repo.Store.Count(context.Background(), &model.ApplicationComponent{}, nil)
	require.NoError(t, err)
	require.Zero(t, n)
	rows, err = store.List(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "task-a", rows[0].(*model.WorkflowQueue).TaskID)
}

func TestAuthFailsClosedWithoutDependencies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Auth(AuthOptions{}))
	r.GET("/api/v1/applications", func(*gin.Context) { t.Fatal("handler reached") })
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/applications", nil))
	require.Equal(t, 503, rec.Code)
}

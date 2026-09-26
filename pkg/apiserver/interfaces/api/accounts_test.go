package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/middleware"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAccountSessionCookieAndResponseSecrets(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	accountLoginResult(c, &account.Login{AccessToken: "test-access", RefreshToken: "test-refresh", User: &model.User{ID: "user", PasswordHash: "test-hash"}}, nil)
	cookies := w.Result().Cookies()
	require.Len(t, cookies, 1)
	entry := cookies[0]
	require.Equal(t, refreshCookie, entry.Name)
	require.Equal(t, "/api/v1/auth", entry.Path)
	require.True(t, entry.Secure)
	require.True(t, entry.HttpOnly)
	require.Equal(t, http.SameSiteLaxMode, entry.SameSite)
	require.Empty(t, entry.Domain)
	require.NotContains(t, w.Body.String(), "test-refresh")
	require.NotContains(t, w.Body.String(), "test-hash")
	require.Contains(t, w.Body.String(), "test-access")
}

func TestAccountInputRejectsUnknownAndTrailingJSON(t *testing.T) {
	for _, raw := range []string{`{"password":"test-password","systemAdmin":true}`, `{"password":"test-password"}{}`, `null {}`} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/auth/password", strings.NewReader(raw))
		_, ok := bindAccount[struct {
			Password string `json:"password"`
		}](c)
		require.False(t, ok)
		require.Equal(t, http.StatusBadRequest, w.Code)
	}
}

// Embed the unused store methods so this fixture only models the query boundary.
type adminUserListStore struct {
	datastore.DataStore
	options *datastore.ListOptions
	rows    []datastore.Entity
}

func (s *adminUserListStore) List(_ context.Context, _ datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	s.options = options
	return s.rows, nil
}

func TestAdminUsersHTTPPaginationAndEnvelope(t *testing.T) {
	for _, tc := range []struct {
		query              string
		status, page, size int
	}{
		{"", 200, 1, 20}, {"?page=2&pageSize=100", 200, 2, 100},
		{"?page=no", 400, 0, 0}, {"?page=0", 400, 0, 0}, {"?pageSize=101", 400, 0, 0},
	} {
		t.Run(tc.query, func(t *testing.T) {
			store := &adminUserListStore{rows: []datastore.Entity{&model.User{ID: "user", PasswordHash: "test-hash"}}}
			handler := &accounts{Accounts: &account.Service{Repo: repository.Accounts{Store: store}}}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/users"+tc.query, nil)
			c.Set(middleware.AuthPrincipalContextKey, &account.Principal{User: &model.User{SystemAdmin: true}})
			handler.users(c)
			require.Equal(t, tc.status, recorder.Code)
			if tc.status != 200 {
				require.Nil(t, store.options)
				return
			}
			require.Equal(t, tc.page, store.options.Page)
			require.Equal(t, tc.size, store.options.PageSize)
			var envelope struct {
				Data []model.User `json:"data"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
			require.Len(t, envelope.Data, 1)
			require.Equal(t, "user", envelope.Data[0].ID)
			require.NotContains(t, recorder.Body.String(), "test-hash")
		})
	}
}

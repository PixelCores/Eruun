package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
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

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

type mockOAuthSettingRepo struct {
	items map[string]*model.SystemSetting
}

func newMockOAuthSettingRepo() *mockOAuthSettingRepo {
	return &mockOAuthSettingRepo{items: make(map[string]*model.SystemSetting)}
}

func (m *mockOAuthSettingRepo) FindByType(_ context.Context, settingType string) (*model.SystemSetting, error) {
	item, ok := m.items[settingType]
	if !ok {
		return nil, datastore.ErrRecordNotExist
	}
	return item, nil
}

func (m *mockOAuthSettingRepo) Create(context.Context, *model.SystemSetting) error { return nil }
func (m *mockOAuthSettingRepo) Update(context.Context, *model.SystemSetting) error { return nil }
func (m *mockOAuthSettingRepo) Delete(context.Context, *model.SystemSetting) error { return nil }
func (m *mockOAuthSettingRepo) List(context.Context, datastore.ListOptions) ([]*model.SystemSetting, error) {
	return nil, nil
}

func TestOAuthAPI_GoogleLoginAndCallback(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			require.NoError(t, r.ParseForm())
			require.Equal(t, "demo-code", r.Form.Get("code"))
			require.NotEmpty(t, r.Form.Get("code_verifier"))
			_, _ = w.Write([]byte(`{"access_token":"google-access-token","token_type":"Bearer","expires_in":3599}`))
		case "/userinfo":
			require.Equal(t, "Bearer google-access-token", r.Header.Get("Authorization"))
			_, _ = w.Write([]byte(`{"sub":"google-user-1","email":"owner@example.com","hd":"example.com"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer oauthServer.Close()

	repo := newMockOAuthSettingRepo()
	repo.items[model.SystemSettingTypeOAuthAuth] = &model.SystemSetting{
		Type:  model.SystemSettingTypeOAuthAuth,
		Value: mustMarshalOAuthSettingValue(t, oauthServer.URL+"/auth", oauthServer.URL+"/token", oauthServer.URL+"/userinfo"),
	}
	repo.items[model.SystemSettingTypeAPIAuth] = &model.SystemSetting{
		Type: model.SystemSettingTypeAPIAuth,
		Value: json.RawMessage(`{
			"enabled": true,
			"jwt": {"algorithms": ["HS256"], "hs256": {"secret": "test-secret"}},
			"authorization": {"defaultEffect":"deny","routes":[{"method":"GET","path":"/api/v1/applications","roles":["reader"]}]}
		}`),
	}

	h := &oauth{
		SettingRepo: repo,
		Cache:       cache.NewMemCache(false),
	}
	r := gin.New()
	r.GET("/auth/oauth2/google/login", h.googleLogin)
	r.GET("/auth/oauth2/google/callback", h.googleCallback)

	loginReq := httptest.NewRequest(http.MethodGet, "/auth/oauth2/google/login", nil)
	loginResp := httptest.NewRecorder()
	r.ServeHTTP(loginResp, loginReq)
	require.Equal(t, http.StatusFound, loginResp.Code)

	location := strings.TrimSpace(loginResp.Header().Get("Location"))
	require.NotEmpty(t, location)
	parsed, err := url.Parse(location)
	require.NoError(t, err)
	state := parsed.Query().Get("state")
	require.NotEmpty(t, state)

	callbackReq := httptest.NewRequest(http.MethodGet, "/auth/oauth2/google/callback?code=demo-code&state="+url.QueryEscape(state), nil)
	callbackResp := httptest.NewRecorder()
	r.ServeHTTP(callbackResp, callbackReq)
	require.Equal(t, http.StatusOK, callbackResp.Code)
	require.Contains(t, callbackResp.Header().Get("Cache-Control"), "no-store")
	require.Equal(t, "no-cache", callbackResp.Header().Get("Pragma"))
	require.Equal(t, "0", callbackResp.Header().Get("Expires"))

	var payload apis.OAuthLoginResponse
	requireSuccessResponse(t, callbackResp.Body.Bytes(), &payload)
	require.Equal(t, "Bearer", payload.TokenType)
	require.Equal(t, "google-user-1", payload.Subject)
	require.Contains(t, payload.Roles, "admin")
	require.NotEmpty(t, payload.AccessToken)
}

func TestOAuthAPI_GoogleCallbackInvalidState(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := newMockOAuthSettingRepo()
	repo.items[model.SystemSettingTypeOAuthAuth] = &model.SystemSetting{
		Type:  model.SystemSettingTypeOAuthAuth,
		Value: mustMarshalOAuthSettingValue(t, "https://accounts.google.com/o/oauth2/v2/auth", "https://oauth2.googleapis.com/token", "https://openidconnect.googleapis.com/v1/userinfo"),
	}
	repo.items[model.SystemSettingTypeAPIAuth] = &model.SystemSetting{
		Type:  model.SystemSettingTypeAPIAuth,
		Value: json.RawMessage(`{"enabled": true, "jwt": {"algorithms": ["HS256"], "hs256": {"secret": "test-secret"}}, "authorization": {"defaultEffect":"deny","routes":[{"method":"GET","path":"/api/v1/applications","roles":["reader"]}]}}`),
	}

	h := &oauth{
		SettingRepo: repo,
		Cache:       cache.NewMemCache(false),
	}
	r := gin.New()
	r.GET("/auth/oauth2/google/callback", h.googleCallback)

	req := httptest.NewRequest(http.MethodGet, "/auth/oauth2/google/callback?code=demo-code&state=missing", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	require.Equal(t, http.StatusUnauthorized, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrOAuthStateInvalid.BusinessCode, envelope.Code)
}

func mustMarshalOAuthSettingValue(t *testing.T, authURL, tokenURL, userInfoURL string) json.RawMessage {
	t.Helper()

	setting := spec.OAuthAuthSettingSpec{
		Enabled: true,
		Providers: spec.OAuthProvidersSpec{
			Google: spec.OAuthGoogleProviderSpec{
				ClientID:     "google-client-id",
				ClientSecret: "google-client-secret",
				RedirectURI:  "https://eruun.example.com/api/v1/auth/oauth2/google/callback",
				Scopes:       []string{"openid", "email", "profile"},
				AuthURL:      authURL,
				TokenURL:     tokenURL,
				UserInfoURL:  userInfoURL,
			},
		},
		JWTIssue: spec.OAuthJWTIssueSpec{
			Issuer:     "eruun",
			Audience:   "eruun-api",
			TTLSeconds: 1800,
		},
		RoleMapping: spec.OAuthRoleMappingSpec{
			DefaultRoles: []string{"reader"},
			GoogleEmailToRoles: map[string][]string{
				"owner@example.com": []string{"admin"},
			},
		},
	}
	raw, err := json.Marshal(setting)
	require.NoError(t, err)
	return json.RawMessage(raw)
}

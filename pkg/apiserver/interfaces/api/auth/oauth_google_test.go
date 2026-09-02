package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

type fakeOAuthSystemSettingRepo struct {
	items map[string]*model.SystemSetting
}

type consumeErrorCache struct {
	cache.ICache
}

func (consumeErrorCache) Consume(string) (string, error) {
	return "", fmt.Errorf("consume unavailable")
}

func newFakeOAuthSystemSettingRepo() *fakeOAuthSystemSettingRepo {
	return &fakeOAuthSystemSettingRepo{items: make(map[string]*model.SystemSetting)}
}

func (f *fakeOAuthSystemSettingRepo) FindByType(_ context.Context, settingType string) (*model.SystemSetting, error) {
	item, ok := f.items[settingType]
	if !ok {
		return nil, datastore.ErrRecordNotExist
	}
	return item, nil
}

func (f *fakeOAuthSystemSettingRepo) Create(context.Context, *model.SystemSetting) error {
	return nil
}

func (f *fakeOAuthSystemSettingRepo) Update(context.Context, *model.SystemSetting) error {
	return nil
}

func (f *fakeOAuthSystemSettingRepo) Delete(context.Context, *model.SystemSetting) error {
	return nil
}

func (f *fakeOAuthSystemSettingRepo) List(context.Context, datastore.ListOptions) ([]*model.SystemSetting, error) {
	return nil, nil
}

func TestOAuthGoogleLoginFlow_BuildLoginURL(t *testing.T) {
	repo := newFakeOAuthSystemSettingRepo()
	repo.items[model.SystemSettingTypeOAuthAuth] = &model.SystemSetting{
		Type:  model.SystemSettingTypeOAuthAuth,
		Value: mustMarshalOAuthSettingJSON(t, "https://accounts.google.com/o/oauth2/v2/auth", "https://oauth2.googleapis.com/token", "https://openidconnect.googleapis.com/v1/userinfo"),
	}

	flow := NewOAuthGoogleLoginFlow(repo, cache.NewMemCache(false), nil)
	loginURL, err := flow.BuildLoginURL(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, loginURL)

	parsed, err := url.Parse(loginURL)
	require.NoError(t, err)
	query := parsed.Query()
	require.Equal(t, "code", query.Get("response_type"))
	require.Equal(t, "google-client-id", query.Get("client_id"))
	require.Equal(t, "S256", query.Get("code_challenge_method"))
	require.NotEmpty(t, query.Get("state"))
	require.NotEmpty(t, query.Get("code_challenge"))
}

func TestOAuthGoogleLoginFlow_HandleCallbackSuccess(t *testing.T) {
	var tokenExchangeCalls int
	var userInfoCalls int

	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			tokenExchangeCalls++
			require.Equal(t, http.MethodPost, r.Method)
			require.NoError(t, r.ParseForm())
			require.Equal(t, "authorization_code", r.Form.Get("grant_type"))
			require.Equal(t, "demo-code", r.Form.Get("code"))
			require.NotEmpty(t, r.Form.Get("code_verifier"))
			_, _ = w.Write([]byte(`{"access_token":"google-access-token","token_type":"Bearer","expires_in":3599}`))
		case "/userinfo":
			userInfoCalls++
			require.Equal(t, "Bearer google-access-token", r.Header.Get("Authorization"))
			_, _ = w.Write([]byte(`{"sub":"google-user-1","email":"owner@example.com","hd":"example.com"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer oauthServer.Close()

	repo := newFakeOAuthSystemSettingRepo()
	repo.items[model.SystemSettingTypeOAuthAuth] = &model.SystemSetting{
		Type: model.SystemSettingTypeOAuthAuth,
		Value: mustMarshalOAuthSettingJSON(
			t,
			oauthServer.URL+"/auth",
			oauthServer.URL+"/token",
			oauthServer.URL+"/userinfo",
		),
	}
	repo.items[model.SystemSettingTypeAPIAuth] = &model.SystemSetting{
		Type: model.SystemSettingTypeAPIAuth,
		Value: json.RawMessage(`{
			"enabled": true,
			"jwt": {
				"algorithms": ["HS256"],
				"issuers": ["eruun"],
				"audience": ["eruun-api"],
				"hs256": {"secret": "test-secret"}
			},
			"authorization": {
				"defaultEffect":"deny",
				"routes":[{"method":"GET","path":"/api/v1/applications","roles":["reader"]}]
			}
		}`),
	}

	flow := NewOAuthGoogleLoginFlow(repo, cache.NewMemCache(false), oauthServer.Client())
	loginURL, err := flow.BuildLoginURL(context.Background())
	require.NoError(t, err)

	state := mustExtractQueryValue(t, loginURL, "state")
	callbackResp, err := flow.HandleCallback(context.Background(), "demo-code", state)
	require.NoError(t, err)
	require.Equal(t, "Bearer", callbackResp.TokenType)
	require.Equal(t, "google-user-1", callbackResp.Subject)
	require.Equal(t, "owner@example.com", callbackResp.Email)
	require.Equal(t, []string{"admin"}, callbackResp.Roles)
	require.NotEmpty(t, callbackResp.AccessToken)
	require.Greater(t, callbackResp.ExpiresIn, int64(0))

	require.Equal(t, 1, tokenExchangeCalls)
	require.Equal(t, 1, userInfoCalls)

	authenticator := NewJWTAuthenticator(func() time.Time { return time.Now() })
	setting := &spec.APIAuthSettingSpec{
		Enabled: true,
		JWT: spec.APIAuthJWTSpec{
			Algorithms: []string{spec.APIAuthAlgorithmHS256},
			Issuers:    []string{"eruun"},
			Audience:   []string{"eruun-api"},
			HS256: spec.APIAuthHS256Spec{
				Secret: "test-secret",
			},
		},
	}
	principal, err := authenticator.Authenticate(context.Background(), callbackResp.AccessToken, setting)
	require.NoError(t, err)
	require.Equal(t, "google-user-1", principal.Subject)
	require.Contains(t, principal.Roles, "admin")
}

func TestOAuthGoogleLoginFlow_HandleCallbackInvalidState(t *testing.T) {
	repo := newFakeOAuthSystemSettingRepo()
	repo.items[model.SystemSettingTypeOAuthAuth] = &model.SystemSetting{
		Type:  model.SystemSettingTypeOAuthAuth,
		Value: mustMarshalOAuthSettingJSON(t, "https://accounts.google.com/o/oauth2/v2/auth", "https://oauth2.googleapis.com/token", "https://openidconnect.googleapis.com/v1/userinfo"),
	}
	repo.items[model.SystemSettingTypeAPIAuth] = &model.SystemSetting{
		Type:  model.SystemSettingTypeAPIAuth,
		Value: json.RawMessage(validAPIAuthSettingJSONForOAuthTest()),
	}

	flow := NewOAuthGoogleLoginFlow(repo, cache.NewMemCache(false), nil)
	_, err := flow.HandleCallback(context.Background(), "demo-code", "missing-state")
	require.ErrorIs(t, err, ErrOAuthStateInvalid)
}

func TestOAuthGoogleLoginFlow_StateCanOnlyBeConsumedOnceConcurrently(t *testing.T) {
	c := cache.NewMemCache(false)
	flow := NewOAuthGoogleLoginFlow(newFakeOAuthSystemSettingRepo(), c, nil)
	state := "concurrent-state"
	payload, err := json.Marshal(oauthStatePayload{CodeVerifier: "verifier", ExpiresAt: time.Now().Add(time.Minute).Unix()})
	require.NoError(t, err)
	require.NoError(t, c.Store(oauthGoogleStateKey(state), string(payload)))

	type result struct {
		verifier string
		err      error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			verifier, consumeErr := flow.loadAndConsumeState(state)
			results <- result{verifier: verifier, err: consumeErr}
		}()
	}
	wg.Wait()
	close(results)

	var successes, rejected int
	for result := range results {
		switch {
		case result.err == nil:
			successes++
			require.Equal(t, "verifier", result.verifier)
		case errors.Is(result.err, ErrOAuthStateInvalid):
			rejected++
		default:
			require.NoError(t, result.err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, rejected)
}

func TestOAuthGoogleLoginFlow_StateConsumeFailureIsRejected(t *testing.T) {
	flow := NewOAuthGoogleLoginFlow(newFakeOAuthSystemSettingRepo(), consumeErrorCache{ICache: cache.NewMemCache(false)}, nil)

	_, err := flow.loadAndConsumeState("state")

	require.ErrorIs(t, err, ErrOAuthStateInvalid)
}

func TestOAuthGoogleLoginFlow_HandleCallbackMissingAPIAuthSecret(t *testing.T) {
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			require.NoError(t, r.ParseForm())
			_, _ = w.Write([]byte(`{"access_token":"google-access-token","token_type":"Bearer","expires_in":3599}`))
		case "/userinfo":
			_, _ = w.Write([]byte(`{"sub":"google-user-1","email":"owner@example.com","hd":"example.com"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer oauthServer.Close()

	repo := newFakeOAuthSystemSettingRepo()
	repo.items[model.SystemSettingTypeOAuthAuth] = &model.SystemSetting{
		Type: model.SystemSettingTypeOAuthAuth,
		Value: mustMarshalOAuthSettingJSON(
			t,
			oauthServer.URL+"/auth",
			oauthServer.URL+"/token",
			oauthServer.URL+"/userinfo",
		),
	}

	flow := NewOAuthGoogleLoginFlow(repo, cache.NewMemCache(false), oauthServer.Client())
	loginURL, err := flow.BuildLoginURL(context.Background())
	require.NoError(t, err)
	state := mustExtractQueryValue(t, loginURL, "state")

	_, err = flow.HandleCallback(context.Background(), "demo-code", state)
	require.ErrorIs(t, err, ErrOAuthTokenIssueFailed)
}

func mustExtractQueryValue(t *testing.T, rawURL, key string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	value := strings.TrimSpace(parsed.Query().Get(key))
	require.NotEmpty(t, value)
	return value
}

func mustMarshalOAuthSettingJSON(t *testing.T, authURL, tokenURL, userInfoURL string) json.RawMessage {
	t.Helper()
	specObj := spec.OAuthAuthSettingSpec{
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
			GoogleHostedDomainToRoles: map[string][]string{
				"example.com": []string{"reader"},
			},
			GoogleEmailToRoles: map[string][]string{
				"owner@example.com": []string{"admin"},
			},
		},
		Security: spec.OAuthSecuritySpec{
			StateTTLSeconds: 300,
		},
	}
	raw, err := json.Marshal(specObj)
	require.NoError(t, err)
	return json.RawMessage(raw)
}

func validAPIAuthSettingJSONForOAuthTest() string {
	return fmt.Sprintf(`{
		"enabled": true,
		"jwt": {
			"algorithms": ["%s"],
			"hs256": {"secret": "test-secret"}
		},
		"authorization": {
			"defaultEffect": "deny",
			"routes": [{"method":"GET","path":"/api/v1/applications","roles":["reader"]}]
		}
	}`, spec.APIAuthAlgorithmHS256)
}

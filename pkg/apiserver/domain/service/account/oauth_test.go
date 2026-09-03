package account

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type oauthUpstream struct {
	claims          map[string]interface{}
	key             *rsa.PrivateKey
	client          *http.Client
	verifier, email string
	verified        bool
	githubID        int64
	tokenFailure    bool
	keysFailure     bool
}

func testOAuthUpstream(t *testing.T, s *Service) *oauthUpstream {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	u := &oauthUpstream{key: key, email: "oauth@example.com", verified: true, githubID: 1001}
	u.claims = map[string]interface{}{"iss": "https://accounts.google.com", "aud": "test-client", "sub": "google-subject", "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix(), "nonce": "nonce", "email": u.email, "email_verified": true, "name": "OAuth user"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/oauth2/v3/certs":
			if u.keysFailure {
				w.WriteHeader(503)
				return
			}
			_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "test-key", Algorithm: "RS256", Use: "sig"}}})
		case "/token", "/login/oauth/access_token":
			if u.tokenFailure {
				w.WriteHeader(400)
				_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"sensitive upstream body"}`))
				return
			}
			_ = r.ParseForm()
			u.verifier = r.Form.Get("code_verifier")
			signer, e := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: u.key, KeyID: "test-key"}}, nil)
			require.NoError(t, e)
			payload, e := json.Marshal(u.claims)
			require.NoError(t, e)
			signed, e := signer.Sign(payload)
			require.NoError(t, e)
			compact, e := signed.CompactSerialize()
			require.NoError(t, e)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "upstream-token", "token_type": "Bearer", "expires_in": 3600, "id_token": compact})
		case "/user":
			require.Equal(t, "Bearer upstream-token", r.Header.Get("Authorization"))
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": u.githubID, "login": "renameable-login", "name": "GitHub user"})
		case "/user/emails":
			_ = json.NewEncoder(w).Encode([]map[string]interface{}{{"email": u.email, "verified": u.verified, "primary": true}})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(server.Close)
	target, _ := url.Parse(server.URL)
	u.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		c := r.Clone(r.Context())
		v := *r.URL
		v.Scheme = target.Scheme
		v.Host = target.Host
		c.URL = &v
		return http.DefaultTransport.RoundTrip(c)
	})}
	s.HTTPClient = u.client
	s.Config.Google = spec.OAuthProviderConfig{Enabled: true, ClientID: "test-client", ClientSecret: "test-secret", RedirectURI: "https://console.example.com/oauth/google"}
	s.Config.GitHub = spec.OAuthProviderConfig{Enabled: true, ClientID: "test-client", ClientSecret: "test-secret", RedirectURI: "https://console.example.com/oauth/github"}
	return u
}

func TestGoogleKeyServerFailureIsUpstreamError(t *testing.T) {
	s, _, _ := testAccounts(t)
	u := testOAuthUpstream(t, s)
	u.keysFailure = true
	state, browser, _ := startOAuth(t, s, u, "google", nil)
	_, err := s.OAuthCallback(context.Background(), "google", "code", state, browser, nil)
	require.ErrorIs(t, err, bcode.ErrUpstreamNotFound)
}
func startOAuth(t *testing.T, s *Service, u *oauthUpstream, provider string, p *Principal) (string, string, string) {
	t.Helper()
	address, browser, err := s.OAuthStart(context.Background(), provider, p)
	require.NoError(t, err)
	parsed, err := url.Parse(address)
	require.NoError(t, err)
	require.Equal(t, "S256", parsed.Query().Get("code_challenge_method"))
	u.claims["nonce"] = parsed.Query().Get("nonce")
	return parsed.Query().Get("state"), browser, parsed.Query().Get("code_challenge")
}

func TestOAuthPKCEStateBrowserAndStableIdentity(t *testing.T) {
	for _, provider := range []string{"google", "github"} {
		t.Run(provider, func(t *testing.T) {
			s, _, _ := testAccounts(t)
			u := testOAuthUpstream(t, s)
			ctx := context.Background()
			state, browser, challenge := startOAuth(t, s, u, provider, nil)
			l, err := s.OAuthCallback(ctx, provider, "code", state, browser, nil)
			require.NoError(t, err)
			sum := sha256.Sum256([]byte(u.verifier))
			require.Equal(t, challenge, base64.RawURLEncoding.EncodeToString(sum[:]))
			_, err = s.OAuthCallback(ctx, provider, "code", state, browser, nil)
			require.ErrorIs(t, err, bcode.ErrUnauthorized)
			state, browser, _ = startOAuth(t, s, u, provider, nil)
			_, err = s.OAuthCallback(ctx, provider, "code", state, strings.Repeat("x", 43), nil)
			require.ErrorIs(t, err, bcode.ErrUnauthorized)
			_, err = s.OAuthCallback(ctx, provider, "code", state, browser, nil)
			require.ErrorIs(t, err, bcode.ErrUnauthorized)
			state, browser, _ = startOAuth(t, s, u, provider, nil)
			_, err = s.OAuthCallback(ctx, provider, "", state, browser, nil)
			require.ErrorIs(t, err, bcode.ErrUnauthorized)
			_, err = s.OAuthCallback(ctx, provider, "code", state, browser, nil)
			require.ErrorIs(t, err, bcode.ErrUnauthorized)
			u.email = "changed@example.com"
			u.claims["email"] = u.email
			state, browser, _ = startOAuth(t, s, u, provider, nil)
			again, err := s.OAuthCallback(ctx, provider, "code", state, browser, nil)
			require.NoError(t, err)
			require.Equal(t, l.User.ID, again.User.ID)
		})
	}
}

func TestGoogleRejectsInvalidClaimsAndSignature(t *testing.T) {
	for _, field := range []string{"iss", "aud", "nonce", "exp", "signature"} {
		t.Run(field, func(t *testing.T) {
			s, _, _ := testAccounts(t)
			u := testOAuthUpstream(t, s)
			state, browser, _ := startOAuth(t, s, u, "google", nil)
			switch field {
			case "exp":
				u.claims[field] = time.Now().Add(-time.Hour).Unix()
			case "signature":
				key, err := rsa.GenerateKey(rand.Reader, 2048)
				require.NoError(t, err)
				u.key = key
			default:
				u.claims[field] = "wrong"
			}
			_, err := s.OAuthCallback(context.Background(), "google", "code", state, browser, nil)
			require.ErrorIs(t, err, bcode.ErrUnauthorized)
			n, err := s.Repo.Store.Count(context.Background(), &model.User{}, nil)
			require.NoError(t, err)
			require.Zero(t, n)
		})
	}
}

func TestOAuthEmailCollisionRequiresExplicitLink(t *testing.T) {
	for _, provider := range []string{"google", "github"} {
		t.Run(provider, func(t *testing.T) {
			s, _, d := testAccounts(t)
			_, p := registerTestUser(t, s, d, "email", "oauth@example.com")
			u := testOAuthUpstream(t, s)
			ctx := context.Background()
			state, browser, _ := startOAuth(t, s, u, provider, nil)
			_, err := s.OAuthCallback(ctx, provider, "code", state, browser, nil)
			require.ErrorIs(t, err, bcode.ErrAccountLinkRequired)
			state, browser, _ = startOAuth(t, s, u, provider, p)
			l, err := s.OAuthCallback(ctx, provider, "code", state, browser, p)
			require.NoError(t, err)
			require.Nil(t, l)
			state, browser, _ = startOAuth(t, s, u, provider, nil)
			l, err = s.OAuthCallback(ctx, provider, "code", state, browser, nil)
			require.NoError(t, err)
			require.Equal(t, p.User.ID, l.User.ID)
			_, other := registerTestUser(t, s, d, "email", "other@example.com")
			state, browser, _ = startOAuth(t, s, u, provider, other)
			_, err = s.OAuthCallback(ctx, provider, "code", state, browser, other)
			require.ErrorIs(t, err, bcode.ErrAccountConflict)
		})
	}
}

func TestOAuthUnverifiedEmailAndSanitizedFailure(t *testing.T) {
	for _, provider := range []string{"google", "github"} {
		t.Run(provider, func(t *testing.T) {
			s, _, _ := testAccounts(t)
			u := testOAuthUpstream(t, s)
			u.verified = false
			u.claims["email_verified"] = false
			ctx := context.Background()
			state, browser, _ := startOAuth(t, s, u, provider, nil)
			l, err := s.OAuthCallback(ctx, provider, "code", state, browser, nil)
			require.NoError(t, err)
			rows, err := s.Repo.Store.List(ctx, &model.Identity{UserID: l.User.ID}, nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, provider, rows[0].(*model.Identity).Provider)
			p, err := s.Authenticate(ctx, l.AccessToken)
			require.NoError(t, err)
			require.ErrorIs(t, s.Unbind(ctx, p, rows[0].(*model.Identity).ID), bcode.ErrAccountConflict)
			u.tokenFailure = true
			state, browser, _ = startOAuth(t, s, u, provider, nil)
			_, err = s.OAuthCallback(ctx, provider, "code", state, browser, nil)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "sensitive")
		})
	}
}

func TestGoogleMissingIDToken(t *testing.T) {
	s, _, _ := testAccounts(t)
	_, _, _, err := s.upstreamIdentity(context.Background(), "google", "client", &oauth2.Token{}, "nonce")
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
}

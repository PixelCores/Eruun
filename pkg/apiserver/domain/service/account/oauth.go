package account

import (
	"context"
	"crypto"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/github"
)

type oauthState struct {
	Provider    string `json:"provider"`
	BrowserHash string `json:"browserHash"`
	Verifier    string `json:"verifier"`
	Nonce       string `json:"nonce"`
	UserID      string `json:"userId,omitempty"`
	SessionID   string `json:"sessionId,omitempty"`
}

func (s *Service) oauthConfig(provider string) (*oauth2.Config, error) {
	var p spec.OAuthProviderConfig
	var endpoint oauth2.Endpoint
	var scopes []string
	switch provider {
	case "google":
		p = s.Config.Google
		endpoint = oauth2.Endpoint{AuthURL: "https://accounts.google.com/o/oauth2/v2/auth", TokenURL: "https://oauth2.googleapis.com/token", AuthStyle: oauth2.AuthStyleInParams}
		scopes = []string{"openid", "email", "profile"}
	case "github":
		p = s.Config.GitHub
		endpoint = github.Endpoint
		scopes = []string{"read:user", "user:email"}
	default:
		return nil, bcode.ErrAccountInput
	}
	if !p.Enabled {
		return nil, bcode.ErrServiceUnavailable
	}
	return &oauth2.Config{ClientID: p.ClientID, ClientSecret: p.ClientSecret, RedirectURL: p.RedirectURI, Endpoint: endpoint, Scopes: scopes}, nil
}

// OAuthStart returns the URL and a browser-only binding credential. The latter
// is delivered in an HttpOnly cookie by the handler, never in the response JSON.
func (s *Service) OAuthStart(ctx context.Context, provider string, p *Principal) (string, string, error) {
	cfg, err := s.oauthConfig(provider)
	if err != nil {
		return "", "", err
	}
	if p != nil {
		if err := s.requireRecentAuthentication(ctx, p); err != nil {
			return "", "", err
		}
	}
	state, err := randomToken()
	if err != nil {
		return "", "", err
	}
	browser, err := randomToken()
	if err != nil {
		return "", "", err
	}
	nonce, err := randomToken()
	if err != nil {
		return "", "", err
	}
	verifier, err := randomToken()
	if err != nil {
		return "", "", err
	}
	payload := oauthState{Provider: provider, BrowserHash: tokenHash(browser), Verifier: verifier, Nonce: nonce}
	if p != nil {
		payload.UserID = p.User.ID
		payload.SessionID = p.Session.ID
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", "", err
	}
	if err = s.Redis.Set(ctx, "eruun:auth:oauth:"+tokenHash(state), raw, 5*time.Minute).Err(); err != nil {
		return "", "", fmt.Errorf("store OAuth state: %w", err)
	}
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	return cfg.AuthCodeURL(state, oauth2.SetAuthURLParam("code_challenge", challenge), oauth2.SetAuthURLParam("code_challenge_method", "S256"), oauth2.SetAuthURLParam("nonce", nonce)), browser, nil
}

func (s *Service) OAuthCallback(ctx context.Context, provider, code, state, browser string, p *Principal) (*Login, error) {
	if len(state) != 43 || len(browser) != 43 {
		return nil, bcode.ErrUnauthorized
	}
	cfg, err := s.oauthConfig(provider)
	if err != nil {
		return nil, err
	}
	raw, err := s.Redis.GetDel(ctx, "eruun:auth:oauth:"+tokenHash(state)).Bytes()
	if err != nil {
		return nil, bcode.ErrUnauthorized
	}
	var flow oauthState
	if json.Unmarshal(raw, &flow) != nil || flow.Provider != provider || flow.BrowserHash != tokenHash(browser) {
		return nil, bcode.ErrUnauthorized
	}
	if code == "" {
		return nil, bcode.ErrUnauthorized
	}
	if flow.UserID != "" {
		if p == nil || p.User.ID != flow.UserID || p.Session.ID != flow.SessionID {
			return nil, bcode.ErrAccountRecentAuth
		}
		if err := s.requireRecentAuthentication(ctx, p); err != nil {
			return nil, err
		}
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, s.HTTPClient)
	token, err := cfg.Exchange(ctx, code, oauth2.VerifierOption(flow.Verifier))
	if err != nil {
		var response *oauth2.RetrieveError
		if errors.As(err, &response) && response.Response != nil && response.Response.StatusCode < 500 {
			return nil, bcode.ErrUnauthorized
		}
		return nil, bcode.ErrUpstreamNotFound
	}
	subject, email, name, err := s.upstreamIdentity(ctx, provider, cfg.ClientID, token, flow.Nonce)
	if err != nil {
		return nil, err
	}
	var result *Login
	err = s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		identity := &model.Identity{Provider: provider, Subject: subject}
		lookup := r.One(ctx, identity)
		if lookup != nil && !errors.Is(lookup, datastore.ErrRecordNotExist) {
			return lookup
		}
		if flow.UserID != "" {
			u := &model.User{ID: flow.UserID}
			if e := r.Lock(ctx, u); e != nil {
				return e
			}
			if u.Disabled || u.SecurityVersion != p.User.SecurityVersion {
				return bcode.ErrUnauthorized
			}
			identity = &model.Identity{Provider: provider, Subject: subject}
			lookup = r.One(ctx, identity)
			if lookup != nil && !errors.Is(lookup, datastore.ErrRecordNotExist) {
				return lookup
			}
			if lookup == nil && identity.UserID != u.ID {
				return bcode.ErrAccountConflict
			}
			if lookup != nil {
				if e := r.Store.Add(ctx, &model.Identity{ID: uuid.NewString(), UserID: u.ID, Provider: provider, Subject: subject}); e != nil {
					return accountConflict(e)
				}
			}
			return nil
		}
		var u *model.User
		if lookup == nil {
			u = &model.User{ID: identity.UserID}
			if e := r.Lock(ctx, u); e != nil {
				return e
			}
			if u.Disabled {
				return bcode.ErrUnauthorized
			}
			current := &model.Identity{Provider: provider, Subject: subject}
			if err := r.One(ctx, current); err != nil || current.UserID != u.ID {
				return bcode.ErrUnauthorized
			}
		} else {
			if email != "" {
				existing := &model.Identity{Provider: "email", Subject: email}
				e := r.One(ctx, existing)
				if e == nil {
					return bcode.ErrAccountLinkRequired
				}
				if !errors.Is(e, datastore.ErrRecordNotExist) {
					return e
				}
			}
			var e error
			u, e = s.createUser(ctx, r, provider, subject, "", name, false)
			if e != nil {
				return e
			}
			if email != "" {
				if e = r.Store.Add(ctx, &model.Identity{ID: uuid.NewString(), UserID: u.ID, Provider: "email", Subject: email}); e != nil {
					return accountConflict(e)
				}
			}
		}
		var e error
		result, e = s.issueSession(ctx, r, u)
		return e
	})
	return result, err
}

func (s *Service) upstreamIdentity(ctx context.Context, provider, clientID string, token *oauth2.Token, nonce string) (string, string, string, error) {
	if provider == "google" {
		raw, ok := token.Extra("id_token").(string)
		if !ok {
			return "", "", "", bcode.ErrUnauthorized
		}
		keys := &googleKeys{client: s.HTTPClient}
		verified, err := oidc.NewVerifier("https://accounts.google.com", keys, &oidc.Config{ClientID: clientID, SupportedSigningAlgs: []string{"RS256"}, Now: s.Now}).Verify(ctx, raw)
		if keys.unavailable {
			return "", "", "", bcode.ErrUpstreamNotFound
		}
		if err != nil || verified.Subject == "" || verified.Nonce != nonce {
			return "", "", "", bcode.ErrUnauthorized
		}
		var claims struct {
			Email    string `json:"email"`
			Verified bool   `json:"email_verified"`
			Name     string `json:"name"`
		}
		if verified.Claims(&claims) != nil {
			return "", "", "", bcode.ErrUnauthorized
		}
		email := ""
		if claims.Verified {
			email, _ = NormalizeIdentity("email", claims.Email)
		}
		return verified.Subject, email, claims.Name, nil
	}
	var user struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
		Name  string `json:"name"`
	}
	if err := s.githubJSON(ctx, "https://api.github.com/user", token.AccessToken, &user); err != nil {
		return "", "", "", err
	}
	if user.ID <= 0 {
		return "", "", "", bcode.ErrUnauthorized
	}
	var emails []struct {
		Email    string `json:"email"`
		Verified bool   `json:"verified"`
		Primary  bool   `json:"primary"`
	}
	if err := s.githubJSON(ctx, "https://api.github.com/user/emails", token.AccessToken, &emails); err != nil {
		return "", "", "", err
	}
	email := ""
	for _, e := range emails {
		if e.Verified && e.Primary {
			email, _ = NormalizeIdentity("email", e.Email)
			break
		}
	}
	name := user.Name
	if name == "" {
		name = user.Login
	}
	return strconv.FormatInt(user.ID, 10), email, name, nil
}

// A key fetch failure is an upstream outage, distinct from an invalid signature.
// Fetch only after the OIDC verifier has accepted the token's claims and algorithm.
type googleKeys struct {
	client      *http.Client
	unavailable bool
}

func (k *googleKeys) VerifySignature(ctx context.Context, token string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.googleapis.com/oauth2/v3/certs", nil)
	if err != nil {
		return nil, err
	}
	resp, err := k.client.Do(req)
	if err != nil {
		k.unavailable = true
		return nil, bcode.ErrUpstreamNotFound
	}
	defer resp.Body.Close()
	var keys jose.JSONWebKeySet
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&keys) != nil || len(keys.Keys) == 0 {
		k.unavailable = true
		return nil, bcode.ErrUpstreamNotFound
	}
	public := make([]crypto.PublicKey, 0, len(keys.Keys))
	for _, key := range keys.Keys {
		if key.Valid() && key.IsPublic() && (key.Use == "" || key.Use == "sig") && (key.Algorithm == "" || key.Algorithm == "RS256") {
			public = append(public, key.Key)
		}
	}
	return (&oidc.StaticKeySet{PublicKeys: public}).VerifySignature(ctx, token)
}

func (s *Service) githubJSON(ctx context.Context, address, token string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return bcode.ErrUpstreamNotFound
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return bcode.ErrUpstreamNotFound
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out); err != nil {
		return bcode.ErrUpstreamNotFound
	}
	return nil
}

package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

const oauthGoogleStateKeyPrefix = "oauth:google:state:"

// OAuthGoogleCallbackResult is callback response data for OAuth login.
type OAuthGoogleCallbackResult struct {
	AccessToken string
	TokenType   string
	ExpiresIn   int64
	Subject     string
	Email       string
	Roles       []string
}

// OAuthGoogleLoginFlow implements google oauth2 code flow and local JWT issue.
type OAuthGoogleLoginFlow struct {
	repo       repository.SystemSettingRepository
	cache      cache.ICache
	httpClient *http.Client
	now        func() time.Time
	random     io.Reader
}

type oauthStatePayload struct {
	CodeVerifier string `json:"codeVerifier"`
	ExpiresAt    int64  `json:"expiresAt"`
}

type googleTokenResponse struct {
	AccessToken      string `json:"access_token"`
	TokenType        string `json:"token_type"`
	ExpiresIn        int64  `json:"expires_in"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

type googleUserInfoResponse struct {
	Subject string `json:"sub"`
	Email   string `json:"email"`
	HD      string `json:"hd"`
}

// NewOAuthGoogleLoginFlow creates OAuthGoogleLoginFlow with defaults.
func NewOAuthGoogleLoginFlow(repo repository.SystemSettingRepository, c cache.ICache, httpClient *http.Client) *OAuthGoogleLoginFlow {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &OAuthGoogleLoginFlow{
		repo:       repo,
		cache:      c,
		httpClient: httpClient,
		now:        time.Now,
		random:     rand.Reader,
	}
}

// BuildLoginURL builds google authorization redirect url and stores state.
func (f *OAuthGoogleLoginFlow) BuildLoginURL(ctx context.Context) (string, error) {
	if f == nil || f.repo == nil || f.cache == nil {
		return "", ErrOAuthConfigInvalid
	}
	oauthCfg, err := f.loadOAuthSetting(ctx)
	if err != nil {
		return "", err
	}

	state, err := generateRandomToken(f.random, 32)
	if err != nil {
		return "", ErrOAuthTokenIssueFailed
	}
	codeVerifier, err := generateRandomToken(f.random, 64)
	if err != nil {
		return "", ErrOAuthTokenIssueFailed
	}

	payload := oauthStatePayload{
		CodeVerifier: codeVerifier,
		ExpiresAt:    f.now().Add(time.Duration(oauthCfg.Security.StateTTLSeconds) * time.Second).Unix(),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", ErrOAuthTokenIssueFailed
	}
	if err := f.cache.Store(oauthGoogleStateKey(state), string(raw)); err != nil {
		return "", ErrOAuthTokenIssueFailed
	}

	authURL, err := buildGoogleAuthorizationURL(oauthCfg, state, codeVerifier)
	if err != nil {
		return "", ErrOAuthConfigInvalid
	}
	return authURL, nil
}

// HandleCallback validates callback data, exchanges code and issues local JWT.
func (f *OAuthGoogleLoginFlow) HandleCallback(ctx context.Context, code, state string) (*OAuthGoogleCallbackResult, error) {
	if f == nil || f.repo == nil || f.cache == nil {
		return nil, ErrOAuthConfigInvalid
	}
	code = strings.TrimSpace(code)
	state = strings.TrimSpace(state)
	if code == "" || state == "" {
		return nil, ErrOAuthCodeExchangeFailed
	}

	oauthCfg, err := f.loadOAuthSetting(ctx)
	if err != nil {
		return nil, err
	}

	codeVerifier, err := f.loadAndConsumeState(state)
	if err != nil {
		return nil, err
	}

	tokenResp, err := f.exchangeCode(ctx, oauthCfg, code, codeVerifier)
	if err != nil {
		return nil, err
	}
	userInfo, err := f.fetchGoogleUserInfo(ctx, oauthCfg, tokenResp.AccessToken)
	if err != nil {
		return nil, err
	}

	roles := resolveOAuthRoles(oauthCfg.RoleMapping, userInfo.Email, userInfo.HD)
	if len(roles) == 0 {
		return nil, ErrOAuthRoleMappingFailed
	}

	signingSecret, err := f.loadHS256SigningSecret(ctx)
	if err != nil {
		return nil, err
	}

	token, expiresAt, err := issueOAuthHS256Token(signingSecret, oauthCfg, userInfo.Subject, userInfo.Email, roles, f.now())
	if err != nil {
		return nil, ErrOAuthTokenIssueFailed
	}
	expiresIn := int64(time.Until(expiresAt).Seconds())
	if expiresIn < 0 {
		expiresIn = 0
	}
	return &OAuthGoogleCallbackResult{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   expiresIn,
		Subject:     userInfo.Subject,
		Email:       userInfo.Email,
		Roles:       roles,
	}, nil
}

func (f *OAuthGoogleLoginFlow) loadOAuthSetting(ctx context.Context) (*spec.OAuthAuthSettingSpec, error) {
	setting, err := f.repo.FindByType(ctx, model.SystemSettingTypeOAuthAuth)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, ErrOAuthSettingNotFound
		}
		return nil, err
	}
	var cfg spec.OAuthAuthSettingSpec
	if err := json.Unmarshal(setting.Value, &cfg); err != nil {
		return nil, ErrOAuthConfigInvalid
	}
	cfg = spec.NormalizeOAuthAuthSetting(cfg)
	if err := spec.ValidateOAuthAuthSetting(cfg); err != nil {
		return nil, ErrOAuthConfigInvalid
	}
	if !cfg.Enabled {
		return nil, ErrOAuthConfigInvalid
	}
	return &cfg, nil
}

func (f *OAuthGoogleLoginFlow) loadHS256SigningSecret(ctx context.Context) (string, error) {
	setting, err := f.repo.FindByType(ctx, model.SystemSettingTypeAPIAuth)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return "", ErrOAuthTokenIssueFailed
		}
		return "", err
	}
	var cfg spec.APIAuthSettingSpec
	if err := json.Unmarshal(setting.Value, &cfg); err != nil {
		return "", ErrOAuthTokenIssueFailed
	}
	cfg = spec.NormalizeAPIAuthSetting(cfg)

	secret := strings.TrimSpace(cfg.JWT.HS256.Secret)
	if secret == "" || secret == spec.APIAuthSecretMaskedValue {
		return "", ErrOAuthTokenIssueFailed
	}

	if len(cfg.JWT.Algorithms) > 0 {
		hasHS256 := false
		for _, alg := range cfg.JWT.Algorithms {
			if strings.EqualFold(strings.TrimSpace(alg), spec.APIAuthAlgorithmHS256) {
				hasHS256 = true
				break
			}
		}
		if !hasHS256 {
			return "", ErrOAuthTokenIssueFailed
		}
	}
	return secret, nil
}

func (f *OAuthGoogleLoginFlow) loadAndConsumeState(state string) (string, error) {
	key := oauthGoogleStateKey(state)
	value, err := f.cache.Consume(key)
	if err != nil {
		return "", ErrOAuthStateInvalid
	}
	if strings.TrimSpace(value) == "" {
		return "", ErrOAuthStateInvalid
	}

	var payload oauthStatePayload
	if err := json.Unmarshal([]byte(value), &payload); err != nil {
		return "", ErrOAuthStateInvalid
	}
	if payload.CodeVerifier == "" || payload.ExpiresAt <= 0 {
		return "", ErrOAuthStateInvalid
	}
	if f.now().Unix() > payload.ExpiresAt {
		return "", ErrOAuthStateInvalid
	}
	return payload.CodeVerifier, nil
}

func (f *OAuthGoogleLoginFlow) exchangeCode(ctx context.Context, cfg *spec.OAuthAuthSettingSpec, code, codeVerifier string) (*googleTokenResponse, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", cfg.Providers.Google.RedirectURI)
	form.Set("client_id", cfg.Providers.Google.ClientID)
	form.Set("client_secret", cfg.Providers.Google.ClientSecret)
	form.Set("code_verifier", codeVerifier)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Providers.Google.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, ErrOAuthCodeExchangeFailed
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, ErrOAuthCodeExchangeFailed
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ErrOAuthCodeExchangeFailed
	}
	var tokenResp googleTokenResponse
	if err := json.Unmarshal(raw, &tokenResp); err != nil {
		return nil, ErrOAuthCodeExchangeFailed
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, ErrOAuthCodeExchangeFailed
	}
	if strings.TrimSpace(tokenResp.AccessToken) == "" {
		return nil, ErrOAuthCodeExchangeFailed
	}
	return &tokenResp, nil
}

func (f *OAuthGoogleLoginFlow) fetchGoogleUserInfo(ctx context.Context, cfg *spec.OAuthAuthSettingSpec, accessToken string) (*googleUserInfoResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.Providers.Google.UserInfoURL, nil)
	if err != nil {
		return nil, ErrOAuthUserInfoFailed
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(accessToken))

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, ErrOAuthUserInfoFailed
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ErrOAuthUserInfoFailed
	}
	var userInfo googleUserInfoResponse
	if err := json.Unmarshal(raw, &userInfo); err != nil {
		return nil, ErrOAuthUserInfoFailed
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, ErrOAuthUserInfoFailed
	}
	userInfo.Subject = strings.TrimSpace(userInfo.Subject)
	userInfo.Email = strings.TrimSpace(userInfo.Email)
	userInfo.HD = strings.TrimSpace(userInfo.HD)
	if userInfo.Subject == "" {
		return nil, ErrOAuthUserInfoFailed
	}
	return &userInfo, nil
}

func resolveOAuthRoles(mapping spec.OAuthRoleMappingSpec, email, hostedDomain string) []string {
	email = strings.ToLower(strings.TrimSpace(email))
	hostedDomain = strings.ToLower(strings.TrimSpace(hostedDomain))

	if email != "" {
		if roles, ok := mapping.GoogleEmailToRoles[email]; ok {
			return append([]string(nil), roles...)
		}
	}
	if hostedDomain != "" {
		if roles, ok := mapping.GoogleHostedDomainToRoles[hostedDomain]; ok {
			return append([]string(nil), roles...)
		}
	}
	if email != "" {
		if domain := extractEmailDomain(email); domain != "" {
			if roles, ok := mapping.GoogleHostedDomainToRoles[domain]; ok {
				return append([]string(nil), roles...)
			}
		}
	}
	return append([]string(nil), mapping.DefaultRoles...)
}

func extractEmailDomain(email string) string {
	email = strings.TrimSpace(email)
	idx := strings.LastIndex(email, "@")
	if idx <= 0 || idx >= len(email)-1 {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(email[idx+1:]))
}

func issueOAuthHS256Token(secret string, cfg *spec.OAuthAuthSettingSpec, subject, email string, roles []string, now time.Time) (string, time.Time, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return "", time.Time{}, ErrOAuthTokenIssueFailed
	}
	ttl := cfg.JWTIssue.TTLSeconds
	if ttl <= 0 {
		ttl = spec.OAuthDefaultJWTTTL
	}
	expiresAt := now.Add(time.Duration(ttl) * time.Second)

	header := map[string]interface{}{
		"alg": spec.APIAuthAlgorithmHS256,
		"typ": "JWT",
	}
	claims := map[string]interface{}{
		"sub":   subject,
		"iss":   cfg.JWTIssue.Issuer,
		"aud":   cfg.JWTIssue.Audience,
		"iat":   now.Unix(),
		"exp":   expiresAt.Unix(),
		"roles": roles,
	}
	email = strings.TrimSpace(email)
	if email != "" {
		claims["email"] = email
	}

	headerPart, err := encodeJWTPartRaw(header)
	if err != nil {
		return "", time.Time{}, err
	}
	claimsPart, err := encodeJWTPartRaw(claims)
	if err != nil {
		return "", time.Time{}, err
	}
	signingInput := headerPart + "." + claimsPart
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signingInput))
	signature := mac.Sum(nil)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), expiresAt, nil
}

func encodeJWTPartRaw(value interface{}) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func buildGoogleAuthorizationURL(cfg *spec.OAuthAuthSettingSpec, state, codeVerifier string) (string, error) {
	challenge, err := buildPKCEChallenge(codeVerifier)
	if err != nil {
		return "", err
	}

	values := url.Values{}
	values.Set("response_type", "code")
	values.Set("client_id", cfg.Providers.Google.ClientID)
	values.Set("redirect_uri", cfg.Providers.Google.RedirectURI)
	values.Set("scope", strings.Join(cfg.Providers.Google.Scopes, " "))
	values.Set("state", state)
	values.Set("code_challenge", challenge)
	values.Set("code_challenge_method", "S256")

	parsed, err := url.Parse(cfg.Providers.Google.AuthURL)
	if err != nil {
		return "", err
	}
	parsed.RawQuery = values.Encode()
	return parsed.String(), nil
}

func buildPKCEChallenge(codeVerifier string) (string, error) {
	codeVerifier = strings.TrimSpace(codeVerifier)
	if codeVerifier == "" {
		return "", fmt.Errorf("empty verifier")
	}
	digest := sha256.Sum256([]byte(codeVerifier))
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func generateRandomToken(random io.Reader, size int) (string, error) {
	if random == nil || size <= 0 {
		return "", fmt.Errorf("invalid random source")
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(random, buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func oauthGoogleStateKey(state string) string {
	return oauthGoogleStateKeyPrefix + strings.TrimSpace(state)
}

package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apiauth "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/auth"
)

type staticPolicyProvider struct {
	policy *spec.APIAuthSettingSpec
	err    error
}

func (s *staticPolicyProvider) Load(context.Context) (*spec.APIAuthSettingSpec, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.policy, nil
}

func TestAuthMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	policy := &spec.APIAuthSettingSpec{
		Enabled: true,
		JWT: spec.APIAuthJWTSpec{
			Algorithms: []string{spec.APIAuthAlgorithmHS256},
			HS256: spec.APIAuthHS256Spec{
				Secret: "test-secret",
			},
		},
		Authorization: spec.APIAuthorizationSpec{
			DefaultEffect: spec.APIAuthDefaultEffectDeny,
			Routes: []spec.APIAuthRouteRuleSpec{
				{
					Method: "GET",
					Path:   "/api/v1/applications",
					Roles:  []string{"reader"},
				},
			},
		},
	}

	router := gin.New()
	router.Use(Auth(AuthOptions{
		PolicyProvider: &staticPolicyProvider{policy: policy},
	}))
	router.GET("/api/v1/applications", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	router.GET("/api/v1/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	router.GET("/api/v1/auth/oauth2/google/login", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	router.GET("/api/v1/auth/oauth2/google/callback", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	withoutTokenReq := httptest.NewRequest(http.MethodGet, "/api/v1/applications", nil)
	withoutTokenResp := httptest.NewRecorder()
	router.ServeHTTP(withoutTokenResp, withoutTokenReq)
	require.Equal(t, http.StatusUnauthorized, withoutTokenResp.Code)

	token := signHS256TokenForMiddlewareTest(t, "test-secret", map[string]interface{}{
		"sub":   "u-1",
		"exp":   time.Now().Add(time.Hour).Unix(),
		"roles": []string{"reader"},
	})
	withTokenReq := httptest.NewRequest(http.MethodGet, "/api/v1/applications", nil)
	withTokenReq.Header.Set("Authorization", "Bearer "+token)
	withTokenResp := httptest.NewRecorder()
	router.ServeHTTP(withTokenResp, withTokenReq)
	require.Equal(t, http.StatusOK, withTokenResp.Code)

	insufficientRoleToken := signHS256TokenForMiddlewareTest(t, "test-secret", map[string]interface{}{
		"sub":   "u-2",
		"exp":   time.Now().Add(time.Hour).Unix(),
		"roles": []string{"writer"},
	})
	forbiddenReq := httptest.NewRequest(http.MethodGet, "/api/v1/applications", nil)
	forbiddenReq.Header.Set("Authorization", "Bearer "+insufficientRoleToken)
	forbiddenResp := httptest.NewRecorder()
	router.ServeHTTP(forbiddenResp, forbiddenReq)
	require.Equal(t, http.StatusForbidden, forbiddenResp.Code)

	healthReq := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	healthResp := httptest.NewRecorder()
	router.ServeHTTP(healthResp, healthReq)
	require.Equal(t, http.StatusOK, healthResp.Code)

	loginReq := httptest.NewRequest(http.MethodGet, "/api/v1/auth/oauth2/google/login", nil)
	loginResp := httptest.NewRecorder()
	router.ServeHTTP(loginResp, loginReq)
	require.Equal(t, http.StatusOK, loginResp.Code)

	callbackReq := httptest.NewRequest(http.MethodGet, "/api/v1/auth/oauth2/google/callback", nil)
	callbackResp := httptest.NewRecorder()
	router.ServeHTTP(callbackResp, callbackReq)
	require.Equal(t, http.StatusOK, callbackResp.Code)
}

func TestAuthMiddlewareDenyWhenPolicyMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(Auth(AuthOptions{
		PolicyProvider: &staticPolicyProvider{err: apiauth.ErrPolicyNotFound},
	}))
	router.GET("/api/v1/applications", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/applications", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusUnauthorized, resp.Code)
}

func TestAuthMiddlewareAllowsWhenPolicyDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(Auth(AuthOptions{
		PolicyProvider: &staticPolicyProvider{policy: &spec.APIAuthSettingSpec{Enabled: false}},
	}))
	router.GET("/api/v1/applications", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/applications", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
}

func signHS256TokenForMiddlewareTest(t *testing.T, secret string, claims map[string]interface{}) string {
	t.Helper()
	header := map[string]interface{}{
		"alg": "HS256",
		"typ": "JWT",
	}
	headerPart := encodeJWTPartForMiddlewareTest(t, header)
	claimPart := encodeJWTPartForMiddlewareTest(t, claims)
	signingInput := headerPart + "." + claimPart
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signingInput))
	signature := mac.Sum(nil)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func encodeJWTPartForMiddlewareTest(t *testing.T, value interface{}) string {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(data)
}

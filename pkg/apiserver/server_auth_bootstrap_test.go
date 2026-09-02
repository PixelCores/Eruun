package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type systemSettingStore struct {
	mu       sync.Mutex
	settings map[string]*model.SystemSetting
	addCalls int
}

func newSystemSettingStore() *systemSettingStore {
	return &systemSettingStore{settings: map[string]*model.SystemSetting{}}
}

func (s *systemSettingStore) Add(_ context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := entity.(*model.SystemSetting)
	if !ok || item == nil {
		return datastore.ErrEntityInvalid
	}
	if _, exists := s.settings[item.Type]; exists {
		return datastore.ErrRecordExist
	}
	s.settings[item.Type] = cloneSystemSetting(item)
	s.addCalls++
	return nil
}

func (s *systemSettingStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (s *systemSettingStore) Put(context.Context, datastore.Entity) error        { return nil }
func (s *systemSettingStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (s *systemSettingStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *systemSettingStore) Get(_ context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := entity.(*model.SystemSetting)
	if !ok || item == nil {
		return datastore.ErrEntityInvalid
	}
	stored, exists := s.settings[item.Type]
	if !exists {
		return datastore.ErrRecordNotExist
	}
	*item = *cloneSystemSetting(stored)
	return nil
}

func (s *systemSettingStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}

func (s *systemSettingStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *systemSettingStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (s *systemSettingStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *systemSettingStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func cloneSystemSetting(in *model.SystemSetting) *model.SystemSetting {
	if in == nil {
		return nil
	}
	out := *in
	if in.Value != nil {
		out.Value = append(json.RawMessage(nil), in.Value...)
	}
	return &out
}

func TestEnsureDefaultAPIAuthSettingCreatesWhenMissing(t *testing.T) {
	store := newSystemSettingStore()
	server := &restServer{dataStore: store}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := server.ensureDefaultAPIAuthSetting(ctx)
	require.NoError(t, err)

	stored := store.settings[model.SystemSettingTypeAPIAuth]
	require.NotNil(t, stored)
	require.Equal(t, 1, store.addCalls)

	var cfg spec.APIAuthSettingSpec
	require.NoError(t, json.Unmarshal(stored.Value, &cfg))
	require.False(t, cfg.Enabled)
}

func TestEnsureDefaultAPIAuthSettingSkipsExisting(t *testing.T) {
	store := newSystemSettingStore()
	existingPayload, err := json.Marshal(spec.APIAuthSettingSpec{Enabled: true})
	require.NoError(t, err)
	store.settings[model.SystemSettingTypeAPIAuth] = &model.SystemSetting{
		Type:  model.SystemSettingTypeAPIAuth,
		Value: existingPayload,
	}

	server := &restServer{dataStore: store}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, server.ensureDefaultAPIAuthSetting(ctx))
	require.Equal(t, 0, store.addCalls)
}

func TestRegisterAPIRouteReloadsAPIAuthPolicyOnNextRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := newSystemSettingStore()
	store.settings[model.SystemSettingTypeAPIAuth] = &model.SystemSetting{
		Type:  model.SystemSettingTypeAPIAuth,
		Value: json.RawMessage(`{"enabled":false}`),
	}
	server := &restServer{
		dataStore:    store,
		webContainer: gin.New(),
	}
	server.RegisterAPIRoute()
	server.webContainer.GET("/api/v1/protected", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	firstResponse := httptest.NewRecorder()
	server.webContainer.ServeHTTP(firstResponse, httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil))
	require.Equal(t, http.StatusNoContent, firstResponse.Code)

	store.mu.Lock()
	store.settings[model.SystemSettingTypeAPIAuth] = &model.SystemSetting{
		Type: model.SystemSettingTypeAPIAuth,
		Value: json.RawMessage(`{
			"enabled":true,
			"jwt":{"algorithms":["HS256"],"hs256":{"secret":"test-secret"}},
			"authorization":{"defaultEffect":"deny","routes":[{"method":"GET","path":"/api/v1/protected","roles":["reader"]}]}
		}`),
	}
	store.mu.Unlock()

	secondResponse := httptest.NewRecorder()
	server.webContainer.ServeHTTP(secondResponse, httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil))
	require.Equal(t, http.StatusUnauthorized, secondResponse.Code)
}

func TestEnsureDefaultURLSecurityPolicyCreatesWhenMissing(t *testing.T) {
	store := newSystemSettingStore()
	server := &restServer{dataStore: store}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := server.ensureDefaultURLSecurityPolicySetting(ctx)
	require.NoError(t, err)

	stored := store.settings[model.SystemSettingTypeURLSecurityPolicy]
	require.NotNil(t, stored)
	require.Equal(t, 1, store.addCalls)

	var cfg spec.URLSecurityPolicySpec
	require.NoError(t, json.Unmarshal(stored.Value, &cfg))
	require.False(t, cfg.AllowPrivateByDefault)
	require.Contains(t, cfg.AllowedHostPatterns, spec.URLSecurityPolicyDefaultClusterDomain)
}

func TestEnsureDefaultURLSecurityPolicyHonorsLegacyAllowPrivateFlag(t *testing.T) {
	store := newSystemSettingStore()
	server := &restServer{
		dataStore: store,
		cfg:       config.Config{AllowPrivateURLTargets: true},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := server.ensureDefaultURLSecurityPolicySetting(ctx)
	require.NoError(t, err)

	stored := store.settings[model.SystemSettingTypeURLSecurityPolicy]
	require.NotNil(t, stored)

	var cfg spec.URLSecurityPolicySpec
	require.NoError(t, json.Unmarshal(stored.Value, &cfg))
	require.True(t, cfg.AllowPrivateByDefault)
}

func TestEnsureDefaultURLSecurityPolicySkipsExisting(t *testing.T) {
	store := newSystemSettingStore()
	existingPayload, err := json.Marshal(spec.URLSecurityPolicySpec{
		AllowPrivateByDefault: true,
		AllowedHostPatterns:   []string{"*.corp.example.com"},
	})
	require.NoError(t, err)
	store.settings[model.SystemSettingTypeURLSecurityPolicy] = &model.SystemSetting{
		Type:  model.SystemSettingTypeURLSecurityPolicy,
		Value: existingPayload,
	}

	server := &restServer{dataStore: store}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, server.ensureDefaultURLSecurityPolicySetting(ctx))
	require.Equal(t, 0, store.addCalls)
}

func TestEnsureDefaultPodRestartMonitorCreatesWhenMissing(t *testing.T) {
	store := newSystemSettingStore()
	server := &restServer{dataStore: store}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := server.ensureDefaultPodRestartMonitorSetting(ctx)
	require.NoError(t, err)

	stored := store.settings[model.SystemSettingTypePodRestartMonitor]
	require.NotNil(t, stored)
	require.Equal(t, 1, store.addCalls)

	cfg, err := spec.ParsePodRestartMonitorSetting(stored.Value)
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, spec.DefaultPodRestartMonitorWindowSeconds, cfg.WindowSeconds)
	require.Equal(t, spec.DefaultPodRestartMonitorThreshold, cfg.Threshold)
}

func TestEnsureDefaultPodRestartMonitorSkipsExisting(t *testing.T) {
	store := newSystemSettingStore()
	existingPayload, err := json.Marshal(spec.PodRestartMonitorSettingSpec{
		Enabled:       false,
		WindowSeconds: 60,
		Threshold:     2,
	})
	require.NoError(t, err)
	store.settings[model.SystemSettingTypePodRestartMonitor] = &model.SystemSetting{
		Type:  model.SystemSettingTypePodRestartMonitor,
		Value: existingPayload,
	}

	server := &restServer{dataStore: store}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, server.ensureDefaultPodRestartMonitorSetting(ctx))
	require.Equal(t, 0, store.addCalls)
}

func TestLoadPodRestartMonitorConfigReadsSetting(t *testing.T) {
	store := newSystemSettingStore()
	payload, err := json.Marshal(spec.PodRestartMonitorSettingSpec{
		Enabled:       true,
		WindowSeconds: 60,
		Threshold:     2,
	})
	require.NoError(t, err)
	store.settings[model.SystemSettingTypePodRestartMonitor] = &model.SystemSetting{
		Type:  model.SystemSettingTypePodRestartMonitor,
		Value: payload,
	}

	server := &restServer{dataStore: store}
	cfg, err := server.loadPodRestartMonitorConfig(context.Background())
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, time.Minute, cfg.Window)
	require.Equal(t, 2, cfg.Threshold)
}

func TestRunBootstrapStepUsesFreshContextEachCall(t *testing.T) {
	server := &restServer{}

	var firstCtx context.Context
	require.NoError(t, server.runBootstrapStep(context.Background(), func(ctx context.Context) error {
		firstCtx = ctx
		return nil
	}))

	var secondCtx context.Context
	require.NoError(t, server.runBootstrapStep(context.Background(), func(ctx context.Context) error {
		secondCtx = ctx
		return nil
	}))

	require.NotNil(t, firstCtx)
	require.NotNil(t, secondCtx)
	require.NotSame(t, firstCtx, secondCtx)
}

func TestRunBootstrapStepReturnsStepError(t *testing.T) {
	server := &restServer{}
	expectedErr := errors.New("bootstrap failure")

	err := server.runBootstrapStep(context.Background(), func(context.Context) error {
		return expectedErr
	})
	require.ErrorIs(t, err, expectedErr)
}

func TestRunBootstrapStepInheritsParentCancellation(t *testing.T) {
	server := &restServer{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := server.runBootstrapStep(ctx, func(stepCtx context.Context) error {
		<-stepCtx.Done()
		return stepCtx.Err()
	})

	require.ErrorIs(t, err, context.Canceled)
}

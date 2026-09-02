package auth

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type fakeSystemSettingRepo struct {
	item  *model.SystemSetting
	calls int
	err   error
}

func (f *fakeSystemSettingRepo) FindByType(_ context.Context, settingType string) (*model.SystemSetting, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if f.item == nil || f.item.Type != settingType {
		return nil, datastore.ErrRecordNotExist
	}
	return f.item, nil
}

func (f *fakeSystemSettingRepo) Create(context.Context, *model.SystemSetting) error {
	return nil
}

func (f *fakeSystemSettingRepo) Update(context.Context, *model.SystemSetting) error {
	return nil
}

func (f *fakeSystemSettingRepo) Delete(context.Context, *model.SystemSetting) error {
	return nil
}

func (f *fakeSystemSettingRepo) List(context.Context, datastore.ListOptions) ([]*model.SystemSetting, error) {
	return nil, nil
}

func TestSystemSettingPolicyProvider_LoadAndCache(t *testing.T) {
	repo := &fakeSystemSettingRepo{
		item: &model.SystemSetting{
			Type:  model.SystemSettingTypeAPIAuth,
			Value: json.RawMessage(validAPIAuthSettingJSON()),
		},
	}

	now := time.Now()
	provider := NewSystemSettingPolicyProvider(repo, time.Minute)
	provider.now = func() time.Time { return now }

	cfg, err := provider.Load(context.Background())
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, 1, repo.calls)

	_, err = provider.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, repo.calls)
}

func TestSystemSettingPolicyProvider_LoadWithoutCacheReflectsChangesAcrossProviders(t *testing.T) {
	repo := &fakeSystemSettingRepo{
		item: &model.SystemSetting{
			Type:  model.SystemSettingTypeAPIAuth,
			Value: json.RawMessage(`{"enabled":false}`),
		},
	}
	firstProvider := NewSystemSettingPolicyProvider(repo, 0)
	secondProvider := NewSystemSettingPolicyProvider(repo, 0)

	policy, err := firstProvider.Load(context.Background())
	require.NoError(t, err)
	require.False(t, policy.Enabled)

	repo.item.Value = json.RawMessage(validAPIAuthSettingJSON())
	policy, err = secondProvider.Load(context.Background())
	require.NoError(t, err)
	require.True(t, policy.Enabled)
	require.Equal(t, "test-secret", policy.JWT.HS256.Secret)
	require.Equal(t, []string{"reader"}, policy.Authorization.Routes[0].Roles)

	repo.item.Value = json.RawMessage(strings.NewReplacer(
		"test-secret", "rotated-secret",
		"reader", "admin",
	).Replace(validAPIAuthSettingJSON()))
	policy, err = firstProvider.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, "rotated-secret", policy.JWT.HS256.Secret)
	require.Equal(t, []string{"admin"}, policy.Authorization.Routes[0].Roles)
	require.Equal(t, 3, repo.calls)
}

func TestSystemSettingPolicyProvider_LoadNotFound(t *testing.T) {
	repo := &fakeSystemSettingRepo{}
	provider := NewSystemSettingPolicyProvider(repo, 0)
	_, err := provider.Load(context.Background())
	require.ErrorIs(t, err, ErrPolicyNotFound)
}

func TestSystemSettingPolicyProvider_LoadInvalidPolicy(t *testing.T) {
	repo := &fakeSystemSettingRepo{
		item: &model.SystemSetting{
			Type:  model.SystemSettingTypeAPIAuth,
			Value: json.RawMessage(`{"enabled":true,"jwt":{"algorithms":["HS256"]},"authorization":{"routes":[{"method":"GET","path":"/api/v1/applications","roles":["reader"]}]}}`),
		},
	}
	provider := NewSystemSettingPolicyProvider(repo, 0)
	_, err := provider.Load(context.Background())
	require.Error(t, err)
}

func validAPIAuthSettingJSON() string {
	return `{
		"enabled": true,
		"jwt": {
			"algorithms": ["HS256"],
			"hs256": {"secret": "test-secret"},
			"clockSkewSeconds": 30
		},
		"authorization": {
			"defaultEffect": "deny",
			"routes": [
				{"method":"GET","path":"/api/v1/applications","roles":["reader"]}
			]
		}
	}`
}

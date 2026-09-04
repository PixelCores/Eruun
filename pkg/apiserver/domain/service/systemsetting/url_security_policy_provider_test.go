package systemsetting

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type fakeSettingStore struct {
	item     *model.SystemSetting
	getCalls int
	err      error
}

func (f *fakeSettingStore) Add(context.Context, datastore.Entity) error { return nil }
func (f *fakeSettingStore) BatchAdd(context.Context, []datastore.Entity) error {
	return nil
}
func (f *fakeSettingStore) Put(context.Context, datastore.Entity) error { return nil }
func (f *fakeSettingStore) Delete(context.Context, datastore.Entity) error {
	return nil
}
func (f *fakeSettingStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (f *fakeSettingStore) Get(_ context.Context, entity datastore.Entity) error {
	f.getCalls++
	if f.err != nil {
		return f.err
	}
	setting, ok := entity.(*model.SystemSetting)
	if !ok || setting == nil {
		return datastore.ErrEntityInvalid
	}
	if f.item == nil || setting.Type != f.item.Type {
		return datastore.ErrRecordNotExist
	}
	*setting = *f.item
	return nil
}
func (f *fakeSettingStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}
func (f *fakeSettingStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (f *fakeSettingStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (f *fakeSettingStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (f *fakeSettingStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func TestProviderLoadAndCache(t *testing.T) {
	value, err := json.Marshal(spec.URLSecurityPolicySpec{
		AllowPrivateByDefault: false,
		AllowedHostPatterns:   []string{"*.svc.cluster.local"},
	})
	require.NoError(t, err)

	store := &fakeSettingStore{
		item: &model.SystemSetting{
			Type:  model.SystemSettingTypeURLSecurityPolicy,
			Value: value,
		},
	}

	provider := NewProvider(store, time.Minute)
	now := time.Now()
	provider.now = func() time.Time { return now }

	cfg, err := provider.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"*.svc.cluster.local"}, cfg.AllowedHostPatterns)
	require.Equal(t, 1, store.getCalls)

	_, err = provider.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, store.getCalls)
}

func TestProviderLoadNotFound(t *testing.T) {
	store := &fakeSettingStore{}
	provider := NewProvider(store, 0)

	_, err := provider.Load(context.Background())
	require.ErrorIs(t, err, ErrNotFound)
}

func TestResolvePolicyRequiresProvider(t *testing.T) {
	policy, err := ResolvePolicy(context.Background(), nil)
	require.Nil(t, policy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "provider is not configured")
}

func TestResolvePolicyReturnsProviderError(t *testing.T) {
	store := &fakeSettingStore{err: errors.New("db unavailable")}
	provider := NewProvider(store, 0)

	policy, err := ResolvePolicy(context.Background(), provider)
	require.Nil(t, policy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load urlSecurityPolicy")
	require.ErrorContains(t, err, "db unavailable")
}

func TestResolvePolicyReturnsNotFound(t *testing.T) {
	provider := NewProvider(&fakeSettingStore{}, 0)

	policy, err := ResolvePolicy(context.Background(), provider)
	require.Nil(t, policy)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestResolvePolicyReturnsLoadedPolicy(t *testing.T) {
	value, err := json.Marshal(spec.URLSecurityPolicySpec{
		AllowPrivateByDefault: true,
	})
	require.NoError(t, err)

	store := &fakeSettingStore{
		item: &model.SystemSetting{
			Type:  model.SystemSettingTypeURLSecurityPolicy,
			Value: value,
		},
	}
	provider := NewProvider(store, 0)

	policy, err := ResolvePolicy(context.Background(), provider)
	require.NoError(t, err)
	require.NotNil(t, policy)
	require.True(t, policy.AllowPrivateByDefault)
}

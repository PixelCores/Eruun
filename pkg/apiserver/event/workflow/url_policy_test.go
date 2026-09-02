package workflow

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/urlpolicy"
)

type workflowURLPolicyStore struct {
	item *model.SystemSetting
	err  error
}

func newTestURLSecurityPolicyProvider(t testing.TB, policy spec.URLSecurityPolicySpec) *urlpolicy.Provider {
	t.Helper()
	value, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("marshal url security policy: %v", err)
	}
	return urlpolicy.NewProvider(&workflowURLPolicyStore{
		item: &model.SystemSetting{
			Type:  model.SystemSettingTypeURLSecurityPolicy,
			Value: value,
		},
	}, 0)
}

func newFailingURLSecurityPolicyProvider(err error) *urlpolicy.Provider {
	return urlpolicy.NewProvider(&workflowURLPolicyStore{err: err}, 0)
}

func (s *workflowURLPolicyStore) Add(context.Context, datastore.Entity) error { return nil }
func (s *workflowURLPolicyStore) BatchAdd(context.Context, []datastore.Entity) error {
	return nil
}
func (s *workflowURLPolicyStore) Put(context.Context, datastore.Entity) error { return nil }
func (s *workflowURLPolicyStore) Delete(context.Context, datastore.Entity) error {
	return nil
}
func (s *workflowURLPolicyStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (s *workflowURLPolicyStore) Get(_ context.Context, entity datastore.Entity) error {
	if s.err != nil {
		return s.err
	}
	setting, ok := entity.(*model.SystemSetting)
	if !ok || setting == nil || s.item == nil || setting.Type != s.item.Type {
		return datastore.ErrRecordNotExist
	}
	*setting = *s.item
	return nil
}
func (s *workflowURLPolicyStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}
func (s *workflowURLPolicyStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (s *workflowURLPolicyStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (s *workflowURLPolicyStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *workflowURLPolicyStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

var _ datastore.DataStore = (*workflowURLPolicyStore)(nil)

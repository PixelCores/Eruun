package validation

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

type inMemoryAppStore struct {
	apps       map[string]*model.Applications
	components map[string]*model.ApplicationComponent
	settings   map[string]*model.SystemSetting
}

func newInMemoryAppStore() *inMemoryAppStore {
	policyValue, _ := json.Marshal(spec.DefaultURLSecurityPolicy())
	return &inMemoryAppStore{
		apps:       make(map[string]*model.Applications),
		components: make(map[string]*model.ApplicationComponent),
		settings: map[string]*model.SystemSetting{
			model.SystemSettingTypeURLSecurityPolicy: {Type: model.SystemSettingTypeURLSecurityPolicy, Value: policyValue},
		},
	}
}

func (s *inMemoryAppStore) Get(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		if app, ok := s.apps[v.ID]; ok {
			*v = *app
			return nil
		}
	case *model.SystemSetting:
		if setting, ok := s.settings[v.Type]; ok {
			*v = *setting
			return nil
		}
	}
	return datastore.ErrRecordNotExist
}

func (s *inMemoryAppStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	switch q := query.(type) {
	case *model.Applications:
		var result []datastore.Entity
		for _, app := range s.apps {
			if q.Name != "" && app.Name != q.Name {
				continue
			}
			if q.TemplateEnabled && !app.TemplateEnabled {
				continue
			}
			result = append(result, app)
		}
		return result, nil
	case *model.ApplicationComponent:
		var result []datastore.Entity
		for _, comp := range s.components {
			if q.AppID != "" && comp.AppID != q.AppID {
				continue
			}
			result = append(result, comp)
		}
		return result, nil
	}
	return nil, nil
}

func (s *inMemoryAppStore) Add(context.Context, datastore.Entity) error        { return nil }
func (s *inMemoryAppStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (s *inMemoryAppStore) Put(context.Context, datastore.Entity) error        { return nil }
func (s *inMemoryAppStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (s *inMemoryAppStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (s *inMemoryAppStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (s *inMemoryAppStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (s *inMemoryAppStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *inMemoryAppStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

type mockServiceWithRepos struct {
	AppRepo       repository.ApplicationRepository
	ComponentRepo repository.ComponentRepository
}

func newMockServiceWithStore(store *inMemoryAppStore) *mockServiceWithRepos {
	return &mockServiceWithRepos{
		AppRepo:       &mockAppRepo{store: store},
		ComponentRepo: &mockComponentRepo{store: store},
	}
}

type mockAppRepo struct{ store *inMemoryAppStore }

func (m *mockAppRepo) FindByID(ctx context.Context, id string) (*model.Applications, error) {
	app := &model.Applications{ID: id}
	if err := m.store.Get(ctx, app); err != nil {
		return nil, err
	}
	return app, nil
}
func (m *mockAppRepo) FindByIDs(_ context.Context, ids []string) ([]*model.Applications, error) {
	applications := make([]*model.Applications, 0, len(ids))
	for _, id := range ids {
		if app, ok := m.store.apps[id]; ok {
			applications = append(applications, app)
		}
	}
	return applications, nil
}
func (m *mockAppRepo) FindByName(ctx context.Context, name string) (*model.Applications, error) {
	for _, app := range m.store.apps {
		if app.Name == name {
			return app, nil
		}
	}
	return nil, datastore.ErrRecordNotExist
}
func (m *mockAppRepo) Create(context.Context, *model.Applications) error { return nil }
func (m *mockAppRepo) Update(context.Context, *model.Applications) error { return nil }
func (m *mockAppRepo) Delete(context.Context, *model.Applications) error { return nil }
func (m *mockAppRepo) List(ctx context.Context, options datastore.ListOptions) ([]*model.Applications, error) {
	return m.ListByQuery(ctx, &model.Applications{}, options)
}
func (m *mockAppRepo) ListByQuery(ctx context.Context, query *model.Applications, options datastore.ListOptions) ([]*model.Applications, error) {
	entities, err := m.store.List(ctx, query, &options)
	if err != nil {
		return nil, err
	}
	result := make([]*model.Applications, 0, len(entities))
	for _, entity := range entities {
		if app, ok := entity.(*model.Applications); ok {
			result = append(result, app)
		}
	}
	return result, nil
}

type mockComponentRepo struct{ store *inMemoryAppStore }

func (m *mockComponentRepo) Create(context.Context, *model.ApplicationComponent) error { return nil }
func (m *mockComponentRepo) BatchAdd(context.Context, []*model.ApplicationComponent) error {
	return nil
}
func (m *mockComponentRepo) DeleteByAppID(context.Context, string) error { return nil }
func (m *mockComponentRepo) FindByAppID(_ context.Context, appID string) ([]*model.ApplicationComponent, error) {
	var result []*model.ApplicationComponent
	for _, comp := range m.store.components {
		if comp.AppID == appID {
			result = append(result, comp)
		}
	}
	return result, nil
}
func (m *mockComponentRepo) Update(context.Context, *model.ApplicationComponent) error { return nil }
func (m *mockComponentRepo) Delete(context.Context, *model.ApplicationComponent) error { return nil }
func (m *mockComponentRepo) FindByName(_ context.Context, appID, name string) (*model.ApplicationComponent, error) {
	for _, comp := range m.store.components {
		if comp.AppID == appID && comp.Name == name {
			return comp, nil
		}
	}
	return nil, datastore.ErrRecordNotExist
}

func newTestURLSecurityPolicyProvider(t testing.TB, policy spec.URLSecurityPolicySpec) *urlpolicy.Provider {
	t.Helper()
	store := newInMemoryAppStore()
	value, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("marshal url security policy: %v", err)
	}
	store.settings[model.SystemSettingTypeURLSecurityPolicy] = &model.SystemSetting{Type: model.SystemSettingTypeURLSecurityPolicy, Value: value}
	return urlpolicy.NewProvider(store, 0)
}

var _ repository.ApplicationRepository = (*mockAppRepo)(nil)
var _ repository.ComponentRepository = (*mockComponentRepo)(nil)
var _ datastore.DataStore = (*inMemoryAppStore)(nil)

func testPersistentStorageTrait(name, claimName string, tmpCreate bool) spec.StorageTraitSpec {
	return spec.StorageTraitSpec{
		Name:      name,
		Type:      config.StorageTypePersistent,
		MountPath: "/data/" + name,
		TmpCreate: tmpCreate,
		ClaimName: claimName,
	}
}

func mustJSONStruct(v interface{}) *model.JSONStruct {
	js, err := model.NewJSONStructByStruct(v)
	if err != nil {
		panic(err)
	}
	return js
}

func validateComponentTraitsForWrite(componentType config.JobType, traits apisv1.Traits, fieldPrefix string) error {
	return applicationservice.ValidateComponentTraitsForWrite(componentType, traits, fieldPrefix)
}

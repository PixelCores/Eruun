package resourceimport

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	validationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/validation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func mustJSON(t testing.TB, value *model.JSONStruct) string {
	t.Helper()
	data, err := value.Bytes()
	require.NoError(t, err)
	return string(data)
}

func NewValidationService() ValidationService {
	return validationservice.NewValidationService()
}

type inMemoryAppStore struct {
	apps       map[string]*model.Applications
	workflows  map[string]*model.Workflow
	components map[string]*model.ApplicationComponent
	settings   map[string]*model.SystemSetting
}

func newInMemoryAppStore() *inMemoryAppStore {
	policyValue, _ := json.Marshal(spec.DefaultURLSecurityPolicy())
	return &inMemoryAppStore{
		apps:       make(map[string]*model.Applications),
		workflows:  make(map[string]*model.Workflow),
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
	case *model.Workflow:
		if workflow, ok := s.workflows[v.ID]; ok {
			*v = *workflow
			return nil
		}
	case *model.ApplicationComponent:
		if component, ok := s.components[v.Name]; ok {
			*v = *component
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
	case *model.Workflow:
		var result []datastore.Entity
		for _, workflow := range s.workflows {
			if q.AppID != "" && workflow.AppID != q.AppID {
				continue
			}
			result = append(result, workflow)
		}
		return result, nil
	case *model.ApplicationComponent:
		var result []datastore.Entity
		for _, component := range s.components {
			if q.AppID != "" && component.AppID != q.AppID {
				continue
			}
			result = append(result, component)
		}
		return result, nil
	}
	return nil, nil
}

func (s *inMemoryAppStore) Add(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		cp := *v
		s.apps[v.ID] = &cp
	case *model.Workflow:
		cp := *v
		s.workflows[v.ID] = &cp
	case *model.ApplicationComponent:
		cp := *v
		s.components[v.Name] = &cp
	}
	return nil
}
func (s *inMemoryAppStore) BatchAdd(ctx context.Context, entities []datastore.Entity) error {
	for _, entity := range entities {
		if err := s.Add(ctx, entity); err != nil {
			return err
		}
	}
	return nil
}
func (s *inMemoryAppStore) Put(ctx context.Context, entity datastore.Entity) error {
	return s.Add(ctx, entity)
}
func (s *inMemoryAppStore) Delete(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		delete(s.apps, v.ID)
	case *model.Workflow:
		delete(s.workflows, v.ID)
	case *model.ApplicationComponent:
		delete(s.components, v.Name)
	}
	return nil
}
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
func (m *mockAppRepo) FindByName(_ context.Context, name string) (*model.Applications, error) {
	for _, app := range m.store.apps {
		if app.Name == name {
			return app, nil
		}
	}
	return nil, datastore.ErrRecordNotExist
}
func (m *mockAppRepo) Create(ctx context.Context, app *model.Applications) error {
	return m.store.Add(ctx, app)
}
func (m *mockAppRepo) Update(ctx context.Context, app *model.Applications) error {
	return m.store.Put(ctx, app)
}
func (m *mockAppRepo) Delete(ctx context.Context, app *model.Applications) error {
	return m.store.Delete(ctx, app)
}
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

type mockWorkflowRepo struct{ store *inMemoryAppStore }

func (m *mockWorkflowRepo) FindByID(ctx context.Context, id string) (*model.Workflow, error) {
	workflow := &model.Workflow{ID: id}
	if err := m.store.Get(ctx, workflow); err != nil {
		return nil, err
	}
	return workflow, nil
}
func (m *mockWorkflowRepo) Create(ctx context.Context, workflow *model.Workflow) error {
	return m.store.Add(ctx, workflow)
}
func (m *mockWorkflowRepo) Update(ctx context.Context, workflow *model.Workflow) error {
	return m.store.Put(ctx, workflow)
}
func (m *mockWorkflowRepo) Delete(ctx context.Context, workflow *model.Workflow) error {
	return m.store.Delete(ctx, workflow)
}
func (m *mockWorkflowRepo) DeleteByAppID(_ context.Context, appID string) error {
	for id, workflow := range m.store.workflows {
		if workflow.AppID == appID {
			delete(m.store.workflows, id)
		}
	}
	return nil
}
func (m *mockWorkflowRepo) FindByAppID(_ context.Context, appID string) ([]*model.Workflow, error) {
	var result []*model.Workflow
	for _, workflow := range m.store.workflows {
		if workflow.AppID == appID {
			result = append(result, workflow)
		}
	}
	return result, nil
}

type mockComponentRepo struct{ store *inMemoryAppStore }

func (m *mockComponentRepo) Create(ctx context.Context, component *model.ApplicationComponent) error {
	return m.store.Add(ctx, component)
}
func (m *mockComponentRepo) BatchAdd(ctx context.Context, components []*model.ApplicationComponent) error {
	for _, component := range components {
		if err := m.store.Add(ctx, component); err != nil {
			return err
		}
	}
	return nil
}
func (m *mockComponentRepo) DeleteByAppID(_ context.Context, appID string) error {
	for name, component := range m.store.components {
		if component.AppID == appID {
			delete(m.store.components, name)
		}
	}
	return nil
}
func (m *mockComponentRepo) FindByAppID(_ context.Context, appID string) ([]*model.ApplicationComponent, error) {
	var result []*model.ApplicationComponent
	for _, component := range m.store.components {
		if component.AppID == appID {
			result = append(result, component)
		}
	}
	return result, nil
}
func (m *mockComponentRepo) Update(ctx context.Context, component *model.ApplicationComponent) error {
	return m.store.Put(ctx, component)
}
func (m *mockComponentRepo) Delete(ctx context.Context, component *model.ApplicationComponent) error {
	return m.store.Delete(ctx, component)
}
func (m *mockComponentRepo) FindByName(_ context.Context, appID, name string) (*model.ApplicationComponent, error) {
	for _, component := range m.store.components {
		if component.AppID == appID && component.Name == name {
			return component, nil
		}
	}
	return nil, datastore.ErrRecordNotExist
}

var _ datastore.DataStore = (*inMemoryAppStore)(nil)
var _ repository.ApplicationRepository = (*mockAppRepo)(nil)
var _ repository.WorkflowRepository = (*mockWorkflowRepo)(nil)
var _ repository.ComponentRepository = (*mockComponentRepo)(nil)

package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// ApplicationRepository defines the interface for application data operations.
type ApplicationRepository interface {
	FindByID(ctx context.Context, id string) (*model.Applications, error)
	FindByIDs(ctx context.Context, ids []string) ([]*model.Applications, error)
	FindByName(ctx context.Context, name string) (*model.Applications, error)
	Create(ctx context.Context, app *model.Applications) error
	Update(ctx context.Context, app *model.Applications) error
	Delete(ctx context.Context, app *model.Applications) error
	List(ctx context.Context, options datastore.ListOptions) ([]*model.Applications, error)
	ListByQuery(ctx context.Context, query *model.Applications, options datastore.ListOptions) ([]*model.Applications, error)
}

type applicationRepository struct {
	Store datastore.DataStore `inject:"datastore"`
}

// NewApplicationRepository creates a new ApplicationRepository.
// Dependencies are injected via struct tags.
func NewApplicationRepository() ApplicationRepository {
	return &applicationRepository{}
}

func (r *applicationRepository) FindByID(ctx context.Context, id string) (*model.Applications, error) {
	return ApplicationByID(ctx, r.Store, id)
}

func (r *applicationRepository) FindByIDs(ctx context.Context, ids []string) ([]*model.Applications, error) {
	if len(ids) == 0 {
		return []*model.Applications{}, nil
	}
	entities, err := r.Store.List(ctx, &model.Applications{}, &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{
			In: []datastore.InQueryOption{{Key: "id", Values: ids}},
		},
	})
	if err != nil {
		return nil, err
	}
	applications := make([]*model.Applications, 0, len(entities))
	for _, entity := range entities {
		application, ok := entity.(*model.Applications)
		if !ok {
			return nil, fmt.Errorf("unexpected application entity type: %T", entity)
		}
		applications = append(applications, application)
	}
	return applications, nil
}

func (r *applicationRepository) FindByName(ctx context.Context, name string) (*model.Applications, error) {
	if name == "" {
		return nil, datastore.ErrRecordNotExist
	}
	apps, err := r.ListByQuery(ctx, &model.Applications{Name: name}, datastore.ListOptions{Page: 1, PageSize: 1})
	if err != nil {
		return nil, err
	}
	if len(apps) == 0 {
		return nil, datastore.ErrRecordNotExist
	}
	return apps[0], nil
}

func (r *applicationRepository) Create(ctx context.Context, app *model.Applications) error {
	return CreateApplications(ctx, r.Store, app)
}

func (r *applicationRepository) Update(ctx context.Context, app *model.Applications) error {
	return r.Store.Put(ctx, app)
}

func (r *applicationRepository) Delete(ctx context.Context, app *model.Applications) error {
	return r.Store.Delete(ctx, app)
}

func (r *applicationRepository) List(ctx context.Context, options datastore.ListOptions) ([]*model.Applications, error) {
	return ListApplications(ctx, r.Store, options)
}

func (r *applicationRepository) ListByQuery(ctx context.Context, query *model.Applications, options datastore.ListOptions) ([]*model.Applications, error) {
	return ListApplicationsByQuery(ctx, r.Store, query, options)
}

// ---- Original Functions (kept for backward compatibility) ----

func ApplicationByID(ctx context.Context, store datastore.DataStore, id string) (*model.Applications, error) {
	app := model.Applications{
		ID: id,
	}
	err := store.Get(ctx, &app)
	if err != nil {
		return nil, err
	}
	return &app, nil
}

func CreateApplications(ctx context.Context, store datastore.DataStore, app *model.Applications) error {
	if err := store.Add(ctx, app); err != nil {
		if errors.Is(err, datastore.ErrRecordExist) {
			if _, getErr := ApplicationByID(ctx, store, app.ID); getErr != nil {
				if errors.Is(getErr, datastore.ErrRecordNotExist) {
					return err
				}
				return getErr
			}
			return store.Put(ctx, app)
		}
		return err
	}
	return nil
}

// ListApplications query the application policies
func ListApplications(ctx context.Context, store datastore.DataStore, listOptions datastore.ListOptions) (list []*model.Applications, err error) {
	return ListApplicationsByQuery(ctx, store, &model.Applications{}, listOptions)
}

func ListApplicationsByQuery(ctx context.Context, store datastore.DataStore, query *model.Applications, listOptions datastore.ListOptions) (list []*model.Applications, err error) {
	if query == nil {
		query = &model.Applications{}
	}
	entities, err := store.List(ctx, query, &listOptions)
	if err != nil {
		return nil, err
	}
	for _, entity := range entities {
		appModel, ok := entity.(*model.Applications)
		if !ok {
			continue
		}
		list = append(list, appModel)
	}

	return list, nil
}

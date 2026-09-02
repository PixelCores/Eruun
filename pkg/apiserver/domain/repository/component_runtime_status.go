package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

var componentRuntimeFieldAllowlist = map[string]struct{}{
	"status":         {},
	"ready_replicas": {},
	"last_abnormal":  {},
}

func UpdateComponentRuntimeFields(ctx context.Context, store datastore.DataStore, component *model.ApplicationComponent, updates map[string]interface{}) error {
	if store == nil {
		return fmt.Errorf("datastore is nil")
	}
	if component == nil {
		return fmt.Errorf("component is nil")
	}
	if len(updates) == 0 {
		return nil
	}

	fields, err := normalizeComponentRuntimeFields(updates)
	if err != nil {
		return err
	}

	entity, conditions, err := componentRuntimeFieldConditions(component)
	if err != nil {
		return err
	}

	conditionalStore, ok := store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return fmt.Errorf("datastore does not support conditional compare and swap")
	}

	updated, err := conditionalStore.CompareAndSwapWithConditions(ctx, entity, conditions, fields)
	if err != nil {
		return err
	}
	if !updated {
		exists, err := componentRuntimeFieldExists(ctx, store, entity, conditions)
		if err != nil {
			return err
		}
		if !exists {
			return datastore.ErrRecordNotExist
		}
	}
	return nil
}

// UpdateComponentRuntimeFieldsIfUnchanged updates informer-owned runtime fields
// only when the persisted runtime snapshot still matches component. A false,
// nil result means a newer writer won the race and the informer event is stale.
func UpdateComponentRuntimeFieldsIfUnchanged(ctx context.Context, store datastore.DataStore, component *model.ApplicationComponent, updates map[string]interface{}) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("datastore is nil")
	}
	if component == nil {
		return false, fmt.Errorf("component is nil")
	}
	if len(updates) == 0 {
		return false, nil
	}

	fields, err := normalizeComponentRuntimeFields(updates)
	if err != nil {
		return false, err
	}

	entity, identityConditions, err := componentRuntimeFieldConditions(component)
	if err != nil {
		return false, err
	}
	conditions := make(map[string]interface{}, len(identityConditions)+3)
	for field, value := range identityConditions {
		conditions[field] = value
	}
	conditions["status"] = component.Status
	conditions["ready_replicas"] = component.ReadyReplicas
	conditions["last_abnormal"] = component.LastAbnormal

	conditionalStore, ok := store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return false, fmt.Errorf("datastore does not support conditional compare and swap")
	}

	updated, err := conditionalStore.CompareAndSwapWithConditions(ctx, entity, conditions, fields)
	if err != nil {
		return false, err
	}
	if updated {
		return true, nil
	}

	exists, err := componentRuntimeFieldExists(ctx, store, entity, identityConditions)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, datastore.ErrRecordNotExist
	}
	return false, nil
}

func normalizeComponentRuntimeFields(updates map[string]interface{}) (map[string]interface{}, error) {
	fields := make(map[string]interface{}, len(updates))
	for field, value := range updates {
		if _, ok := componentRuntimeFieldAllowlist[field]; !ok {
			return nil, fmt.Errorf("unsupported component runtime field %q", field)
		}
		if value == nil {
			return nil, fmt.Errorf("component runtime field %q cannot be nil", field)
		}
		fields[field] = value
	}
	return fields, nil
}

func componentRuntimeFieldExists(ctx context.Context, store datastore.DataStore, entity *model.ApplicationComponent, conditions map[string]interface{}) (bool, error) {
	var dest model.ApplicationComponent
	return store.IsExistByCondition(ctx, entity.TableName(), conditions, &dest)
}

func componentRuntimeFieldConditions(component *model.ApplicationComponent) (*model.ApplicationComponent, map[string]interface{}, error) {
	appID := strings.TrimSpace(component.AppID)
	name := strings.TrimSpace(component.Name)
	if appID == "" {
		return nil, nil, fmt.Errorf("component app_id is empty")
	}
	if name == "" {
		return nil, nil, datastore.ErrPrimaryEmpty
	}

	entity := &model.ApplicationComponent{
		ID:    component.ID,
		AppID: appID,
		Name:  name,
	}
	conditions := map[string]interface{}{
		"app_id": appID,
	}
	if component.ID > 0 {
		conditions["id"] = component.ID
	} else {
		conditions["name"] = name
	}
	return entity, conditions, nil
}

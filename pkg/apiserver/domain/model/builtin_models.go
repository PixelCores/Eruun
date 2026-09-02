package model

import (
	"errors"
	"fmt"
	"reflect"
)

// BuiltinModels returns a validated, ordered snapshot of models owned by the server.
func BuiltinModels() ([]Interface, error) {
	return validateModelSet([]Interface{
		&Applications{},
		&ApplicationComponent{},
		&Workflow{},
		&WorkflowQueue{},
		&JobInfo{},
		&SystemInfo{},
		&SystemSetting{},
		&ProgrammingLanguage{},
		&JobResultOutbox{},
		&WorkflowSchedule{},
	})
}

func validateModelSet(models []Interface) ([]Interface, error) {
	validated := make([]Interface, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	var validationErrors []error
	for _, item := range models {
		if item == nil || (reflect.ValueOf(item).Kind() == reflect.Ptr && reflect.ValueOf(item).IsNil()) {
			validationErrors = append(validationErrors, fmt.Errorf("validate model set: nil model"))
			continue
		}
		tableName := item.TableName()
		if tableName == "" {
			validationErrors = append(validationErrors, fmt.Errorf("validate model %T: empty table name", item))
			continue
		}
		if _, exists := seen[tableName]; exists {
			validationErrors = append(validationErrors, fmt.Errorf("model table name %s conflict", tableName))
			continue
		}
		seen[tableName] = struct{}{}
		validated = append(validated, item)
	}
	if len(validationErrors) > 0 {
		return nil, errors.Join(validationErrors...)
	}
	return validated, nil
}

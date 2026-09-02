package application

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func requireVersionUpdateCleanupInfo(t *testing.T, task *model.WorkflowQueue) model.VersionUpdateCleanupInfo {
	return requireVersionUpdateCleanupInfoVersion(t, task, model.VersionUpdateCleanupInfoVersionV1)
}

func requireVersionUpdateCleanupInfoVersion(t *testing.T, task *model.WorkflowQueue, wantVersion int) model.VersionUpdateCleanupInfo {
	t.Helper()
	require.NotNil(t, task)
	require.NotEmpty(t, task.CleanupInfo)
	var cleanupInfo model.VersionUpdateCleanupInfo
	require.NoError(t, json.Unmarshal([]byte(task.CleanupInfo), &cleanupInfo))
	require.Equal(t, config.JobInfoSourceVersionUpdateRemove, cleanupInfo.Source)
	require.Equal(t, wantVersion, cleanupInfo.Version)
	return cleanupInfo
}

func requireVersionUpdateCleanupComponent(t *testing.T, cleanupInfo model.VersionUpdateCleanupInfo, name string) model.VersionUpdateCleanupComponent {
	t.Helper()
	for _, cleanupComponent := range cleanupInfo.Components {
		if cleanupComponent.Component != nil && cleanupComponent.Component.Name == name {
			return cleanupComponent
		}
	}
	t.Fatalf("cleanup component %s not found", name)
	return model.VersionUpdateCleanupComponent{}
}

func requireVersionUpdateResourceActionInfo(t *testing.T, task *model.WorkflowQueue) model.VersionUpdateResourceActionInfo {
	t.Helper()
	require.NotNil(t, task)
	require.NotEmpty(t, task.ResourceActionInfo)
	var info model.VersionUpdateResourceActionInfo
	require.NoError(t, json.Unmarshal([]byte(task.ResourceActionInfo), &info))
	require.Equal(t, config.JobInfoSourceVersionUpdateAction, info.Source)
	require.Equal(t, 1, info.Version)
	return info
}

func versionUpdateCleanupIndexes(cleanupInfo model.VersionUpdateCleanupInfo) map[string]int {
	indexes := make(map[string]int)
	for _, cleanupComponent := range cleanupInfo.Components {
		if cleanupComponent.Component == nil {
			continue
		}
		indexes[cleanupComponent.Component.Name] = cleanupComponent.InsertBeforeStepIndex
	}
	return indexes
}

func diffFieldNames(fields []apisv1.VersionComponentField) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Field)
	}
	return names
}

func findComponentByAppAndName(store *inMemoryAppStore, appID, name string) *model.ApplicationComponent {
	if store == nil {
		return nil
	}
	for _, component := range store.components {
		if component != nil && component.AppID == appID && component.Name == name {
			return component
		}
	}
	return nil
}

type syncFailWorkflowRepo struct {
	*mockWorkflowRepo
	updateErr error
}

func (m *syncFailWorkflowRepo) Update(ctx context.Context, workflow *model.Workflow) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	return m.mockWorkflowRepo.Update(ctx, workflow)
}

func decodeWorkflowSteps(t *testing.T, js *model.JSONStruct) *model.WorkflowSteps {
	t.Helper()
	var steps model.WorkflowSteps
	if js == nil {
		return &steps
	}
	if err := json.Unmarshal([]byte(mustJSON(t, js)), &steps); err != nil {
		t.Fatalf("decode workflow steps: %v", err)
	}
	return &steps
}

func mustJSON(t testing.TB, value *model.JSONStruct) string {
	t.Helper()
	data, err := value.Bytes()
	require.NoError(t, err)
	return string(data)
}

func requireWorkflowCallbackSuccess(t *testing.T, js *model.JSONStruct, expected string) {
	t.Helper()
	require.NotNil(t, js)
	var callback model.WorkflowCallback
	err := decodeJSONStruct(js, &callback)
	require.NoError(t, err)
	require.Equal(t, expected, callback.Success)
}

type inMemoryAppStore struct {
	apps       map[string]*model.Applications
	workflows  map[string]*model.Workflow
	components map[string]*model.ApplicationComponent
	tasks      map[string]*model.WorkflowQueue
	jobs       []*model.JobInfo
	settings   map[string]*model.SystemSetting

	addWorkflowQueueErr       error
	addJobInfoErr             error
	runtimeUpdateErr          error
	beforeTransaction         func(*inMemoryAppStore)
	skipNilCallbackOnPut      bool
	errExistingApplicationAdd bool
}

func newInMemoryAppStore() *inMemoryAppStore {
	policyValue, _ := json.Marshal(spec.DefaultURLSecurityPolicy())
	return &inMemoryAppStore{
		apps:       make(map[string]*model.Applications),
		workflows:  make(map[string]*model.Workflow),
		components: make(map[string]*model.ApplicationComponent),
		tasks:      make(map[string]*model.WorkflowQueue),
		settings: map[string]*model.SystemSetting{
			model.SystemSettingTypeURLSecurityPolicy: &model.SystemSetting{
				Type:  model.SystemSettingTypeURLSecurityPolicy,
				Value: policyValue,
			},
		},
	}
}

func (s *inMemoryAppStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	snapshot := s.snapshot()
	if s.beforeTransaction != nil {
		s.beforeTransaction(s)
	}
	if err := fn(s); err != nil {
		s.restore(snapshot)
		return err
	}
	return nil
}

func (s *inMemoryAppStore) snapshot() *inMemoryAppStore {
	return &inMemoryAppStore{
		apps:                      cloneApplicationsMap(s.apps),
		workflows:                 cloneWorkflowsMap(s.workflows),
		components:                cloneComponentsMap(s.components),
		tasks:                     cloneWorkflowQueueMap(s.tasks),
		jobs:                      cloneJobInfoSlice(s.jobs),
		settings:                  cloneSystemSettingsMap(s.settings),
		addWorkflowQueueErr:       s.addWorkflowQueueErr,
		addJobInfoErr:             s.addJobInfoErr,
		runtimeUpdateErr:          s.runtimeUpdateErr,
		beforeTransaction:         s.beforeTransaction,
		skipNilCallbackOnPut:      s.skipNilCallbackOnPut,
		errExistingApplicationAdd: s.errExistingApplicationAdd,
	}
}

func (s *inMemoryAppStore) restore(snapshot *inMemoryAppStore) {
	s.apps = cloneApplicationsMap(snapshot.apps)
	s.workflows = cloneWorkflowsMap(snapshot.workflows)
	s.components = cloneComponentsMap(snapshot.components)
	s.tasks = cloneWorkflowQueueMap(snapshot.tasks)
	s.jobs = cloneJobInfoSlice(snapshot.jobs)
	s.settings = cloneSystemSettingsMap(snapshot.settings)
	s.addWorkflowQueueErr = snapshot.addWorkflowQueueErr
	s.addJobInfoErr = snapshot.addJobInfoErr
	s.runtimeUpdateErr = snapshot.runtimeUpdateErr
	s.beforeTransaction = snapshot.beforeTransaction
	s.skipNilCallbackOnPut = snapshot.skipNilCallbackOnPut
	s.errExistingApplicationAdd = snapshot.errExistingApplicationAdd
}

func cloneApplicationsMap(in map[string]*model.Applications) map[string]*model.Applications {
	out := make(map[string]*model.Applications, len(in))
	for key, value := range in {
		if value == nil {
			continue
		}
		cp := *value
		cp.Callback = cloneJSONStruct(value.Callback)
		out[key] = &cp
	}
	return out
}

func cloneWorkflowsMap(in map[string]*model.Workflow) map[string]*model.Workflow {
	out := make(map[string]*model.Workflow, len(in))
	for key, value := range in {
		if value == nil {
			continue
		}
		cp := *value
		cp.Steps = cloneJSONStruct(value.Steps)
		cp.Callback = cloneJSONStruct(value.Callback)
		out[key] = &cp
	}
	return out
}

func cloneComponentsMap(in map[string]*model.ApplicationComponent) map[string]*model.ApplicationComponent {
	out := make(map[string]*model.ApplicationComponent, len(in))
	for key, value := range in {
		if value == nil {
			continue
		}
		cp := *value
		cp.Properties = cloneJSONStruct(value.Properties)
		cp.Traits = cloneJSONStruct(value.Traits)
		if value.SourceWorkloadUID != nil {
			sourceWorkloadUID := *value.SourceWorkloadUID
			cp.SourceWorkloadUID = &sourceWorkloadUID
		}
		if value.ResumeReplicas != nil {
			resumeReplicas := *value.ResumeReplicas
			cp.ResumeReplicas = &resumeReplicas
		}
		out[key] = &cp
	}
	return out
}

func cloneWorkflowQueueMap(in map[string]*model.WorkflowQueue) map[string]*model.WorkflowQueue {
	out := make(map[string]*model.WorkflowQueue, len(in))
	for key, value := range in {
		if value == nil {
			continue
		}
		cp := *value
		if value.IdempotencyKey != nil {
			idempotencyKey := *value.IdempotencyKey
			cp.IdempotencyKey = &idempotencyKey
		}
		cp.Callback = cloneJSONStruct(value.Callback)
		out[key] = &cp
	}
	return out
}

func cloneJobInfoSlice(in []*model.JobInfo) []*model.JobInfo {
	out := make([]*model.JobInfo, 0, len(in))
	for _, value := range in {
		if value == nil {
			continue
		}
		cp := *value
		out = append(out, &cp)
	}
	return out
}

func cloneSystemSettingsMap(in map[string]*model.SystemSetting) map[string]*model.SystemSetting {
	out := make(map[string]*model.SystemSetting, len(in))
	for key, value := range in {
		if value == nil {
			continue
		}
		cp := *value
		if value.Value != nil {
			cp.Value = append([]byte(nil), value.Value...)
		}
		out[key] = &cp
	}
	return out
}

func cloneJSONStruct(raw *model.JSONStruct) *model.JSONStruct {
	if raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var cp model.JSONStruct
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil
	}
	return &cp
}

func (s *inMemoryAppStore) Add(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		if s.errExistingApplicationAdd {
			if _, ok := s.apps[v.ID]; ok {
				return datastore.ErrRecordExist
			}
		}
		cp := *v
		s.apps[v.ID] = &cp
	case *model.Workflow:
		cp := *v
		s.workflows[v.ID] = &cp
	case *model.ApplicationComponent:
		cp := *v
		s.components[v.Name] = &cp
	case *model.WorkflowQueue:
		if s.addWorkflowQueueErr != nil {
			return s.addWorkflowQueueErr
		}
		cp := *v
		cp.Callback = cloneJSONStruct(v.Callback)
		s.tasks[v.TaskID] = &cp
	case *model.JobInfo:
		if s.addJobInfoErr != nil {
			return s.addJobInfoErr
		}
		cp := *v
		s.jobs = append(s.jobs, &cp)
	case *model.SystemSetting:
		cp := *v
		if s.settings == nil {
			s.settings = make(map[string]*model.SystemSetting)
		}
		s.settings[v.Type] = &cp
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

func (s *inMemoryAppStore) Put(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Workflow:
		if existing, ok := s.workflows[v.ID]; ok {
			callback := existing.Callback
			*existing = *v
			if s.skipNilCallbackOnPut && v.Callback == nil {
				existing.Callback = callback
			}
		} else {
			cp := *v
			s.workflows[v.ID] = &cp
		}
	case *model.Applications:
		if existing, ok := s.apps[v.ID]; ok {
			callback := existing.Callback
			*existing = *v
			if s.skipNilCallbackOnPut && v.Callback == nil {
				existing.Callback = callback
			}
		} else {
			cp := *v
			s.apps[v.ID] = &cp
		}
	case *model.ApplicationComponent:
		if existing, ok := s.components[v.Name]; ok {
			*existing = *v
		} else {
			cp := *v
			s.components[v.Name] = &cp
		}
	case *model.WorkflowQueue:
		if existing, ok := s.tasks[v.TaskID]; ok {
			*existing = *v
			existing.Callback = cloneJSONStruct(v.Callback)
		} else {
			cp := *v
			cp.Callback = cloneJSONStruct(v.Callback)
			s.tasks[v.TaskID] = &cp
		}
	case *model.SystemSetting:
		cp := *v
		if s.settings == nil {
			s.settings = make(map[string]*model.SystemSetting)
		}
		s.settings[v.Type] = &cp
	}
	return nil
}

func (s *inMemoryAppStore) Delete(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		delete(s.apps, v.ID)
	case *model.Workflow:
		delete(s.workflows, v.ID)
	case *model.ApplicationComponent:
		delete(s.components, v.Name)
	case *model.WorkflowQueue:
		delete(s.tasks, v.TaskID)
	}
	return nil
}

func (s *inMemoryAppStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *inMemoryAppStore) Get(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		if v.ID != "" {
			if app, ok := s.apps[v.ID]; ok {
				*v = *app
				return nil
			}
		} else if v.Name != "" {
			for _, app := range s.apps {
				if app.Name == v.Name {
					*v = *app
					return nil
				}
			}
		}
		return datastore.ErrRecordNotExist
	case *model.Workflow:
		if wf, ok := s.workflows[v.ID]; ok {
			*v = *wf
			return nil
		}
		return datastore.ErrRecordNotExist
	case *model.ApplicationComponent:
		if v.Name != "" {
			if comp, ok := s.components[v.Name]; ok {
				*v = *comp
				return nil
			}
		}
		for _, comp := range s.components {
			if v.AppID != "" && comp.AppID == v.AppID {
				*v = *comp
				return nil
			}
		}
		return datastore.ErrRecordNotExist
	case *model.WorkflowQueue:
		if task, ok := s.tasks[v.TaskID]; ok {
			*v = *task
			return nil
		}
		return datastore.ErrRecordNotExist
	case *model.SystemSetting:
		if setting, ok := s.settings[v.Type]; ok {
			*v = *setting
			return nil
		}
		return datastore.ErrRecordNotExist
	default:
		return nil
	}
}

func (s *inMemoryAppStore) List(_ context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	switch q := query.(type) {
	case *model.Applications:
		apps := make([]*model.Applications, 0, len(s.apps))
		for _, app := range s.apps {
			if q.ID != "" && app.ID != q.ID {
				continue
			}
			if q.Name != "" && app.Name != q.Name {
				continue
			}
			if q.Version != "" && app.Version != q.Version {
				continue
			}
			if q.Project != "" && app.Project != q.Project {
				continue
			}
			if q.TemplateEnabled && !app.TemplateEnabled {
				continue
			}
			cp := *app
			apps = append(apps, &cp)
		}
		sortApplicationsForList(apps, opts)
		return paginateApplicationsForList(apps, opts), nil
	case *model.Workflow:
		var result []datastore.Entity
		for _, wf := range s.workflows {
			if q.AppID != "" && wf.AppID != q.AppID {
				continue
			}
			result = append(result, wf)
		}
		return result, nil
	case *model.ApplicationComponent:
		var result []datastore.Entity
		for _, comp := range s.components {
			if q.AppID != "" && comp.AppID != q.AppID {
				continue
			}
			if !componentMatchesInFilters(comp, opts) {
				continue
			}
			result = append(result, comp)
		}
		return result, nil
	case *model.WorkflowQueue:
		var result []datastore.Entity
		for _, task := range s.tasks {
			if q.AppID != "" && task.AppID != q.AppID {
				continue
			}
			if q.TaskID != "" && task.TaskID != q.TaskID {
				continue
			}
			if !workflowQueueMatchesInFilters(task, opts) {
				continue
			}
			result = append(result, task)
		}
		return result, nil
	case *model.JobInfo:
		var result []datastore.Entity
		for _, job := range s.jobs {
			if q.TaskID != "" && job.TaskID != q.TaskID {
				continue
			}
			result = append(result, job)
		}
		return result, nil
	case *model.SystemSetting:
		var result []datastore.Entity
		for _, setting := range s.settings {
			if q.Type != "" && setting.Type != q.Type {
				continue
			}
			result = append(result, setting)
		}
		return result, nil
	default:
		return nil, nil
	}
}

func componentMatchesInFilters(component *model.ApplicationComponent, opts *datastore.ListOptions) bool {
	if component == nil || opts == nil {
		return true
	}
	for _, in := range opts.FilterOptions.In {
		if in.Key != workflowAppIDColumn {
			continue
		}
		if len(in.Values) == 0 {
			return false
		}
		matched := false
		for _, value := range in.Values {
			if component.AppID == value {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func sortApplicationsForList(apps []*model.Applications, opts *datastore.ListOptions) {
	if opts == nil || len(opts.SortBy) == 0 {
		return
	}
	sort.Slice(apps, func(i, j int) bool {
		left := apps[i]
		right := apps[j]
		for _, sortOption := range opts.SortBy {
			switch sortOption.Key {
			case "update_time":
				leftValue := left.UpdateTime.UnixNano()
				rightValue := right.UpdateTime.UnixNano()
				if leftValue == rightValue {
					continue
				}
				if sortOption.Order == datastore.SortOrderDescending {
					return leftValue > rightValue
				}
				return leftValue < rightValue
			case "id":
				if left.ID == right.ID {
					continue
				}
				if sortOption.Order == datastore.SortOrderDescending {
					return left.ID > right.ID
				}
				return left.ID < right.ID
			}
		}
		return false
	})
}

func paginateApplicationsForList(apps []*model.Applications, opts *datastore.ListOptions) []datastore.Entity {
	if opts == nil || opts.PageSize <= 0 || opts.Page <= 0 {
		result := make([]datastore.Entity, 0, len(apps))
		for _, app := range apps {
			result = append(result, app)
		}
		return result
	}
	start := (opts.Page - 1) * opts.PageSize
	if start >= len(apps) {
		return []datastore.Entity{}
	}
	end := start + opts.PageSize
	if end > len(apps) {
		end = len(apps)
	}
	result := make([]datastore.Entity, 0, end-start)
	for _, app := range apps[start:end] {
		result = append(result, app)
	}
	return result
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

func (s *inMemoryAppStore) CompareAndSwap(_ context.Context, entity datastore.Entity, conditionField string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	switch v := entity.(type) {
	case *model.Applications:
		app, ok := s.apps[v.ID]
		if !ok || !matchesInMemoryCondition(app.ID, conditionField, conditionValue) {
			return false, nil
		}
		applyInMemoryApplicationUpdates(app, updates)
		return true, nil
	case *model.Workflow:
		workflow, ok := s.workflows[v.ID]
		if !ok || !matchesInMemoryCondition(workflow.ID, conditionField, conditionValue) {
			return false, nil
		}
		applyInMemoryWorkflowUpdates(workflow, updates)
		return true, nil
	default:
		return false, nil
	}
}

func (s *inMemoryAppStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if s.runtimeUpdateErr != nil {
		return false, s.runtimeUpdateErr
	}
	if _, ok := entity.(*model.ApplicationComponent); !ok {
		return false, nil
	}
	for _, component := range s.components {
		if !matchesComponentRuntimeConditions(component, conditions) {
			continue
		}
		applyComponentRuntimeUpdates(component, updates)
		return true, nil
	}
	return false, nil
}

func workflowQueueMatchesInFilters(task *model.WorkflowQueue, opts *datastore.ListOptions) bool {
	if task == nil || opts == nil {
		return true
	}
	for _, filter := range opts.In {
		switch strings.ToLower(filter.Key) {
		case "status":
			if !stringInList(string(task.Status), filter.Values) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func stringInList(value string, values []string) bool {
	for _, item := range values {
		if value == item {
			return true
		}
	}
	return false
}

func matchesInMemoryCondition(id, conditionField string, conditionValue interface{}) bool {
	if strings.ToLower(conditionField) != "id" {
		return false
	}
	value, ok := conditionValue.(string)
	return ok && value == id
}

func applyInMemoryApplicationUpdates(app *model.Applications, updates map[string]interface{}) {
	for field, value := range updates {
		switch strings.ToLower(field) {
		case "callback":
			app.Callback = callbackUpdateValue(value)
		}
	}
}

func applyInMemoryWorkflowUpdates(workflow *model.Workflow, updates map[string]interface{}) {
	for field, value := range updates {
		switch strings.ToLower(field) {
		case "callback":
			workflow.Callback = callbackUpdateValue(value)
		}
	}
}

func matchesComponentRuntimeConditions(component *model.ApplicationComponent, conditions map[string]interface{}) bool {
	if component == nil {
		return false
	}
	for field, value := range conditions {
		switch strings.ToLower(field) {
		case "app_id":
			appID, ok := value.(string)
			if !ok || component.AppID != appID {
				return false
			}
		case "name":
			name, ok := value.(string)
			if !ok || component.Name != name {
				return false
			}
		case "id":
			id, ok := value.(int)
			if !ok || component.ID != id {
				return false
			}
		case "source_workload_uid":
			sourceUID, ok := value.(string)
			if !ok || component.SourceWorkloadUID == nil || *component.SourceWorkloadUID != sourceUID {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func applyComponentRuntimeUpdates(component *model.ApplicationComponent, updates map[string]interface{}) {
	if component == nil {
		return
	}
	for field, value := range updates {
		switch strings.ToLower(field) {
		case "status":
			if status, ok := value.(string); ok {
				component.Status = status
			}
		case "ready_replicas":
			if readyReplicas, ok := value.(int32); ok {
				component.ReadyReplicas = readyReplicas
			}
		case "last_abnormal":
			if lastAbnormal, ok := value.(string); ok {
				component.LastAbnormal = lastAbnormal
			}
		case "resume_replicas":
			switch resumeReplicas := value.(type) {
			case int32:
				component.ResumeReplicas = &resumeReplicas
			case *int32:
				if resumeReplicas == nil {
					component.ResumeReplicas = nil
					continue
				}
				valueCopy := *resumeReplicas
				component.ResumeReplicas = &valueCopy
			}
		}
	}
}

func callbackUpdateValue(value interface{}) *model.JSONStruct {
	if value == nil {
		return nil
	}
	callback, _ := value.(*model.JSONStruct)
	return cloneJSONStruct(callback)
}

var _ datastore.DataStore = (*inMemoryAppStore)(nil)

type cleanupStore struct {
	app              *model.Applications
	components       []*model.ApplicationComponent
	applications     map[string]*model.Applications
	runtimeUpdateErr error
}

func (c *cleanupStore) Add(context.Context, datastore.Entity) error { return nil }

func (c *cleanupStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (c *cleanupStore) Put(context.Context, datastore.Entity) error { return nil }

func (c *cleanupStore) Delete(context.Context, datastore.Entity) error { return nil }

func (c *cleanupStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (c *cleanupStore) Get(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		if app, ok := c.applications[v.ID]; ok {
			*v = *app
			return nil
		}
		return datastore.ErrRecordNotExist
	default:
		return datastore.ErrRecordNotExist
	}
}

func (c *cleanupStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	switch query.(type) {
	case *model.ApplicationComponent:
		entities := make([]datastore.Entity, len(c.components))
		for i, comp := range c.components {
			entities[i] = comp
		}
		return entities, nil
	default:
		return nil, nil
	}
}

func (c *cleanupStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (c *cleanupStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (c *cleanupStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (c *cleanupStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func (c *cleanupStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if c.runtimeUpdateErr != nil {
		return false, c.runtimeUpdateErr
	}
	if _, ok := entity.(*model.ApplicationComponent); !ok {
		return false, nil
	}
	for _, component := range c.components {
		if !matchesComponentRuntimeConditions(component, conditions) {
			continue
		}
		applyComponentRuntimeUpdates(component, updates)
		return true, nil
	}
	return false, nil
}

var _ datastore.DataStore = (*cleanupStore)(nil)

func boolPtr(b bool) *bool {
	return &b
}

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

package systemsetting

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type mockSystemSettingRepo struct {
	items     map[string]*model.SystemSetting
	lookupErr error
	createErr error
	deleteErr error
}

func newMockSystemSettingRepo() *mockSystemSettingRepo {
	return &mockSystemSettingRepo{items: make(map[string]*model.SystemSetting)}
}

func (m *mockSystemSettingRepo) FindByType(_ context.Context, settingType string) (*model.SystemSetting, error) {
	if m.lookupErr != nil {
		return nil, m.lookupErr
	}
	item, ok := m.items[settingType]
	if !ok {
		return nil, datastore.ErrRecordNotExist
	}
	return item, nil
}

func (m *mockSystemSettingRepo) Create(_ context.Context, setting *model.SystemSetting) error {
	if m.createErr != nil {
		return m.createErr
	}
	if _, ok := m.items[setting.Type]; ok {
		return datastore.ErrRecordExist
	}
	clone := *setting
	m.items[setting.Type] = &clone
	return nil
}

func (m *mockSystemSettingRepo) Update(_ context.Context, setting *model.SystemSetting) error {
	if _, ok := m.items[setting.Type]; !ok {
		return datastore.ErrRecordNotExist
	}
	clone := *setting
	m.items[setting.Type] = &clone
	return nil
}

func (m *mockSystemSettingRepo) Delete(_ context.Context, setting *model.SystemSetting) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	if _, ok := m.items[setting.Type]; !ok {
		return datastore.ErrRecordNotExist
	}
	delete(m.items, setting.Type)
	return nil
}

func (m *mockSystemSettingRepo) List(_ context.Context, _ datastore.ListOptions) ([]*model.SystemSetting, error) {
	out := make([]*model.SystemSetting, 0, len(m.items))
	for _, item := range m.items {
		out = append(out, item)
	}
	return out, nil
}

func TestSystemSettingService_CreateAndGet(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	value := json.RawMessage(`{"nodeSelector":{"node.kubernetes.io/test":"on"}}`)
	created, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeNodeSelector,
		Value: value,
	})
	require.NoError(t, err)
	require.Equal(t, model.SystemSettingTypeNodeSelector, created.Type)

	got, err := svc.Get(ctx, model.SystemSettingTypeNodeSelector)
	require.NoError(t, err)
	require.JSONEq(t, string(value), string(got.Value))
}

func TestSystemSettingServiceMapsWrappedRepositoryErrors(t *testing.T) {
	for _, operation := range []string{"create", "get", "update", "delete"} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wrapped=%t", operation, wrapped), func(t *testing.T) {
				repoErr := error(datastore.ErrRecordNotExist)
				wantErr := error(bcode.ErrSystemSettingNotFound)
				if operation == "create" {
					repoErr = datastore.ErrRecordExist
					wantErr = bcode.ErrSystemSettingExists
				}
				if wrapped {
					repoErr = fmt.Errorf("system setting repository: %w", repoErr)
				}
				repo := newMockSystemSettingRepo()
				repo.lookupErr, repo.createErr, repo.deleteErr = repoErr, repoErr, repoErr
				svc := &systemSettingServiceImpl{SettingRepo: repo}
				ctx := context.Background()
				settingType := model.SystemSettingTypeNodeSelector
				var err error
				switch operation {
				case "create":
					_, err = svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: settingType, Value: json.RawMessage(`{}`)})
				case "get":
					_, err = svc.Get(ctx, settingType)
				case "update":
					_, err = svc.Update(ctx, settingType, apisv1.UpdateSystemSettingRequest{Value: json.RawMessage(`{}`)})
				case "delete":
					err = svc.Delete(ctx, settingType)
				}
				require.ErrorIs(t, err, wantErr)
			})
		}
	}
}

func TestSystemSettingServiceListHidesInternalRows(t *testing.T) {
	repo := newMockSystemSettingRepo()
	repo.items[model.SystemSettingTypeNodeSelector] = &model.SystemSetting{
		Type:  model.SystemSettingTypeNodeSelector,
		Value: json.RawMessage(`{}`),
	}
	repo.items["migration.application-management-mode.v1"] = &model.SystemSetting{
		Type:  "migration.application-management-mode.v1",
		Value: json.RawMessage(`{"completed":true}`),
	}
	svc := &systemSettingServiceImpl{SettingRepo: repo}

	items, err := svc.List(context.Background())
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, model.SystemSettingTypeNodeSelector, items[0].Type)
}

func TestSystemSettingService_Validation(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	_, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: "", Value: json.RawMessage(`{}`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingTypeInvalid)

	_, err = svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: model.SystemSettingTypeNodeSelector, Value: json.RawMessage(`"text"`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: model.SystemSettingTypeRBACPolicies, Value: json.RawMessage(`{"serviceAccount":"sa"}`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	validURLSecurityPolicy := buildValidURLSecurityPolicySettingValue(t)
	_, err = svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: model.SystemSettingTypeURLSecurityPolicy, Value: validURLSecurityPolicy})
	require.NoError(t, err)

	_, err = svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypePodRestartMonitor,
		Value: json.RawMessage(`{"enabled":true,"windowSeconds":1800,"threshold":3}`),
	})
	require.NoError(t, err)

	_, err = svc.Update(ctx, model.SystemSettingTypePodRestartMonitor, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled":true,"windowSeconds":1800}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypePodRestartMonitor, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled":true,"windowSeconds":0,"threshold":3}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypePodRestartMonitor, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled":true,"windowSeconds":1800,"threshold":0}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypeURLSecurityPolicy, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"allowedHostPatterns":["*.*.svc.cluster.local"]}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypeURLSecurityPolicy, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"allowedCIDRs":["10.0.0.0"]}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)
}

func TestSystemSettingServiceRejectsUnsupportedType(t *testing.T) {
	// Invalid types must fail before reaching persistence, even with valid JSON.
	svc := &systemSettingServiceImpl{}
	ctx := context.Background()
	_, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: "unsupported", Value: json.RawMessage(`{}`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingTypeInvalid)
	_, err = svc.Update(ctx, "unsupported", apisv1.UpdateSystemSettingRequest{Value: json.RawMessage(`{}`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingTypeInvalid)
	_, err = svc.Get(ctx, "unsupported")
	require.ErrorIs(t, err, bcode.ErrSystemSettingTypeInvalid)
	require.ErrorIs(t, svc.Delete(ctx, "unsupported"), bcode.ErrSystemSettingTypeInvalid)
}

func buildValidURLSecurityPolicySettingValue(t *testing.T) json.RawMessage {
	t.Helper()
	payload := map[string]interface{}{
		"allowPrivateByDefault": false,
		"allowedHostPatterns": []string{
			"*.svc.cluster.local",
			"*.paas.example.com",
		},
		"allowedCIDRs": []string{
			"10.0.0.0/8",
		},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return json.RawMessage(data)
}

func TestSystemSettingService_UpdateNotFound(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	_, err := svc.Update(ctx, model.SystemSettingTypeNodeSelector, apisv1.UpdateSystemSettingRequest{Value: json.RawMessage(`{}`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingNotFound)
}

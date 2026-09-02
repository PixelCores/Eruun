package systemsetting

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	wfcloudjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob"
	wfcloudcontract "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type mockSystemSettingRepo struct {
	items map[string]*model.SystemSetting
}

func newMockSystemSettingRepo() *mockSystemSettingRepo {
	return &mockSystemSettingRepo{items: make(map[string]*model.SystemSetting)}
}

func (m *mockSystemSettingRepo) FindByType(_ context.Context, settingType string) (*model.SystemSetting, error) {
	item, ok := m.items[settingType]
	if !ok {
		return nil, datastore.ErrRecordNotExist
	}
	return item, nil
}

func (m *mockSystemSettingRepo) Create(_ context.Context, setting *model.SystemSetting) error {
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

type fakeCloudSettingProvider struct {
	settingType        string
	connectivityErr    error
	connectivityChecks int
}

func (f *fakeCloudSettingProvider) Name() string {
	return "aliyun"
}

func (f *fakeCloudSettingProvider) NewRuntime(context.Context, *wfcloudcontract.CloudJobRequest) (wfcloudcontract.CloudRuntime, error) {
	return nil, nil
}

func (f *fakeCloudSettingProvider) ActionRegistry() wfcloudcontract.CloudActionRegistry {
	return nil
}

func (f *fakeCloudSettingProvider) SystemSettingType() string {
	return f.settingType
}

func (f *fakeCloudSettingProvider) NormalizeSystemSettingValue(value json.RawMessage) (json.RawMessage, error) {
	return spec.NormalizeAliyunCloudSettingValue(value)
}

func (f *fakeCloudSettingProvider) SanitizeSystemSettingValue(value json.RawMessage) json.RawMessage {
	setting, err := spec.ParseAliyunCloudSetting(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	setting.AccessKeySecret = spec.AliyunCloudSecretMaskedValue
	out, err := json.Marshal(setting)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(out)
}

func (f *fakeCloudSettingProvider) ValidateSystemSettingConnectivity(context.Context, json.RawMessage) error {
	f.connectivityChecks++
	return f.connectivityErr
}

func registerAliyunCloudSettingProvider(t *testing.T, connectivityErr error) *fakeCloudSettingProvider {
	t.Helper()
	provider := &fakeCloudSettingProvider{
		settingType:     model.SystemSettingTypeAliyunCloud,
		connectivityErr: connectivityErr,
	}
	wfcloudjob.ResetCloudProvidersForTest()
	wfcloudjob.RegisterCloudProvider(provider)
	t.Cleanup(wfcloudjob.RestoreBuiltinCloudProvidersForTest)
	return provider
}

func resetCloudProviderRegistryForTest(t *testing.T) {
	t.Helper()
	wfcloudjob.ResetCloudProvidersForTest()
	t.Cleanup(wfcloudjob.RestoreBuiltinCloudProvidersForTest)
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
	cloudProvider := registerAliyunCloudSettingProvider(t, nil)

	_, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: "", Value: json.RawMessage(`{}`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingTypeInvalid)

	_, err = svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: model.SystemSettingTypeNodeSelector, Value: json.RawMessage(`"text"`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: model.SystemSettingTypeRBACPolicies, Value: json.RawMessage(`{"serviceAccount":"sa"}`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	validAPIAuth := buildValidAPIAuthSettingValue(t)
	_, err = svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: model.SystemSettingTypeAPIAuth, Value: validAPIAuth})
	require.NoError(t, err)

	validOAuthAuth := buildValidOAuthAuthSettingValue(t)
	_, err = svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: model.SystemSettingTypeOAuthAuth, Value: validOAuthAuth})
	require.NoError(t, err)

	validAliyunCloud := buildValidAliyunCloudSettingValue(t)
	createdAliyunCloud, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: model.SystemSettingTypeAliyunCloud, Value: validAliyunCloud})
	require.NoError(t, err)
	require.JSONEq(t, `{
		"accessKeyId":"test-ak",
		"accessKeySecret":"******",
		"endpoint":"nas.cn-hangzhou.aliyuncs.com",
		"regionId":"cn-hangzhou",
		"zoneId":"cn-hangzhou-i",
		"vpcId":"vpc-001",
		"vswId":"vsw-001"
	}`, string(createdAliyunCloud.Value))

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

	_, err = svc.Update(ctx, model.SystemSettingTypeAPIAuth, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled": true, "jwt": {"algorithms": ["HS256"]}, "authorization": {"routes":[{"method":"GET","path":"/api/v1/applications","roles":["reader"]}]}}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypeAPIAuth, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled": true, "jwt": {"algorithms": ["HS256"], "hs256": {"secret": "x"}}, "authorization": {"routes":[{"method":"TRACE","path":"/api/v1/applications","roles":["reader"]}]}}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypeAPIAuth, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled": true, "jwt": {"algorithms": ["HS256"], "hs256": {"secret": "x"}}, "authorization": {"routes":[{"method":"GET","path":"","roles":["reader"]}]}}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypeAPIAuth, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled": true, "jwt": {"algorithms": ["RS256"], "rs256": {"publicKeys": [{"kid":"bad","pem":"not-a-pem"}]}}, "authorization": {"routes":[{"method":"GET","path":"/api/v1/applications","roles":["reader"]}]}}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypeAPIAuth, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled": true, "jwt": {"algorithms": ["HS256"], "hs256": {"secret": "******"}}, "authorization": {"routes":[{"method":"GET","path":"/api/v1/applications","roles":["reader"]}]}}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypeAPIAuth, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled": false}`),
	})
	require.NoError(t, err)

	_, err = svc.Update(ctx, model.SystemSettingTypeOAuthAuth, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"enabled": true, "providers": {"google": {"clientId":"id","clientSecret":"******","redirectURI":"https://eruun.example.com/api/v1/auth/oauth2/google/callback"}}}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypeAliyunCloud, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"accessKeyId":"test-ak","accessKeySecret":"******","regionId":"cn-hangzhou"}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.Update(ctx, model.SystemSettingTypeAliyunCloud, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{"accessKeyId":"test-ak","accessKeySecret":"test-sk","regionId":"cn-hangzhou","region_endpoint":"nas.cn-hangzhou.aliyuncs.com"}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)
	require.Equal(t, 1, cloudProvider.connectivityChecks)
}

func TestSystemSettingService_APIAuthValueRedaction(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	input := json.RawMessage(`{
		"enabled": true,
		"jwt": {
			"algorithms": ["HS256"],
			"hs256": {"secret": "super-secret"}
		},
		"authorization": {
			"defaultEffect": "deny",
			"routes": [
				{"method": "GET", "path": "/api/v1/applications", "roles": ["reader"]}
			]
		}
	}`)

	created, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeAPIAuth,
		Value: input,
	})
	require.NoError(t, err)
	require.NotContains(t, string(created.Value), "super-secret")
	require.Contains(t, string(created.Value), apiAuthSecretMaskedValue)

	stored, ok := repo.items[model.SystemSettingTypeAPIAuth]
	require.True(t, ok)
	require.Contains(t, string(stored.Value), "super-secret")

	got, err := svc.Get(ctx, model.SystemSettingTypeAPIAuth)
	require.NoError(t, err)
	require.NotContains(t, string(got.Value), "super-secret")
	require.Contains(t, string(got.Value), apiAuthSecretMaskedValue)

	list, err := svc.List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, model.SystemSettingTypeAPIAuth, list[0].Type)
	require.NotContains(t, string(list[0].Value), "super-secret")
	require.Contains(t, string(list[0].Value), apiAuthSecretMaskedValue)
}

func TestSystemSettingService_APIAuthValueRedactionCaseInsensitiveKeys(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	input := json.RawMessage(`{
		"enabled": true,
		"JWT": {
			"algorithms": ["HS256"],
			"HS256": {"SECRET": "super-secret-mixed"}
		},
		"authorization": {
			"defaultEffect": "deny",
			"routes": [
				{"method": "GET", "path": "/api/v1/applications", "roles": ["reader"]}
			]
		}
	}`)

	created, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeAPIAuth,
		Value: input,
	})
	require.NoError(t, err)
	require.NotContains(t, string(created.Value), "super-secret-mixed")
	require.Contains(t, string(created.Value), apiAuthSecretMaskedValue)

	stored, ok := repo.items[model.SystemSettingTypeAPIAuth]
	require.True(t, ok)
	require.Contains(t, string(stored.Value), "super-secret-mixed")

	got, err := svc.Get(ctx, model.SystemSettingTypeAPIAuth)
	require.NoError(t, err)
	require.NotContains(t, string(got.Value), "super-secret-mixed")
	require.Contains(t, string(got.Value), apiAuthSecretMaskedValue)
}

func TestSystemSettingService_APIAuthValueRedactionMasksDuplicateCaseVariants(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	input := json.RawMessage(`{
		"enabled": true,
		"JWT": {
			"algorithms": ["HS256"],
			"HS256": {"SECRET": "upper-secret"}
		},
		"jwt": {
			"algorithms": ["HS256"],
			"hs256": {"secret": "lower-secret"}
		},
		"authorization": {
			"defaultEffect": "deny",
			"routes": [
				{"method": "GET", "path": "/api/v1/applications", "roles": ["reader"]}
			]
		}
	}`)

	created, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeAPIAuth,
		Value: input,
	})
	require.NoError(t, err)
	require.NotContains(t, string(created.Value), "upper-secret")
	require.NotContains(t, string(created.Value), "lower-secret")
	require.Contains(t, string(created.Value), apiAuthSecretMaskedValue)
}

func TestSystemSettingService_OAuthAuthValueRedaction(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	input := buildValidOAuthAuthSettingValue(t)
	created, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeOAuthAuth,
		Value: input,
	})
	require.NoError(t, err)
	require.NotContains(t, string(created.Value), "google-secret")
	require.Contains(t, string(created.Value), oauthClientSecretMaskedValue)

	stored, ok := repo.items[model.SystemSettingTypeOAuthAuth]
	require.True(t, ok)
	require.Contains(t, string(stored.Value), "google-secret")

	got, err := svc.Get(ctx, model.SystemSettingTypeOAuthAuth)
	require.NoError(t, err)
	require.NotContains(t, string(got.Value), "google-secret")
	require.Contains(t, string(got.Value), oauthClientSecretMaskedValue)

	list, err := svc.List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, model.SystemSettingTypeOAuthAuth, list[0].Type)
	require.NotContains(t, string(list[0].Value), "google-secret")
	require.Contains(t, string(list[0].Value), oauthClientSecretMaskedValue)
}

func TestSystemSettingService_OAuthAuthValueRedactionCaseInsensitiveKeys(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	input := json.RawMessage(`{
		"enabled": true,
		"Providers": {
			"Google": {
				"ClientID": "google-client-id",
				"ClientSecret": "google-secret-mixed",
				"RedirectURI": "https://eruun.example.com/api/v1/auth/oauth2/google/callback",
				"Scopes": ["openid", "email", "profile"]
			}
		},
		"JWTIssue": {
			"Issuer": "eruun",
			"Audience": "eruun-api",
			"TTLSeconds": 1800
		},
		"RoleMapping": {
			"DefaultRoles": ["reader"]
		},
		"Security": {
			"StateTTLSeconds": 300
		}
	}`)

	created, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeOAuthAuth,
		Value: input,
	})
	require.NoError(t, err)
	require.NotContains(t, string(created.Value), "google-secret-mixed")
	require.Contains(t, string(created.Value), oauthClientSecretMaskedValue)

	stored, ok := repo.items[model.SystemSettingTypeOAuthAuth]
	require.True(t, ok)
	require.Contains(t, string(stored.Value), "google-secret-mixed")

	got, err := svc.Get(ctx, model.SystemSettingTypeOAuthAuth)
	require.NoError(t, err)
	require.NotContains(t, string(got.Value), "google-secret-mixed")
	require.Contains(t, string(got.Value), oauthClientSecretMaskedValue)
}

func TestSystemSettingService_OAuthAuthValueRedactionMasksDuplicateCaseVariants(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	input := json.RawMessage(`{
		"enabled": true,
		"Providers": {
			"Google": {
				"ClientID": "google-client-id",
				"ClientSecret": "upper-google-secret",
				"RedirectURI": "https://eruun.example.com/api/v1/auth/oauth2/google/callback",
				"Scopes": ["openid", "email", "profile"]
			}
		},
		"providers": {
			"google": {
				"clientId": "google-client-id",
				"clientSecret": "lower-google-secret",
				"redirectURI": "https://eruun.example.com/api/v1/auth/oauth2/google/callback",
				"scopes": ["openid", "email", "profile"]
			}
		},
		"jwtIssue": {
			"issuer": "eruun",
			"audience": "eruun-api",
			"ttlSeconds": 1800
		},
		"roleMapping": {
			"defaultRoles": ["reader"]
		},
		"security": {
			"stateTTLSeconds": 300
		}
	}`)

	created, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeOAuthAuth,
		Value: input,
	})
	require.NoError(t, err)
	require.NotContains(t, string(created.Value), "upper-google-secret")
	require.NotContains(t, string(created.Value), "lower-google-secret")
	require.Contains(t, string(created.Value), oauthClientSecretMaskedValue)
}

func TestSystemSettingService_AliyunCloudValueNormalizationAndRedaction(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()
	cloudProvider := registerAliyunCloudSettingProvider(t, nil)

	input := json.RawMessage(`{
		"access_key_id": " test-ak ",
		"access_key_secret": " test-sk ",
		"endpoint": " nas.cn-hangzhou.aliyuncs.com ",
		"region_id": " cn-hangzhou ",
		"zone_id": " cn-hangzhou-i ",
		"vpc_id": " vpc-001 ",
		"vsw_id": " vsw-001 "
	}`)

	created, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeAliyunCloud,
		Value: input,
	})
	require.NoError(t, err)
	require.NotContains(t, string(created.Value), "test-sk")
	require.Contains(t, string(created.Value), spec.AliyunCloudSecretMaskedValue)
	require.Equal(t, 1, cloudProvider.connectivityChecks)

	stored, ok := repo.items[model.SystemSettingTypeAliyunCloud]
	require.True(t, ok)
	require.JSONEq(t, `{
		"accessKeyId":"test-ak",
		"accessKeySecret":"test-sk",
		"endpoint":"nas.cn-hangzhou.aliyuncs.com",
		"regionId":"cn-hangzhou",
		"zoneId":"cn-hangzhou-i",
		"vpcId":"vpc-001",
		"vswId":"vsw-001"
	}`, string(stored.Value))

	got, err := svc.Get(ctx, model.SystemSettingTypeAliyunCloud)
	require.NoError(t, err)
	require.NotContains(t, string(got.Value), "test-sk")
	require.Contains(t, string(got.Value), spec.AliyunCloudSecretMaskedValue)
	require.JSONEq(t, `{
		"accessKeyId":"test-ak",
		"accessKeySecret":"******",
		"endpoint":"nas.cn-hangzhou.aliyuncs.com",
		"regionId":"cn-hangzhou",
		"zoneId":"cn-hangzhou-i",
		"vpcId":"vpc-001",
		"vswId":"vsw-001"
	}`, string(got.Value))
}

func TestSystemSettingService_AliyunCloudFallbackSupportWithoutRegistry(t *testing.T) {
	resetCloudProviderRegistryForTest(t)

	repo := newMockSystemSettingRepo()
	repo.items[model.SystemSettingTypeAliyunCloud] = &model.SystemSetting{
		Type: model.SystemSettingTypeAliyunCloud,
		Value: json.RawMessage(`{
			"accessKeyId":"test-ak",
			"accessKeySecret":"test-sk",
			"endpoint":"nas.cn-hangzhou.aliyuncs.com",
			"regionId":"cn-hangzhou",
			"zoneId":"cn-hangzhou-i",
			"vpcId":"vpc-001",
			"vswId":"vsw-001"
		}`),
	}
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	got, err := svc.Get(ctx, model.SystemSettingTypeAliyunCloud)
	require.NoError(t, err)
	require.NotContains(t, string(got.Value), "test-sk")
	require.Contains(t, string(got.Value), spec.AliyunCloudSecretMaskedValue)

	list, err := svc.List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.NotContains(t, string(list[0].Value), "test-sk")
	require.Contains(t, string(list[0].Value), spec.AliyunCloudSecretMaskedValue)

	normalized, err := normalizeAndValidateSettingValue(model.SystemSettingTypeAliyunCloud, json.RawMessage(`{
		"access_key_id":" next-ak ",
		"access_key_secret":" next-sk ",
		"endpoint":" nas.cn-shanghai.aliyuncs.com ",
		"region_id":" cn-shanghai ",
		"zone_id":" cn-shanghai-b ",
		"vpc_id":" vpc-002 ",
		"vsw_id":" vsw-002 "
	}`))
	require.NoError(t, err)
	require.JSONEq(t, `{
		"accessKeyId":"next-ak",
		"accessKeySecret":"next-sk",
		"endpoint":"nas.cn-shanghai.aliyuncs.com",
		"regionId":"cn-shanghai",
		"zoneId":"cn-shanghai-b",
		"vpcId":"vpc-002",
		"vswId":"vsw-002"
	}`, string(normalized))
}

func TestSystemSettingService_AliyunCloudUpdateRunsConnectivityCheck(t *testing.T) {
	repo := newMockSystemSettingRepo()
	repo.items[model.SystemSettingTypeAliyunCloud] = &model.SystemSetting{
		Type:  model.SystemSettingTypeAliyunCloud,
		Value: buildValidAliyunCloudSettingValue(t),
	}
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()
	cloudProvider := registerAliyunCloudSettingProvider(t, nil)

	updated, err := svc.Update(ctx, model.SystemSettingTypeAliyunCloud, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{
			"access_key_id": " next-ak ",
			"access_key_secret": " next-sk ",
			"endpoint": " nas.cn-shanghai.aliyuncs.com ",
			"region_id": " cn-shanghai ",
			"zone_id": " cn-shanghai-b ",
			"vpc_id": " vpc-002 ",
			"vsw_id": " vsw-002 "
		}`),
	})
	require.NoError(t, err)
	require.Equal(t, 1, cloudProvider.connectivityChecks)
	require.NotContains(t, string(updated.Value), "next-sk")
	require.Contains(t, string(updated.Value), spec.AliyunCloudSecretMaskedValue)

	stored, ok := repo.items[model.SystemSettingTypeAliyunCloud]
	require.True(t, ok)
	require.JSONEq(t, `{
		"accessKeyId":"next-ak",
		"accessKeySecret":"next-sk",
		"endpoint":"nas.cn-shanghai.aliyuncs.com",
		"regionId":"cn-shanghai",
		"zoneId":"cn-shanghai-b",
		"vpcId":"vpc-002",
		"vswId":"vsw-002"
	}`, string(stored.Value))
}

func TestSystemSettingService_AliyunCloudConnectivityCheckFailureOnCreate(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()
	cloudProvider := registerAliyunCloudSettingProvider(t, errors.New("connectivity failed"))

	_, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeAliyunCloud,
		Value: buildValidAliyunCloudSettingValue(t),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingConnectivityCheckFailed)
	require.Equal(t, 1, cloudProvider.connectivityChecks)
	_, ok := repo.items[model.SystemSettingTypeAliyunCloud]
	require.False(t, ok)
}

func TestSystemSettingService_AliyunCloudCreateDuplicateSkipsConnectivityCheck(t *testing.T) {
	repo := newMockSystemSettingRepo()
	oldValue := buildValidAliyunCloudSettingValue(t)
	repo.items[model.SystemSettingTypeAliyunCloud] = &model.SystemSetting{
		Type:  model.SystemSettingTypeAliyunCloud,
		Value: oldValue,
	}
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()
	cloudProvider := registerAliyunCloudSettingProvider(t, errors.New("connectivity failed"))

	_, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeAliyunCloud,
		Value: buildValidAliyunCloudSettingValue(t),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingExists)
	require.Equal(t, 0, cloudProvider.connectivityChecks)

	stored, ok := repo.items[model.SystemSettingTypeAliyunCloud]
	require.True(t, ok)
	require.JSONEq(t, string(oldValue), string(stored.Value))
}

func TestSystemSettingService_AliyunCloudConnectivityCheckFailureOnUpdate(t *testing.T) {
	repo := newMockSystemSettingRepo()
	oldValue := buildValidAliyunCloudSettingValue(t)
	repo.items[model.SystemSettingTypeAliyunCloud] = &model.SystemSetting{
		Type:  model.SystemSettingTypeAliyunCloud,
		Value: oldValue,
	}
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()
	cloudProvider := registerAliyunCloudSettingProvider(t, errors.New("connectivity failed"))

	_, err := svc.Update(ctx, model.SystemSettingTypeAliyunCloud, apisv1.UpdateSystemSettingRequest{
		Value: json.RawMessage(`{
			"accessKeyId":"next-ak",
			"accessKeySecret":"next-sk",
			"endpoint":"nas.cn-shanghai.aliyuncs.com",
			"regionId":"cn-shanghai",
			"zoneId":"cn-shanghai-b",
			"vpcId":"vpc-002",
			"vswId":"vsw-002"
		}`),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingConnectivityCheckFailed)
	require.Equal(t, 1, cloudProvider.connectivityChecks)

	stored, ok := repo.items[model.SystemSettingTypeAliyunCloud]
	require.True(t, ok)
	require.JSONEq(t, string(oldValue), string(stored.Value))
}

func TestSystemSettingService_AliyunCloudUpdateNotFoundSkipsConnectivityCheck(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()
	cloudProvider := registerAliyunCloudSettingProvider(t, errors.New("connectivity failed"))

	_, err := svc.Update(ctx, model.SystemSettingTypeAliyunCloud, apisv1.UpdateSystemSettingRequest{
		Value: buildValidAliyunCloudSettingValue(t),
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingNotFound)
	require.Equal(t, 0, cloudProvider.connectivityChecks)
}

func TestSystemSettingService_APIAuthorizationManagement(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	_, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeAPIAuth,
		Value: buildValidAPIAuthSettingValue(t),
	})
	require.NoError(t, err)

	listed, err := svc.GetAPIAuthorization(ctx)
	require.NoError(t, err)
	require.Equal(t, spec.APIAuthDefaultEffectDeny, listed.DefaultEffect)
	require.Len(t, listed.Routes, 1)

	upserted, err := svc.UpsertAPIAuthorizationRoute(ctx, apisv1.UpsertAPIAuthorizationRouteRequest{
		Method: " post ",
		Path:   " /api/v1/applications ",
		Roles:  []string{"admin", " admin ", "reader"},
	})
	require.NoError(t, err)

	foundPostRoute := false
	for _, route := range upserted.Routes {
		if route.Method == "POST" && route.Path == "/api/v1/applications" {
			foundPostRoute = true
			require.Equal(t, []string{"admin", "reader"}, route.Roles)
			break
		}
	}
	require.True(t, foundPostRoute)

	stored, ok := repo.items[model.SystemSettingTypeAPIAuth]
	require.True(t, ok)
	storedCfg := decodeAPIAuthSettingSpec(t, stored.Value)
	require.Equal(t, "test-secret", storedCfg.JWT.HS256.Secret)

	patched, err := svc.UpdateAPIAuthorizationDefaultEffect(ctx, apisv1.UpdateAPIAuthorizationDefaultEffectRequest{
		DefaultEffect: "ALLOW",
	})
	require.NoError(t, err)
	require.Equal(t, spec.APIAuthDefaultEffectAllow, patched.DefaultEffect)

	deleted, err := svc.DeleteAPIAuthorizationRoute(ctx, "post", " /api/v1/applications ")
	require.NoError(t, err)
	for _, route := range deleted.Routes {
		require.NotEqual(t, "POST", route.Method)
	}
}

func TestSystemSettingService_APIAuthorizationManagementNotFound(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	_, err := svc.GetAPIAuthorization(ctx)
	require.ErrorIs(t, err, bcode.ErrSystemSettingNotFound)

	_, err = svc.UpsertAPIAuthorizationRoute(ctx, apisv1.UpsertAPIAuthorizationRouteRequest{
		Method: "GET",
		Path:   "/api/v1/applications",
		Roles:  []string{"reader"},
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingNotFound)

	_, err = svc.DeleteAPIAuthorizationRoute(ctx, "GET", "/api/v1/applications")
	require.ErrorIs(t, err, bcode.ErrSystemSettingNotFound)

	_, err = svc.UpdateAPIAuthorizationDefaultEffect(ctx, apisv1.UpdateAPIAuthorizationDefaultEffectRequest{
		DefaultEffect: spec.APIAuthDefaultEffectDeny,
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingNotFound)
}

func TestSystemSettingService_APIAuthorizationManagementValidation(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	_, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{
		Type:  model.SystemSettingTypeAPIAuth,
		Value: buildValidAPIAuthSettingValue(t),
	})
	require.NoError(t, err)

	_, err = svc.UpsertAPIAuthorizationRoute(ctx, apisv1.UpsertAPIAuthorizationRouteRequest{
		Method: "TRACE",
		Path:   "/api/v1/applications",
		Roles:  []string{"reader"},
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.UpsertAPIAuthorizationRoute(ctx, apisv1.UpsertAPIAuthorizationRouteRequest{
		Method: "GET",
		Path:   "/api/v1/applications",
		Roles:  []string{},
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.DeleteAPIAuthorizationRoute(ctx, "", "/api/v1/applications")
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.DeleteAPIAuthorizationRoute(ctx, "TRACE", "/api/v1/applications")
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.DeleteAPIAuthorizationRoute(ctx, "GET", "api/v1/applications")
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)

	_, err = svc.UpdateAPIAuthorizationDefaultEffect(ctx, apisv1.UpdateAPIAuthorizationDefaultEffectRequest{
		DefaultEffect: "block",
	})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)
}

func buildValidAPIAuthSettingValue(t *testing.T) json.RawMessage {
	t.Helper()
	keyPEM := mustGenerateRSAPublicKeyPEM(t)
	payload := map[string]interface{}{
		"enabled": true,
		"jwt": map[string]interface{}{
			"algorithms": []string{"HS256", "RS256"},
			"hs256": map[string]interface{}{
				"secret": "test-secret",
			},
			"rs256": map[string]interface{}{
				"publicKeys": []map[string]string{
					{
						"kid": "test-key",
						"pem": keyPEM,
					},
				},
			},
		},
		"authorization": map[string]interface{}{
			"defaultEffect": "deny",
			"routes": []map[string]interface{}{
				{
					"method": "GET",
					"path":   "/api/v1/applications",
					"roles":  []string{"admin", "reader"},
				},
			},
		},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return json.RawMessage(data)
}

func buildValidOAuthAuthSettingValue(t *testing.T) json.RawMessage {
	t.Helper()
	payload := map[string]interface{}{
		"enabled": true,
		"providers": map[string]interface{}{
			"google": map[string]interface{}{
				"clientId":     "google-client-id",
				"clientSecret": "google-secret",
				"redirectURI":  "https://eruun.example.com/api/v1/auth/oauth2/google/callback",
				"scopes":       []string{"openid", "email", "profile"},
			},
		},
		"jwtIssue": map[string]interface{}{
			"issuer":     "eruun",
			"audience":   "eruun-api",
			"ttlSeconds": 1800,
		},
		"roleMapping": map[string]interface{}{
			"defaultRoles": []string{"reader"},
			"googleHostedDomainToRoles": map[string]interface{}{
				"example.com": []string{"admin"},
			},
			"googleEmailToRoles": map[string]interface{}{
				"owner@example.com": []string{"admin"},
			},
		},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return json.RawMessage(data)
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

func buildValidAliyunCloudSettingValue(t *testing.T) json.RawMessage {
	t.Helper()
	payload := map[string]interface{}{
		"accessKeyId":     "test-ak",
		"accessKeySecret": "test-sk",
		"endpoint":        "nas.cn-hangzhou.aliyuncs.com",
		"regionId":        "cn-hangzhou",
		"zoneId":          "cn-hangzhou-i",
		"vpcId":           "vpc-001",
		"vswId":           "vsw-001",
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return json.RawMessage(data)
}

func decodeAPIAuthSettingSpec(t *testing.T, raw json.RawMessage) spec.APIAuthSettingSpec {
	t.Helper()
	var cfg spec.APIAuthSettingSpec
	require.NoError(t, json.Unmarshal(raw, &cfg))
	return spec.NormalizeAPIAuthSetting(cfg)
}

func mustGenerateRSAPublicKeyPEM(t *testing.T) string {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	}))
}

func TestSystemSettingService_UpdateNotFound(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()

	_, err := svc.Update(ctx, model.SystemSettingTypeNodeSelector, apisv1.UpdateSystemSettingRequest{Value: json.RawMessage(`{}`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingNotFound)
}

package conversion

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	validationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/validation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/urlpolicy"
)

func NewValidationService() ValidationService {
	return validationservice.NewValidationService()
}

func newTestURLSecurityPolicyProvider(t testing.TB, policy spec.URLSecurityPolicySpec) *urlpolicy.Provider {
	t.Helper()
	value, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("marshal url security policy: %v", err)
	}
	return urlpolicy.NewProvider(testURLPolicyStore{value: value}, 0)
}

type testURLPolicyStore struct {
	datastore.DataStore
	value json.RawMessage
}

func (s testURLPolicyStore) Get(_ context.Context, entity datastore.Entity) error {
	setting, ok := entity.(*model.SystemSetting)
	if !ok || setting.Type != model.SystemSettingTypeURLSecurityPolicy {
		return datastore.ErrRecordNotExist
	}
	setting.Value = append([]byte(nil), s.value...)
	return nil
}
